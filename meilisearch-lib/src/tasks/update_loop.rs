use std::future::ready;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use chrono::Utc;
use futures::Future;
use tokio::sync::{watch, RwLock};
use tokio::time::{interval, interval_at, Interval};

use super::batch::Batch;
use super::error::Result;
use super::scheduler::Pending;
use super::{Scheduler, TaskPerformer};
use crate::tasks::task::TaskEvent;

/// The scheduler roles is to perform batches of tasks one at a time. It will monitor the TaskStore
/// for new tasks, put them in a batch, and process the batch as soon as possible.
///
/// When a batch is currently processing, the scheduler is just waiting.
pub struct UpdateLoop<P: TaskPerformer> {
    scheduler: Arc<RwLock<Scheduler>>,
    performer: Arc<P>,

    notifier: Option<watch::Receiver<()>>,
    debounce_duration: Option<Duration>,
}

impl<P> UpdateLoop<P>
where
    P: TaskPerformer + Send + Sync + 'static,
{
    pub fn new(
        scheduler: Arc<RwLock<Scheduler>>,
        performer: Arc<P>,
        debuf_duration: Option<Duration>,
        notifier: watch::Receiver<()>,
    ) -> Self {
        Self {
            scheduler,
            performer,
            debounce_duration: debuf_duration,
            notifier: Some(notifier),
        }
    }

    pub async fn run(mut self) {
        let mut timer = WaitOrNever::never();
        let mut notifier = self.notifier.take().unwrap();

        loop {
            if timer.is_never() {
                if notifier.changed().await.is_err() {
                    break;
                }

                timer = match self.debounce_duration {
                    Some(t) => WaitOrNever::wait(dbg!(t)),
                    None => WaitOrNever::now(),
                };
            }

            timer.await;

            if let Err(e) = self.process_next_batch().await {
                log::error!("an error occured while processing an update batch: {}", e);
            }

            // debounce time is elapsed, we can process the batch now
            timer = WaitOrNever::never();
        }
    }

    async fn process_next_batch(&self) -> Result<()> {
        let pending = { self.scheduler.write().await.prepare().await? };
        dbg!(&pending);
        match pending {
            Pending::Batch(mut batch) => {
                for task in &mut batch.tasks {
                    task.events.push(TaskEvent::Processing(Utc::now()));
                }

                dbg!();
                // the jobs are ignored
                batch.tasks = {
                    self.scheduler
                        .read()
                        .await
                        .update_tasks(batch.tasks)
                        .await?
                };

                dbg!();
                let performer = self.performer.clone();

                let batch = performer.process_batch(batch).await;

                self.handle_batch_result(batch).await?;
            }
            Pending::Job(job) => {
                let performer = self.performer.clone();
                dbg!();
                performer.process_job(job).await;
            }
            Pending::Nothing => (),
        }
        dbg!();

        Ok(())
    }

    /// Handles the result from a batch processing.
    ///
    /// When a task is processed, the result of the processing is pushed to its event list. The
    /// handle batch result make sure that the new state is save into its store.
    /// The tasks are then removed from the processing queue.
    async fn handle_batch_result(&self, mut batch: Batch) -> Result<()> {
        let mut scheduler = self.scheduler.write().await;
        let tasks = scheduler.update_tasks(batch.tasks).await?;
        scheduler.finish();
        drop(scheduler);
        batch.tasks = tasks;
        self.performer.finish(&batch).await;
        Ok(())
    }
}

enum WaitOrNever {
    Wait(Pin<Box<dyn Future<Output = ()>>>),
    Never(Interval),
}

impl WaitOrNever {
    // Never is not actually never to prevent the loop from completely blocking and making any
    // further progress.
    fn never() -> Self {
        Self::Never(interval(Duration::from_secs(5)))
    }

    fn is_never(&self) -> bool {
        match self {
            Self::Never(_) => true,
            _ => false,
        }
    }

    fn wait(t: Duration) -> Self {
        let interval = async move {
            let mut interval = interval_at(tokio::time::Instant::now() + t, t);
            interval.tick().await;
        };

        Self::Wait(Box::pin(interval))
    }

    fn now() -> Self {
        Self::Wait(Box::pin(ready(())))
    }
}

impl Future for WaitOrNever {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match *self {
            WaitOrNever::Wait(ref mut fut) => fut.as_mut().poll(cx),
            WaitOrNever::Never(_) => Poll::Pending,
        }
    }
}
