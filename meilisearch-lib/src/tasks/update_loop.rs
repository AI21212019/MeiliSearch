use std::future::ready;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use chrono::Utc;
use futures::{pin_mut, Future};
use tokio::sync::{watch, RwLock};
use tokio::time::interval_at;

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
            let notified = notifier.changed();
            pin_mut!(notified);

            tokio::select! {
                _ = notified => {
                    // we have been notified that a new update had been, we can start to debuf, and
                    // see if we receive any more.

                    if let WaitOrNever::Never = timer {
                        // we are not currently debuffing, we can add a timer
                        timer = match self.debounce_duration {
                            Some(t) => WaitOrNever::wait(t),
                            None => WaitOrNever::now(),
                        };
                    }
                }
                 _ = &mut timer => {
                     // debounce time is elapsed, we can process the batch now
                    timer = WaitOrNever::never();

                    if let Err(e) = self.process_next_batch().await {
                        log::error!("an error occured while processing an update batch: {}", e);
                    }
                }
            }
        }
    }

    async fn process_next_batch(&self) -> Result<()> {
        let work = { self.scheduler.write().await.prepare().await? };
        match work {
            Pending::Batch(mut batch) => {
                for task in &mut batch.tasks {
                    task.events.push(TaskEvent::Processing(Utc::now()));
                }

                // the jobs are ignored
                batch.tasks = {
                    self.scheduler
                        .read()
                        .await
                        .update_tasks(batch.tasks)
                        .await?
                };

                let performer = self.performer.clone();

                tokio::time::sleep(Duration::from_secs(15)).await;
                let batch = performer.process_batch(batch).await;

                self.handle_batch_result(batch).await?;
            }
            Pending::Job(job) => {
                let performer = self.performer.clone();
                performer.process_job(job).await;
            }
            Pending::Nothing => (),
        }

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
    Never,
}

impl WaitOrNever {
    fn never() -> Self {
        Self::Never
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
            WaitOrNever::Never => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod test {
    use nelson::Mocker;

    use crate::index_resolver::IndexUid;
    use crate::tasks::task::Task;
    use crate::tasks::task_store::TaskFilter;

    use super::super::task::{TaskContent, TaskEvent, TaskId, TaskResult};
    use super::super::MockTaskPerformer;
    use super::*;

    #[tokio::test]
    async fn test_prepare_batch_full() {
        let mocker = Mocker::default();

        mocker
            .when::<(TaskId, Option<TaskFilter>), Result<Option<Task>>>("get_task")
            .once()
            .then(|(id, _filter)| {
                let task = Task {
                    id,
                    index_uid: IndexUid::new("Test".to_string()).unwrap(),
                    content: TaskContent::IndexDeletion,
                    events: vec![TaskEvent::Created(Utc::now())],
                };
                Ok(Some(task))
            });

        mocker
            .when::<(), Option<Pending<TaskId>>>("peek_pending_task")
            .then(|()| Some(Pending::Task(1)));

        let store = TaskStore::mock(mocker);
        let performer = Arc::new(MockTaskPerformer::new());

        let scheduler = UpdateLoop {
            store,
            performer,
            task_store_check_interval: Duration::from_millis(1),
        };

        let batch = scheduler.prepare_batch().await.unwrap().unwrap();

        assert_eq!(batch.tasks.len(), 1);
        assert!(
            matches!(batch.tasks[0], Pending::Task(Task { id: 1, .. })),
            "{:?}",
            batch.tasks[0]
        );
    }

    #[tokio::test]
    async fn test_prepare_batch_empty() {
        let mocker = Mocker::default();
        mocker
            .when::<(), Option<Pending<TaskId>>>("peek_pending_task")
            .then(|()| None);

        let store = TaskStore::mock(mocker);
        let performer = Arc::new(MockTaskPerformer::new());

        let scheduler = UpdateLoop {
            store,
            performer,
            task_store_check_interval: Duration::from_millis(1),
        };

        assert!(scheduler.prepare_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_loop_run_normal() {
        let mocker = Mocker::default();
        let mut id = Some(1);
        mocker
            .when::<(), Option<Pending<TaskId>>>("peek_pending_task")
            .then(move |()| id.take().map(Pending::Task));
        mocker
            .when::<(TaskId, Option<TaskFilter>), Result<Task>>("get_task")
            .once()
            .then(|(id, _)| {
                let task = Task {
                    id,
                    index_uid: IndexUid::new("Test".to_string()).unwrap(),
                    content: TaskContent::IndexDeletion,
                    events: vec![TaskEvent::Created(Utc::now())],
                };
                Ok(task)
            });

        mocker
            .when::<Vec<Pending<Task>>, Result<Vec<Pending<Task>>>>("update_tasks")
            .times(2)
            .then(|tasks| {
                assert_eq!(tasks.len(), 1);
                Ok(tasks)
            });

        mocker.when::<(), ()>("delete_pending").once().then(|_| ());

        let store = TaskStore::mock(mocker);

        let mut performer = MockTaskPerformer::new();
        performer.expect_process().once().returning(|mut batch| {
            batch.tasks.iter_mut().for_each(|t| match t {
                Pending::Task(Task { ref mut events, .. }) => events.push(TaskEvent::Succeded {
                    result: TaskResult::Other,
                    timestamp: Utc::now(),
                }),
                _ => panic!("expected a task, found a job"),
            });

            batch
        });

        performer.expect_finish().once().returning(|_| ());

        let performer = Arc::new(performer);

        let scheduler = UpdateLoop {
            store,
            performer,
            task_store_check_interval: Duration::from_millis(1),
        };

        let handle = tokio::spawn(scheduler.run());

        if let Ok(r) = tokio::time::timeout(Duration::from_millis(100), handle).await {
            r.unwrap();
        }
    }
}
