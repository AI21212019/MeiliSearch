use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use scheduler::Scheduler;
pub use task_store::{Pending, TaskFilter};

#[cfg(test)]
pub use task_store::test::MockTaskStore as TaskStore;
#[cfg(not(test))]
pub use task_store::TaskStore;

use batch::Batch;
use error::Result;

pub mod batch;
pub mod error;
mod scheduler;
pub mod task;
mod task_store;
pub mod update_loop;

#[cfg_attr(test, mockall::automock(type Error=test::DebugError;))]
#[async_trait]
pub trait TaskPerformer: Sync + Send + 'static {
    type Error: Serialize + for<'de> Deserialize<'de> + std::error::Error + Sync + Send + 'static;
    /// Processes the `Task` batch returning the batch with the `Task` updated.
    async fn process(&self, batch: Batch) -> Batch;
    /// `finish` is called when the result of `process` has been commited to the task store. This
    /// method can be used to perform cleanup after the update has been completed for example.
    async fn finish(&self, batch: &Batch);
}

#[cfg(test)]
mod test {
    use serde::{Deserialize, Serialize};
    use std::fmt::Display;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct DebugError;

    impl Display for DebugError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an error")
        }
    }

    impl std::error::Error for DebugError {}
}
