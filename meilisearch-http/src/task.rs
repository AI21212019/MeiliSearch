use chrono::{Duration, DateTime, Utc};
use meilisearch_tasks::task::{DocumentAdditionMergeStrategy, DocumentDeletion, Task, TaskContent, TaskEvent, TaskId};
use serde::{Serialize, Serializer};

use crate::error::ResponseError;

// TODO: Remove
#[allow(dead_code)]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
enum TaskType {
    IndexCreation,
    IndexUpdate,
    IndexDeletion,
    DocumentsAddition,
    DocumentsPartial,
    DocumentsDeletion,
    SettingsUpdate,
    ClearAll,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
enum TaskStatus {
    Enqueued,
    Processing,
    Succeeded,
    Failed,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum TaskDetails {
    #[serde(rename_all = "camelCase")]
    DocumentsUpdate { number_of_documents: usize },
}

fn serialize_duration<S: Serializer>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error> {
    match duration {
        Some(duration) => {
            let duration_str = duration.to_string();
            serializer.serialize_str(&duration_str)
        },
        None => serializer.serialize_none(),
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskResponse {
    uid: TaskId,
    index_uid: String,
    status: TaskStatus,
    #[serde(rename = "type")]
    task_type: TaskType,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<TaskDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ResponseError>,
    #[serde(serialize_with = "serialize_duration")]
    duration: Option<Duration>,
    enqueued_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
}

impl From<Task> for TaskResponse {
    fn from(task: Task) -> Self {
        let Task {
            id,
            index_uid,
            content,
            events,
        } = task;

        // An event always has at least one event: "Created"
        let (status, error, finished_at) = match events.last().unwrap() {
            TaskEvent::Created(_) => (TaskStatus::Enqueued, None, None),
            TaskEvent::Batched { .. } => (TaskStatus::Enqueued, None, None),
            TaskEvent::Processing(_) => (TaskStatus::Processing, None, None),
            TaskEvent::Succeded { timestamp, .. } => {
                (TaskStatus::Succeeded, None, Some(*timestamp))
            }
            TaskEvent::Failed { timestamp, .. } => (TaskStatus::Failed, None, Some(*timestamp)),
        };

        let (task_type, details) = match content {
            TaskContent::DocumentAddition {
                merge_strategy,
                documents_count,
                ..
            } => {
                let details = TaskDetails::DocumentsUpdate {
                    number_of_documents: documents_count,
                };

                let task_type = match merge_strategy {
                    DocumentAdditionMergeStrategy::UpdateDocument => TaskType::DocumentsPartial,
                    DocumentAdditionMergeStrategy::ReplaceDocument => TaskType::DocumentsAddition,
                };

                (task_type, Some(details))
            }
            TaskContent::DocumentDeletion(DocumentDeletion::Ids(ids)) => (
                TaskType::DocumentsDeletion,
                Some(TaskDetails::DocumentsUpdate {
                    number_of_documents: ids.len(),
                }),
            ),
            TaskContent::DocumentDeletion(DocumentDeletion::Clear) => (TaskType::ClearAll, None),
            TaskContent::IndexDeletion => (TaskType::IndexDeletion, None),
            TaskContent::SettingsUpdate => (TaskType::SettingsUpdate, None),
            TaskContent::CreateIndex { .. } => (TaskType::IndexCreation, None),
        };

        let enqueued_at = match events.first().unwrap() {
            TaskEvent::Created(ts) => *ts,
            _ => unreachable!("A task first element should always be a cretion event."),
        };

        let duration = finished_at.map(|ts| (ts - enqueued_at));

        let started_at = events.iter().find_map(|e| match e {
            TaskEvent::Processing(ts) => Some(*ts),
            _ => None,
        });

        Self {
            uid: id,
            index_uid,
            status,
            task_type,
            details,
            error,
            duration,
            enqueued_at,
            started_at,
            finished_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TaskListResponse {
    results: Vec<TaskResponse>,
}

impl From<Vec<TaskResponse>> for TaskListResponse {
    fn from(results: Vec<TaskResponse>) -> Self {
        Self { results }
    }
}
