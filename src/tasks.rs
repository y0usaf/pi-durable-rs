use serde::{Deserialize, Serialize};

use crate::ids::{ConversationId, TaskId};
use crate::types::{JsonObject, JsonValue, present};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum JoinPolicy {
    FailFast,
    AllSettled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Pending,
    Running,
    Waiting,
    Completing,
    Terminal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskOutcomeError {
    pub message: String,
    #[serde(
        default = "crate::types::absent_default",
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub detail: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum TaskOutcome<R> {
    Completed {
        result: R,
    },
    Failed {
        error: TaskOutcomeError,
        #[serde(
            default = "crate::types::absent_default",
            deserialize_with = "present",
            skip_serializing_if = "Option::is_none"
        )]
        result: Option<R>,
    },
    Aborted {
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        #[serde(
            default = "crate::types::absent_default",
            deserialize_with = "present",
            skip_serializing_if = "Option::is_none"
        )]
        result: Option<R>,
    },
    Orphaned {
        reason: String,
    },
    Faulted {
        error: TaskOutcomeError,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum TaskState<S, R> {
    Pending {
        checkpoint: S,
    },
    Running {
        checkpoint: S,
    },
    Waiting {
        checkpoint: S,
        on: Vec<TaskId>,
        policy: JoinPolicy,
    },
    Completing {
        outcome: TaskOutcome<R>,
    },
    Terminal {
        outcome: TaskOutcome<R>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRecord<I, S, R> {
    pub id: TaskId<R>,
    pub conversation_id: ConversationId,
    pub kind: String,
    pub version: u64,
    pub input: I,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<TaskId>,
    pub background: bool,
    pub abort_requested: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<u64>,
    #[serde(flatten)]
    pub state: TaskState<S, R>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memos: Option<JsonObject>,
}

impl<I, S, R> TaskRecord<I, S, R> {
    pub fn status(&self) -> TaskStatus {
        match self.state {
            TaskState::Pending { .. } => TaskStatus::Pending,
            TaskState::Running { .. } => TaskStatus::Running,
            TaskState::Waiting { .. } => TaskStatus::Waiting,
            TaskState::Completing { .. } => TaskStatus::Completing,
            TaskState::Terminal { .. } => TaskStatus::Terminal,
        }
    }
}
