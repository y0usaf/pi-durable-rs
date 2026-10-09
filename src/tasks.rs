use std::collections::BTreeMap;
use std::ops::Deref;

use serde::{Deserialize, Serialize};

use std::sync::Arc;

use crate::ai::models::Models;
use crate::cancel::CancelToken;
use crate::env::ExecutionEnv;
use crate::errors::{Error, Result};
use crate::harness::agent::Agent;
use crate::harness::registry::RegistrySnapshot;
use crate::harness::types::Settings;
use crate::ids::{ConversationId, EntryId, TaskId};
use crate::session::types::{DocumentObserver, DocumentReader, Tx};
use crate::types::{AnyTaskRecord, EntryRecord, JsonObject, JsonValue, present};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOwnership {
    Conversation,
    Task { task_id: TaskId },
}

pub trait Phase {
    fn phase(&self) -> &str;
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunningTask<I, S, R> {
    record: TaskRecord<I, S, R>,
}

impl<I, S, R> RunningTask<I, S, R> {
    pub fn new(record: TaskRecord<I, S, R>) -> Result<Self> {
        match record.state {
            TaskState::Running { .. } => Ok(Self { record }),
            _ => Err(Error::Invalid(format!("Task {} is not running", record.id))),
        }
    }

    pub fn checkpoint(&self) -> &S {
        match &self.record.state {
            TaskState::Running { checkpoint } => checkpoint,
            _ => unreachable!("RunningTask is constructed only in the running state"),
        }
    }

    pub fn into_record(self) -> TaskRecord<I, S, R> {
        self.record
    }
}

impl<I, S, R> Deref for RunningTask<I, S, R> {
    type Target = TaskRecord<I, S, R>;

    fn deref(&self) -> &Self::Target {
        &self.record
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum NextTaskState<S, R> {
    Running {
        checkpoint: S,
    },
    Waiting {
        checkpoint: S,
        on: Vec<TaskId>,
        policy: JoinPolicy,
    },
    Terminal {
        outcome: TaskOutcome<R>,
    },
}

impl<S, R> NextTaskState<S, R> {
    pub fn into_state(self) -> TaskState<S, R> {
        match self {
            Self::Running { checkpoint } => TaskState::Running { checkpoint },
            Self::Waiting {
                checkpoint,
                on,
                policy,
            } => TaskState::Waiting {
                checkpoint,
                on,
                policy,
            },
            Self::Terminal { outcome } => TaskState::Terminal { outcome },
        }
    }
}

pub type PhaseHandler<I, S, R, H> = Box<
    dyn Fn(RunningTask<I, S, R>, &(dyn TaskRuntime<I, S, R, H> + Sync), &CancelToken) -> Result<()>
        + Send
        + Sync,
>;

pub trait HookRunner<H> {
    fn each(&self, name: &str, invoke: &mut dyn FnMut(&H) -> Result<()>) -> Result<()>;
}

pub type RuntimeChange<'a, I, S, R> =
    Box<dyn FnOnce(&mut dyn Tx, RunningTask<I, S, R>) -> Result<Option<NextTaskState<S, R>>> + 'a>;

pub trait TaskRuntime<I, S, R, H>: DocumentObserver + DocumentReader {
    fn task_id(&self) -> TaskId<R>;
    fn conversation_id(&self) -> ConversationId;
    fn agent(&self, cancel: &CancelToken) -> Result<Agent>;
    fn models(&self) -> &dyn Models;
    fn env(&self, cancel: &CancelToken) -> Result<Option<Arc<dyn ExecutionEnv>>>;
    fn registry(&self) -> Arc<RegistrySnapshot>;
    fn settings(&self) -> &Settings;
    fn signal(&self) -> &CancelToken;
    fn hooks(&self) -> &dyn HookRunner<H>;
    fn commit(&self, change: RuntimeChange<'_, I, S, R>, cancel: &CancelToken) -> Result<()>;
    fn memo(&self, name: &str, cancel: &CancelToken) -> Result<Option<JsonValue>>;
    fn memo_or_set(
        &self,
        name: &str,
        candidate: JsonValue,
        cancel: &CancelToken,
    ) -> Result<JsonValue>;
    fn get_task(&self, id: TaskId, cancel: &CancelToken) -> Result<Option<AnyTaskRecord>>;
    fn outcomes(&self, ids: &[TaskId], cancel: &CancelToken)
    -> Result<Vec<TaskOutcome<JsonValue>>>;
    fn entry(&self, id: EntryId, cancel: &CancelToken) -> Result<Option<EntryRecord>>;
    fn now(&self) -> u64;
    fn report(&self, error: Error);
    fn sleep(&self, until: u64, cancel: &CancelToken) -> Result<()>;
}

pub type PhaseMap<I, S, R, H> = BTreeMap<String, PhaseHandler<I, S, R, H>>;

pub type Initial<I, S> = Box<dyn Fn(&I) -> S + Send + Sync>;

pub type Migrate<I, S> = Box<dyn Fn(&JsonValue, &JsonValue, u64) -> Result<(I, S)> + Send + Sync>;

pub struct TaskDefinition<I, S, R, H> {
    pub name: String,
    pub version: u64,
    pub initial: Initial<I, S>,
    pub phases: PhaseMap<I, S, R, H>,
    pub abort: PhaseHandler<I, S, R, H>,
    pub migrate: Option<Migrate<I, S>>,
    pub hooks: Option<H>,
}

pub struct Task<I, S, R, H> {
    pub definition: TaskDefinition<I, S, R, H>,
}

pub fn define_task<I, S, R, H>(definition: TaskDefinition<I, S, R, H>) -> Task<I, S, R, H> {
    Task { definition }
}

type Defines<H> = Box<dyn Fn(&H, &str) -> bool + Send + Sync>;

pub struct PhaseHooks<H> {
    handlers: Vec<H>,
    defines: Defines<H>,
    signal: CancelToken,
    report: Box<dyn Fn(Error) + Send + Sync>,
}

impl<H> PhaseHooks<H> {
    pub fn new(
        handlers: Vec<H>,
        defines: impl Fn(&H, &str) -> bool + Send + Sync + 'static,
        signal: CancelToken,
        report: impl Fn(Error) + Send + Sync + 'static,
    ) -> Self {
        Self {
            handlers,
            defines: Box::new(defines),
            signal,
            report: Box::new(report),
        }
    }
}

impl<H> HookRunner<H> for PhaseHooks<H> {
    fn each(&self, name: &str, invoke: &mut dyn FnMut(&H) -> Result<()>) -> Result<()> {
        for handler in &self.handlers {
            if !(self.defines)(handler, name) {
                continue;
            }
            if let Err(error) = invoke(handler) {
                if self.signal.is_cancelled() {
                    return Err(error);
                }
                (self.report)(error);
            }
        }
        Ok(())
    }
}
