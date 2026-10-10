use std::collections::BTreeMap;

use crate::cancel::CancelToken;
use crate::errors::Result;
use crate::harness::context::ContextView;
use crate::harness::types::ConversationHandle;
use crate::ids::{ConversationId, EntryId, TaskId};
use crate::tasks::{RunningTask, TaskOutcome, TaskRuntime};
use crate::types::JsonValue;

pub trait HarnessTaskRuntime<I, S, R, H>: TaskRuntime<I, S, R, H> {
    fn context(&self, at: Option<EntryId>, cancel: &CancelToken) -> Result<ContextView>;
    fn wait_for_task(&self, id: TaskId, cancel: &CancelToken) -> Result<TaskOutcome<JsonValue>>;
    fn conversation(
        &self,
        id: ConversationId,
        cancel: &CancelToken,
    ) -> Result<Option<Box<dyn ConversationHandle>>>;
}

pub type HarnessPhaseHandler<I, S, R, H> = Box<
    dyn Fn(
            RunningTask<I, S, R>,
            &(dyn HarnessTaskRuntime<I, S, R, H> + Sync),
            &CancelToken,
        ) -> Result<()>
        + Send
        + Sync,
>;

pub struct HarnessTaskDefinition<I, S, R, H> {
    pub name: String,
    pub version: u64,
    pub initial: Box<dyn Fn(&I) -> S + Send + Sync>,
    pub phases: BTreeMap<String, HarnessPhaseHandler<I, S, R, H>>,
    pub abort: HarnessPhaseHandler<I, S, R, H>,
}
