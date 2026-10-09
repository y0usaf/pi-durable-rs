use serde::{Deserialize, Serialize};

use crate::ai::types::{AssistantMessage, Message, StopReason};
use crate::entries::assistant_entry;
use crate::errors::Result;
use crate::harness::live::{LiveState, RunState};
use crate::harness::usage::{UsageBucket, record_usage};
use crate::ids::{ConversationId, SubmissionId, TaskId};
use crate::session::types::Tx;
use crate::types::{EntryDraft, EntryRecord};

pub const GENERATION_KIND: &str = "pi.generation";
pub const GENERATION_VERSION: u64 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "lowercase")]
pub enum GenerationCheckpoint {
    Prepare { attempt: u64 },
}

pub fn create_generation(tx: &mut dyn Tx, conversation_id: ConversationId) -> Result<TaskId> {
    let checkpoint = GenerationCheckpoint::Prepare { attempt: 1 };
    tx.create_task(
        crate::session::types::TaskCreate {
            kind: GENERATION_KIND.to_owned(),
            version: GENERATION_VERSION,
            input: serde_json::json!({}),
            checkpoint: serde_json::to_value(checkpoint)?,
        },
        crate::session::types::TaskOptions {
            ownership: crate::tasks::TaskOwnership::Conversation,
            conversation_id: Some(conversation_id),
            background: None,
        },
    )
}

pub fn start_run(
    tx: &mut dyn Tx,
    conversation_id: ConversationId,
    live: &mut LiveState,
    inputs: Vec<SubmissionId>,
) -> Result<()> {
    let task_id = create_generation(tx, conversation_id)?;
    live.run = Some(RunState { task_id, inputs });
    Ok(())
}

pub fn hand_over(live: &mut LiveState, from: TaskId, to: TaskId) {
    if let Some(run) = live.run.as_mut().filter(|run| run.task_id == from) {
        run.task_id = to;
    }
}

pub fn convert_partial(
    tx: &mut dyn Tx,
    live: &LiveState,
    conversation_id: ConversationId,
) -> Result<()> {
    let Some(partial) = live
        .generation
        .as_ref()
        .and_then(|status| status.message.as_ref())
    else {
        return Ok(());
    };
    let aborted = AssistantMessage {
        stop_reason: StopReason::Aborted,
        ..partial.clone()
    };
    append_assistant(tx, conversation_id, &aborted)?;
    Ok(())
}

pub fn append_assistant(
    tx: &mut dyn Tx,
    conversation_id: ConversationId,
    message: &AssistantMessage,
) -> Result<EntryRecord> {
    let key = format!("{}/{}", message.provider, message.model);
    record_usage(
        tx,
        conversation_id,
        UsageBucket::Models,
        &key,
        &message.usage,
    )?;
    tx.append_entry(
        conversation_id,
        EntryDraft {
            kind: assistant_entry().kind().to_owned(),
            model: Some(vec![Message::Assistant(Box::new(message.clone()))]),
            data: None,
            head: None,
            edits: None,
        },
    )
}
