use serde::{Deserialize, Serialize};

use crate::ai::types::AssistantMessage;
use crate::documents::{DocSemantics, DocToken};
use crate::errors::{Error, Result};
use crate::harness::compaction::COMPACTION_KIND;
use crate::harness::generation::{
    GENERATION_KIND, convert_partial, end_run, read_live, write_live,
};
use crate::harness::tool::TOOL_KIND;
use crate::harness::util::doc_definition;
use crate::ids::{EntryId, SubmissionId, TaskId};
use crate::session::types::Tx;
use crate::tasks::TaskOutcome;
use crate::types::{AnyTaskRecord, CompactionReason, SubmissionSettlement, ToolDiagnostic};
use crate::types::{ConversationSemantics, JsonValue, LatestFork};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SlotStatus {
    Pending,
    Running,
    Done,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolSlot {
    pub call_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<TaskId>,
    pub status: SlotStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dropped_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dropped_lines: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<Vec<ToolDiagnostic>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry: Option<EntryId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionStatus {
    pub task_id: TaskId,
    pub reason: CompactionReason,
    pub blocking: bool,
    pub attempt: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryStatus {
    pub at: u64,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunState {
    pub task_id: TaskId,
    pub inputs: Vec<SubmissionId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerationStatus {
    pub attempt: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<AssistantMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deferred: Option<DeferredStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeferredStatus {
    pub poll_at: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<RunState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<GenerationStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ToolSlot>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compactions: Option<Vec<CompactionStatus>>,
}

pub fn live_doc() -> Result<DocToken<LiveState>> {
    doc_definition(
        "pi.live",
        1,
        DocSemantics::Conversation(ConversationSemantics::Latest {
            fork: LatestFork::Initial,
        }),
        LiveState::default,
        Some(|state: &LiveState| {
            state.generation.is_none()
                && !state
                    .tools
                    .iter()
                    .flatten()
                    .any(|slot| slot.status == SlotStatus::Running)
        }),
    )
}

impl LiveState {
    pub fn tool_slot(&mut self, task_id: TaskId) -> Option<&mut ToolSlot> {
        self.tools
            .as_mut()?
            .iter_mut()
            .find(|slot| slot.task_id == Some(task_id))
    }

    pub fn add_compaction_status(&mut self, status: CompactionStatus) {
        self.compactions.get_or_insert_with(Vec::new).push(status);
    }

    pub fn compaction_status(&mut self, task_id: TaskId) -> Option<&mut CompactionStatus> {
        self.compactions
            .as_mut()?
            .iter_mut()
            .find(|status| status.task_id == task_id)
    }

    pub fn remove_compaction_status(&mut self, task_id: TaskId) {
        let Some(statuses) = self.compactions.as_mut() else {
            return;
        };
        if let Some(index) = statuses.iter().position(|status| status.task_id == task_id) {
            statuses.remove(index);
        }
        if statuses.is_empty() {
            self.compactions = None;
        }
    }

    pub fn end_run(&mut self, task_id: TaskId) -> Option<Vec<SubmissionId>> {
        let inputs = match &self.run {
            Some(run) if run.task_id == task_id => {
                let inputs = run.inputs.clone();
                self.run = None;
                Some(inputs)
            }
            _ => None,
        };
        self.generation = None;
        self.tools = None;
        inputs
    }
}

impl ToolSlot {
    pub fn finish(&mut self, entry: Option<EntryId>) {
        self.status = SlotStatus::Done;
        if let Some(entry) = entry {
            self.entry = Some(entry);
        }
        self.clear_progress();
    }

    pub fn clear_progress(&mut self) {
        self.output = None;
        self.dropped_bytes = None;
        self.dropped_lines = None;
        self.details = None;
        self.diagnostics = None;
    }
}

pub fn settle_scheduler_outcome(
    tx: &mut dyn Tx,
    record: &AnyTaskRecord,
    outcome: &TaskOutcome<JsonValue>,
) -> Result<()> {
    let conversation_id = record.conversation_id;
    match record.kind.as_str() {
        TOOL_KIND => {
            let mut live = read_live(tx, conversation_id)?;
            if let Some(slot) = live.tool_slot(record.id) {
                slot.finish(None);
            }
            write_live(tx, conversation_id, &live)
        }
        COMPACTION_KIND => {
            let mut live = read_live(tx, conversation_id)?;
            live.remove_compaction_status(record.id);
            write_live(tx, conversation_id, &live)
        }
        GENERATION_KIND => {
            let mut live = read_live(tx, conversation_id)?;
            if live.run.as_ref().map(|run| run.task_id) != Some(record.id) {
                return Ok(());
            }
            convert_partial(tx, &live, conversation_id)?;
            let settlement = match outcome {
                TaskOutcome::Faulted { error } => SubmissionSettlement::Unanswered {
                    reason: "faulted".to_owned(),
                    detail: Some(JsonValue::String(error.message.clone())),
                },
                TaskOutcome::Orphaned { reason } => SubmissionSettlement::Unanswered {
                    reason: reason.clone(),
                    detail: None,
                },
                _ => {
                    return Err(Error::Invalid(format!(
                        "Task {} settled without a scheduler outcome",
                        record.id
                    )));
                }
            };
            end_run(tx, &mut live, record.id, settlement)?;
            write_live(tx, conversation_id, &live)
        }
        _ => Ok(()),
    }
}
