use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::ai::types::{AssistantBlock, AssistantMessage, Message, Usage};
use crate::cancel::CancelToken;
use crate::chord::delta::{Op, Seg};
use crate::documents::DocDefinition;
use crate::errors::{Error, Result};
use crate::harness::agent::{AgentState, agent_doc};
use crate::harness::inbox::{InboxItem, InboxState};
use crate::harness::live::{
    CompactionStatus, DeferredStatus, GenerationStatus, LiveState, SlotStatus, ToolSlot,
};
use crate::harness::usage::{UsageState, usage_doc};
use crate::harness::util::scan_all;
use crate::harness::view::{Change, ConversationViews, Observer};
use crate::ids::{ConversationId, EntryId, SubmissionId, TaskId};
use crate::session::kernel::Session;
use crate::session::observation::WatchShared;
use crate::session::types::WatchEnd;
use crate::tasks::{TaskOutcome, TaskState, TaskStatus};
use crate::types::{
    AnyTaskRecord, CommitChange, CompactionReason, EntryRecord, JsonObject, JsonValue, Storage,
    SubmissionRecord, TableChange, TaskQuery, ToolDiagnostic,
};

const GENERATION: &str = "pi.generation";
const SCAN_PAGE_SIZE: usize = 100;
const LOCK: &str = "agent event lock poisoned";

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum MessageChange {
    TextStart {
        content_index: usize,
        block: Box<AssistantBlock>,
    },
    ThinkingStart {
        content_index: usize,
        block: Box<AssistantBlock>,
    },
    ToolcallStart {
        content_index: usize,
        block: Box<AssistantBlock>,
    },
    TextDelta {
        content_index: usize,
        delta: String,
    },
    ThinkingDelta {
        content_index: usize,
        delta: String,
    },
    ToolcallDelta {
        content_index: usize,
        path: Vec<JsonValue>,
        delta: String,
    },
    Block {
        content_index: usize,
        block: Box<AssistantBlock>,
    },
    Message {
        message: Box<AssistantMessage>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunInputs {
    pub inputs: Vec<SubmissionId>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueuedItem {
    pub id: SubmissionId,
    pub mode: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotEvent {
    pub entries: Vec<EntryRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run: Option<RunInputs>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<GenerationStatus>,
    pub tools: Vec<ToolSlot>,
    pub compactions: Vec<CompactionStatus>,
    pub inbox: Vec<QueuedItem>,
    pub agent: AgentState,
    pub usage: UsageState,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ToolOutput {
    #[serde(rename_all = "camelCase")]
    Edit {
        #[serde(skip_serializing_if = "Option::is_none")]
        trim_start: Option<usize>,
        #[serde(skip_serializing_if = "Option::is_none")]
        append: Option<String>,
    },
    Set {
        set: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum AgentEvent {
    Snapshot(Box<SnapshotEvent>),
    RunStart {
        inputs: Vec<SubmissionId>,
    },
    RunEnd {
        inputs: Vec<SubmissionId>,
    },
    TurnStart,
    TurnEnd,
    MessageStart {
        message: Message,
    },
    MessageUpdate {
        usage: Usage,
        changes: Vec<MessageChange>,
    },
    MessageEnd {
        entry: EntryRecord,
    },
    ToolExecutionStart {
        tool_call_id: String,
        tool_name: String,
        args: JsonObject,
    },
    ToolExecutionUpdate {
        tool_call_id: String,
        tool_name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        output: Option<ToolOutput>,
        #[serde(skip_serializing_if = "Option::is_none")]
        details: Option<JsonValue>,
        #[serde(skip_serializing_if = "Option::is_none")]
        diagnostics: Option<Vec<ToolDiagnostic>>,
    },
    ToolExecutionEnd {
        tool_call_id: String,
        tool_name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        entry: Option<EntryRecord>,
    },
    InboxUpdate {
        items: Vec<QueuedItem>,
    },
    Submission {
        record: SubmissionRecord,
    },
    AutoRetryStart {
        attempt: u64,
        at: u64,
        error_message: String,
    },
    AutoRetryEnd {
        attempt: u64,
    },
    DeferredPoll {
        poll_at: u64,
    },
    EntryAppended {
        entry: EntryRecord,
    },
    AgentChanged {
        agent: AgentState,
    },
    UsageChanged {
        usage: UsageState,
    },
    TaskFailed {
        task_id: TaskId,
        kind: String,
        message: String,
    },
    CompactionStart {
        task_id: TaskId,
        reason: CompactionReason,
        blocking: bool,
    },
    CompactionEnd {
        task_id: TaskId,
        reason: CompactionReason,
    },
}

pub struct AgentEventStream {
    pub snapshot: SnapshotEvent,
    watch: Arc<WatchShared<Vec<AgentEvent>>>,
}

impl AgentEventStream {
    pub fn start(
        &self,
        mut listener: impl FnMut(&[AgentEvent], &CancelToken) -> Result<()> + Send + 'static,
    ) -> Result<()> {
        self.watch
            .start(Box::new(move |events, _ops, cancel| match events {
                Some(events) => listener(events, cancel),
                None => Ok(()),
            }))
    }

    pub fn stop(&self) -> WatchEnd {
        self.watch.terminate(WatchEnd::Stopped)
    }

    pub fn closed(&self) -> Option<WatchEnd> {
        self.watch.closed()
    }
}

pub fn watch_events(
    session: &Session,
    views: &ConversationViews,
    conversation: ConversationId,
    cancel: &CancelToken,
) -> Result<AgentEventStream> {
    if cancel.is_cancelled() {
        return Err(Error::Aborted);
    }
    let deliveries = session.deliveries();
    let (_, guard, (watch, snapshot)) =
        views.attach_with(session, conversation, cancel, |value, storage| {
            let held = completing_generations(storage, conversation, cancel)?;
            let snapshot = snapshot_of(value)?;
            let watch = WatchShared::new(Some(Vec::new()), deliveries);
            let held = Mutex::new(held);
            let feed = Arc::clone(&watch);
            let observer: Observer = Arc::new(move |change: &Change<'_>| {
                let mut held = held.lock().expect(LOCK);
                match translate(conversation, change, &mut held) {
                    Ok(events) if events.is_empty() => {}
                    Ok(events) => feed.advance(Some(events), Vec::new(), |_| {
                        snapshot_of(change.after).map(|snapshot| {
                            (
                                Some(vec![AgentEvent::Snapshot(Box::new(snapshot))]),
                                Vec::new(),
                            )
                        })
                    }),
                    Err(error) => {
                        feed.terminate(WatchEnd::ListenerError(Arc::new(error)));
                    }
                }
            });
            Ok((observer, (watch, snapshot)))
        })?;
    let guard = Arc::new(Mutex::new(Some(guard)));
    let release = Arc::clone(&guard);
    watch.set_detach(Box::new(move || {
        release.lock().expect(LOCK).take();
    }));
    watch.observe_cancellation(cancel);
    Ok(AgentEventStream { snapshot, watch })
}

fn completing_generations(
    storage: &dyn Storage,
    conversation: ConversationId,
    cancel: &CancelToken,
) -> Result<HashSet<TaskId>> {
    let query = TaskQuery {
        conversation_id: Some(conversation),
        kind: Some(GENERATION.to_owned()),
        status: Some(TaskStatus::Completing),
        ..TaskQuery::default()
    };
    let records =
        scan_all(|cursor| storage.scan_tasks(&query, SCAN_PAGE_SIZE, cursor.as_ref(), cancel))?;
    Ok(records.into_iter().map(|record| record.id).collect())
}

fn key(name: &str) -> Seg {
    Seg::Key(name.to_owned())
}

fn partial_path() -> Vec<Seg> {
    vec![
        key("docs"),
        key("pi.live"),
        key("generation"),
        key("message"),
    ]
}

fn path_of(op: &Op) -> Option<&[Seg]> {
    match op {
        Op::Replace(_) => None,
        Op::Set(path, _)
        | Op::Delete(path)
        | Op::Append(path, _)
        | Op::Truncate(path, _)
        | Op::Splice(path, ..)
        | Op::Permute(path, _) => Some(path),
    }
}

fn starts_with(path: &[Seg], prefix: &[Seg]) -> bool {
    prefix.len() <= path.len() && path[..prefix.len()] == *prefix
}

fn seg_json(seg: &Seg) -> JsonValue {
    match seg {
        Seg::Key(name) => json!(name),
        Seg::Index(index) => json!(index),
    }
}

fn doc<T: DeserializeOwned>(view: &Value, kind: &str) -> Result<Option<T>> {
    view.get("docs")
        .and_then(|docs| docs.get(kind))
        .map(|value| serde_json::from_value::<T>(value.clone()).map_err(Error::Json))
        .transpose()
}

fn initial<T: DeserializeOwned>(definition: &DocDefinition) -> Result<T> {
    serde_json::from_value(Value::Object((definition.initial)(None))).map_err(Error::Json)
}

fn agent_initial() -> Result<AgentState> {
    initial(agent_doc()?.definition())
}

fn usage_initial() -> Result<UsageState> {
    initial(usage_doc()?.definition())
}

struct Parts {
    live: LiveState,
    inbox: Option<InboxState>,
    agent: Option<AgentState>,
    usage: Option<UsageState>,
}

fn parts(view: &Value) -> Result<Parts> {
    Ok(Parts {
        live: doc(view, "pi.live")?.unwrap_or_default(),
        inbox: doc(view, "pi.inbox")?,
        agent: doc(view, "pi.agent")?,
        usage: doc(view, "pi.usage")?,
    })
}

impl From<&InboxItem> for QueuedItem {
    fn from(item: &InboxItem) -> Self {
        match item {
            InboxItem::Steer { id, .. } => QueuedItem {
                id: *id,
                mode: "steer".to_owned(),
            },
            InboxItem::FollowUp { id, .. } => QueuedItem {
                id: *id,
                mode: "followUp".to_owned(),
            },
            InboxItem::Write { id, .. } => QueuedItem {
                id: *id,
                mode: "write".to_owned(),
            },
        }
    }
}

fn queued(inbox: Option<&InboxState>) -> Vec<QueuedItem> {
    inbox.map_or_else(Vec::new, |state| {
        state.items.iter().map(QueuedItem::from).collect()
    })
}

fn snapshot_of(view: &Value) -> Result<SnapshotEvent> {
    let docs = parts(view)?;
    let entries = serde_json::from_value(view.get("entries").cloned().unwrap_or_else(|| json!([])))
        .map_err(Error::Json)?;
    let agent = match docs.agent {
        Some(agent) => agent,
        None => agent_initial()?,
    };
    let usage = match docs.usage {
        Some(usage) => usage,
        None => usage_initial()?,
    };
    Ok(SnapshotEvent {
        entries,
        run: docs.live.run.map(|run| RunInputs { inputs: run.inputs }),
        generation: docs.live.generation,
        tools: docs.live.tools.unwrap_or_default(),
        compactions: docs.live.compactions.unwrap_or_default(),
        inbox: queued(docs.inbox.as_ref()),
        agent,
        usage,
    })
}

fn upsert(tasks: &mut Vec<AnyTaskRecord>, task: &AnyTaskRecord) {
    match tasks.iter_mut().find(|known| known.id == task.id) {
        Some(known) => *known = task.clone(),
        None => tasks.push(task.clone()),
    }
}

fn find_task(tasks: &[AnyTaskRecord], id: TaskId) -> Option<&AnyTaskRecord> {
    tasks.iter().find(|task| task.id == id)
}

fn find_slot<'a>(slots: &'a [ToolSlot], call_id: &str) -> Option<&'a ToolSlot> {
    slots.iter().find(|slot| slot.call_id == call_id)
}

fn checkpoint(state: &TaskState<JsonValue, JsonValue>) -> Option<&JsonValue> {
    match state {
        TaskState::Pending { checkpoint }
        | TaskState::Running { checkpoint }
        | TaskState::Waiting { checkpoint, .. } => Some(checkpoint),
        TaskState::Completing { .. } | TaskState::Terminal { .. } => None,
    }
}

fn result_of<'a>(entries: &'a [EntryRecord], call_id: &str) -> Option<&'a EntryRecord> {
    entries.iter().find(|entry| {
        matches!(entry.model.as_ref().and_then(|model| model.first()), Some(Message::ToolResult(result)) if result.tool_call_id == call_id)
    })
}

fn push_end(
    ends: &mut Vec<(Option<EntryId>, AgentEvent)>,
    entries: &[EntryRecord],
    call_id: &str,
    name: &str,
    entry_id: Option<EntryId>,
) {
    let entry = entry_id
        .and_then(|id| entries.iter().find(|candidate| candidate.id == id))
        .cloned();
    let found = entry.as_ref().map(|entry| entry.id);
    ends.push((
        found,
        AgentEvent::ToolExecutionEnd {
            tool_call_id: call_id.to_owned(),
            tool_name: name.to_owned(),
            entry,
        },
    ));
}

fn translate(
    conversation: ConversationId,
    change: &Change<'_>,
    held: &mut HashSet<TaskId>,
) -> Result<Vec<AgentEvent>> {
    let mut entries: Vec<EntryRecord> = Vec::new();
    let mut tasks: Vec<AnyTaskRecord> = Vec::new();
    let mut submissions: Vec<SubmissionRecord> = Vec::new();
    for commit in &change.publication.changes {
        let CommitChange::Table(table) = commit else {
            continue;
        };
        match table {
            TableChange::Entry(entry) if entry.conversation_id == conversation => {
                entries.push(entry.clone())
            }
            TableChange::Task(task) if task.conversation_id == conversation => {
                upsert(&mut tasks, task)
            }
            TableChange::Submission(record) if record.conversation_id == conversation => {
                submissions.push(record.clone())
            }
            _ => {}
        }
    }
    if change.ops.is_empty() && entries.is_empty() && tasks.is_empty() && submissions.is_empty() {
        return Ok(Vec::new());
    }
    submissions.sort_by_key(|record| record.id);
    let was = parts(change.before)?;
    let now = parts(change.after)?;
    let tools_before = was.live.tools.as_deref().unwrap_or(&[]);
    let slots = now.live.tools.as_deref().unwrap_or(&[]);
    let partial_before = was
        .live
        .generation
        .as_ref()
        .and_then(|generation| generation.message.as_ref());
    let partial = now
        .live
        .generation
        .as_ref()
        .and_then(|generation| generation.message.as_ref());
    let mut events = Vec::new();

    for slot in slots {
        if slot.status != SlotStatus::Running {
            continue;
        }
        if find_slot(tools_before, &slot.call_id)
            .is_some_and(|previous| previous.status == SlotStatus::Running)
        {
            continue;
        }
        let args = slot
            .task_id
            .and_then(|id| find_task(&tasks, id))
            .and_then(|task| checkpoint(&task.state))
            .and_then(|state| state.get("arguments"))
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        events.push(AgentEvent::ToolExecutionStart {
            tool_call_id: slot.call_id.clone(),
            tool_name: slot.name.clone(),
            args,
        });
    }
    match (partial, partial_before) {
        (Some(message), None) => events.push(AgentEvent::MessageStart {
            message: Message::Assistant(Box::new(message.clone())),
        }),
        (Some(message), Some(previous)) if message != previous => {
            events.push(AgentEvent::MessageUpdate {
                usage: message.usage.clone(),
                changes: message_changes(change.ops, message)?,
            })
        }
        _ => {}
    }
    for (index, slot) in slots.iter().enumerate() {
        let Some(previous) = find_slot(tools_before, &slot.call_id) else {
            continue;
        };
        if slot.status != SlotStatus::Running || previous.status != SlotStatus::Running {
            continue;
        }
        let output = tool_output(change.ops, index, slot, previous);
        let details =
            (slot.details != previous.details).then(|| slot.details.clone().unwrap_or(Value::Null));
        let diagnostics = (slot.diagnostics != previous.diagnostics)
            .then(|| slot.diagnostics.clone().unwrap_or_default());
        if output.is_none() && details.is_none() && diagnostics.is_none() {
            continue;
        }
        events.push(AgentEvent::ToolExecutionUpdate {
            tool_call_id: slot.call_id.clone(),
            tool_name: slot.name.clone(),
            output,
            details,
            diagnostics,
        });
    }
    let generation = now.live.generation.as_ref();
    let generation_before = was.live.generation.as_ref();
    let retry_before = generation_before.and_then(|generation| generation.retry.as_ref());
    if let Some(current) = generation
        && let (Some(retry), None) = (&current.retry, retry_before)
    {
        events.push(AgentEvent::AutoRetryStart {
            attempt: current.attempt,
            at: retry.at,
            error_message: retry.error.clone(),
        });
    }
    if let Some(previous) = generation_before
        && previous.retry.is_some()
        && generation
            .and_then(|generation| generation.retry.as_ref())
            .is_none()
    {
        events.push(AgentEvent::AutoRetryEnd {
            attempt: previous.attempt,
        });
    }
    if let Some(DeferredStatus { poll_at }) = generation.and_then(|generation| generation.deferred)
        && Some(poll_at)
            != generation_before
                .and_then(|generation| generation.deferred)
                .map(|deferred| deferred.poll_at)
    {
        events.push(AgentEvent::DeferredPoll { poll_at });
    }

    let mut tool_ends: Vec<(Option<EntryId>, AgentEvent)> = Vec::new();
    for previous in tools_before {
        if previous.status == SlotStatus::Done {
            continue;
        }
        match find_slot(slots, &previous.call_id) {
            Some(slot) if slot.status == SlotStatus::Done => {
                push_end(
                    &mut tool_ends,
                    &entries,
                    &previous.call_id,
                    &previous.name,
                    slot.entry,
                );
            }
            None => {
                let result = result_of(&entries, &previous.call_id).map(|entry| entry.id);
                push_end(
                    &mut tool_ends,
                    &entries,
                    &previous.call_id,
                    &previous.name,
                    result,
                );
            }
            Some(_) => {}
        }
    }
    for slot in slots {
        if slot.status == SlotStatus::Done && find_slot(tools_before, &slot.call_id).is_none() {
            push_end(
                &mut tool_ends,
                &entries,
                &slot.call_id,
                &slot.name,
                slot.entry,
            );
        }
    }

    let mut assistant_appended = false;
    for entry in &entries {
        for (found, end) in &tool_ends {
            if *found == Some(entry.id) {
                events.push(end.clone());
            }
        }
        match entry.model.as_ref().and_then(|model| model.first()) {
            None => events.push(AgentEvent::EntryAppended {
                entry: entry.clone(),
            }),
            Some(message) => {
                let assistant = matches!(message, Message::Assistant(_));
                let streamed = assistant && partial_before.is_some() && !assistant_appended;
                if assistant {
                    assistant_appended = true;
                }
                if !streamed {
                    events.push(AgentEvent::MessageStart {
                        message: message.clone(),
                    });
                }
                events.push(AgentEvent::MessageEnd {
                    entry: entry.clone(),
                });
            }
        }
    }
    for (found, end) in tool_ends {
        if found.is_none() {
            events.push(end);
        }
    }

    let compactions_before = was.live.compactions.as_deref().unwrap_or(&[]);
    let compactions = now.live.compactions.as_deref().unwrap_or(&[]);
    for previous in compactions_before {
        if !compactions
            .iter()
            .any(|status| status.task_id == previous.task_id)
        {
            events.push(AgentEvent::CompactionEnd {
                task_id: previous.task_id,
                reason: previous.reason,
            });
        }
    }
    let mut turn_ended = false;
    for task in &tasks {
        let status = task.status();
        let generation_task = task.kind == GENERATION;
        if generation_task && status == TaskStatus::Completing && held.insert(task.id) {
            turn_ended = true;
        }
        if status != TaskStatus::Terminal {
            continue;
        }
        if generation_task && !held.remove(&task.id) {
            turn_ended = true;
        }
        if let TaskState::Terminal { outcome } = &task.state {
            let message = match outcome {
                TaskOutcome::Faulted { error } => Some(error.message.clone()),
                TaskOutcome::Orphaned { reason } => Some(reason.clone()),
                _ => None,
            };
            if let Some(message) = message {
                events.push(AgentEvent::TaskFailed {
                    task_id: task.id,
                    kind: task.kind.clone(),
                    message,
                });
            }
        }
    }
    if turn_ended {
        events.push(AgentEvent::TurnEnd);
    }
    let run = now.live.run.as_ref();
    let run_before = was.live.run.as_ref();
    let run_changed = run.map(|run| run.inputs.first()) != run_before.map(|run| run.inputs.first());
    if let (Some(previous), true) = (run_before, run_changed) {
        events.push(AgentEvent::RunEnd {
            inputs: previous.inputs.clone(),
        });
    }
    for record in &submissions {
        events.push(AgentEvent::Submission {
            record: record.clone(),
        });
    }
    if now.inbox != was.inbox {
        events.push(AgentEvent::InboxUpdate {
            items: queued(now.inbox.as_ref()),
        });
    }
    if now.agent != was.agent {
        let agent = match now.agent {
            Some(agent) => agent,
            None => agent_initial()?,
        };
        events.push(AgentEvent::AgentChanged { agent });
    }
    if now.usage != was.usage {
        let usage = match now.usage {
            Some(usage) => usage,
            None => usage_initial()?,
        };
        events.push(AgentEvent::UsageChanged { usage });
    }
    for status in compactions {
        if !compactions_before
            .iter()
            .any(|previous| previous.task_id == status.task_id)
        {
            events.push(AgentEvent::CompactionStart {
                task_id: status.task_id,
                reason: status.reason,
                blocking: status.blocking,
            });
        }
    }
    if let (Some(run), true) = (run, run_changed) {
        events.push(AgentEvent::RunStart {
            inputs: run.inputs.clone(),
        });
    }
    if let Some(run) = run {
        let started = run_before.map(|previous| previous.task_id) != Some(run.task_id);
        if started && find_task(&tasks, run.task_id).is_some_and(|task| task.kind == GENERATION) {
            events.push(AgentEvent::TurnStart);
        }
    }
    Ok(events)
}

fn whole_message(message: &AssistantMessage) -> Result<Vec<MessageChange>> {
    Ok(vec![MessageChange::Message {
        message: Box::new(message.clone()),
    }])
}

fn start_change(content_index: usize, block: AssistantBlock) -> MessageChange {
    match block {
        AssistantBlock::Text(_) => MessageChange::TextStart {
            content_index,
            block: Box::new(block),
        },
        AssistantBlock::Thinking(_) => MessageChange::ThinkingStart {
            content_index,
            block: Box::new(block),
        },
        AssistantBlock::ToolCall(_) => MessageChange::ToolcallStart {
            content_index,
            block: Box::new(block),
        },
    }
}

fn message_changes(ops: &[Op], message: &AssistantMessage) -> Result<Vec<MessageChange>> {
    let partial = partial_path();
    let mut changes = Vec::new();
    let mut whole = HashSet::new();
    for op in ops {
        let Some(path) = path_of(op) else {
            continue;
        };
        if !starts_with(path, &partial) {
            if starts_with(&partial, path) {
                return whole_message(message);
            }
            continue;
        }
        let rest = &path[partial.len()..];
        match rest.first() {
            Some(Seg::Key(name)) if name == "usage" => continue,
            Some(Seg::Key(name)) if name == "content" => {}
            _ => return whole_message(message),
        }
        if rest.len() == 1 {
            let Op::Splice(_, at, 0, blocks) = op else {
                return whole_message(message);
            };
            for (offset, block) in blocks.iter().enumerate() {
                let block =
                    serde_json::from_value::<AssistantBlock>(block.clone()).map_err(Error::Json)?;
                changes.push(start_change(at + offset, block));
            }
            continue;
        }
        let Seg::Index(content_index) = rest[1] else {
            return whole_message(message);
        };
        if whole.contains(&content_index) {
            continue;
        }
        match (op, rest.get(2)) {
            (Op::Append(_, delta), Some(Seg::Key(field))) if rest.len() == 3 && field == "text" => {
                changes.push(MessageChange::TextDelta {
                    content_index,
                    delta: delta.clone(),
                });
            }
            (Op::Append(_, delta), Some(Seg::Key(field)))
                if rest.len() == 3 && field == "thinking" =>
            {
                changes.push(MessageChange::ThinkingDelta {
                    content_index,
                    delta: delta.clone(),
                });
            }
            (Op::Append(_, delta), Some(Seg::Key(field))) if field == "arguments" => {
                changes.push(MessageChange::ToolcallDelta {
                    content_index,
                    path: rest[3..].iter().map(seg_json).collect(),
                    delta: delta.clone(),
                });
            }
            _ => {
                whole.insert(content_index);
                let block = message.content.get(content_index).cloned().ok_or_else(|| {
                    Error::Invalid(format!("Message has no content block {content_index}"))
                })?;
                changes.push(MessageChange::Block {
                    content_index,
                    block: Box::new(block),
                });
            }
        }
    }
    Ok(changes)
}

fn tool_output(
    ops: &[Op],
    index: usize,
    slot: &ToolSlot,
    previous: &ToolSlot,
) -> Option<ToolOutput> {
    let output = [
        key("docs"),
        key("pi.live"),
        key("tools"),
        Seg::Index(index),
        key("output"),
    ];
    let mut trim = 0usize;
    let mut append = String::new();
    let mut set = false;
    for op in ops {
        let Some(path) = path_of(op) else {
            continue;
        };
        if !starts_with(path, &output) {
            continue;
        }
        match op {
            Op::Truncate(_, count) => trim += count,
            Op::Append(_, text) => append.push_str(text),
            _ => set = true,
        }
    }
    if set || (slot.output != previous.output && trim == 0 && append.is_empty()) {
        return Some(ToolOutput::Set {
            set: slot.output.clone().unwrap_or_default(),
        });
    }
    (trim > 0 || !append.is_empty()).then(|| ToolOutput::Edit {
        trim_start: (trim > 0).then_some(trim),
        append: (!append.is_empty()).then_some(append),
    })
}
