use serde_json::json;
use std::sync::Arc;

use crate::ai::types::{
    AssistantBlock, Message, StopReason, TextContent, TextType, ToolCall, ToolResultMessage,
    ToolResultRole, UserBlock,
};
use crate::cancel::CancelToken;
use crate::errors::{Error, Result};
use crate::ids::{ConversationId, EntryId};
use crate::types::{
    ContextAction, ContextEdit, Cursor, EntryQuery, EntryRecord, HeadMarker, Storage,
};

const SCAN_PAGE_SIZE: usize = 256;
const MISSING_RESULT_TEXT: &str =
    "Tool result unavailable: history ends before this call completed.";

#[derive(Debug, Clone)]
pub struct ContextBounds {
    pub head: Option<HeadMarker>,
    pub tail: EntryId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContextView {
    pub head: Option<EntryRecord>,
    pub entries: Vec<Arc<EntryRecord>>,
    pub contributions: Vec<Arc<[Message]>>,
    pub messages: Vec<Message>,
}

#[derive(Debug, Clone)]
pub struct ContextRange {
    pub bounds: ContextBounds,
    pub entries: Vec<Arc<EntryRecord>>,
    pub view: Arc<ContextView>,
    pub edited: std::collections::BTreeSet<EntryId>,
    pub settled: Vec<Message>,
    pub open: Vec<Message>,
    pub led: usize,
}

pub fn empty_view() -> ContextView {
    ContextView {
        head: None,
        entries: Vec::new(),
        contributions: Vec::new(),
        messages: Vec::new(),
    }
}

pub fn capture_context_bounds(
    storage: &dyn Storage,
    conversation_id: ConversationId,
    at: Option<EntryId>,
    cancel: &CancelToken,
) -> Result<Option<ContextBounds>> {
    let tail = match at {
        None => {
            let query = EntryQuery {
                conversation_id,
                min_entry_id: None,
                max_entry_id: None,
                order: None,
            };
            match storage.scan_entries(&query, 1, None, cancel)?.items.first() {
                Some(entry) => entry.id,
                None => return Ok(None),
            }
        }
        Some(at) => {
            if storage
                .visible_entry(conversation_id, at, cancel)?
                .is_none()
            {
                return Err(Error::Invalid(format!(
                    "Entry {at} is not visible from conversation {conversation_id}"
                )));
            }
            at
        }
    };
    let head = storage.find_latest_head_marker(conversation_id, Some(tail), cancel)?;
    Ok(Some(ContextBounds { head, tail }))
}

pub fn read_context_from(
    storage: &dyn Storage,
    conversation_id: ConversationId,
    bounds: Option<ContextBounds>,
    previous: Option<Arc<ContextRange>>,
    cancel: &CancelToken,
) -> Result<(Arc<ContextView>, Option<Arc<ContextRange>>)> {
    let Some(bounds) = bounds else {
        return Ok((Arc::new(empty_view()), None));
    };
    let head_id = |head: &Option<HeadMarker>| head.as_ref().map(|marker| marker.entry.id);
    let range = match previous {
        Some(previous) if head_id(&previous.bounds.head) == head_id(&bounds.head) => {
            if bounds.tail == previous.bounds.tail {
                previous
            } else if bounds.tail < previous.bounds.tail {
                let kept = previous
                    .entries
                    .iter()
                    .filter(|entry| entry.id <= bounds.tail)
                    .cloned()
                    .collect();
                Arc::new(derive_range(bounds, kept))
            } else {
                let min = EntryId(previous.bounds.tail.0 + 1);
                let added = scan_range(
                    storage,
                    conversation_id,
                    Some(min),
                    Some(bounds.tail),
                    cancel,
                )?;
                Arc::new(extend_range(previous, bounds, added))
            }
        }
        _ => {
            let (min, max) = range_bounds(&bounds);
            let entries = scan_range(storage, conversation_id, min, max, cancel)?;
            Arc::new(derive_range(bounds, entries))
        }
    };
    let view = Arc::clone(&range.view);
    Ok((view, Some(range)))
}

pub fn derive_context(
    storage: &dyn Storage,
    conversation_id: ConversationId,
    bounds: Option<ContextBounds>,
    cancel: &CancelToken,
) -> Result<ContextView> {
    let Some(bounds) = bounds else {
        return Ok(empty_view());
    };
    let (min, max) = range_bounds(&bounds);
    let entries = scan_range(storage, conversation_id, min, max, cancel)?;
    Ok(Arc::unwrap_or_clone(derive_range(bounds, entries).view))
}

pub fn active_entries(
    storage: &dyn Storage,
    conversation_id: ConversationId,
    bounds: Option<ContextBounds>,
    cancel: &CancelToken,
) -> Result<Vec<EntryRecord>> {
    let Some(bounds) = bounds else {
        return Ok(Vec::new());
    };
    let (min, max) = range_bounds(&bounds);
    let entries = scan_range(storage, conversation_id, min, max, cancel)?;
    Ok(select_active(bounds.head.as_ref(), &entries)
        .into_iter()
        .map(Arc::unwrap_or_clone)
        .collect())
}

fn range_bounds(bounds: &ContextBounds) -> (Option<EntryId>, Option<EntryId>) {
    (
        bounds.head.as_ref().map(|head| head.head),
        Some(bounds.tail),
    )
}

fn scan_range(
    storage: &dyn Storage,
    conversation_id: ConversationId,
    min_entry_id: Option<EntryId>,
    max_entry_id: Option<EntryId>,
    cancel: &CancelToken,
) -> Result<Vec<Arc<EntryRecord>>> {
    let query = EntryQuery {
        conversation_id,
        min_entry_id,
        max_entry_id,
        order: None,
    };
    let mut entries = Vec::new();
    let mut cursor: Option<Cursor> = None;
    loop {
        let page = storage.scan_entries(&query, SCAN_PAGE_SIZE, cursor.as_ref(), cancel)?;
        entries.extend(page.items);
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    entries.reverse();
    Ok(entries.into_iter().map(Arc::new).collect())
}

fn derive_range(bounds: ContextBounds, entries: Vec<Arc<EntryRecord>>) -> ContextRange {
    let mut edits: std::collections::BTreeMap<EntryId, ContextEdit> = Default::default();
    for entry in &entries {
        for edit in entry.edits.iter().flatten() {
            edits.insert(edit.target, edit.clone());
        }
    }
    let active = select_active(bounds.head.as_ref(), &entries);
    let contributions: Vec<Arc<[Message]>> = active
        .iter()
        .map(|entry| contribute(entry, edits.get(&entry.id)))
        .collect();
    let flat: Vec<Message> = contributions
        .iter()
        .flat_map(|c| c.iter())
        .cloned()
        .collect();
    let (settled, open) = settle(Vec::new(), flat);
    let (messages, led) = lead_with_system(concat(&settled, &order_tool_results(&open)));
    let view = Arc::new(ContextView {
        head: bounds.head.as_ref().map(|head| head.entry.clone()),
        entries: active,
        contributions,
        messages,
    });
    ContextRange {
        bounds,
        entries,
        view,
        edited: edits.keys().copied().collect(),
        settled,
        open,
        led,
    }
}

fn extend_range(
    previous: Arc<ContextRange>,
    bounds: ContextBounds,
    added: Vec<Arc<EntryRecord>>,
) -> ContextRange {
    let rebuild = added.iter().any(|entry| {
        entry.edits.is_some() || entry.head.is_some() || previous.edited.contains(&entry.id)
    });
    let mut owned = Arc::try_unwrap(previous).unwrap_or_else(|shared| (*shared).clone());
    owned.entries.extend(added.iter().cloned());
    if rebuild {
        return derive_range(bounds, owned.entries);
    }
    grow_range(owned, bounds, added)
}

fn grow_range(
    previous: ContextRange,
    bounds: ContextBounds,
    added: Vec<Arc<EntryRecord>>,
) -> ContextRange {
    let ContextRange {
        entries,
        view,
        edited,
        mut settled,
        mut open,
        led,
        ..
    } = previous;
    let contributions: Vec<Arc<[Message]>> =
        added.iter().map(|entry| contribute(entry, None)).collect();
    open.extend(contributions.iter().flat_map(|c| c.iter()).cloned());
    let kept = settled.len();
    let (mut view_entries, mut view_contributions, mut messages) = match Arc::try_unwrap(view) {
        Ok(view) => (view.entries, view.contributions, view.messages),
        Err(shared) => (
            shared.entries.clone(),
            shared.contributions.clone(),
            shared.messages.clone(),
        ),
    };
    view_entries.extend(added);
    view_contributions.extend(contributions);
    let promoted = match open.iter().rposition(is_assistant) {
        Some(last) if last > 0 => {
            let promoted = order_tool_results(&open[..last]);
            open.drain(..last);
            promoted
        }
        _ => Vec::new(),
    };
    settled.extend(promoted.iter().cloned());
    let ordered = order_tool_results(&open);
    let (messages, led) = if led <= kept && messages.len() >= kept {
        messages.truncate(kept);
        messages.extend(promoted);
        messages.extend(ordered);
        let (messages, touched) = lead_with_system(messages);
        (messages, led.max(touched))
    } else {
        lead_with_system(concat(&settled, &ordered))
    };
    let view = Arc::new(ContextView {
        head: bounds.head.as_ref().map(|head| head.entry.clone()),
        entries: view_entries,
        contributions: view_contributions,
        messages,
    });
    ContextRange {
        bounds,
        entries,
        view,
        edited,
        settled,
        open,
        led,
    }
}

fn concat(first: &[Message], second: &[Message]) -> Vec<Message> {
    first.iter().chain(second).cloned().collect()
}

fn lead_with_system(mut messages: Vec<Message>) -> (Vec<Message>, usize) {
    let Some(index) = messages.iter().position(|message| !is_role_user(message)) else {
        return (messages, 0);
    };
    if index == 0 || !matches!(messages[index], Message::System(_)) {
        return (messages, 0);
    }
    let system = messages.remove(index);
    messages.insert(0, system);
    (messages, index + 1)
}

fn is_role_user(message: &Message) -> bool {
    matches!(message, Message::User(_))
}

fn is_assistant(message: &Message) -> bool {
    matches!(message, Message::Assistant(_))
}

fn is_tool_result(message: &Message) -> bool {
    matches!(message, Message::ToolResult(_))
}

fn excluded_stop(message: &Message) -> bool {
    matches!(
        message,
        Message::Assistant(assistant)
            if matches!(assistant.stop_reason, StopReason::Aborted | StopReason::Error | StopReason::Deferred)
    )
}

fn contribute(entry: &EntryRecord, edit: Option<&ContextEdit>) -> Arc<[Message]> {
    let contributed = match edit.map(|edit| &edit.action) {
        Some(ContextAction::Omit) => return Arc::from([]),
        Some(ContextAction::Replace { messages }) => messages.clone(),
        None => entry.model.clone().unwrap_or_default(),
    };
    contributed
        .into_iter()
        .filter(|message| !excluded_stop(message))
        .collect()
}

fn settle(settled: Vec<Message>, open: Vec<Message>) -> (Vec<Message>, Vec<Message>) {
    match open.iter().rposition(is_assistant) {
        Some(last) if last > 0 => {
            let settled = concat(&settled, &order_tool_results(&open[..last]));
            (settled, open[last..].to_vec())
        }
        _ => (settled, open),
    }
}

fn select_active(head: Option<&HeadMarker>, range: &[Arc<EntryRecord>]) -> Vec<Arc<EntryRecord>> {
    match head {
        None => range.to_vec(),
        Some(head) => std::iter::once(Arc::new(head.entry.clone()))
            .chain(range.iter().filter(|entry| entry.head.is_none()).cloned())
            .collect(),
    }
}

pub fn order_tool_results(messages: &[Message]) -> Vec<Message> {
    let mut ordered = Vec::new();
    for (index, message) in messages.iter().enumerate() {
        if is_tool_result(message) {
            continue;
        }
        ordered.push(message.clone());
        let Message::Assistant(assistant) = message else {
            continue;
        };
        let calls: Vec<&ToolCall> = assistant
            .content
            .iter()
            .filter_map(|block| match block {
                AssistantBlock::ToolCall(call) => Some(call),
                _ => None,
            })
            .collect();
        if calls.is_empty() {
            continue;
        }
        let mut results: std::collections::HashMap<&str, usize> = Default::default();
        for (next, candidate) in messages.iter().enumerate().skip(index + 1) {
            if is_assistant(candidate) {
                break;
            }
            if let Message::ToolResult(result) = candidate {
                results.entry(result.tool_call_id.as_str()).or_insert(next);
            }
        }
        for call in calls {
            match results.get(call.id.as_str()) {
                Some(&found) => ordered.push(messages[found].clone()),
                None => ordered.push(missing_result(call, assistant.timestamp)),
            }
        }
    }
    ordered
}

fn missing_result(call: &ToolCall, timestamp: u64) -> Message {
    Message::ToolResult(Arc::new(ToolResultMessage {
        role: ToolResultRole,
        tool_call_id: call.id.clone(),
        tool_name: call.name.clone(),
        content: vec![UserBlock::Text(TextContent {
            kind: TextType,
            text: MISSING_RESULT_TEXT.to_owned(),
            text_signature: None,
        })],
        details: Some(json!({ "reason": "missing_result" })),
        usage: None,
        nested_calls: None,
        is_error: true,
        timestamp,
        duration_ms: None,
    }))
}
