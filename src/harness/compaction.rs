use std::collections::HashSet;

use crate::ai::types::{AssistantBlock, AssistantMessage, Message};
use crate::ai::utils::estimate::{calculate_context_tokens, estimate_message_tokens};
use crate::errors::{Error, Result};
use crate::harness::context::ContextView;
use crate::harness::types::CompactionReason;
use crate::ids::{ConversationId, TaskId};
use crate::session::types::Tx;

pub fn estimate_context(view: &ContextView, extra: &[Message]) -> u64 {
    let after = view.head.as_ref().map(|head| head.id);
    let measured: Option<&AssistantMessage> = (0..view.entries.len())
        .rev()
        .filter(|&index| after.is_none_or(|after| view.entries[index].id > after))
        .find_map(|index| {
            view.contributions[index]
                .iter()
                .rev()
                .find_map(|message| match message {
                    Message::Assistant(assistant)
                        if calculate_context_tokens(&assistant.usage) > 0 =>
                    {
                        Some(assistant.as_ref())
                    }
                    _ => None,
                })
        });
    let (from, mut tokens) = match measured {
        None => (0, 0),
        Some(measured) => {
            let position = view
                .messages
                .iter()
                .rposition(
                    |message| matches!(message, Message::Assistant(a) if a.as_ref() == measured),
                )
                .map_or(0, |index| index + 1);
            (position, calculate_context_tokens(&measured.usage))
        }
    };
    for message in view.messages.iter().skip(from).chain(extra) {
        tokens += estimate_message_tokens(message);
    }
    tokens
}

pub fn select_cut(view: &ContextView, keep_recent_tokens: u64) -> Option<usize> {
    let contributions = &view.contributions;
    let start = usize::from(view.head.is_some());
    let candidates: Vec<usize> = (start..contributions.len())
        .filter(|&index| is_candidate(contributions, index))
        .collect();
    let mut kept = 0;
    let mut cut = None;
    for index in (start..contributions.len()).rev() {
        kept += contributions[index]
            .iter()
            .map(estimate_message_tokens)
            .sum::<u64>();
        if kept < keep_recent_tokens {
            continue;
        }
        cut = candidates
            .iter()
            .copied()
            .find(|&candidate| candidate >= index)
            .or(candidates.last().copied());
        break;
    }
    let cut = cut?;
    (start..cut)
        .any(|index| !contributions[index].is_empty())
        .then_some(cut)
}

fn is_candidate(contributions: &[Vec<Message>], index: usize) -> bool {
    match contributions[index].first() {
        Some(Message::Assistant(_)) => return true,
        Some(Message::User(_)) => {}
        _ => return false,
    }
    let calls: Vec<String> = contributions[..index]
        .iter()
        .rev()
        .find_map(|messages| {
            messages.iter().rev().find_map(|message| match message {
                Message::Assistant(assistant) => Some(
                    assistant
                        .content
                        .iter()
                        .filter_map(|content| match content {
                            AssistantBlock::ToolCall(call) => Some(call.id.clone()),
                            _ => None,
                        })
                        .collect(),
                ),
                _ => None,
            })
        })
        .unwrap_or_default();
    if calls.is_empty() {
        return true;
    }
    let calls: HashSet<String> = calls.into_iter().collect();
    for (after, messages) in contributions.iter().enumerate().skip(index) {
        for (position, message) in messages.iter().enumerate() {
            match message {
                Message::Assistant(_) if after > index || position > 0 => return true,
                Message::ToolResult(result) if calls.contains(&result.tool_call_id) => {
                    return false;
                }
                _ => {}
            }
        }
    }
    true
}

pub fn create_compaction(
    _tx: &mut dyn Tx,
    _conversation_id: ConversationId,
    _reason: CompactionReason,
    _owner: Option<TaskId>,
) -> Result<TaskId> {
    Err(Error::Invalid(
        "compaction task is not registered yet: the compaction worker provides create_compaction"
            .to_owned(),
    ))
}
