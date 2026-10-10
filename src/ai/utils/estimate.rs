use super::text::{system_message_text, utf16_len};
use crate::ai::types::{AssistantBlock, Message, StopReason, Usage, UserBlock, UserContent};

const CHARS_PER_TOKEN: f64 = 3.5;
const ESTIMATED_IMAGE_CHARS: usize = 4800;

pub fn calculate_context_tokens(usage: &Usage) -> u64 {
    if usage.total_tokens != 0 {
        usage.total_tokens
    } else {
        usage.input + usage.output + usage.cache_read + usage.cache_write
    }
}

fn tokens_for_chars(chars: usize) -> u64 {
    (chars as f64 / CHARS_PER_TOKEN).ceil() as u64
}

fn estimate_text_tokens(text: &str) -> u64 {
    tokens_for_chars(utf16_len(text))
}

fn json_len(value: &impl serde::Serialize) -> usize {
    serde_json::to_string(value).map_or(0, |json| utf16_len(&json))
}

fn estimate_tools_tokens<T: serde::Serialize>(tools: Option<&[T]>) -> u64 {
    match tools {
        Some(tools) if !tools.is_empty() => tokens_for_chars(json_len(&tools)),
        _ => 0,
    }
}

fn estimate_user_content_tokens(content: &UserContent) -> u64 {
    let chars = match content {
        UserContent::Text(text) => utf16_len(text),
        UserContent::Blocks(blocks) => blocks.iter().map(user_block_chars).sum(),
    };
    tokens_for_chars(chars)
}

fn user_block_chars(block: &UserBlock) -> usize {
    match block {
        UserBlock::Text(text) => utf16_len(&text.text),
        UserBlock::Image(_) => ESTIMATED_IMAGE_CHARS,
    }
}

pub fn estimate_message_tokens(message: &Message) -> u64 {
    match message {
        Message::System(system) => {
            estimate_text_tokens(&system_message_text(system))
                + estimate_tools_tokens(system.tools_added.as_deref())
                + estimate_tools_tokens(system.tools_removed.as_deref())
        }
        Message::User(user) => estimate_user_content_tokens(&user.content),
        Message::ToolResult(result) => {
            tokens_for_chars(result.content.iter().map(user_block_chars).sum())
        }
        Message::Assistant(assistant) => {
            let chars: usize = assistant
                .content
                .iter()
                .map(|block| match block {
                    AssistantBlock::Text(text) => utf16_len(&text.text),
                    AssistantBlock::Thinking(thinking) => utf16_len(&thinking.thinking),
                    AssistantBlock::ToolCall(call) => {
                        utf16_len(&call.name) + json_len(&call.arguments)
                    }
                })
                .sum();
            tokens_for_chars(chars)
        }
    }
}

fn message_timestamp(message: &Message) -> u64 {
    match message {
        Message::System(system) => system.timestamp,
        Message::User(user) => user.timestamp,
        Message::Assistant(assistant) => assistant.timestamp,
        Message::ToolResult(result) => result.timestamp,
    }
}

fn last_usage_index(messages: &[Message]) -> Option<(usize, u64)> {
    let mut latest_prefix_timestamp = 0;
    let mut usage = None;
    for (index, message) in messages.iter().enumerate() {
        if let Message::Assistant(assistant) = message {
            let applies_to_prefix = assistant.timestamp >= latest_prefix_timestamp;
            let tokens = calculate_context_tokens(&assistant.usage);
            if applies_to_prefix
                && !matches!(
                    assistant.stop_reason,
                    StopReason::Aborted | StopReason::Error
                )
                && tokens > 0
            {
                usage = Some((index, tokens));
            }
        }
        latest_prefix_timestamp = latest_prefix_timestamp.max(message_timestamp(message));
    }
    usage
}

/// Estimate the tokens a transcript occupies. Uses the newest usable assistant usage
/// figure as the base and adds estimates for messages after it; without one, estimates
/// every message.
pub fn estimate_context_tokens(messages: &[Message]) -> u64 {
    match last_usage_index(messages) {
        Some((index, usage_tokens)) => {
            usage_tokens
                + messages[index + 1..]
                    .iter()
                    .map(estimate_message_tokens)
                    .sum::<u64>()
        }
        None => messages.iter().map(estimate_message_tokens).sum(),
    }
}
