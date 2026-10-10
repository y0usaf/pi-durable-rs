use std::collections::{HashMap, HashSet};

use crate::ai::event_stream::now_ms;
use crate::ai::json_parse::trim_js;
use crate::ai::types::{
    AssistantBlock, AssistantMessage, Message, Model, StopReason, TextContent, TextType, ToolCall,
    ToolResultMessage, ToolResultRole, UserBlock, UserContent, UserMessage,
};

/// Maps a tool call id to the id a different model accepts. Called only for calls
/// from another model, and only when [transform_messages] is given one.
pub type ToolCallIdNormalizer<'a> = dyn Fn(&str, &Model, &AssistantMessage) -> String + 'a;

const NON_VISION_USER_IMAGE_PLACEHOLDER: &str = "(image omitted: model does not support images)";
const NON_VISION_TOOL_IMAGE_PLACEHOLDER: &str =
    "(tool image omitted: model does not support images)";
const NO_RESULT_TEXT: &str = "No result provided";

fn text_block(text: &str) -> UserBlock {
    UserBlock::Text(TextContent {
        kind: TextType,
        text: text.to_owned(),
        text_signature: None,
    })
}

fn replace_images_with_placeholder(content: &[UserBlock], placeholder: &str) -> Vec<UserBlock> {
    let mut result = Vec::with_capacity(content.len());
    let mut previous_was_placeholder = false;
    for block in content {
        match block {
            UserBlock::Image(_) => {
                if !previous_was_placeholder {
                    result.push(text_block(placeholder));
                }
                previous_was_placeholder = true;
            }
            UserBlock::Text(text) => {
                previous_was_placeholder = text.text == placeholder;
                result.push(block.clone());
            }
        }
    }
    result
}

fn downgrade_unsupported_images(messages: &[Message], model: &Model) -> Vec<Message> {
    if model.input.iter().any(|input| input == "image") {
        return messages.to_vec();
    }
    messages
        .iter()
        .map(|message| match message {
            Message::User(user) => match &user.content {
                UserContent::Blocks(blocks) => Message::User(UserMessage {
                    content: UserContent::Blocks(replace_images_with_placeholder(
                        blocks,
                        NON_VISION_USER_IMAGE_PLACEHOLDER,
                    )),
                    ..user.clone()
                }),
                UserContent::Text(_) => message.clone(),
            },
            Message::ToolResult(result) => {
                let mut result = (**result).clone();
                result.content = replace_images_with_placeholder(
                    &result.content,
                    NON_VISION_TOOL_IMAGE_PLACEHOLDER,
                );
                Message::ToolResult(Box::new(result))
            }
            Message::System(_) | Message::Assistant(_) => message.clone(),
        })
        .collect()
}

fn is_same_model(message: &AssistantMessage, model: &Model) -> bool {
    message.provider == model.provider && message.api == model.api && message.model == model.id
}

fn is_non_empty(value: Option<&String>) -> bool {
    value.is_some_and(|text| !text.is_empty())
}

fn plain_text(text: String) -> AssistantBlock {
    AssistantBlock::Text(TextContent {
        kind: TextType,
        text,
        text_signature: None,
    })
}

fn transform_assistant_content(
    message: &AssistantMessage,
    model: &Model,
    normalize: Option<&ToolCallIdNormalizer<'_>>,
    id_map: &mut HashMap<String, String>,
) -> Vec<AssistantBlock> {
    let same_model = is_same_model(message, model);
    let mut content = Vec::with_capacity(message.content.len());
    for block in &message.content {
        match block {
            AssistantBlock::Thinking(thinking) => {
                if thinking.redacted == Some(true) {
                    if same_model {
                        content.push(block.clone());
                    }
                } else if same_model && is_non_empty(thinking.thinking_signature.as_ref()) {
                    content.push(block.clone());
                } else if !trim_js(&thinking.thinking).is_empty() {
                    content.push(if same_model {
                        block.clone()
                    } else {
                        plain_text(thinking.thinking.clone())
                    });
                }
            }
            AssistantBlock::Text(text) => {
                if same_model {
                    content.push(block.clone());
                } else {
                    content.push(plain_text(text.text.clone()));
                }
            }
            AssistantBlock::ToolCall(call) => {
                let mut call: ToolCall = call.clone();
                if !same_model {
                    call.thought_signature = None;
                    if let Some(normalize) = normalize {
                        let normalized = normalize(&call.id, model, message);
                        if normalized != call.id {
                            id_map.insert(call.id.clone(), normalized.clone());
                            call.id = normalized;
                        }
                    }
                }
                content.push(AssistantBlock::ToolCall(call));
            }
        }
    }
    content
}

fn synthetic_result(call: &ToolCall) -> Message {
    Message::ToolResult(Box::new(ToolResultMessage {
        role: ToolResultRole,
        tool_call_id: call.id.clone(),
        tool_name: call.name.clone(),
        content: vec![text_block(NO_RESULT_TEXT)],
        details: None,
        usage: None,
        nested_calls: None,
        is_error: true,
        duration_ms: None,
        timestamp: now_ms(),
    }))
}

#[derive(Default)]
struct PendingCalls {
    calls: Vec<ToolCall>,
    answered: HashSet<String>,
    held_system: Vec<Message>,
}

impl PendingCalls {
    fn close(&mut self, result: &mut Vec<Message>) {
        for call in self.calls.drain(..) {
            if !self.answered.contains(&call.id) {
                result.push(synthetic_result(&call));
            }
        }
        self.answered.clear();
        result.append(&mut self.held_system);
    }
}

/// Prepare a transcript for a request to `model`, like pi-ai's transformMessages.
/// Unsupported images become text placeholders. Thinking and tool call ids from other
/// models are converted, and redacted or signature-less thinking is dropped. Errored
/// and aborted assistant turns are skipped. Tool calls without a result get a synthetic
/// error result before the next user or assistant turn. System messages that fall
/// between a tool call and its results are moved after those results.
pub fn transform_messages(
    messages: &[Message],
    model: &Model,
    normalize: Option<&ToolCallIdNormalizer<'_>>,
) -> Vec<Message> {
    let mut id_map: HashMap<String, String> = HashMap::new();
    let mut transformed = Vec::with_capacity(messages.len());
    for message in downgrade_unsupported_images(messages, model) {
        transformed.push(match message {
            Message::System(_) | Message::User(_) => message,
            Message::ToolResult(result) => match id_map.get(&result.tool_call_id) {
                Some(id) if !id.is_empty() && *id != result.tool_call_id => {
                    let mut result = *result;
                    result.tool_call_id = id.clone();
                    Message::ToolResult(Box::new(result))
                }
                _ => Message::ToolResult(result),
            },
            Message::Assistant(mut assistant) => {
                assistant.content =
                    transform_assistant_content(&assistant, model, normalize, &mut id_map);
                Message::Assistant(assistant)
            }
        });
    }

    let mut result = Vec::with_capacity(transformed.len());
    let mut pending = PendingCalls::default();
    for message in transformed {
        match message {
            Message::Assistant(assistant) => {
                pending.close(&mut result);
                if matches!(
                    assistant.stop_reason,
                    StopReason::Error | StopReason::Aborted
                ) {
                    continue;
                }
                let calls: Vec<ToolCall> = assistant
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        AssistantBlock::ToolCall(call) => Some(call.clone()),
                        AssistantBlock::Text(_) | AssistantBlock::Thinking(_) => None,
                    })
                    .collect();
                if !calls.is_empty() {
                    pending.calls = calls;
                    pending.answered.clear();
                }
                result.push(Message::Assistant(assistant));
            }
            Message::ToolResult(tool_result) => {
                pending.answered.insert(tool_result.tool_call_id.clone());
                result.push(Message::ToolResult(tool_result));
            }
            Message::System(system) => {
                if pending.calls.is_empty() {
                    result.push(Message::System(system));
                } else {
                    pending.held_system.push(Message::System(system));
                }
            }
            Message::User(user) => {
                pending.close(&mut result);
                result.push(Message::User(user));
            }
        }
    }
    pending.close(&mut result);
    result
}
