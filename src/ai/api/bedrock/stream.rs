use std::io::Read;

use serde_json::{Map, Value};

use super::eventstream::read_frame;
use crate::ai::cost::calculate_cost;
use crate::ai::event_stream::{ABORTED_TEXT, AssistantMessageEventStream};
use crate::ai::json_parse::parse_streaming_json;
use crate::ai::types::{
    AssistantBlock, AssistantMessage, AssistantMessageEvent, Model, StopReason, TextContent,
    TextType, ThinkingContent, ThinkingType, ToolCall, ToolCallType,
};
use crate::cancel::CancelToken;

const STREAM_EXCEPTIONS: [&str; 5] = [
    "internalServerException",
    "modelStreamErrorException",
    "validationException",
    "throttlingException",
    "serviceUnavailableException",
];
const REDACTED_TEXT: &str = "[Reasoning redacted]";

struct Slot {
    index: u64,
    partial_json: String,
}

enum Finished {
    Text(String),
    Thinking(String),
    Tool(ToolCall),
}

pub fn read(
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
    mut body: impl Read,
    model: &Model,
    signal: Option<&CancelToken>,
) -> Result<(), String> {
    let mut slots: Vec<Slot> = Vec::new();
    loop {
        if signal.is_some_and(CancelToken::is_cancelled) {
            return Err(ABORTED_TEXT.to_owned());
        }
        let Some(frame) = read_frame(&mut body)? else {
            break;
        };
        let (kind, payload) = frame.into_event()?;
        let data: Value = if payload.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&payload)
                .map_err(|error| format!("Could not parse Bedrock event {kind}: {error}"))?
        };
        handle(out, output, &mut slots, model, &kind, &data)?;
    }
    if signal.is_some_and(CancelToken::is_cancelled) {
        return Err(ABORTED_TEXT.to_owned());
    }
    match output.stop_reason {
        StopReason::Pending => Err("Bedrock stream ended without a stop reason".to_owned()),
        StopReason::Aborted | StopReason::Error => Err(output
            .error_message
            .clone()
            .unwrap_or_else(|| "An unknown error occurred".to_owned())),
        _ => Ok(()),
    }
}

fn handle(
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
    slots: &mut Vec<Slot>,
    model: &Model,
    kind: &str,
    data: &Value,
) -> Result<(), String> {
    if STREAM_EXCEPTIONS.contains(&kind) {
        let message = data["message"].as_str().unwrap_or("Unknown");
        return Err(format!("{kind}: {message}"));
    }
    let index = data["contentBlockIndex"].as_u64().unwrap_or(0);
    match kind {
        "contentBlockStart" => {
            let tool = &data["start"]["toolUse"];
            if tool.is_object() {
                output.content.push(AssistantBlock::ToolCall(ToolCall {
                    kind: ToolCallType,
                    id: tool["toolUseId"].as_str().unwrap_or_default().to_owned(),
                    name: tool["name"].as_str().unwrap_or_default().to_owned(),
                    arguments: Map::new(),
                    thought_signature: None,
                    namespace: None,
                }));
                slots.push(Slot {
                    index,
                    partial_json: String::new(),
                });
                out.push(AssistantMessageEvent::ToolcallStart {
                    content_index: last(output),
                    partial: output.clone(),
                });
            }
        }
        "contentBlockDelta" => content_delta(out, output, slots, index, &data["delta"]),
        "contentBlockStop" => content_stop(out, output, slots, index),
        "messageStop" => {
            let reason = data["stopReason"].as_str().unwrap_or_default();
            output.raw_stop_reason = Some(reason.to_owned());
            let (stop, message) = map_stop_reason(reason);
            output.stop_reason = stop;
            output.error_message = message;
        }
        "metadata" => {
            let usage = &data["usage"];
            set_count(&usage["inputTokens"], &mut output.usage.input);
            set_count(&usage["outputTokens"], &mut output.usage.output);
            set_count(&usage["cacheReadInputTokens"], &mut output.usage.cache_read);
            set_count(
                &usage["cacheWriteInputTokens"],
                &mut output.usage.cache_write,
            );
            output.usage.total_tokens = match usage["totalTokens"].as_u64() {
                Some(total) => total,
                None => {
                    output.usage.input
                        + output.usage.output
                        + output.usage.cache_read
                        + output.usage.cache_write
                }
            };
            calculate_cost(model, &mut output.usage);
        }
        _ => {}
    }
    Ok(())
}

fn content_delta(
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
    slots: &mut Vec<Slot>,
    index: u64,
    delta: &Value,
) {
    if let Some(text) = delta["text"].as_str() {
        let position = ensure_block(out, output, slots, index, Block::Text);
        if let AssistantBlock::Text(block) = &mut output.content[position] {
            block.text.push_str(text);
            out.push(AssistantMessageEvent::TextDelta {
                content_index: position as u64,
                delta: text.to_owned(),
                partial: output.clone(),
            });
        }
    } else if let Some(input) = delta["toolUse"]["input"].as_str() {
        let Some(position) = position(slots, index) else {
            return;
        };
        slots[position].partial_json.push_str(input);
        if let AssistantBlock::ToolCall(call) = &mut output.content[position] {
            call.arguments = match parse_streaming_json(Some(&slots[position].partial_json)) {
                Value::Object(map) => map,
                _ => Map::new(),
            };
            out.push(AssistantMessageEvent::ToolcallDelta {
                content_index: position as u64,
                delta: input.to_owned(),
                partial: output.clone(),
            });
        }
    } else if delta["reasoningContent"].is_object() {
        reasoning_delta(out, output, slots, index, &delta["reasoningContent"]);
    }
}

fn reasoning_delta(
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
    slots: &mut Vec<Slot>,
    index: u64,
    reasoning: &Value,
) {
    if let Some(data) = reasoning["redactedContent"].as_str() {
        let position = ensure_block(out, output, slots, index, Block::Redacted);
        if let AssistantBlock::Thinking(block) = &mut output.content[position] {
            block.thinking_signature = Some(data.to_owned());
        }
        return;
    }
    let text = reasoning["text"].as_str();
    let signature = reasoning["signature"].as_str();
    let position = ensure_block(out, output, slots, index, Block::Thinking);
    if let (Some(signature), AssistantBlock::Thinking(block)) =
        (signature, &mut output.content[position])
    {
        block.thinking_signature = Some(signature.to_owned());
    }
    if let Some(text) = text {
        if let AssistantBlock::Thinking(block) = &mut output.content[position] {
            block.thinking.push_str(text);
        }
        out.push(AssistantMessageEvent::ThinkingDelta {
            content_index: position as u64,
            delta: text.to_owned(),
            partial: output.clone(),
        });
    }
}

fn content_stop(
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
    slots: &[Slot],
    index: u64,
) {
    let Some(position) = position(slots, index) else {
        return;
    };
    let finished = match &mut output.content[position] {
        AssistantBlock::Text(block) => Finished::Text(block.text.clone()),
        AssistantBlock::Thinking(block) => Finished::Thinking(block.thinking.clone()),
        AssistantBlock::ToolCall(call) => {
            call.arguments = match parse_streaming_json(Some(&slots[position].partial_json)) {
                Value::Object(map) => map,
                _ => Map::new(),
            };
            Finished::Tool(call.clone())
        }
    };
    let content_index = position as u64;
    let partial = output.clone();
    out.push(match finished {
        Finished::Text(content) => AssistantMessageEvent::TextEnd {
            content_index,
            content,
            partial,
        },
        Finished::Thinking(content) => AssistantMessageEvent::ThinkingEnd {
            content_index,
            content,
            partial,
        },
        Finished::Tool(tool_call) => AssistantMessageEvent::ToolcallEnd {
            content_index,
            tool_call,
            partial,
        },
    });
}

#[derive(Clone, Copy)]
enum Block {
    Text,
    Thinking,
    Redacted,
}

fn ensure_block(
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
    slots: &mut Vec<Slot>,
    index: u64,
    block: Block,
) -> usize {
    if let Some(position) = position(slots, index) {
        return position;
    }
    let thinking = |redacted: bool| {
        AssistantBlock::Thinking(ThinkingContent {
            kind: ThinkingType,
            thinking: if redacted {
                REDACTED_TEXT.to_owned()
            } else {
                String::new()
            },
            thinking_signature: None,
            redacted: redacted.then_some(true),
        })
    };
    let (created, started) = match block {
        Block::Text => (
            AssistantBlock::Text(TextContent {
                kind: TextType,
                text: String::new(),
                text_signature: None,
            }),
            AssistantMessageEvent::TextStart {
                content_index: output.content.len() as u64,
                partial: output.clone(),
            },
        ),
        Block::Thinking | Block::Redacted => {
            let redacted = matches!(block, Block::Redacted);
            (
                thinking(redacted),
                AssistantMessageEvent::ThinkingStart {
                    content_index: output.content.len() as u64,
                    partial: output.clone(),
                },
            )
        }
    };
    output.content.push(created);
    slots.push(Slot {
        index,
        partial_json: String::new(),
    });
    out.push(started);
    output.content.len() - 1
}

fn position(slots: &[Slot], index: u64) -> Option<usize> {
    slots.iter().rposition(|slot| slot.index == index)
}

fn last(output: &AssistantMessage) -> u64 {
    output.content.len() as u64 - 1
}

fn set_count(value: &Value, slot: &mut u64) {
    if let Some(count) = value.as_u64() {
        *slot = count;
    }
}

fn map_stop_reason(reason: &str) -> (StopReason, Option<String>) {
    match reason {
        "end_turn" | "stop_sequence" => (StopReason::Stop, None),
        "max_tokens" | "model_context_window_exceeded" => (StopReason::Length, None),
        "tool_use" => (StopReason::ToolUse, None),
        "" => (StopReason::Error, None),
        other => (
            StopReason::Error,
            Some(format!("Provider stopped with: {other}")),
        ),
    }
}
