//! Server-sent event handling for the Anthropic Messages API: folds each event into the partial
//! assistant message and emits the matching stream events, as pi-ai's Anthropic adapter does.

use std::io::BufRead;

use serde_json::{Map, Value};

use super::request::from_claude_code_name;
use crate::ai::cost::calculate_cost;
use crate::ai::event_stream::{ABORTED_TEXT, AssistantMessageEventStream};
use crate::ai::json_parse::parse_streaming_json;
use crate::ai::sse::SseReader;
use crate::ai::types::{
    AssistantBlock, AssistantMessage, AssistantMessageEvent, Model, ModelCost, StopReason,
    TextContent, TextType, ThinkingContent, ThinkingType, Tool, ToolCall, ToolCallType, Usage,
};
use crate::cancel::CancelToken;

struct Scratch {
    index: Option<u64>,
    partial_json: String,
}

pub fn read(
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
    body: impl BufRead,
    model: &Model,
    oauth: bool,
    tools: &[Tool],
    signal: Option<&CancelToken>,
) -> Result<(), String> {
    let mut priced = model.clone();
    let mut scratch: Vec<Scratch> = Vec::new();
    let mut saw_start = false;
    let mut saw_end = false;
    for event in SseReader::new(body) {
        if signal.is_some_and(CancelToken::is_cancelled) {
            return Err(ABORTED_TEXT.to_owned());
        }
        let event = event.map_err(|error| io_failure(&error, signal))?;
        let name = event.event.as_deref().unwrap_or("");
        if name == "error" {
            return Err(event.data);
        }
        if !matches!(
            name,
            "message_start"
                | "message_delta"
                | "message_stop"
                | "content_block_start"
                | "content_block_delta"
                | "content_block_stop"
                | "ping"
        ) {
            continue;
        }
        let data: Value = serde_json::from_str(&event.data).map_err(|error| {
            format!(
                "Could not parse Anthropic SSE event {name}: {error}; data={}",
                event.data
            )
        })?;
        match name {
            "message_start" => {
                saw_start = true;
                let message = &data["message"];
                output.response_id = message["id"].as_str().map(str::to_owned);
                let response_model = message["model"].as_str().unwrap_or_default();
                if response_model != model.id {
                    output.response_model = Some(response_model.to_owned());
                    priced = fallback_pricing(model, response_model);
                }
                let usage = &message["usage"];
                output.usage.input = count(&usage["input_tokens"]);
                output.usage.output = count(&usage["output_tokens"]);
                output.usage.cache_read = count(&usage["cache_read_input_tokens"]);
                output.usage.cache_write = count(&usage["cache_creation_input_tokens"]);
                output.usage.cache_write_1h =
                    Some(count(&usage["cache_creation"]["ephemeral_1h_input_tokens"]));
                finish_usage(&mut output.usage, &priced);
            }
            "content_block_start" => {
                let block = &data["content_block"];
                let index = data["index"].as_u64();
                match block["type"].as_str() {
                    Some("fallback") => {
                        if !output.content.is_empty() {
                            return Err(
                                "Anthropic performed an unsupported mid-output model fallback"
                                    .to_owned(),
                            );
                        }
                        continue;
                    }
                    Some("text") => {
                        output.content.push(AssistantBlock::Text(TextContent {
                            kind: TextType,
                            text: block["text"].as_str().unwrap_or_default().to_owned(),
                            text_signature: None,
                        }));
                        scratch.push(Scratch {
                            index,
                            partial_json: String::new(),
                        });
                        out.push(AssistantMessageEvent::TextStart {
                            content_index: last(output),
                            partial: output.clone(),
                        });
                    }
                    Some("thinking") => {
                        output
                            .content
                            .push(AssistantBlock::Thinking(ThinkingContent {
                                kind: ThinkingType,
                                thinking: block["thinking"].as_str().unwrap_or_default().to_owned(),
                                thinking_signature: Some(
                                    block["signature"].as_str().unwrap_or_default().to_owned(),
                                ),
                                redacted: None,
                            }));
                        scratch.push(Scratch {
                            index,
                            partial_json: String::new(),
                        });
                        out.push(AssistantMessageEvent::ThinkingStart {
                            content_index: last(output),
                            partial: output.clone(),
                        });
                    }
                    Some("redacted_thinking") => {
                        output
                            .content
                            .push(AssistantBlock::Thinking(ThinkingContent {
                                kind: ThinkingType,
                                thinking: "[Reasoning redacted]".to_owned(),
                                thinking_signature: Some(
                                    block["data"].as_str().unwrap_or_default().to_owned(),
                                ),
                                redacted: Some(true),
                            }));
                        scratch.push(Scratch {
                            index,
                            partial_json: String::new(),
                        });
                        out.push(AssistantMessageEvent::ThinkingStart {
                            content_index: last(output),
                            partial: output.clone(),
                        });
                    }
                    Some("tool_use") => {
                        let name = block["name"].as_str().unwrap_or_default();
                        let arguments = match block["input"].clone() {
                            Value::Object(map) => map,
                            _ => Map::new(),
                        };
                        output.content.push(AssistantBlock::ToolCall(ToolCall {
                            kind: ToolCallType,
                            id: block["id"].as_str().unwrap_or_default().to_owned(),
                            name: if oauth {
                                from_claude_code_name(name, tools)
                            } else {
                                name.to_owned()
                            },
                            arguments,
                            thought_signature: None,
                            namespace: None,
                        }));
                        scratch.push(Scratch {
                            index,
                            partial_json: String::new(),
                        });
                        out.push(AssistantMessageEvent::ToolcallStart {
                            content_index: last(output),
                            partial: output.clone(),
                        });
                    }
                    _ => {}
                }
            }
            "content_block_delta" => {
                let Some(position) = position(&scratch, data["index"].as_u64()) else {
                    continue;
                };
                let delta = &data["delta"];
                match (delta["type"].as_str(), &mut output.content[position]) {
                    (Some("text_delta"), AssistantBlock::Text(text)) => {
                        let piece = delta["text"].as_str().unwrap_or_default().to_owned();
                        text.text.push_str(&piece);
                        out.push(AssistantMessageEvent::TextDelta {
                            content_index: position as u64,
                            delta: piece,
                            partial: output.clone(),
                        });
                    }
                    (Some("thinking_delta"), AssistantBlock::Thinking(thinking)) => {
                        let piece = delta["thinking"].as_str().unwrap_or_default().to_owned();
                        thinking.thinking.push_str(&piece);
                        out.push(AssistantMessageEvent::ThinkingDelta {
                            content_index: position as u64,
                            delta: piece,
                            partial: output.clone(),
                        });
                    }
                    (Some("signature_delta"), AssistantBlock::Thinking(thinking)) => {
                        thinking
                            .thinking_signature
                            .get_or_insert_with(String::new)
                            .push_str(delta["signature"].as_str().unwrap_or_default());
                    }
                    (Some("input_json_delta"), AssistantBlock::ToolCall(call)) => {
                        let piece = delta["partial_json"]
                            .as_str()
                            .unwrap_or_default()
                            .to_owned();
                        scratch[position].partial_json.push_str(&piece);
                        call.arguments =
                            object(parse_streaming_json(Some(&scratch[position].partial_json)));
                        out.push(AssistantMessageEvent::ToolcallDelta {
                            content_index: position as u64,
                            delta: piece,
                            partial: output.clone(),
                        });
                    }
                    _ => {}
                }
            }
            "content_block_stop" => {
                let Some(position) = position(&scratch, data["index"].as_u64()) else {
                    continue;
                };
                scratch[position].index = None;
                let content_index = position as u64;
                match &mut output.content[position] {
                    AssistantBlock::Text(text) => {
                        let content = text.text.clone();
                        out.push(AssistantMessageEvent::TextEnd {
                            content_index,
                            content,
                            partial: output.clone(),
                        });
                    }
                    AssistantBlock::Thinking(thinking) => {
                        let content = thinking.thinking.clone();
                        out.push(AssistantMessageEvent::ThinkingEnd {
                            content_index,
                            content,
                            partial: output.clone(),
                        });
                    }
                    AssistantBlock::ToolCall(call) => {
                        call.arguments =
                            object(parse_streaming_json(Some(&scratch[position].partial_json)));
                        let tool_call = call.clone();
                        out.push(AssistantMessageEvent::ToolcallEnd {
                            content_index,
                            tool_call,
                            partial: output.clone(),
                        });
                    }
                }
            }
            "message_delta" => {
                if let Some(reason) = data["delta"]["stop_reason"].as_str() {
                    output.raw_stop_reason = Some(reason.to_owned());
                    let (stop, message) = map_stop_reason(reason, &data["delta"]["stop_details"])?;
                    output.stop_reason = stop;
                    if let Some(message) = message {
                        output.error_message = Some(message);
                    }
                }
                let usage = &data["usage"];
                if usage.is_object() {
                    set_if(&usage["input_tokens"], &mut output.usage.input);
                    set_if(&usage["output_tokens"], &mut output.usage.output);
                    set_if(
                        &usage["cache_read_input_tokens"],
                        &mut output.usage.cache_read,
                    );
                    set_if(
                        &usage["cache_creation_input_tokens"],
                        &mut output.usage.cache_write,
                    );
                    if let Some(tokens) =
                        usage["cache_creation"]["ephemeral_1h_input_tokens"].as_u64()
                    {
                        output.usage.cache_write_1h = Some(tokens);
                    }
                    if let Some(tokens) = usage["output_tokens_details"]["thinking_tokens"].as_u64()
                    {
                        output.usage.reasoning = Some(tokens);
                    }
                }
                finish_usage(&mut output.usage, &priced);
            }
            "message_stop" => saw_end = true,
            _ => {}
        }
    }
    if signal.is_some_and(CancelToken::is_cancelled) {
        return Err(ABORTED_TEXT.to_owned());
    }
    if saw_start && !saw_end {
        return Err("Anthropic stream ended before message_stop".to_owned());
    }
    match output.stop_reason {
        StopReason::Pending => Err("Anthropic stream ended without a stop reason".to_owned()),
        StopReason::Aborted | StopReason::Error => Err(output
            .error_message
            .clone()
            .unwrap_or_else(|| "An unknown error occurred".to_owned())),
        _ => Ok(()),
    }
}

fn io_failure(error: &std::io::Error, signal: Option<&CancelToken>) -> String {
    if signal.is_some_and(CancelToken::is_cancelled) {
        ABORTED_TEXT.to_owned()
    } else {
        error.to_string()
    }
}

fn last(output: &AssistantMessage) -> u64 {
    output.content.len() as u64 - 1
}

fn position(scratch: &[Scratch], index: Option<u64>) -> Option<usize> {
    let index = index?;
    scratch.iter().position(|slot| slot.index == Some(index))
}

fn count(value: &Value) -> u64 {
    value.as_u64().unwrap_or(0)
}

fn set_if(value: &Value, slot: &mut u64) {
    if let Some(tokens) = value.as_u64() {
        *slot = tokens;
    }
}

fn object(value: crate::chord::json::JsonValue) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        _ => Map::new(),
    }
}

fn finish_usage(usage: &mut Usage, model: &Model) {
    usage.total_tokens = usage.input + usage.output + usage.cache_read + usage.cache_write;
    calculate_cost(model, usage);
}

fn fallback_pricing(model: &Model, response_model: &str) -> Model {
    let mut priced = model.clone();
    let fallback = model
        .compat
        .as_ref()
        .and_then(|compat| compat.get("allowedFallbackModels"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|fallback| {
            fallback["provider"].as_str() == Some(model.provider.as_str())
                && fallback["model"].as_str() == Some(response_model)
        });
    if let Some(cost) = fallback
        .and_then(|fallback| serde_json::from_value::<ModelCost>(fallback["cost"].clone()).ok())
    {
        priced.id = response_model.to_owned();
        priced.cost = Some(cost);
    }
    priced
}

fn map_stop_reason(reason: &str, details: &Value) -> Result<(StopReason, Option<String>), String> {
    Ok(match reason {
        "end_turn" | "pause_turn" | "stop_sequence" => (StopReason::Stop, None),
        "max_tokens" => (StopReason::Length, None),
        "tool_use" => (StopReason::ToolUse, None),
        "refusal" => (
            StopReason::Error,
            Some(
                details["explanation"]
                    .as_str()
                    .filter(|text| !text.is_empty())
                    .unwrap_or("The model refused to complete the request")
                    .to_owned(),
            ),
        ),
        "sensitive" => (
            StopReason::Error,
            Some("Provider stopped with: sensitive".to_owned()),
        ),
        other => return Err(format!("Unhandled stop reason: {other}")),
    })
}
