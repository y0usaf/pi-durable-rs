use std::collections::HashMap;

use serde_json::Value;

use crate::ai::event_stream::{AssistantMessageEventStream, now_ms};
use crate::ai::json_parse::parse_streaming_json;
use crate::ai::types::{
    AssistantBlock, AssistantMessage, AssistantMessageDiagnostic, AssistantMessageEvent,
    StopReason, TextContent, TextType, ThinkingContent, ThinkingType, ToolCall, ToolCallType,
    Usage,
};
use crate::chord::json::JsonObject;

#[derive(Default)]
pub struct Converter {
    tool_json: HashMap<usize, String>,
}

impl Converter {
    pub fn apply(
        &mut self,
        out: &AssistantMessageEventStream,
        output: &mut AssistantMessage,
        event: &Value,
    ) -> Result<bool, String> {
        let kind = event["type"].as_str().unwrap_or_default();
        match kind {
            "start" => {
                out.push(AssistantMessageEvent::Start {
                    partial: output.clone(),
                });
                Ok(false)
            }
            "done" => {
                finish(output, event)?;
                Ok(true)
            }
            "error" => {
                finish(output, event)?;
                let message = event["errorMessage"]
                    .as_str()
                    .filter(|message| !message.is_empty())
                    .unwrap_or("An unknown error occurred");
                output.error_message = Some(message.to_owned());
                Err(message.to_owned())
            }
            "text_start" | "thinking_start" | "toolcall_start" => {
                self.start_block(out, output, kind, event)?;
                Ok(false)
            }
            "text_delta" | "thinking_delta" | "toolcall_delta" => {
                self.delta_block(out, output, kind, event)?;
                Ok(false)
            }
            "text_end" | "thinking_end" | "toolcall_end" => {
                self.end_block(out, output, kind, event)?;
                Ok(false)
            }
            other => Err(format!("unknown pi-messages event type {other:?}")),
        }
    }

    fn start_block(
        &mut self,
        out: &AssistantMessageEventStream,
        output: &mut AssistantMessage,
        kind: &str,
        event: &Value,
    ) -> Result<(), String> {
        let index = content_index(output, kind, event, true)?;
        let block = match kind {
            "text_start" => AssistantBlock::Text(TextContent {
                kind: TextType,
                text: String::new(),
                text_signature: None,
            }),
            "thinking_start" => AssistantBlock::Thinking(ThinkingContent {
                kind: ThinkingType,
                thinking: String::new(),
                thinking_signature: None,
                redacted: None,
            }),
            _ => {
                self.tool_json.insert(index, String::new());
                AssistantBlock::ToolCall(ToolCall {
                    kind: ToolCallType,
                    id: event["id"].as_str().unwrap_or_default().to_owned(),
                    name: event["toolName"].as_str().unwrap_or_default().to_owned(),
                    arguments: JsonObject::new(),
                    thought_signature: None,
                    namespace: None,
                })
            }
        };
        if index == output.content.len() {
            output.content.push(block);
        } else {
            output.content[index] = block;
        }
        let content_index = index as u64;
        let partial = output.clone();
        out.push(match kind {
            "text_start" => AssistantMessageEvent::TextStart {
                content_index,
                partial,
            },
            "thinking_start" => AssistantMessageEvent::ThinkingStart {
                content_index,
                partial,
            },
            _ => AssistantMessageEvent::ToolcallStart {
                content_index,
                partial,
            },
        });
        Ok(())
    }

    fn delta_block(
        &mut self,
        out: &AssistantMessageEventStream,
        output: &mut AssistantMessage,
        kind: &str,
        event: &Value,
    ) -> Result<(), String> {
        let index = content_index(output, kind, event, false)?;
        let delta = event["delta"].as_str().unwrap_or_default().to_owned();
        let content_index = index as u64;
        let partial_kind = match &mut output.content[index] {
            AssistantBlock::Text(block) => {
                block.text.push_str(&delta);
                AssistantMessageEvent::TextDelta {
                    content_index,
                    delta,
                    partial: output.clone(),
                }
            }
            AssistantBlock::Thinking(block) => {
                block.thinking.push_str(&delta);
                AssistantMessageEvent::ThinkingDelta {
                    content_index,
                    delta,
                    partial: output.clone(),
                }
            }
            AssistantBlock::ToolCall(block) => {
                let json = self.tool_json.entry(index).or_default();
                json.push_str(&delta);
                block.arguments = match parse_streaming_json(Some(json.as_str())) {
                    Value::Object(map) => map,
                    _ => JsonObject::new(),
                };
                AssistantMessageEvent::ToolcallDelta {
                    content_index,
                    delta,
                    partial: output.clone(),
                }
            }
        };
        out.push(partial_kind);
        Ok(())
    }

    fn end_block(
        &mut self,
        out: &AssistantMessageEventStream,
        output: &mut AssistantMessage,
        kind: &str,
        event: &Value,
    ) -> Result<(), String> {
        let index = content_index(output, kind, event, false)?;
        let content_index = index as u64;
        let signature = event["contentSignature"].as_str().map(str::to_owned);
        let text = event["content"].as_str().unwrap_or_default().to_owned();
        let finished = match &output.content[index] {
            AssistantBlock::Text(_) => {
                output.content[index] = AssistantBlock::Text(TextContent {
                    kind: TextType,
                    text: text.clone(),
                    text_signature: signature,
                });
                AssistantMessageEvent::TextEnd {
                    content_index,
                    content: text,
                    partial: output.clone(),
                }
            }
            AssistantBlock::Thinking(_) => {
                output.content[index] = AssistantBlock::Thinking(ThinkingContent {
                    kind: ThinkingType,
                    thinking: text.clone(),
                    thinking_signature: signature,
                    redacted: event["redacted"].as_bool(),
                });
                AssistantMessageEvent::ThinkingEnd {
                    content_index,
                    content: text,
                    partial: output.clone(),
                }
            }
            AssistantBlock::ToolCall(existing) => {
                let mut call = existing.clone();
                if let Some(incoming) = event.get("toolCall").filter(|call| call.is_object()) {
                    merge_tool_call(&mut call, incoming);
                }
                self.tool_json.remove(&index);
                output.content[index] = AssistantBlock::ToolCall(call.clone());
                AssistantMessageEvent::ToolcallEnd {
                    content_index,
                    tool_call: call,
                    partial: output.clone(),
                }
            }
        };
        out.push(finished);
        Ok(())
    }
}

fn content_index(
    output: &AssistantMessage,
    kind: &str,
    event: &Value,
    allow_append: bool,
) -> Result<usize, String> {
    let raw = event["contentIndex"].as_i64().unwrap_or(0);
    let len = output.content.len();
    usize::try_from(raw)
        .ok()
        .filter(|index| {
            if allow_append {
                *index <= len
            } else {
                *index < len
            }
        })
        .ok_or_else(|| format!("pi-messages {kind} has invalid contentIndex {raw}"))
}

fn merge_tool_call(call: &mut ToolCall, incoming: &Value) {
    if let Some(id) = incoming["id"].as_str().filter(|id| !id.is_empty()) {
        call.id = id.to_owned();
    }
    if let Some(name) = incoming["name"].as_str().filter(|name| !name.is_empty()) {
        call.name = name.to_owned();
    }
    if let Some(arguments) = incoming["arguments"].as_object() {
        call.arguments = arguments.clone();
    }
    if let Some(signature) = incoming["thoughtSignature"]
        .as_str()
        .filter(|value| !value.is_empty())
    {
        call.thought_signature = Some(signature.to_owned());
    }
    if let Some(namespace) = incoming["namespace"]
        .as_str()
        .filter(|value| !value.is_empty())
    {
        call.namespace = Some(namespace.to_owned());
    }
}

fn finish(output: &mut AssistantMessage, event: &Value) -> Result<(), String> {
    output.stop_reason = serde_json::from_value::<StopReason>(event["reason"].clone())
        .map_err(|error| format!("invalid pi-messages stop reason: {error}"))?;
    if let Some(usage) = event.get("usage").filter(|usage| !usage.is_null()) {
        output.usage = serde_json::from_value::<Usage>(usage.clone())
            .map_err(|error| format!("invalid pi-messages usage: {error}"))?;
    }
    if let Some(id) = event["responseId"].as_str() {
        output.response_id = Some(id.to_owned());
    }
    if let Some(level) = event["providerThinkingLevel"].as_str() {
        output.provider_thinking_level = Some(level.to_owned());
    }
    if let Some(rewrite) = event["rewrite"].as_object() {
        output
            .diagnostics
            .get_or_insert_with(Vec::new)
            .push(AssistantMessageDiagnostic {
                kind: "pi_messages_rewrite".to_owned(),
                timestamp: now_ms(),
                error: None,
                details: Some(rewrite.clone()),
            });
    }
    Ok(())
}
