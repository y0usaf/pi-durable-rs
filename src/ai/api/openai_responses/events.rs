use std::collections::HashMap;
use std::io::BufRead;

use serde_json::{Value, json};

use super::request::encode_text_signature;
use crate::ai::cost::calculate_cost;
use crate::ai::event_stream::AssistantMessageEventStream;
use crate::ai::http::io_error;
use crate::ai::json_parse::parse_streaming_json;
use crate::ai::sse::SseReader;
use crate::ai::types::{
    AssistantBlock, AssistantMessage, AssistantMessageEvent, Cost, Model, StopReason, TextContent,
    TextType, ThinkingContent, ThinkingType, ToolCall, ToolCallType, Usage,
};
use crate::cancel::CancelToken;
use crate::chord::json::JsonObject;

enum Slot {
    Thinking(usize),
    Text(usize),
    ToolCall { index: usize, partial: String },
}

struct Process<'a> {
    model: &'a Model,
    out: &'a AssistantMessageEventStream,
    output: &'a mut AssistantMessage,
    slots: HashMap<u64, Slot>,
    reasoning: HashMap<String, usize>,
    terminal: bool,
}

pub fn process(
    reader: impl BufRead,
    model: &Model,
    signal: Option<&CancelToken>,
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
) -> Result<(), String> {
    let mut state = Process {
        model,
        out,
        output,
        slots: HashMap::new(),
        reasoning: HashMap::new(),
        terminal: false,
    };
    for event in SseReader::new(reader) {
        if signal.is_some_and(CancelToken::is_cancelled) {
            return Ok(());
        }
        let event = event.map_err(|error| io_error(&error, signal).to_string())?;
        if event.data == "[DONE]" {
            break;
        }
        if event.data.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&event.data)
            .map_err(|error| format!("invalid stream event: {error}"))?;
        state.handle(&value)?;
    }
    state.finish()
}

fn str_field(value: &Value) -> String {
    value.as_str().unwrap_or_default().to_owned()
}

fn parse_arguments(text: &str) -> JsonObject {
    match parse_streaming_json(Some(text)) {
        Value::Object(map) => map,
        _ => JsonObject::new(),
    }
}

fn usage_from(usage: &Value) -> Usage {
    let details = &usage["input_tokens_details"];
    let cached = details["cached_tokens"].as_u64().unwrap_or(0);
    let cache_write = details["cache_write_tokens"].as_u64().unwrap_or(0);
    let input = usage["input_tokens"].as_u64().unwrap_or(0);
    Usage {
        input: input.saturating_sub(cached + cache_write),
        output: usage["output_tokens"].as_u64().unwrap_or(0),
        cache_read: cached,
        cache_write,
        cache_write_1h: None,
        reasoning: Some(
            usage["output_tokens_details"]["reasoning_tokens"]
                .as_u64()
                .unwrap_or(0),
        ),
        total_tokens: usage["total_tokens"].as_u64().unwrap_or(0),
        cost: Cost {
            input: 0.0,
            output: 0.0,
            cache_read: 0.0,
            cache_write: 0.0,
            total: 0.0,
        },
    }
}

fn map_stop_reason(
    status: Option<&str>,
    reason: Option<&str>,
) -> Result<(StopReason, Option<String>), String> {
    match status {
        None | Some("completed" | "in_progress" | "queued") => Ok((StopReason::Stop, None)),
        Some("incomplete") if reason == Some("max_output_tokens") => Ok((StopReason::Length, None)),
        Some("incomplete") => Ok((
            StopReason::Error,
            Some(match reason {
                Some(reason) => format!("Response incomplete: {reason}"),
                None => "Response incomplete without a provider reason".to_owned(),
            }),
        )),
        Some("failed" | "cancelled") => Ok((StopReason::Error, None)),
        Some(other) => Err(format!("Unhandled stop reason: {other}")),
    }
}

impl Process<'_> {
    fn handle(&mut self, event: &Value) -> Result<(), String> {
        let output_index = event["output_index"].as_u64().unwrap_or(u64::MAX);
        let delta = event["delta"].as_str().unwrap_or_default();
        match event["type"].as_str().unwrap_or_default() {
            "response.created" => {
                if let Some(id) = event["response"]["id"].as_str() {
                    self.output.response_id = Some(id.to_owned());
                }
            }
            "response.output_item.added" => self.create_slot(output_index, &event["item"]),
            "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
                if let Some(index) = self.thinking_slot(output_index) {
                    self.push_thinking(index, delta);
                }
            }
            "response.reasoning_summary_part.done" => {
                if let Some(index) = self.thinking_slot(output_index) {
                    self.push_thinking(index, "\n\n");
                }
            }
            "response.output_text.delta" | "response.refusal.delta" => {
                if let Some(index) = self.text_slot(output_index) {
                    self.push_text(index, delta);
                }
            }
            "response.function_call_arguments.delta" => {
                self.arguments_delta(output_index, delta);
            }
            "response.function_call_arguments.done" => {
                self.arguments_done(
                    output_index,
                    event["arguments"].as_str().unwrap_or_default(),
                );
            }
            "response.output_item.done" => self.item_done(output_index, &event["item"]),
            "response.completed" | "response.incomplete" => self.finalize(&event["response"])?,
            "error" if self.model.api == "openai-codex-responses" => {
                return Err(super::codex::stream_error(event));
            }
            "response.failed" if self.model.api == "openai-codex-responses" => {
                return Err(super::codex::failed_message(&event["response"]));
            }
            "error" => {
                return Err(format!(
                    "Error Code {}: {}",
                    str_field(&event["code"]),
                    str_field(&event["message"])
                ));
            }
            "response.failed" => return Err(self.failed(&event["response"])),
            _ => {}
        }
        Ok(())
    }

    fn finish(self) -> Result<(), String> {
        if !self.terminal {
            return Err(
                "OpenAI Responses stream ended before a terminal response event".to_owned(),
            );
        }
        if self.output.stop_reason == StopReason::ToolUse {
            let unfinished = self
                .slots
                .values()
                .filter_map(|slot| match slot {
                    Slot::ToolCall { index, .. } => Some(*index),
                    _ => None,
                })
                .min();
            if let Some(AssistantBlock::ToolCall(call)) =
                unfinished.and_then(|index| self.output.content.get(index))
            {
                return Err(format!(
                    "OpenAI Responses stream completed with an unfinished tool call: {} ({})",
                    call.name, call.id
                ));
            }
        }
        Ok(())
    }

    fn push_block(&mut self, block: AssistantBlock) -> usize {
        self.output.content.push(block);
        self.output.content.len() - 1
    }

    fn thinking_slot(&self, output_index: u64) -> Option<usize> {
        match self.slots.get(&output_index) {
            Some(Slot::Thinking(index)) => Some(*index),
            _ => None,
        }
    }

    fn text_slot(&self, output_index: u64) -> Option<usize> {
        match self.slots.get(&output_index) {
            Some(Slot::Text(index)) => Some(*index),
            _ => None,
        }
    }

    fn apply_phase(&mut self, item: &Value) {
        if item["type"] == "message" && item["phase"] == "final_answer" {
            self.output.stop_reason = StopReason::Stop;
        }
    }

    fn create_slot(&mut self, output_index: u64, item: &Value) {
        self.apply_phase(item);
        match item["type"].as_str() {
            Some("reasoning") => {
                let index = self.push_block(AssistantBlock::Thinking(ThinkingContent {
                    kind: ThinkingType,
                    thinking: String::new(),
                    thinking_signature: None,
                    redacted: None,
                }));
                self.slots.insert(output_index, Slot::Thinking(index));
                self.out.push(AssistantMessageEvent::ThinkingStart {
                    content_index: index as u64,
                    partial: self.output.clone(),
                });
            }
            Some("message") => {
                let index = self.push_block(AssistantBlock::Text(TextContent {
                    kind: TextType,
                    text: String::new(),
                    text_signature: None,
                }));
                self.slots.insert(output_index, Slot::Text(index));
                self.out.push(AssistantMessageEvent::TextStart {
                    content_index: index as u64,
                    partial: self.output.clone(),
                });
            }
            Some("function_call") => {
                let namespace = item["namespace"].as_str().map(str::to_owned);
                let index = self.push_block(AssistantBlock::ToolCall(ToolCall {
                    kind: ToolCallType,
                    id: format!("{}|{}", str_field(&item["call_id"]), str_field(&item["id"])),
                    name: str_field(&item["name"]),
                    arguments: JsonObject::new(),
                    thought_signature: None,
                    namespace,
                }));
                let partial = str_field(&item["arguments"]);
                self.slots
                    .insert(output_index, Slot::ToolCall { index, partial });
                self.out.push(AssistantMessageEvent::ToolcallStart {
                    content_index: index as u64,
                    partial: self.output.clone(),
                });
            }
            _ => {}
        }
    }

    fn push_thinking(&mut self, index: usize, delta: &str) {
        if let Some(AssistantBlock::Thinking(block)) = self.output.content.get_mut(index) {
            block.thinking.push_str(delta);
        }
        self.out.push(AssistantMessageEvent::ThinkingDelta {
            content_index: index as u64,
            delta: delta.to_owned(),
            partial: self.output.clone(),
        });
    }

    fn push_text(&mut self, index: usize, delta: &str) {
        if let Some(AssistantBlock::Text(block)) = self.output.content.get_mut(index) {
            block.text.push_str(delta);
        }
        self.out.push(AssistantMessageEvent::TextDelta {
            content_index: index as u64,
            delta: delta.to_owned(),
            partial: self.output.clone(),
        });
    }

    fn set_arguments(&mut self, index: usize, arguments: JsonObject) {
        if let Some(AssistantBlock::ToolCall(block)) = self.output.content.get_mut(index) {
            block.arguments = arguments;
        }
    }

    fn arguments_delta(&mut self, output_index: u64, delta: &str) {
        let Some(Slot::ToolCall { index, partial }) = self.slots.get_mut(&output_index) else {
            return;
        };
        partial.push_str(delta);
        let index = *index;
        let arguments = parse_arguments(partial);
        self.set_arguments(index, arguments);
        self.out.push(AssistantMessageEvent::ToolcallDelta {
            content_index: index as u64,
            delta: delta.to_owned(),
            partial: self.output.clone(),
        });
    }

    fn arguments_done(&mut self, output_index: u64, arguments: &str) {
        let Some(Slot::ToolCall { index, partial }) = self.slots.get_mut(&output_index) else {
            return;
        };
        let previous = std::mem::replace(partial, arguments.to_owned());
        let index = *index;
        let parsed = parse_arguments(arguments);
        self.set_arguments(index, parsed);
        if let Some(rest) = arguments.strip_prefix(previous.as_str())
            && !rest.is_empty()
        {
            self.out.push(AssistantMessageEvent::ToolcallDelta {
                content_index: index as u64,
                delta: rest.to_owned(),
                partial: self.output.clone(),
            });
        }
    }

    fn item_done(&mut self, output_index: u64, item: &Value) {
        self.apply_phase(item);
        if !self.slots.contains_key(&output_index) {
            self.create_slot(output_index, item);
        }
        match item["type"].as_str() {
            Some("reasoning") => {
                if let Some(index) = self.thinking_slot(output_index) {
                    self.finish_reasoning(output_index, index, item);
                }
            }
            Some("message") => {
                if let Some(index) = self.text_slot(output_index) {
                    self.finish_text(output_index, index, item);
                }
            }
            Some("function_call") => {
                if let Some(Slot::ToolCall { index, partial }) = self.slots.get(&output_index) {
                    let (index, partial) = (*index, partial.clone());
                    self.finish_call(output_index, index, item, partial);
                }
            }
            _ => {}
        }
    }

    fn finish_reasoning(&mut self, output_index: u64, index: usize, item: &Value) {
        let joined = |parts: &Value, field: &str| {
            parts
                .as_array()
                .map(|parts| {
                    parts
                        .iter()
                        .map(|part| part[field].as_str().unwrap_or_default())
                        .collect::<Vec<_>>()
                        .join("\n\n")
                })
                .unwrap_or_default()
        };
        let summary = joined(&item["summary"], "text");
        let content = joined(&item["content"], "text");
        let signature = item.to_string();
        let thinking = {
            let Some(AssistantBlock::Thinking(block)) = self.output.content.get_mut(index) else {
                return;
            };
            block.thinking = [summary, content, block.thinking.clone()]
                .into_iter()
                .find(|text| !text.is_empty())
                .unwrap_or_default();
            block.thinking_signature = Some(signature);
            block.thinking.clone()
        };
        self.reasoning.insert(str_field(&item["id"]), index);
        self.out.push(AssistantMessageEvent::ThinkingEnd {
            content_index: index as u64,
            content: thinking,
            partial: self.output.clone(),
        });
        self.slots.remove(&output_index);
    }

    fn finish_text(&mut self, output_index: u64, index: usize, item: &Value) {
        let text: String = item["content"]
            .as_array()
            .map(|parts| {
                parts
                    .iter()
                    .map(|part| {
                        let field = if part["type"] == "output_text" {
                            "text"
                        } else {
                            "refusal"
                        };
                        part[field].as_str().unwrap_or_default()
                    })
                    .collect()
            })
            .unwrap_or_default();
        let phase = item["phase"].as_str();
        if let Some(AssistantBlock::Text(block)) = self.output.content.get_mut(index) {
            block.text = text.clone();
            block.text_signature = Some(encode_text_signature(&str_field(&item["id"]), phase));
        }
        self.out.push(AssistantMessageEvent::TextEnd {
            content_index: index as u64,
            content: text,
            partial: self.output.clone(),
        });
        self.slots.remove(&output_index);
    }

    fn finish_call(&mut self, output_index: u64, index: usize, item: &Value, partial: String) {
        let raw = match item["arguments"].as_str() {
            Some(arguments) if !arguments.is_empty() => arguments.to_owned(),
            _ if !partial.is_empty() => partial,
            _ => "{}".to_owned(),
        };
        let arguments = parse_arguments(&raw);
        let namespace = item["namespace"].as_str().map(str::to_owned);
        let call = match self.output.content.get_mut(index) {
            Some(AssistantBlock::ToolCall(block)) => {
                block.arguments = arguments;
                if namespace.is_some() {
                    block.namespace = namespace;
                }
                block.clone()
            }
            _ => return,
        };
        self.out.push(AssistantMessageEvent::ToolcallEnd {
            content_index: index as u64,
            tool_call: call,
            partial: self.output.clone(),
        });
        self.slots.remove(&output_index);
    }

    fn backfill_signatures(&mut self, response_output: &[Value]) -> Result<(), String> {
        for item in response_output {
            let Some(encrypted) = item["encrypted_content"]
                .as_str()
                .filter(|text| !text.is_empty())
            else {
                continue;
            };
            if item["type"] != "reasoning" {
                continue;
            }
            let Some(&index) = self.reasoning.get(&str_field(&item["id"])) else {
                continue;
            };
            let Some(AssistantBlock::Thinking(block)) = self.output.content.get_mut(index) else {
                continue;
            };
            let Some(signature) = block.thinking_signature.clone() else {
                continue;
            };
            let mut stored: Value = serde_json::from_str(&signature)
                .map_err(|error| format!("invalid reasoning signature: {error}"))?;
            if stored["encrypted_content"]
                .as_str()
                .is_some_and(|text| !text.is_empty())
            {
                continue;
            }
            stored["encrypted_content"] = json!(encrypted);
            block.thinking_signature = Some(stored.to_string());
        }
        Ok(())
    }

    fn finalize(&mut self, response: &Value) -> Result<(), String> {
        self.terminal = true;
        self.backfill_signatures(response["output"].as_array().map_or(&[], Vec::as_slice))?;
        if let Some(id) = response["id"].as_str() {
            self.output.response_id = Some(id.to_owned());
        }
        if !response["usage"].is_null() {
            self.output.usage = usage_from(&response["usage"]);
        }
        calculate_cost(self.model, &mut self.output.usage);
        let status = response["status"].as_str();
        let reason = response["incomplete_details"]["reason"].as_str();
        self.output.raw_stop_reason = match reason {
            Some(reason) => Some(format!("{}.{reason}", status.unwrap_or("undefined"))),
            None => status.map(str::to_owned),
        };
        let (stop, error) = map_stop_reason(status, reason)?;
        self.output.stop_reason = stop;
        self.output.error_message = error;
        let has_calls = self
            .output
            .content
            .iter()
            .any(|block| matches!(block, AssistantBlock::ToolCall(_)));
        if has_calls && self.output.stop_reason == StopReason::Stop {
            self.output.stop_reason = StopReason::ToolUse;
        }
        Ok(())
    }

    fn failed(&mut self, response: &Value) -> String {
        self.terminal = true;
        self.output.raw_stop_reason = response["status"].as_str().map(str::to_owned);
        let error = &response["error"];
        let reason = response["incomplete_details"]["reason"]
            .as_str()
            .filter(|reason| !reason.is_empty());
        if !error.is_null() {
            let code = error["code"]
                .as_str()
                .filter(|code| !code.is_empty())
                .unwrap_or("unknown");
            let message = error["message"]
                .as_str()
                .filter(|message| !message.is_empty())
                .unwrap_or("no message");
            format!("{code}: {message}")
        } else if let Some(reason) = reason {
            format!("incomplete: {reason}")
        } else {
            "Unknown error (no error details in response)".to_owned()
        }
    }
}
