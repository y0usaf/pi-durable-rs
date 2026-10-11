use std::collections::HashMap;
use std::io::BufRead;

use serde_json::Value;

use super::derive_tool_call_id;
use crate::ai::cost::calculate_cost;
use crate::ai::event_stream::{ABORTED_TEXT, AssistantMessageEventStream};
use crate::ai::http::io_error;
use crate::ai::json_parse::parse_streaming_json;
use crate::ai::sse::SseReader;
use crate::ai::types::{
    AssistantBlock, AssistantMessage, AssistantMessageEvent, Model, StopReason, TextContent,
    TextType, ThinkingContent, ThinkingType, ToolCall, ToolCallType,
};
use crate::cancel::CancelToken;
use crate::chord::json::JsonObject;

#[derive(Clone, PartialEq, Eq, Hash)]
enum ToolKey {
    Index(u64),
    Id(String),
}

#[derive(Clone, Copy)]
enum Open {
    Text(usize),
    Thinking(usize),
}

#[derive(Default)]
struct Blocks {
    open: Option<Open>,
    tools: HashMap<ToolKey, usize>,
    tool_order: Vec<usize>,
    tool_json: HashMap<usize, String>,
}

pub fn consume(
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
    model: &Model,
    cancel: Option<&CancelToken>,
    reader: impl BufRead,
) -> Result<(), String> {
    let cancelled = || cancel.is_some_and(CancelToken::is_cancelled);
    out.push(AssistantMessageEvent::Start {
        partial: output.clone(),
    });
    let mut blocks = Blocks::default();
    for event in SseReader::new(reader) {
        if cancelled() {
            return Err(ABORTED_TEXT.to_owned());
        }
        let event = event.map_err(|error| io_error(&error, cancel).to_string())?;
        let data = event.data.trim();
        if data.is_empty() {
            continue;
        }
        if data == "[DONE]" {
            break;
        }
        let chunk: Value =
            serde_json::from_str(data).map_err(|error| format!("invalid stream chunk: {error}"))?;
        blocks.chunk(out, output, model, &chunk)?;
    }
    if cancelled() {
        return Err("Request was aborted".to_owned());
    }
    blocks.finish(out, output);
    match output.stop_reason {
        StopReason::Pending => Err("Mistral stream ended without a finish reason".to_owned()),
        StopReason::Aborted | StopReason::Error => Err(output
            .error_message
            .clone()
            .unwrap_or_else(|| "An unknown error occurred".to_owned())),
        _ => Ok(()),
    }
}

impl Blocks {
    fn chunk(
        &mut self,
        out: &AssistantMessageEventStream,
        output: &mut AssistantMessage,
        model: &Model,
        chunk: &Value,
    ) -> Result<(), String> {
        if output.response_id.is_none()
            && let Some(id) = chunk
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
        {
            output.response_id = Some(id.to_owned());
        }
        if let Some(usage) = chunk.get("usage").filter(|usage| !usage.is_null()) {
            apply_usage(output, model, usage);
        }
        let Some(choice) = chunk
            .get("choices")
            .and_then(|choices| choices.get(0))
            .filter(|choice| truthy(choice))
        else {
            return Ok(());
        };
        if let Some(reason) = choice
            .get("finish_reason")
            .and_then(Value::as_str)
            .filter(|reason| !reason.is_empty())
        {
            output.raw_stop_reason = Some(reason.to_owned());
            let (stop, message) = map_stop_reason(reason);
            output.stop_reason = stop;
            if let Some(message) = message {
                output.error_message = Some(message);
            }
        }
        let delta = match choice.get("delta") {
            None => {
                return Err("Cannot read properties of undefined (reading 'content')".to_owned());
            }
            Some(Value::Null) => {
                return Err("Cannot read properties of null (reading 'content')".to_owned());
            }
            Some(delta) if delta.is_object() => delta,
            Some(_) => return Err("invalid stream delta".to_owned()),
        };
        self.content(out, output, delta.get("content"))?;
        if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
            self.tool_calls(out, output, calls)?;
        }
        Ok(())
    }

    fn content(
        &mut self,
        out: &AssistantMessageEventStream,
        output: &mut AssistantMessage,
        content: Option<&Value>,
    ) -> Result<(), String> {
        match content {
            None | Some(Value::Null) => Ok(()),
            Some(Value::String(text)) => {
                self.text_delta(out, output, text);
                Ok(())
            }
            Some(Value::Array(items)) => {
                for item in items {
                    self.content_item(out, output, item)?;
                }
                Ok(())
            }
            Some(_) => Err("contentItems is not iterable".to_owned()),
        }
    }

    fn content_item(
        &mut self,
        out: &AssistantMessageEventStream,
        output: &mut AssistantMessage,
        item: &Value,
    ) -> Result<(), String> {
        match item {
            Value::String(text) => self.text_delta(out, output, text),
            Value::Null => {
                return Err("Cannot read properties of null (reading 'type')".to_owned());
            }
            Value::Object(object) => match object.get("type").and_then(Value::as_str) {
                Some("thinking") => {
                    let text: String = object
                        .get("thinking")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(|part| part.get("text").and_then(Value::as_str))
                        .collect();
                    self.thinking_delta(out, output, &text);
                }
                Some("text") => {
                    let text = object
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    self.text_delta(out, output, text);
                }
                _ => {}
            },
            _ => return Err("invalid content item".to_owned()),
        }
        Ok(())
    }

    fn text_delta(
        &mut self,
        out: &AssistantMessageEventStream,
        output: &mut AssistantMessage,
        delta: &str,
    ) {
        if delta.is_empty() {
            return;
        }
        let index = match self.open {
            Some(Open::Text(index)) => index,
            _ => {
                self.close_open(out, output);
                let index = output.content.len();
                output.content.push(AssistantBlock::Text(TextContent {
                    kind: TextType,
                    text: String::new(),
                    text_signature: None,
                }));
                self.open = Some(Open::Text(index));
                out.push(AssistantMessageEvent::TextStart {
                    content_index: index as u64,
                    partial: output.clone(),
                });
                index
            }
        };
        if let AssistantBlock::Text(block) = &mut output.content[index] {
            block.text.push_str(delta);
        }
        out.push(AssistantMessageEvent::TextDelta {
            content_index: index as u64,
            delta: delta.to_owned(),
            partial: output.clone(),
        });
    }

    fn thinking_delta(
        &mut self,
        out: &AssistantMessageEventStream,
        output: &mut AssistantMessage,
        delta: &str,
    ) {
        if delta.is_empty() {
            return;
        }
        let index = match self.open {
            Some(Open::Thinking(index)) => index,
            _ => {
                self.close_open(out, output);
                let index = output.content.len();
                output
                    .content
                    .push(AssistantBlock::Thinking(ThinkingContent {
                        kind: ThinkingType,
                        thinking: String::new(),
                        thinking_signature: None,
                        redacted: None,
                    }));
                self.open = Some(Open::Thinking(index));
                out.push(AssistantMessageEvent::ThinkingStart {
                    content_index: index as u64,
                    partial: output.clone(),
                });
                index
            }
        };
        if let AssistantBlock::Thinking(block) = &mut output.content[index] {
            block.thinking.push_str(delta);
        }
        out.push(AssistantMessageEvent::ThinkingDelta {
            content_index: index as u64,
            delta: delta.to_owned(),
            partial: output.clone(),
        });
    }

    fn close_open(&mut self, out: &AssistantMessageEventStream, output: &AssistantMessage) {
        match self.open.take() {
            Some(Open::Text(index)) => {
                let content = match output.content.get(index) {
                    Some(AssistantBlock::Text(block)) => block.text.clone(),
                    _ => String::new(),
                };
                out.push(AssistantMessageEvent::TextEnd {
                    content_index: index as u64,
                    content,
                    partial: output.clone(),
                });
            }
            Some(Open::Thinking(index)) => {
                let content = match output.content.get(index) {
                    Some(AssistantBlock::Thinking(block)) => block.thinking.clone(),
                    _ => String::new(),
                };
                out.push(AssistantMessageEvent::ThinkingEnd {
                    content_index: index as u64,
                    content,
                    partial: output.clone(),
                });
            }
            None => {}
        }
    }

    fn tool_calls(
        &mut self,
        out: &AssistantMessageEventStream,
        output: &mut AssistantMessage,
        calls: &[Value],
    ) -> Result<(), String> {
        for call in calls {
            self.close_open(out, output);
            let index = call.get("index").and_then(Value::as_u64).unwrap_or(0);
            let id = match call.get("id").and_then(Value::as_str) {
                Some(id) if !id.is_empty() && id != "null" => id.to_owned(),
                _ => derive_tool_call_id(&format!("toolcall:{index}"), 0),
            };
            let key = match call.get("index") {
                Some(value) if !value.is_null() => ToolKey::Index(index),
                _ => ToolKey::Id(id.clone()),
            };
            let existing = self.tools.get(&key).copied();
            let function = match call.get("function") {
                Some(function) if !function.is_null() => function,
                other => {
                    let holder = if other.is_none() { "undefined" } else { "null" };
                    let property = if existing.is_some() {
                        "arguments"
                    } else {
                        "name"
                    };
                    return Err(format!(
                        "Cannot read properties of {holder} (reading '{property}')"
                    ));
                }
            };
            let index = match existing {
                Some(index) => index,
                None => {
                    let content_index = output.content.len();
                    output.content.push(AssistantBlock::ToolCall(ToolCall {
                        kind: ToolCallType,
                        id,
                        name: function
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        arguments: JsonObject::new(),
                        thought_signature: None,
                        namespace: None,
                    }));
                    self.tools.insert(key, content_index);
                    self.tool_order.push(content_index);
                    out.push(AssistantMessageEvent::ToolcallStart {
                        content_index: content_index as u64,
                        partial: output.clone(),
                    });
                    content_index
                }
            };
            let delta = arguments_delta(function.get("arguments"));
            let json = self.tool_json.entry(index).or_default();
            json.push_str(&delta);
            let arguments = match parse_streaming_json(Some(json.as_str())) {
                Value::Object(map) => map,
                _ => JsonObject::new(),
            };
            if let Some(AssistantBlock::ToolCall(block)) = output.content.get_mut(index) {
                block.arguments = arguments;
            }
            out.push(AssistantMessageEvent::ToolcallDelta {
                content_index: index as u64,
                delta,
                partial: output.clone(),
            });
        }
        Ok(())
    }

    fn finish(&mut self, out: &AssistantMessageEventStream, output: &mut AssistantMessage) {
        self.close_open(out, output);
        for &index in &self.tool_order {
            if let Some(AssistantBlock::ToolCall(call)) = output.content.get(index) {
                out.push(AssistantMessageEvent::ToolcallEnd {
                    content_index: index as u64,
                    tool_call: call.clone(),
                    partial: output.clone(),
                });
            }
        }
    }
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
        Value::String(text) => !text.is_empty(),
        _ => true,
    }
}

fn arguments_delta(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => text.clone(),
        Some(value) if truthy(value) => value.to_string(),
        _ => "{}".to_owned(),
    }
}

fn apply_usage(output: &mut AssistantMessage, model: &Model, usage: &Value) {
    let prompt = usage["prompt_tokens"].as_u64().unwrap_or(0);
    let cached = cached_prompt_tokens(usage, prompt);
    let completion = usage["completion_tokens"].as_u64().unwrap_or(0);
    let input = prompt.saturating_sub(cached);
    let total = usage["total_tokens"].as_u64().unwrap_or(0);
    output.usage.input = input;
    output.usage.output = completion;
    output.usage.cache_read = cached;
    output.usage.cache_write = 0;
    output.usage.total_tokens = if total == 0 {
        input + completion + cached
    } else {
        total
    };
    calculate_cost(model, &mut output.usage);
}

fn cached_prompt_tokens(usage: &Value, prompt: u64) -> u64 {
    let details = [
        "promptTokensDetails",
        "prompt_tokens_details",
        "promptTokenDetails",
        "prompt_token_details",
    ];
    let nested = details
        .iter()
        .flat_map(|key| ["cachedTokens", "cached_tokens"].map(|field| usage[*key][field].as_i64()));
    let flat = ["numCachedTokens", "num_cached_tokens"].map(|field| usage[field].as_i64());
    nested
        .chain(flat)
        .flatten()
        .next()
        .map_or(0, |cached| (cached.max(0) as u64).min(prompt))
}

fn map_stop_reason(reason: &str) -> (StopReason, Option<String>) {
    match reason {
        "stop" => (StopReason::Stop, None),
        "length" | "model_length" => (StopReason::Length, None),
        "tool_calls" => (StopReason::ToolUse, None),
        "error" => (
            StopReason::Error,
            Some("Provider stopped with: error".to_owned()),
        ),
        other => (
            StopReason::Error,
            Some(format!("Provider stopped with: {other}")),
        ),
    }
}
