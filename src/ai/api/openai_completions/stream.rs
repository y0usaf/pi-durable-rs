use std::collections::HashMap;
use std::io::BufRead;

use serde_json::Value;

use super::compat::Compat;
use crate::ai::cost::calculate_cost;
use crate::ai::event_stream::{ABORTED_TEXT, AssistantMessageEventStream};
use crate::ai::json_parse::parse_streaming_json;
use crate::ai::sse::SseReader;
use crate::ai::types::{
    AssistantBlock, AssistantMessage, AssistantMessageEvent, Model, StopReason, TextContent,
    TextType, ThinkingContent, ThinkingType, ToolCall, ToolCallType, Usage,
};
use crate::cancel::CancelToken;
use crate::chord::json::JsonObject;

const REASONING_FIELDS: [&str; 3] = ["reasoning_content", "reasoning", "reasoning_text"];

#[derive(Default)]
struct Blocks {
    text: Option<usize>,
    thinking: Option<usize>,
    by_stream_index: HashMap<u64, usize>,
    by_id: HashMap<String, usize>,
    partial_args: HashMap<usize, String>,
    has_finish_reason: bool,
}

pub fn consume(
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
    model: &Model,
    compat: &Compat,
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
        let event = event.map_err(|error| crate::ai::http::io_error(&error, cancel).to_string())?;
        if event.data.trim() == "[DONE]" {
            break;
        }
        let chunk: Value = serde_json::from_str(&event.data)
            .map_err(|error| format!("invalid stream chunk: {error}"))?;
        blocks.chunk(out, output, model, &chunk)?;
    }
    if cancelled() {
        return Err(ABORTED_TEXT.to_owned());
    }
    blocks.finish(out, output)?;
    if !blocks.has_finish_reason && !compat.supports_finish_reason {
        output.stop_reason = if output
            .content
            .iter()
            .any(|block| matches!(block, AssistantBlock::ToolCall(_)))
        {
            StopReason::ToolUse
        } else {
            StopReason::Stop
        };
    }
    match output.stop_reason {
        StopReason::Error => Err(output
            .error_message
            .clone()
            .unwrap_or_else(|| "Provider returned an error stop reason".to_owned())),
        StopReason::Aborted => Err(ABORTED_TEXT.to_owned()),
        StopReason::Pending => Err("Stream ended without finish_reason".to_owned()),
        _ if compat.supports_finish_reason && !blocks.has_finish_reason => {
            Err("Stream ended without finish_reason".to_owned())
        }
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
        if output.response_id.is_none() {
            output.response_id = chunk.get("id").and_then(Value::as_str).map(str::to_owned);
        }
        if let Some(name) = chunk.get("model").and_then(Value::as_str)
            && !name.is_empty()
            && name != model.id
            && output.response_model.is_none()
        {
            output.response_model = Some(name.to_owned());
        }
        if let Some(usage) = chunk.get("usage").filter(|usage| !usage.is_null()) {
            output.usage = parse_usage(usage, model);
        }
        let Some(choice) = chunk.get("choices").and_then(|choices| choices.get(0)) else {
            return Ok(());
        };
        if chunk.get("usage").is_none_or(Value::is_null)
            && let Some(usage) = choice.get("usage").filter(|usage| !usage.is_null())
        {
            output.usage = parse_usage(usage, model);
        }
        if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
            output.raw_stop_reason = Some(reason.to_owned());
            let (stop, message) = map_stop_reason(reason);
            output.stop_reason = stop;
            if let Some(message) = message {
                output.error_message = Some(message);
            }
            self.has_finish_reason = true;
        }
        let Some(delta) = choice.get("delta") else {
            return Ok(());
        };
        if let Some(text) = delta.get("content").and_then(Value::as_str)
            && !text.is_empty()
        {
            let index = self.ensure_text(out, output);
            if let AssistantBlock::Text(block) = &mut output.content[index] {
                block.text.push_str(text);
            }
            out.push(AssistantMessageEvent::TextDelta {
                content_index: index as u64,
                delta: text.to_owned(),
                partial: output.clone(),
            });
        }
        if let Some((field, text)) = REASONING_FIELDS.iter().find_map(|field| {
            delta
                .get(*field)
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
                .map(|text| (*field, text))
        }) {
            let signature = if model.provider == "opencode-go" && field == "reasoning" {
                "reasoning_content"
            } else {
                field
            };
            let index = self.ensure_thinking(out, output, signature);
            if let AssistantBlock::Thinking(block) = &mut output.content[index] {
                block.thinking.push_str(text);
            }
            out.push(AssistantMessageEvent::ThinkingDelta {
                content_index: index as u64,
                delta: text.to_owned(),
                partial: output.clone(),
            });
        }
        if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                self.tool_call(out, output, call)?;
            }
        }
        Ok(())
    }

    fn ensure_text(
        &mut self,
        out: &AssistantMessageEventStream,
        output: &mut AssistantMessage,
    ) -> usize {
        if let Some(index) = self.text {
            return index;
        }
        output.content.push(AssistantBlock::Text(TextContent {
            kind: TextType,
            text: String::new(),
            text_signature: None,
        }));
        let index = output.content.len() - 1;
        self.text = Some(index);
        out.push(AssistantMessageEvent::TextStart {
            content_index: index as u64,
            partial: output.clone(),
        });
        index
    }

    fn ensure_thinking(
        &mut self,
        out: &AssistantMessageEventStream,
        output: &mut AssistantMessage,
        signature: &str,
    ) -> usize {
        if let Some(index) = self.thinking {
            return index;
        }
        output
            .content
            .push(AssistantBlock::Thinking(ThinkingContent {
                kind: ThinkingType,
                thinking: String::new(),
                thinking_signature: Some(signature.to_owned()),
                redacted: None,
            }));
        let index = output.content.len() - 1;
        self.thinking = Some(index);
        out.push(AssistantMessageEvent::ThinkingStart {
            content_index: index as u64,
            partial: output.clone(),
        });
        index
    }

    fn tool_call(
        &mut self,
        out: &AssistantMessageEventStream,
        output: &mut AssistantMessage,
        call: &Value,
    ) -> Result<(), String> {
        if call.get("custom").is_some_and(|custom| !custom.is_null()) {
            return Err("custom tool calls are not supported by openai-completions".to_owned());
        }
        let stream_index = call.get("index").and_then(Value::as_u64);
        let id = call
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty());
        let name = call
            .pointer("/function/name")
            .and_then(Value::as_str)
            .unwrap_or("");
        let existing = stream_index
            .and_then(|index| self.by_stream_index.get(&index).copied())
            .or_else(|| id.and_then(|id| self.by_id.get(id).copied()));
        let index = match existing {
            Some(index) => index,
            None => {
                output.content.push(AssistantBlock::ToolCall(ToolCall {
                    kind: ToolCallType,
                    id: id.unwrap_or("").to_owned(),
                    name: name.to_owned(),
                    arguments: JsonObject::new(),
                    thought_signature: None,
                    namespace: None,
                }));
                let index = output.content.len() - 1;
                self.partial_args.insert(index, String::new());
                out.push(AssistantMessageEvent::ToolcallStart {
                    content_index: index as u64,
                    partial: output.clone(),
                });
                index
            }
        };
        if let Some(stream_index) = stream_index {
            self.by_stream_index.entry(stream_index).or_insert(index);
        }
        let AssistantBlock::ToolCall(block) = &mut output.content[index] else {
            return Err("tool call delta points at a non-tool block".to_owned());
        };
        if let Some(id) = id {
            if block.id.is_empty() {
                block.id = id.to_owned();
            }
            self.by_id.insert(id.to_owned(), index);
        }
        if block.name.is_empty() && !name.is_empty() {
            block.name = name.to_owned();
        }
        let arguments = call
            .pointer("/function/arguments")
            .and_then(Value::as_str)
            .unwrap_or("");
        let partial = self.partial_args.entry(index).or_default();
        partial.push_str(arguments);
        block.arguments = parse_arguments(partial);
        out.push(AssistantMessageEvent::ToolcallDelta {
            content_index: index as u64,
            delta: arguments.to_owned(),
            partial: output.clone(),
        });
        Ok(())
    }

    fn finish(
        &mut self,
        out: &AssistantMessageEventStream,
        output: &mut AssistantMessage,
    ) -> Result<(), String> {
        for index in 0..output.content.len() {
            let content_index = index as u64;
            match &output.content[index] {
                AssistantBlock::Text(block) => {
                    let content = block.text.clone();
                    out.push(AssistantMessageEvent::TextEnd {
                        content_index,
                        content,
                        partial: output.clone(),
                    });
                }
                AssistantBlock::Thinking(block) => {
                    let content = block.thinking.clone();
                    out.push(AssistantMessageEvent::ThinkingEnd {
                        content_index,
                        content,
                        partial: output.clone(),
                    });
                }
                AssistantBlock::ToolCall(_) => {
                    let partial = self.partial_args.remove(&index).unwrap_or_default();
                    let AssistantBlock::ToolCall(block) = &mut output.content[index] else {
                        return Err("tool call index points at a non-tool block".to_owned());
                    };
                    block.arguments = parse_arguments(&partial);
                    let tool_call = block.clone();
                    out.push(AssistantMessageEvent::ToolcallEnd {
                        content_index,
                        tool_call,
                        partial: output.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

fn parse_arguments(partial: &str) -> JsonObject {
    parse_streaming_json(Some(partial))
        .as_object()
        .cloned()
        .unwrap_or_default()
}

fn map_stop_reason(reason: &str) -> (StopReason, Option<String>) {
    match reason {
        "stop" | "end" => (StopReason::Stop, None),
        "length" => (StopReason::Length, None),
        "function_call" | "tool_calls" => (StopReason::ToolUse, None),
        "content_filter" => (
            StopReason::Error,
            Some("Provider finish_reason: content_filter".to_owned()),
        ),
        "network_error" => (
            StopReason::Error,
            Some("Provider finish_reason: network_error".to_owned()),
        ),
        other => (
            StopReason::Error,
            Some(format!("Provider finish_reason: {other}")),
        ),
    }
}

fn parse_usage(raw: &Value, model: &Model) -> Usage {
    let count = |pointer: &str| raw.pointer(pointer).and_then(Value::as_u64);
    let prompt = count("/prompt_tokens").unwrap_or(0);
    let cache_read = count("/prompt_tokens_details/cached_tokens")
        .or_else(|| count("/prompt_cache_hit_tokens"))
        .or_else(|| count("/cached_tokens"))
        .unwrap_or(0);
    let cache_write = count("/prompt_tokens_details/cache_write_tokens").unwrap_or(0);
    let mut usage = Usage {
        input: prompt.saturating_sub(cache_read + cache_write),
        output: count("/completion_tokens").unwrap_or(0),
        cache_read,
        cache_write,
        cache_write_1h: None,
        reasoning: Some(count("/completion_tokens_details/reasoning_tokens").unwrap_or(0)),
        total_tokens: 0,
        cost: crate::ai::types::Cost {
            input: 0.0,
            output: 0.0,
            cache_read: 0.0,
            cache_write: 0.0,
            total: 0.0,
        },
    };
    usage.total_tokens = usage.input + usage.output + usage.cache_read + usage.cache_write;
    calculate_cost(model, &mut usage);
    usage
}
