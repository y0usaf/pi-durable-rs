use std::io::BufRead;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Map, Value};

use crate::ai::cost::calculate_cost;
use crate::ai::event_stream::{ABORTED_TEXT, AssistantMessageEventStream, now_ms};
use crate::ai::sse::SseReader;
use crate::ai::types::{
    AssistantBlock, AssistantMessage, AssistantMessageEvent, Model, StopReason, TextContent,
    TextType, ThinkingContent, ThinkingType, ToolCall, ToolCallType, Usage,
};
use crate::cancel::CancelToken;

static GENERATIVE_CALLS: AtomicU64 = AtomicU64::new(0);
static VERTEX_CALLS: AtomicU64 = AtomicU64::new(0);

pub fn read(
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
    body: impl BufRead,
    model: &Model,
    vertex: bool,
    signal: Option<&CancelToken>,
) -> Result<(), String> {
    let mut folder = Folder {
        out,
        output,
        vertex,
        active_text: None,
        active_thinking: None,
        saw_tool_call: false,
        finish: None,
    };
    let result = folder.consume(body, model, signal);
    folder.end_text();
    folder.end_thinking();
    result
}

struct Folder<'a> {
    out: &'a AssistantMessageEventStream,
    output: &'a mut AssistantMessage,
    vertex: bool,
    active_text: Option<usize>,
    active_thinking: Option<usize>,
    saw_tool_call: bool,
    finish: Option<String>,
}

impl Folder<'_> {
    fn consume(
        &mut self,
        body: impl BufRead,
        model: &Model,
        signal: Option<&CancelToken>,
    ) -> Result<(), String> {
        for event in SseReader::new(body) {
            if signal.is_some_and(CancelToken::is_cancelled) {
                return Err(ABORTED_TEXT.to_owned());
            }
            let event = event.map_err(|error| io_failure(&error, signal))?;
            if event.data.is_empty() {
                continue;
            }
            let chunk: Value = serde_json::from_str(&event.data)
                .map_err(|error| format!("google: invalid SSE JSON: {error}"))?;
            check_chunk_error(&chunk)?;
            self.handle(&chunk, model);
        }
        if signal.is_some_and(CancelToken::is_cancelled) {
            return Err(ABORTED_TEXT.to_owned());
        }
        self.finish()
    }

    fn handle(&mut self, chunk: &Value, model: &Model) {
        if let Some(id) = chunk["responseId"].as_str().filter(|id| !id.is_empty())
            && self.output.response_id.is_none()
        {
            self.output.response_id = Some(id.to_owned());
        }
        let candidate = &chunk["candidates"][0];
        if let Some(parts) = candidate["content"]["parts"].as_array() {
            for part in parts {
                self.part(part);
            }
        }
        if let Some(reason) = candidate["finishReason"].as_str().filter(|r| !r.is_empty()) {
            self.finish = Some(reason.to_owned());
            self.output.raw_stop_reason = Some(reason.to_owned());
            self.output.stop_reason = map_finish(reason).unwrap_or(StopReason::Error);
        }
        if chunk["usageMetadata"].is_object() {
            self.usage(&chunk["usageMetadata"], model);
        }
    }

    fn part(&mut self, part: &Value) {
        let text = part["text"].as_str().unwrap_or("");
        let thought = part.get("thought");
        let is_thinking = thought.and_then(Value::as_bool) == Some(true);
        let signature = part["thoughtSignature"]
            .as_str()
            .filter(|sig| !sig.is_empty());
        if !text.is_empty() || thought.is_some() {
            if is_thinking {
                self.thinking_start();
                if let (Some(sig), Some(index)) = (signature, self.active_thinking)
                    && let AssistantBlock::Thinking(block) = &mut self.output.content[index]
                {
                    block.thinking_signature = Some(sig.to_owned());
                }
                self.thinking_delta(text);
            } else {
                self.text_start();
                if let (Some(sig), Some(index)) = (signature, self.active_text)
                    && let AssistantBlock::Text(block) = &mut self.output.content[index]
                {
                    block.text_signature = Some(sig.to_owned());
                }
                self.text_delta(text);
            }
        }
        if let Some(call) = part.get("functionCall") {
            self.tool_call(call, signature);
        }
    }

    fn text_start(&mut self) -> usize {
        self.end_thinking();
        if let Some(index) = self.active_text {
            return index;
        }
        let index = self.output.content.len();
        self.output.content.push(AssistantBlock::Text(TextContent {
            kind: TextType,
            text: String::new(),
            text_signature: None,
        }));
        self.active_text = Some(index);
        self.out.push(AssistantMessageEvent::TextStart {
            content_index: index as u64,
            partial: self.output.clone(),
        });
        index
    }

    fn text_delta(&mut self, delta: &str) {
        let index = self.text_start();
        if let AssistantBlock::Text(block) = &mut self.output.content[index] {
            block.text.push_str(delta);
        }
        self.out.push(AssistantMessageEvent::TextDelta {
            content_index: index as u64,
            delta: delta.to_owned(),
            partial: self.output.clone(),
        });
    }

    fn end_text(&mut self) {
        if let Some(index) = self.active_text.take() {
            let content = match &self.output.content[index] {
                AssistantBlock::Text(block) => block.text.clone(),
                _ => String::new(),
            };
            self.out.push(AssistantMessageEvent::TextEnd {
                content_index: index as u64,
                content,
                partial: self.output.clone(),
            });
        }
    }

    fn thinking_start(&mut self) -> usize {
        self.end_text();
        if let Some(index) = self.active_thinking {
            return index;
        }
        let index = self.output.content.len();
        self.output
            .content
            .push(AssistantBlock::Thinking(ThinkingContent {
                kind: ThinkingType,
                thinking: String::new(),
                thinking_signature: None,
                redacted: None,
            }));
        self.active_thinking = Some(index);
        self.out.push(AssistantMessageEvent::ThinkingStart {
            content_index: index as u64,
            partial: self.output.clone(),
        });
        index
    }

    fn thinking_delta(&mut self, delta: &str) {
        let index = self.thinking_start();
        if let AssistantBlock::Thinking(block) = &mut self.output.content[index] {
            block.thinking.push_str(delta);
        }
        self.out.push(AssistantMessageEvent::ThinkingDelta {
            content_index: index as u64,
            delta: delta.to_owned(),
            partial: self.output.clone(),
        });
    }

    fn end_thinking(&mut self) {
        if let Some(index) = self.active_thinking.take() {
            let content = match &self.output.content[index] {
                AssistantBlock::Thinking(block) => block.thinking.clone(),
                _ => String::new(),
            };
            self.out.push(AssistantMessageEvent::ThinkingEnd {
                content_index: index as u64,
                content,
                partial: self.output.clone(),
            });
        }
    }

    fn tool_call(&mut self, call: &Value, signature: Option<&str>) {
        self.saw_tool_call = true;
        self.end_text();
        self.end_thinking();
        let name = call["name"].as_str().unwrap_or_default();
        let id = tool_call_id(
            self.vertex,
            name,
            call["id"].as_str().unwrap_or_default(),
            &self.output.content,
        );
        let arguments: Map<String, Value> = match &call["args"] {
            Value::Object(map) => map.clone(),
            _ => Map::new(),
        };
        let delta = Value::Object(arguments.clone()).to_string();
        let block = ToolCall {
            kind: ToolCallType,
            id,
            name: name.to_owned(),
            arguments,
            thought_signature: signature.map(str::to_owned),
            namespace: None,
        };
        let index = self.output.content.len();
        self.output
            .content
            .push(AssistantBlock::ToolCall(block.clone()));
        self.out.push(AssistantMessageEvent::ToolcallStart {
            content_index: index as u64,
            partial: self.output.clone(),
        });
        self.out.push(AssistantMessageEvent::ToolcallDelta {
            content_index: index as u64,
            delta,
            partial: self.output.clone(),
        });
        self.out.push(AssistantMessageEvent::ToolcallEnd {
            content_index: index as u64,
            tool_call: block,
            partial: self.output.clone(),
        });
    }

    fn usage(&mut self, metadata: &Value, model: &Model) {
        let field = |key: &str| metadata[key].as_u64().unwrap_or(0);
        let prompt = field("promptTokenCount");
        let cached = field("cachedContentTokenCount");
        let thoughts = field("thoughtsTokenCount");
        let usage: &mut Usage = &mut self.output.usage;
        usage.input = prompt.saturating_sub(cached);
        usage.output = field("candidatesTokenCount") + thoughts;
        usage.reasoning = Some(thoughts);
        usage.cache_read = cached;
        usage.total_tokens = field("totalTokenCount");
        calculate_cost(model, usage);
    }

    fn finish(&mut self) -> Result<(), String> {
        let reason = self
            .finish
            .clone()
            .filter(|reason| !reason.is_empty())
            .ok_or_else(|| "Google stream ended without a finish reason".to_owned())?;
        let stop = map_finish(&reason)?;
        self.output.stop_reason = if stop == StopReason::Stop && self.saw_tool_call {
            StopReason::ToolUse
        } else {
            stop
        };
        Ok(())
    }
}

fn map_finish(reason: &str) -> Result<StopReason, String> {
    match reason {
        "STOP" => Ok(StopReason::Stop),
        "MAX_TOKENS" => Ok(StopReason::Length),
        other => Err(format!("Provider stopped with: {other}")),
    }
}

fn tool_call_id(vertex: bool, name: &str, provided: &str, content: &[AssistantBlock]) -> String {
    let reused = content
        .iter()
        .any(|block| matches!(block, AssistantBlock::ToolCall(call) if call.id == provided));
    if !provided.is_empty() && !reused {
        return provided.to_owned();
    }
    let counter = if vertex {
        &VERTEX_CALLS
    } else {
        &GENERATIVE_CALLS
    };
    format!(
        "{name}_{}_{}",
        now_ms(),
        counter.fetch_add(1, Ordering::Relaxed) + 1
    )
}

fn check_chunk_error(chunk: &Value) -> Result<(), String> {
    let Some(error) = chunk.get("error").filter(|error| error.is_object()) else {
        return Ok(());
    };
    let code = match &error["code"] {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    };
    if !code.is_some_and(|code| (400.0..600.0).contains(&code)) {
        return Ok(());
    }
    let status = match &error["status"] {
        Value::Null => "undefined".to_owned(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    Err(format!("got status: {status}. {chunk}"))
}

fn io_failure(error: &std::io::Error, signal: Option<&CancelToken>) -> String {
    if signal.is_some_and(CancelToken::is_cancelled) {
        ABORTED_TEXT.to_owned()
    } else {
        error.to_string()
    }
}
