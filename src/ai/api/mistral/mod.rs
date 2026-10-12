mod stream;

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use super::openai_responses::request::{render_update, short_hash};
use crate::ai::error_body::{MAX_PROVIDER_ERROR_BODY_CHARS, truncate_error_text};
use crate::ai::event_stream::AssistantMessageEventStream;
use crate::ai::http::{Request, send};
use crate::ai::registry::Api;
use crate::ai::simple_options::cache_retention;
use crate::ai::stream::spawn;
use crate::ai::thinking::{clamp_thinking_level, level_name};
use crate::ai::transform::{ToolCallIdNormalizer, transform_messages};
use crate::ai::types::{
    AssistantBlock, AssistantMessage, CacheRetention, ImageContent, Message, Model,
    ModelThinkingLevel, SimpleStreamOptions, Tool, ToolResultMessage, TranscriptContext, UserBlock,
    UserContent,
};
use crate::ai::utils::text::system_message_text;
use crate::ai::utils::transcript::{
    get_current_tools, get_initial_system_message, resolve_transcript,
    without_initial_system_message,
};

const DEFAULT_BASE_URL: &str = "https://api.mistral.ai";
const DEFAULT_TIMEOUT_MS: u64 = 60_000;
const TOOL_CALL_ID_LENGTH: usize = 9;

pub fn api() -> Arc<dyn Api> {
    Arc::new(Mistral)
}

struct Mistral;

impl Api for Mistral {
    fn stream_simple(
        &self,
        model: &Model,
        context: TranscriptContext,
        options: SimpleStreamOptions,
    ) -> AssistantMessageEventStream {
        let signal = options.signal.clone();
        let run_model = model.clone();
        spawn(model, signal.as_ref(), move |out, output| {
            run(&run_model, &context, &options, out, output)
        })
    }
}

fn run(
    model: &Model,
    context: &TranscriptContext,
    options: &SimpleStreamOptions,
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
) -> Result<(), String> {
    let api_key = options
        .api_key
        .as_deref()
        .filter(|key| !key.is_empty())
        .ok_or("no API key for Mistral provider")?;
    let mid_convo = model
        .compat
        .as_ref()
        .and_then(|compat| compat.get("supportsMidConvoSystemMessages"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let resolved = resolve_transcript(context, mid_convo);
    let normalizer = RefCell::new(IdNormalizer::default());
    let normalize =
        |id: &str, _: &Model, _: &AssistantMessage| normalizer.borrow_mut().normalize(id);
    let normalize: &ToolCallIdNormalizer = &normalize;
    let messages = transform_messages(&resolved.messages, model, Some(normalize));

    let mut wire = Vec::new();
    if let Some(system) = get_initial_system_message(&messages) {
        let text = system_message_text(system);
        if !text.is_empty() {
            wire.push(json!({"role": "system", "content": text}));
        }
    }
    let supports_images = model.input.iter().any(|kind| kind == "image");
    wire.extend(convert_messages(
        without_initial_system_message(&messages),
        supports_images,
    ));

    let tools = get_current_tools(&messages);
    let mut body = json!({
        "model": model.id,
        "stream": true,
        "messages": wire,
    });
    if !tools.is_empty() {
        body["tools"] = Value::Array(tools.iter().map(convert_tool).collect());
    }
    if let Some(max_tokens) = options.max_tokens.filter(|tokens| *tokens > 0) {
        body["max_tokens"] = json!(max_tokens);
    }
    if let Some(temperature) = options.temperature {
        body["temperature"] = json!(temperature);
    }
    if let Some(choice) = &options.tool_choice {
        body["tool_choice"] = json!(choice);
    }
    let (effort, prompt_mode) = reasoning(model, options);
    if let Some(effort) = effort {
        body["reasoning_effort"] = json!(effort);
    }
    if let Some(prompt_mode) = prompt_mode {
        body["prompt_mode"] = json!(prompt_mode);
    }
    let session = options
        .session_id
        .as_deref()
        .filter(|session| !session.is_empty());
    let caching = cache_retention(options) != CacheRetention::None;
    if let Some(session) = session.filter(|_| caching) {
        body["prompt_cache_key"] = json!(session);
    }

    let base = Some(model.base_url.as_str())
        .filter(|base| !base.is_empty())
        .unwrap_or(DEFAULT_BASE_URL);
    let mut headers: Vec<(String, String)> = Vec::new();
    set_header(&mut headers, "Accept", "text/event-stream".to_owned());
    set_header(&mut headers, "Authorization", format!("Bearer {api_key}"));
    for (name, value) in model.headers.iter().flatten() {
        set_header(&mut headers, name, value.clone());
    }
    if let Some(session) = session.filter(|_| caching)
        && !headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("x-affinity"))
    {
        set_header(&mut headers, "x-affinity", session.to_owned());
    }
    for (name, value) in options.headers.iter().flatten() {
        set_header(&mut headers, name, value.clone());
    }
    let mut request = Request::new("POST", chat_completions_url(base))
        .json(&body)
        .timeout(Some(Duration::from_millis(
            options.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS),
        )));
    for (name, value) in &headers {
        request = request.header(name, value.clone());
    }

    let response = send(&request, options.signal.as_ref()).map_err(|error| error.to_string())?;
    if !response.ok() {
        let status = response.status;
        let text = response.text().map_err(|error| error.to_string())?;
        return Err(status_error(status, text.trim()));
    }
    stream::consume(
        out,
        output,
        model,
        options.signal.as_ref(),
        response.into_reader(),
    )
}

fn chat_completions_url(base: &str) -> String {
    let end = base.find(['?', '#']).unwrap_or(base.len());
    format!("{}/v1/chat/completions", base[..end].trim_end_matches('/'))
}

fn status_error(status: u16, text: &str) -> String {
    let text = if text.is_empty() {
        format!("Request failed with status {status}")
    } else {
        truncate_error_text(text, MAX_PROVIDER_ERROR_BODY_CHARS)
    };
    format!("Mistral API error ({status}): {text}")
}

fn reasoning(
    model: &Model,
    options: &SimpleStreamOptions,
) -> (Option<String>, Option<&'static str>) {
    if !model.reasoning {
        return (None, None);
    }
    let level = options
        .reasoning
        .map(|level| clamp_thinking_level(model, level.into()))
        .filter(|level| *level != ModelThinkingLevel::Off);
    match &model.thinking_level_map {
        Some(map) => {
            let mapped = |key: &str| map.get(key).and_then(Value::as_str).map(str::to_owned);
            let effort = match level {
                None => mapped(level_name(ModelThinkingLevel::Off)),
                Some(level) => Some(mapped(level_name(level)).unwrap_or_else(|| "high".to_owned())),
            };
            (effort, None)
        }
        None => (None, level.map(|_| "reasoning")),
    }
}

fn convert_messages(messages: &[Message], supports_images: bool) -> Vec<Value> {
    let mut wire = Vec::new();
    for message in messages {
        match message {
            Message::System(system) => {
                let text = render_update(system);
                if !text.is_empty() {
                    wire.push(json!({"role": "system", "content": text}));
                }
            }
            Message::User(user) => {
                if let Some(converted) = convert_user(&user.content) {
                    wire.push(converted);
                }
            }
            Message::Assistant(assistant) => {
                if let Some(converted) = convert_assistant(&assistant.content) {
                    wire.push(converted);
                }
            }
            Message::ToolResult(result) => {
                wire.push(convert_tool_result(result, supports_images));
            }
        }
    }
    wire
}

fn convert_user(content: &UserContent) -> Option<Value> {
    match content {
        UserContent::Text(text) => Some(json!({"role": "user", "content": text})),
        UserContent::Blocks(blocks) => {
            let chunks: Vec<Value> = blocks.iter().map(user_chunk).collect();
            (!chunks.is_empty()).then(|| json!({"role": "user", "content": chunks}))
        }
    }
}

fn user_chunk(block: &UserBlock) -> Value {
    match block {
        UserBlock::Text(text) => json!({"type": "text", "text": text.text}),
        UserBlock::Image(image) => image_chunk(image),
    }
}

fn image_chunk(image: &ImageContent) -> Value {
    json!({
        "type": "image_url",
        "image_url": format!("data:{};base64,{}", image.mime_type, image.data),
    })
}

fn convert_assistant(blocks: &[AssistantBlock]) -> Option<Value> {
    let mut content = Vec::new();
    let mut tool_calls = Vec::new();
    for block in blocks {
        match block {
            AssistantBlock::Text(text) if !text.text.trim().is_empty() => {
                content.push(json!({"type": "text", "text": text.text}));
            }
            AssistantBlock::Thinking(thinking) if !thinking.thinking.trim().is_empty() => {
                content.push(json!({
                    "type": "thinking",
                    "thinking": [{"type": "text", "text": thinking.thinking}],
                }));
            }
            AssistantBlock::ToolCall(call) => tool_calls.push(json!({
                "id": call.id,
                "type": "function",
                "function": {
                    "name": call.name,
                    "arguments": serde_json::to_string(&call.arguments).unwrap_or_default(),
                },
                "index": 0,
            })),
            _ => {}
        }
    }
    if content.is_empty() && tool_calls.is_empty() {
        return None;
    }
    let mut message = json!({"role": "assistant", "prefix": false});
    if !content.is_empty() {
        message["content"] = Value::Array(content);
    }
    if !tool_calls.is_empty() {
        message["tool_calls"] = Value::Array(tool_calls);
    }
    Some(message)
}

fn convert_tool_result(result: &ToolResultMessage, supports_images: bool) -> Value {
    let mut texts = Vec::new();
    let mut has_images = false;
    for block in &result.content {
        match block {
            UserBlock::Text(text) => texts.push(text.text.as_str()),
            UserBlock::Image(_) => has_images = true,
        }
    }
    let text = tool_result_text(
        &texts.join(
            "
",
        ),
        has_images,
        supports_images,
        result.is_error,
    );
    let mut content = vec![json!({"type": "text", "text": text})];
    if supports_images {
        for block in &result.content {
            if let UserBlock::Image(image) = block {
                content.push(image_chunk(image));
            }
        }
    }
    json!({
        "role": "tool",
        "content": content,
        "tool_call_id": result.tool_call_id,
        "name": result.tool_name,
    })
}

fn tool_result_text(text: &str, has_images: bool, supports_images: bool, is_error: bool) -> String {
    let prefix = if is_error { "[tool error] " } else { "" };
    let text = text.trim();
    if !text.is_empty() {
        let note = if has_images && !supports_images {
            "\n[tool image omitted: model does not support images]"
        } else {
            ""
        };
        return format!("{prefix}{text}{note}");
    }
    let body = match (has_images, supports_images) {
        (true, true) => "(see attached image)",
        (true, false) => "(image omitted: model does not support images)",
        (false, _) => "(no tool output)",
    };
    format!("{prefix}{body}")
}

fn convert_tool(tool: &Tool) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.parameters,
            "strict": false,
        },
    })
}

fn set_header(headers: &mut Vec<(String, String)>, name: &str, value: String) {
    match headers
        .iter_mut()
        .find(|(known, _)| known.eq_ignore_ascii_case(name))
    {
        Some(slot) => slot.1 = value,
        None => headers.push((name.to_owned(), value)),
    }
}

pub(crate) fn derive_tool_call_id(id: &str, attempt: u32) -> String {
    let normalized: String = id.chars().filter(char::is_ascii_alphanumeric).collect();
    if attempt == 0 && normalized.len() == TOOL_CALL_ID_LENGTH {
        return normalized;
    }
    let base = if normalized.is_empty() {
        id
    } else {
        &normalized
    };
    let seed = if attempt > 0 {
        format!("{base}:{attempt}")
    } else {
        base.to_owned()
    };
    short_hash(&seed)
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(TOOL_CALL_ID_LENGTH)
        .collect()
}

#[derive(Default)]
struct IdNormalizer {
    forward: HashMap<String, String>,
    reverse: HashMap<String, String>,
}

impl IdNormalizer {
    fn normalize(&mut self, id: &str) -> String {
        if let Some(existing) = self.forward.get(id) {
            return existing.clone();
        }
        for attempt in 0..1000 {
            let candidate = derive_tool_call_id(id, attempt);
            let taken = self
                .reverse
                .get(&candidate)
                .is_some_and(|owner| owner != id);
            if !taken {
                self.forward.insert(id.to_owned(), candidate.clone());
                self.reverse.insert(candidate.clone(), id.to_owned());
                return candidate;
            }
        }
        id.to_owned()
    }
}
