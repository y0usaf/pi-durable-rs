use serde_json::json;

use super::compat::{Compat, ThinkingFormat};
use crate::ai::simple_options::{
    clamp_max_tokens_to_context, clamp_thinking_budget_to_answer_room, resolve_sampling_params,
    thinking_budget_for_level,
};
use crate::ai::thinking::level_name;
use crate::ai::transform::{ToolCallIdNormalizer, transform_messages};
use crate::ai::types::{
    AssistantBlock, AssistantMessage, CacheRetention, Message, Model, ModelThinkingLevel,
    SystemMessage, ThinkingContent, ThinkingLevel, Tool, ToolCall, ToolResultMessage,
    TranscriptContext, UserBlock, UserContent,
};
use crate::ai::utils::text::{system_content_text, system_message_text};
use crate::ai::utils::transcript::{resolve_transcript, resolve_transcript_tools};
use crate::chord::json::{JsonObject, JsonValue};

const PROCESSED_TOOL_RESULTS: &str = "I have processed the tool results.";
const REASONING_FIELDS: [&str; 3] = ["reasoning_content", "reasoning", "reasoning_text"];
const PROMPT_CACHE_KEY_MAX_CHARS: usize = 64;
const TOOL_CALL_ID_MAX_CHARS: usize = 40;
const OPENAI_HOST: &str = "api.openai.com";

pub struct Settings<'a> {
    pub base_url: &'a str,
    pub effort: Option<ModelThinkingLevel>,
    pub max_tokens: u64,
    pub retention: CacheRetention,
    pub session_id: Option<&'a str>,
}

pub fn build(
    model: &Model,
    compat: &Compat,
    context: &TranscriptContext,
    settings: &Settings,
) -> JsonObject {
    let transcript = resolve_transcript(context, compat.mid_convo_system_messages);
    let tools = resolve_transcript_tools(
        &transcript.messages,
        compat.mid_convo_system_messages && compat.mid_convo_tool_additions,
    )
    .request_tools;
    let max_tokens = clamp_max_tokens_to_context(model, &transcript.messages, settings.max_tokens);
    let mut body = JsonObject::new();
    body.insert("model".to_owned(), json!(model.id));
    body.insert(
        "messages".to_owned(),
        JsonValue::Array(convert_messages(model, compat, &transcript.messages)),
    );
    body.insert("stream".to_owned(), json!(true));
    let long_retention = settings.retention == CacheRetention::Long && compat.long_cache_retention;
    let cache_key_wanted = (settings.base_url.contains(OPENAI_HOST)
        && settings.retention != CacheRetention::None)
        || long_retention;
    if let Some(session) = settings.session_id.filter(|_| cache_key_wanted) {
        body.insert(
            "prompt_cache_key".to_owned(),
            json!(
                session
                    .chars()
                    .take(PROMPT_CACHE_KEY_MAX_CHARS)
                    .collect::<String>()
            ),
        );
    }
    if long_retention {
        body.insert("prompt_cache_retention".to_owned(), json!("24h"));
    }
    if compat.supports_usage_in_streaming {
        body.insert("stream_options".to_owned(), json!({"include_usage": true}));
    }
    if compat.supports_store {
        body.insert("store".to_owned(), json!(false));
    }
    if max_tokens > 0 {
        let key = if compat.legacy_max_tokens {
            "max_tokens"
        } else {
            "max_completion_tokens"
        };
        body.insert(key.to_owned(), json!(max_tokens));
    }
    if !tools.is_empty() {
        body.insert("tools".to_owned(), convert_tools(&tools, compat));
        if compat.zai_tool_stream {
            body.insert("tool_stream".to_owned(), json!(true));
        }
    } else if has_tool_history(&transcript.messages) {
        body.insert("tools".to_owned(), json!([]));
    }
    if let Some(priority) = &compat.vllm_priority {
        body.insert("priority".to_owned(), priority.clone());
    }
    if model.reasoning {
        thinking_params(&mut body, model, compat, settings.effort);
        if let (Some(field), Some(level)) = (
            compat.thinking_token_budget_field.as_deref(),
            settings.effort.and_then(budget_level),
        ) {
            let budget = clamp_thinking_budget_to_answer_room(
                thinking_budget_for_level(level, None),
                max_tokens,
            );
            body.insert(field.to_owned(), json!(budget));
        }
    }
    if let Some(routing) = &compat.open_router_routing {
        body.insert("provider".to_owned(), routing.clone());
    }
    if let Some(routing) = &compat.vercel_gateway_routing
        && (routing.get("only").is_some_and(|v| !v.is_null())
            || routing.get("order").is_some_and(|v| !v.is_null()))
    {
        let mut gateway = JsonObject::new();
        for key in ["only", "order"] {
            if let Some(value) = routing.get(key).filter(|value| !value.is_null()) {
                gateway.insert(key.to_owned(), value.clone());
            }
        }
        body.insert(
            "providerOptions".to_owned(),
            json!({ "gateway": JsonValue::Object(gateway) }),
        );
    }
    let sampling = resolve_sampling_params(
        model,
        settings.effort.unwrap_or(ModelThinkingLevel::Off),
        None,
    );
    for (key, value) in sampling.into_iter().flatten() {
        body.insert(key, value);
    }
    body
}

fn budget_level(level: ModelThinkingLevel) -> Option<ThinkingLevel> {
    Some(match level {
        ModelThinkingLevel::Off => return None,
        ModelThinkingLevel::Minimal => ThinkingLevel::Minimal,
        ModelThinkingLevel::Low => ThinkingLevel::Low,
        ModelThinkingLevel::Medium => ThinkingLevel::Medium,
        ModelThinkingLevel::High => ThinkingLevel::High,
        ModelThinkingLevel::Xhigh => ThinkingLevel::Xhigh,
        ModelThinkingLevel::Max => ThinkingLevel::Max,
    })
}

fn mapped_effort(model: &Model, level: ModelThinkingLevel) -> JsonValue {
    model
        .thinking_level_map
        .as_ref()
        .and_then(|map| map.get(level_name(level)))
        .filter(|value| !value.is_null())
        .cloned()
        .unwrap_or_else(|| json!(level_name(level)))
}

fn off_value(model: &Model) -> Option<JsonValue> {
    match model.thinking_level_map.as_ref()?.get("off") {
        None => Some(json!("none")),
        Some(JsonValue::Null) => None,
        Some(value) => Some(value.clone()),
    }
}

fn thinking_params(
    body: &mut JsonObject,
    model: &Model,
    compat: &Compat,
    effort: Option<ModelThinkingLevel>,
) {
    let mapped = effort.map(|level| mapped_effort(model, level));
    let reasoning_effort = mapped.clone().filter(|_| compat.supports_reasoning_effort);
    let off = off_value(model);
    match compat.thinking_format {
        ThinkingFormat::Zai => {
            let thinking = if effort.is_some() {
                json!({"type": "enabled", "clear_thinking": false})
            } else {
                json!({"type": "disabled"})
            };
            body.insert("thinking".to_owned(), thinking);
            insert_some(body, "reasoning_effort", reasoning_effort);
        }
        ThinkingFormat::Qwen => {
            body.insert("enable_thinking".to_owned(), json!(effort.is_some()));
            insert_some(body, "reasoning_effort", reasoning_effort);
        }
        ThinkingFormat::QwenChatTemplate => {
            body.insert(
                "chat_template_kwargs".to_owned(),
                json!({"enable_thinking": effort.is_some(), "preserve_thinking": true}),
            );
        }
        ThinkingFormat::DeepSeek => {
            if effort.is_some() {
                body.insert("thinking".to_owned(), json!({"type": "enabled"}));
            } else if model
                .thinking_level_map
                .as_ref()
                .and_then(|map| map.get("off"))
                != Some(&JsonValue::Null)
            {
                body.insert("thinking".to_owned(), json!({"type": "disabled"}));
            }
            insert_some(body, "reasoning_effort", reasoning_effort);
        }
        ThinkingFormat::OpenRouter => {
            if let Some(value) = mapped {
                body.insert("reasoning".to_owned(), json!({"effort": value}));
            } else if let Some(value) = off {
                body.insert("reasoning".to_owned(), json!({"effort": value}));
            }
        }
        ThinkingFormat::AntLing => {
            if let Some(JsonValue::String(text)) = mapped {
                body.insert("reasoning".to_owned(), json!({"effort": text}));
            }
        }
        ThinkingFormat::Together => {
            body.insert("reasoning".to_owned(), json!({"enabled": effort.is_some()}));
            insert_some(body, "reasoning_effort", reasoning_effort);
        }
        ThinkingFormat::StringThinking => {
            if let Some(value) = mapped {
                body.insert("thinking".to_owned(), value);
            } else if let Some(value) = off {
                body.insert("thinking".to_owned(), value);
            }
        }
        ThinkingFormat::OpenAi => {
            if effort.is_some() {
                insert_some(body, "reasoning_effort", reasoning_effort);
            } else if compat.supports_reasoning_effort
                && let Some(JsonValue::String(text)) = off
            {
                body.insert("reasoning_effort".to_owned(), json!(text));
            }
        }
    }
}

fn insert_some(body: &mut JsonObject, key: &str, value: Option<JsonValue>) {
    if let Some(value) = value {
        body.insert(key.to_owned(), value);
    }
}

fn convert_tools(tools: &[Tool], compat: &Compat) -> JsonValue {
    JsonValue::Array(
        tools
            .iter()
            .map(|tool| {
                let mut function = json!({
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.parameters,
                });
                if compat.supports_strict_mode {
                    function["strict"] = json!(false);
                }
                json!({"type": "function", "function": function})
            })
            .collect(),
    )
}

fn has_tool_history(messages: &[Message]) -> bool {
    messages.iter().any(|message| match message {
        Message::ToolResult(_) => true,
        Message::Assistant(assistant) => assistant
            .content
            .iter()
            .any(|block| matches!(block, AssistantBlock::ToolCall(_))),
        Message::System(_) | Message::User(_) => false,
    })
}

fn sanitize_id_part(text: &str) -> String {
    text.encode_utf16()
        .map(|unit| match u8::try_from(unit) {
            Ok(byte) if byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-' => {
                char::from(byte)
            }
            _ => '_',
        })
        .collect()
}

fn normalize_tool_call_id(id: &str, model: &Model) -> String {
    if let Some((call, item)) = id.split_once('|') {
        let call = sanitize_id_part(call);
        let item = sanitize_id_part(item);
        let combined = if item.is_empty() {
            call.clone()
        } else {
            format!("{call}_{item}")
        };
        if combined.encode_utf16().count() <= TOOL_CALL_ID_MAX_CHARS {
            return combined;
        }
        let hash: String = short_hash(id).chars().take(8).collect();
        let prefix: String = call
            .chars()
            .take(TOOL_CALL_ID_MAX_CHARS - hash.len() - 1)
            .collect();
        return format!("{prefix}_{hash}");
    }
    if model.provider == "openai" {
        return id.chars().take(TOOL_CALL_ID_MAX_CHARS).collect();
    }
    id.to_owned()
}

fn short_hash(text: &str) -> String {
    let mut h1: u32 = 0xdeadbeef;
    let mut h2: u32 = 0x41c6ce57;
    for unit in text.encode_utf16() {
        let ch = u32::from(unit);
        h1 = (h1 ^ ch).wrapping_mul(2654435761);
        h2 = (h2 ^ ch).wrapping_mul(1597334677);
    }
    h1 = (h1 ^ (h1 >> 16)).wrapping_mul(2246822507) ^ (h2 ^ (h2 >> 13)).wrapping_mul(3266489909);
    h2 = (h2 ^ (h2 >> 16)).wrapping_mul(2246822507) ^ (h1 ^ (h1 >> 13)).wrapping_mul(3266489909);
    format!("{}{}", to_base36(h2), to_base36(h1))
}

fn to_base36(mut value: u32) -> String {
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut digits = Vec::new();
    loop {
        digits.push(char::from(DIGITS[(value % 36) as usize]));
        value /= 36;
        if value == 0 {
            break;
        }
    }
    digits.into_iter().rev().collect()
}

fn data_url(mime_type: &str, data: &str) -> String {
    format!("data:{mime_type};base64,{data}")
}

fn user_parts(blocks: &[UserBlock]) -> Vec<JsonValue> {
    blocks
        .iter()
        .filter_map(|block| match block {
            UserBlock::Text(text) if text.text.is_empty() => None,
            UserBlock::Text(text) => Some(json!({"type": "text", "text": text.text})),
            UserBlock::Image(image) => Some(json!({
                "type": "image_url",
                "image_url": {"url": data_url(&image.mime_type, &image.data)},
            })),
        })
        .collect()
}

fn system_update_text(system: &SystemMessage) -> String {
    let mut parts = Vec::new();
    let text = system_content_text(&system.content);
    if !text.is_empty() {
        parts.push(text);
    }
    if let Some(sections) = &system.sections {
        for (name, value) in &sections.0 {
            parts.push(match value {
                None => format!("Removed system prompt section \"{name}\"."),
                Some(value) => format!("Updated system prompt section \"{name}\":\n\n{value}"),
            });
        }
    }
    parts.join("\n\n")
}

fn assistant_message(
    model: &Model,
    compat: &Compat,
    assistant: &AssistantMessage,
) -> Option<JsonValue> {
    let text_parts: Vec<&str> = assistant
        .content
        .iter()
        .filter_map(|block| match block {
            AssistantBlock::Text(text) if !text.text.trim().is_empty() => Some(text.text.as_str()),
            _ => None,
        })
        .collect();
    let thinking: Vec<&ThinkingContent> = assistant
        .content
        .iter()
        .filter_map(|block| match block {
            AssistantBlock::Thinking(thinking) if !thinking.thinking.trim().is_empty() => {
                Some(thinking)
            }
            _ => None,
        })
        .collect();
    let tool_calls: Vec<&ToolCall> = assistant
        .content
        .iter()
        .filter_map(|block| match block {
            AssistantBlock::ToolCall(call) => Some(call),
            _ => None,
        })
        .collect();
    let joined_text = text_parts.concat();
    let mut message = json!({
        "role": "assistant",
        "content": if compat.requires_assistant_after_tool_result { json!("") } else { JsonValue::Null },
    });
    if !joined_text.is_empty() {
        message["content"] = json!(joined_text);
    }
    if let Some(first) = thinking.first() {
        if compat.requires_thinking_as_text {
            let mut parts =
                vec![json!({"type": "text", "text": joined_thinking(&thinking, "\n\n")})];
            parts.extend(
                text_parts
                    .iter()
                    .map(|text| json!({"type": "text", "text": text})),
            );
            message["content"] = JsonValue::Array(parts);
        } else if let Some(field) = first
            .thinking_signature
            .as_deref()
            .map(|signature| {
                if assistant.provider == "opencode-go" && signature == "reasoning" {
                    "reasoning_content"
                } else {
                    signature
                }
            })
            .filter(|field| REASONING_FIELDS.contains(field))
        {
            message[field] = json!(joined_thinking(&thinking, "\n"));
        }
    }
    if !tool_calls.is_empty() {
        message["tool_calls"] = JsonValue::Array(
            tool_calls
                .iter()
                .map(|call| {
                    json!({
                        "id": call.id,
                        "type": "function",
                        "function": {
                            "name": call.name,
                            "arguments": JsonValue::Object(call.arguments.clone()).to_string(),
                        },
                    })
                })
                .collect(),
        );
    }
    if compat.requires_reasoning_content
        && model.reasoning
        && message.get("reasoning_content").is_none()
    {
        message["reasoning_content"] = json!("");
    }
    let has_content = match message.get("content") {
        Some(JsonValue::String(text)) => !text.is_empty(),
        Some(JsonValue::Array(parts)) => !parts.is_empty(),
        _ => false,
    };
    if !has_content && tool_calls.is_empty() {
        return None;
    }
    Some(message)
}

fn joined_thinking(thinking: &[&ThinkingContent], separator: &str) -> String {
    thinking
        .iter()
        .map(|block| block.thinking.as_str())
        .collect::<Vec<_>>()
        .join(separator)
}

fn tool_message(compat: &Compat, result: &ToolResultMessage) -> JsonValue {
    let text = result
        .content
        .iter()
        .filter_map(|block| match block {
            UserBlock::Text(text) => Some(text.text.as_str()),
            UserBlock::Image(_) => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    let has_images = result
        .content
        .iter()
        .any(|block| matches!(block, UserBlock::Image(_)));
    let content = if !text.is_empty() {
        text
    } else if has_images {
        "(see attached image)".to_owned()
    } else {
        "(no tool output)".to_owned()
    };
    let mut message = json!({
        "role": "tool",
        "content": content,
        "tool_call_id": result.tool_call_id,
    });
    if compat.requires_tool_result_name && !result.tool_name.is_empty() {
        message["name"] = json!(result.tool_name);
    }
    message
}

fn image_parts(blocks: &[UserBlock]) -> Vec<JsonValue> {
    blocks
        .iter()
        .filter_map(|block| match block {
            UserBlock::Image(image) => Some(json!({
                "type": "image_url",
                "image_url": {"url": data_url(&image.mime_type, &image.data)},
            })),
            UserBlock::Text(_) => None,
        })
        .collect()
}

fn convert_messages(model: &Model, compat: &Compat, messages: &[Message]) -> Vec<JsonValue> {
    let normalize =
        |id: &str, model: &Model, _: &AssistantMessage| normalize_tool_call_id(id, model);
    let normalizer: &ToolCallIdNormalizer = &normalize;
    let transformed = transform_messages(messages, model, Some(normalizer));
    let instruction_role = if model.reasoning && compat.supports_developer_role {
        "developer"
    } else {
        "system"
    };
    let accepts_images = model.input.iter().any(|kind| kind == "image");
    let mut out = Vec::new();
    let mut last_role: Option<&str> = None;
    let mut i = 0;
    while i < transformed.len() {
        let message = &transformed[i];
        if compat.requires_assistant_after_tool_result
            && last_role == Some("toolResult")
            && matches!(message, Message::User(_))
        {
            out.push(json!({"role": "assistant", "content": PROCESSED_TOOL_RESULTS}));
        }
        let role = match message {
            Message::System(system) => {
                let text = if i == 0 {
                    system_message_text(system)
                } else {
                    system_update_text(system)
                };
                if !text.is_empty() {
                    out.push(json!({"role": instruction_role, "content": text}));
                }
                "system"
            }
            Message::User(user) => {
                match &user.content {
                    UserContent::Text(text) => out.push(json!({"role": "user", "content": text})),
                    UserContent::Blocks(blocks) => {
                        let content = user_parts(blocks);
                        if content.is_empty() {
                            i += 1;
                            continue;
                        }
                        out.push(json!({"role": "user", "content": content}));
                    }
                }
                "user"
            }
            Message::Assistant(assistant) => {
                let Some(converted) = assistant_message(model, compat, assistant) else {
                    i += 1;
                    continue;
                };
                out.push(converted);
                "assistant"
            }
            Message::ToolResult(_) => {
                let mut images = Vec::new();
                let mut j = i;
                while let Some(Message::ToolResult(result)) = transformed.get(j) {
                    out.push(tool_message(compat, result));
                    if accepts_images {
                        images.extend(image_parts(&result.content));
                    }
                    j += 1;
                }
                if images.is_empty() {
                    last_role = Some("toolResult");
                } else {
                    if compat.requires_assistant_after_tool_result {
                        out.push(json!({"role": "assistant", "content": PROCESSED_TOOL_RESULTS}));
                    }
                    let mut content = vec![json!({
                        "type": "text",
                        "text": "Attached image(s) from tool result:",
                    })];
                    content.extend(images);
                    out.push(json!({"role": "user", "content": content}));
                    last_role = Some("user");
                }
                i = j;
                continue;
            }
        };
        last_role = Some(role);
        i += 1;
    }
    out
}
