use std::sync::LazyLock;

use regex::Regex;
use serde_json::{Map, Value, json};

use crate::ai::json_parse::trim_js;
use crate::ai::thinking::{clamp_thinking_level, level_name};
use crate::ai::types::{
    AssistantBlock, AssistantMessage, ImageContent, Message, Model, ModelThinkingLevel,
    SimpleStreamOptions, ThinkingLevel, Tool, ToolResultMessage, UserBlock, UserContent,
};

static GEMINI_MAJOR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^gemini(?:-live)?-([0-9]+)").expect("valid pattern"));
static GEMINI3_PRO: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)gemini-3(?:\.\d+)?-pro").expect("valid pattern"));
static GEMINI3_FLASH: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)gemini-3(?:\.\d+)?-flash").expect("valid pattern"));
static GEMMA4: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)gemma-?4").expect("valid pattern"));

pub fn contents(model: &Model, messages: &[Message]) -> Vec<Value> {
    let supports_images = model.input.iter().any(|kind| kind == "image");
    let mut contents: Vec<Value> = Vec::new();
    for message in messages {
        match message {
            Message::User(user) => {
                let parts = user_parts(&user.content);
                if !parts.is_empty() {
                    contents.push(json!({"role": "user", "parts": parts}));
                }
            }
            Message::Assistant(assistant) => {
                let parts = assistant_parts(model, assistant);
                if !parts.is_empty() {
                    contents.push(json!({"role": "model", "parts": parts}));
                }
            }
            Message::ToolResult(result) => {
                tool_result(model, result, supports_images, &mut contents);
            }
            Message::System(_) => {}
        }
    }
    contents
}

pub fn build(
    model: &Model,
    contents: Vec<Value>,
    system_prompt: Option<String>,
    tools: &[Tool],
    options: &SimpleStreamOptions,
) -> Result<Value, String> {
    let mut body = json!({"contents": contents});
    if let Some(text) = system_prompt {
        body["systemInstruction"] = json!({"role": "user", "parts": [{"text": text}]});
    }
    if !tools.is_empty() {
        body["tools"] = json!([{"functionDeclarations": declarations(tools)}]);
        if let Some(mode) = options
            .tool_choice
            .as_deref()
            .filter(|choice| !choice.is_empty())
            .map(|choice| match choice {
                "none" => "NONE",
                "any" => "ANY",
                _ => "AUTO",
            })
        {
            body["toolConfig"] = json!({"functionCallingConfig": {"mode": mode}});
        }
    }
    let mut generation = Map::new();
    if let Some(temperature) = options.temperature {
        generation.insert("temperature".to_owned(), json!(temperature));
    }
    if let Some(max_tokens) = options.max_tokens.filter(|tokens| *tokens > 0) {
        generation.insert("maxOutputTokens".to_owned(), json!(max_tokens));
    }
    if let Some(level) = options.reasoning.filter(|_| model.reasoning) {
        generation.insert("thinkingConfig".to_owned(), thinking_config(model, level)?);
    }
    if !generation.is_empty() {
        body["generationConfig"] = Value::Object(generation);
    }
    Ok(body)
}

fn user_parts(content: &UserContent) -> Vec<Value> {
    match content {
        UserContent::Text(text) => vec![json!({"text": text})],
        UserContent::Blocks(blocks) => blocks
            .iter()
            .map(|block| match block {
                UserBlock::Text(text) => json!({"text": text.text}),
                UserBlock::Image(image) => inline_data(image),
            })
            .collect(),
    }
}

fn inline_data(image: &ImageContent) -> Value {
    json!({"inlineData": {"mimeType": image.mime_type, "data": image.data}})
}

fn assistant_parts(model: &Model, message: &AssistantMessage) -> Vec<Value> {
    let same = message.provider == model.provider && message.model == model.id;
    let mut parts = Vec::new();
    for block in &message.content {
        match block {
            AssistantBlock::Text(text) => {
                let signature = text
                    .text_signature
                    .as_deref()
                    .filter(|sig| same && valid_signature(sig));
                if !trim_js(&text.text).is_empty() || signature.is_some() {
                    let mut part = json!({"text": text.text});
                    if let Some(signature) = signature {
                        part["thoughtSignature"] = json!(signature);
                    }
                    parts.push(part);
                }
            }
            AssistantBlock::Thinking(thinking) if same => {
                let signature = thinking
                    .thinking_signature
                    .as_deref()
                    .filter(|sig| valid_signature(sig));
                if trim_js(&thinking.thinking).is_empty() && signature.is_none() {
                    continue;
                }
                let mut part = json!({"text": thinking.thinking, "thought": true});
                if let Some(signature) = signature {
                    part["thoughtSignature"] = json!(signature);
                }
                parts.push(part);
            }
            AssistantBlock::Thinking(thinking) => {
                if !trim_js(&thinking.thinking).is_empty() {
                    parts.push(json!({"text": thinking.thinking}));
                }
            }
            AssistantBlock::ToolCall(call) => {
                let mut function = json!({"name": call.name, "args": call.arguments});
                if requires_tool_call_id(&model.id) {
                    function["id"] = json!(normalize_tool_call_id(&model.id, &call.id));
                }
                let mut part = json!({"functionCall": function});
                if let Some(signature) = call
                    .thought_signature
                    .as_deref()
                    .filter(|sig| same && valid_signature(sig))
                {
                    part["thoughtSignature"] = json!(signature);
                }
                parts.push(part);
            }
        }
    }
    parts
}

fn tool_result(
    model: &Model,
    result: &ToolResultMessage,
    supports_images: bool,
    contents: &mut Vec<Value>,
) {
    let mut texts: Vec<&str> = Vec::new();
    let mut images: Vec<Value> = Vec::new();
    for block in &result.content {
        match block {
            UserBlock::Text(text) => texts.push(&text.text),
            UserBlock::Image(image) if supports_images => images.push(inline_data(image)),
            UserBlock::Image(_) => {}
        }
    }
    let mut text = texts.join("\n");
    if text.is_empty() && !images.is_empty() {
        text = "(see attached image)".to_owned();
    }
    let payload = if result.is_error {
        json!({"error": text})
    } else {
        json!({"output": text})
    };
    let multimodal = supports_multimodal_function_response(&model.id);
    let mut response = json!({"name": result.tool_name, "response": payload});
    if multimodal && !images.is_empty() {
        response["parts"] = Value::Array(images.clone());
    }
    if requires_tool_call_id(&model.id) {
        response["id"] = json!(normalize_tool_call_id(&model.id, &result.tool_call_id));
    }
    let response_part = json!({"functionResponse": response});
    let continues_last = contents.last().is_some_and(|last| {
        last["role"] == "user" && last["parts"][0].get("functionResponse").is_some()
    });
    match contents.last_mut() {
        Some(last) if continues_last => {
            if let Some(parts) = last["parts"].as_array_mut() {
                parts.push(response_part);
            }
        }
        _ => contents.push(json!({"role": "user", "parts": [response_part]})),
    }
    if !images.is_empty() && !multimodal {
        let mut parts = vec![json!({"text": "Tool result image:"})];
        parts.extend(images);
        contents.push(json!({"role": "user", "parts": parts}));
    }
}

fn declarations(tools: &[Tool]) -> Vec<Value> {
    tools
        .iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "description": tool.description,
                "parametersJsonSchema": tool.parameters,
            })
        })
        .collect()
}

fn thinking_config(model: &Model, level: ThinkingLevel) -> Result<Value, String> {
    let id = model.id.as_str();
    let uses_level = GEMINI3_PRO.is_match(id)
        || GEMINI3_FLASH.is_match(id)
        || GEMMA4.is_match(id)
        || matches!(id, "gemini-flash-latest" | "gemini-flash-lite-latest");
    let requested = ModelThinkingLevel::from(level);
    let clamped = clamp_thinking_level(model, requested);
    if requested == ModelThinkingLevel::Off || clamped == ModelThinkingLevel::Off {
        if !uses_level {
            return Ok(json!({"thinkingBudget": 0}));
        }
        let fallback = clamp_thinking_level(model, ModelThinkingLevel::Off);
        if fallback == ModelThinkingLevel::Off {
            return Ok(json!({"thinkingBudget": 0}));
        }
        return Ok(json!({"thinkingLevel": resolve_level(model, fallback)?.to_uppercase()}));
    }
    let resolved = resolve_level(model, clamped)?;
    let mut config = json!({"includeThoughts": true});
    if uses_level {
        config["thinkingLevel"] = json!(resolved.to_uppercase());
    } else {
        config["thinkingBudget"] = json!(thinking_budget(resolved, id));
    }
    Ok(config)
}

fn resolve_level(model: &Model, level: ModelThinkingLevel) -> Result<&'static str, String> {
    let key = level_name(level);
    let mapped = model
        .thinking_level_map
        .as_ref()
        .and_then(|map| map.get(key));
    let text = match mapped {
        Some(Value::String(text)) => text.to_lowercase(),
        _ => key.to_owned(),
    };
    match text.as_str() {
        "minimal" => Ok("minimal"),
        "low" => Ok("low"),
        "medium" => Ok("medium"),
        "high" => Ok("high"),
        _ => {
            let mapping = match mapped {
                None => "undefined".to_owned(),
                Some(Value::String(raw)) => raw.clone(),
                Some(_) => "null".to_owned(),
            };
            Err(format!(
                "Unsupported Google thinking level mapping for {}/{}: {key} -> {mapping}",
                model.provider, model.id
            ))
        }
    }
}

fn thinking_budget(level: &str, model_id: &str) -> i64 {
    let (minimal, low, medium, high) = if model_id.contains("2.5-pro") {
        (128, 2048, 8192, 32768)
    } else if model_id.contains("2.5-flash-lite") {
        (512, 2048, 8192, 24576)
    } else if model_id.contains("2.5-flash") {
        (128, 2048, 8192, 24576)
    } else {
        return -1;
    };
    match level {
        "minimal" => minimal,
        "low" => low,
        "medium" => medium,
        _ => high,
    }
}

fn major_version(model_id: &str) -> Option<u64> {
    GEMINI_MAJOR
        .captures(model_id)
        .and_then(|captures| captures[1].parse().ok())
}

fn requires_tool_call_id(model_id: &str) -> bool {
    if model_id.starts_with("claude-") || model_id.starts_with("gpt-oss-") {
        return true;
    }
    major_version(model_id).is_some_and(|major| major >= 3)
}

fn supports_multimodal_function_response(model_id: &str) -> bool {
    major_version(model_id).is_none_or(|major| major >= 3)
}

fn normalize_tool_call_id(model_id: &str, id: &str) -> String {
    if !requires_tool_call_id(model_id) {
        return id.to_owned();
    }
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect()
}

fn valid_signature(sig: &str) -> bool {
    let bytes = sig.as_bytes();
    if bytes.is_empty() || !bytes.len().is_multiple_of(4) {
        return false;
    }
    let pad = bytes.iter().rev().take_while(|byte| **byte == b'=').count();
    pad <= 2
        && pad < bytes.len()
        && bytes[..bytes.len() - pad]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'+' || *byte == b'/')
}
