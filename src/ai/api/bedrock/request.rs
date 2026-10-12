use serde_json::{Value, json};

use super::config::env;
use crate::ai::simple_options::{
    adjust_max_tokens_for_thinking, cache_retention, resolve_sampling_params,
};
use crate::ai::thinking::level_name;
use crate::ai::types::{
    AssistantBlock, CachePoint, CacheRetention, ImageContent, Message, Model, ModelThinkingLevel,
    SimpleStreamOptions, ThinkingLevel, Tool, ToolResultMessage, UserBlock, UserContent,
};
use crate::ai::utils::transcript::without_initial_system_message;

const PLACEHOLDER: &str = "<empty>";
const INTERLEAVED_BETA: &str = "interleaved-thinking-2025-05-14";
const THINKING_BINDING_BETA: &str = "thinking-binding-controls-2026-08-01";
const MAX_TOOL_ID_LEN: usize = 64;
const ADAPTIVE: [&str; 7] = [
    "opus-4-6",
    "opus-4-7",
    "opus-4-8",
    "opus-5",
    "sonnet-4-6",
    "sonnet-5",
    "fable-5",
];
const NEXT_GEN: [&str; 5] = ["opus-4-7", "opus-4-8", "opus-5", "sonnet-5", "fable-5"];

pub fn build(
    model: &Model,
    messages: &[Message],
    system: Option<&str>,
    tools: &[Tool],
    options: &SimpleStreamOptions,
) -> Result<Value, String> {
    let claude = has(model, &["claude"]);
    let retention = cache_retention(options);
    let caching = retention != CacheRetention::None && prompt_caching(model);
    let mark = caching.then(|| cache_point(retention));
    let conversation = without_initial_system_message(messages);
    let offset = messages.len() - conversation.len();
    let targets = if caching {
        cache_targets(messages, &options.cache_points)?
            .into_iter()
            .map(|(index, block)| (index - offset, block))
            .collect()
    } else {
        Vec::new()
    };
    let wire = conversation_json(conversation, &targets, mark.as_ref(), claude)?;
    let mut body = json!({
        "messages": wire,
        "inferenceConfig": {},
    });
    let max_tokens = options
        .max_tokens
        .filter(|tokens| *tokens > 0)
        .or(claude.then_some(model.max_tokens))
        .filter(|tokens| *tokens > 0);
    if let Some(tokens) = max_tokens {
        body["inferenceConfig"]["maxTokens"] = json!(tokens);
    }
    if let Some(temperature) = temperature(model, options) {
        body["inferenceConfig"]["temperature"] = json!(temperature);
    }
    if let Some(text) = system.filter(|text| !text.is_empty()) {
        let mut blocks = vec![json!({ "text": text })];
        blocks.extend(mark);
        body["system"] = Value::Array(blocks);
    }
    if !tools.is_empty() {
        let specs: Vec<Value> = tools.iter().map(tool_spec).collect();
        body["toolConfig"] = json!({ "tools": specs });
    }
    if let Some(fields) = additional_fields(model, options) {
        body["additionalModelRequestFields"] = fields;
    }
    Ok(body)
}

fn temperature(model: &Model, options: &SimpleStreamOptions) -> Option<f64> {
    let level = options
        .reasoning
        .map_or(ModelThinkingLevel::Off, ModelThinkingLevel::from);
    resolve_sampling_params(model, level, None)?
        .get("temperature")?
        .as_f64()
}

fn candidates(model: &Model) -> Vec<String> {
    [model.id.as_str(), model.name.as_str()]
        .into_iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .flat_map(|lower| {
            let normalized = lower.replace([' ', '_', '.', ':'], "-");
            [lower, normalized]
        })
        .collect()
}

fn has(model: &Model, needles: &[&str]) -> bool {
    let candidates = candidates(model);
    needles.iter().any(|needle| {
        candidates
            .iter()
            .any(|candidate| candidate.contains(needle))
    })
}

fn prompt_caching(model: &Model) -> bool {
    if !has(model, &["claude"]) {
        return env("AWS_BEDROCK_FORCE_CACHE").as_deref() == Some("1");
    }
    has(model, &["fable-5", "opus-5", "sonnet-5"])
        || has(model, &["-4-", "claude-3-7-sonnet", "claude-3-5-haiku"])
}

fn govcloud(model: &Model) -> bool {
    let region = env("AWS_REGION")
        .or_else(|| env("AWS_DEFAULT_REGION"))
        .unwrap_or_default();
    let id = model.id.to_ascii_lowercase();
    region.starts_with("us-gov-") || id.starts_with("us-gov.") || id.starts_with("arn:aws-us-gov:")
}

fn cache_point(retention: CacheRetention) -> Value {
    let mut point = json!({ "type": "default" });
    if retention == CacheRetention::Long {
        point["ttl"] = json!("1h");
    }
    json!({ "cachePoint": point })
}

fn additional_fields(model: &Model, options: &SimpleStreamOptions) -> Option<Value> {
    let level = options.reasoning?;
    if !model.reasoning || !has(model, &["claude"]) {
        return None;
    }
    let gov = govcloud(model);
    let mut thinking = json!({});
    if !gov {
        thinking["display"] = json!("summarized");
    }
    if has(model, &ADAPTIVE) {
        thinking["type"] = json!("adaptive");
        let mut fields = json!({ "output_config": { "effort": effort(model, level) } });
        if !gov && has(model, &NEXT_GEN) {
            thinking["block_binding"] = json!({ "prefix_mismatch_behavior": "drop_block" });
            fields["anthropic_beta"] = json!([THINKING_BINDING_BETA]);
        }
        fields["thinking"] = thinking;
        return Some(fields);
    }
    let budget = adjust_max_tokens_for_thinking(
        options.max_tokens.filter(|tokens| *tokens > 0),
        model.max_tokens,
        level,
        None,
    )
    .thinking_budget;
    thinking["type"] = json!("enabled");
    thinking["budget_tokens"] = json!(budget);
    Some(json!({
        "thinking": thinking,
        "anthropic_beta": [INTERLEAVED_BETA],
    }))
}

fn effort(model: &Model, level: ThinkingLevel) -> String {
    if level == ThinkingLevel::Xhigh && has(model, &NEXT_GEN) {
        return "xhigh".to_owned();
    }
    let key = level_name(ModelThinkingLevel::from(level));
    if let Some(mapped) = model
        .thinking_level_map
        .as_ref()
        .and_then(|map| map.get(key))
        .and_then(Value::as_str)
    {
        return mapped.to_owned();
    }
    match level {
        ThinkingLevel::Minimal | ThinkingLevel::Low => "low",
        ThinkingLevel::Medium => "medium",
        ThinkingLevel::High | ThinkingLevel::Xhigh | ThinkingLevel::Max => "high",
    }
    .to_owned()
}

fn mark_target(messages: &[Message], point: &CachePoint) -> Option<(usize, Option<usize>)> {
    match (messages.get(point.message)?, point.block) {
        (Message::User(_), None) => Some((point.message, None)),
        (Message::User(user), Some(block)) => match &user.content {
            UserContent::Blocks(blocks) => blocks
                .get(block)
                .filter(|block| user_block_kept(block))
                .map(|_| (point.message, Some(block))),
            UserContent::Text(_) => (block == 0).then_some((point.message, None)),
        },
        (Message::ToolResult(_), None | Some(0)) => Some((point.message, None)),
        _ => None,
    }
}

fn user_block_kept(block: &UserBlock) -> bool {
    match block {
        UserBlock::Text(text) => !text.text.trim().is_empty(),
        UserBlock::Image(_) => true,
    }
}

fn cache_targets(
    messages: &[Message],
    points: &[CachePoint],
) -> Result<Vec<(usize, Option<usize>)>, String> {
    points
        .iter()
        .map(|point| {
            mark_target(messages, point).ok_or_else(|| {
                format!(
                    "amazon-bedrock: cache point {}:{:?} names no markable block",
                    point.message, point.block
                )
            })
        })
        .collect()
}

fn conversation_json(
    messages: &[Message],
    targets: &[(usize, Option<usize>)],
    mark: Option<&Value>,
    claude: bool,
) -> Result<Vec<Value>, String> {
    let marked = |index: usize, block: Option<usize>| {
        mark.filter(|_| targets.contains(&(index, block))).cloned()
    };
    let mut out = Vec::new();
    let mut results = Vec::new();
    for (index, message) in messages.iter().enumerate() {
        if let Message::ToolResult(result) = message {
            results.push(tool_result(result)?);
            results.extend(marked(index, None));
            continue;
        }
        flush_results(&mut out, &mut results);
        match message {
            Message::User(user) => push_message(
                &mut out,
                "user",
                user_blocks(&user.content, |block| marked(index, block))?,
            ),
            Message::Assistant(assistant) => push_message(
                &mut out,
                "assistant",
                assistant_blocks(&assistant.content, claude),
            ),
            Message::System(_) | Message::ToolResult(_) => {}
        }
    }
    flush_results(&mut out, &mut results);
    if let Some(mark) = mark
        && let Some(last) = out.last_mut().filter(|last| last["role"] == "user")
        && let Some(content) = last["content"].as_array_mut()
        && content.last() != Some(mark)
    {
        content.push(mark.clone());
    }
    Ok(out)
}

fn flush_results(out: &mut Vec<Value>, results: &mut Vec<Value>) {
    if !results.is_empty() {
        push_message(out, "user", std::mem::take(results));
    }
}

fn push_message(out: &mut Vec<Value>, role: &str, content: Vec<Value>) {
    if !content.is_empty() {
        out.push(json!({ "role": role, "content": content }));
    }
}

fn text_block(text: &str) -> Option<Value> {
    (!text.trim().is_empty()).then(|| json!({ "text": text }))
}

fn user_blocks(
    content: &UserContent,
    marked: impl Fn(Option<usize>) -> Option<Value>,
) -> Result<Vec<Value>, String> {
    let mut out = Vec::new();
    match content {
        UserContent::Text(text) => {
            let text = if text.trim().is_empty() {
                PLACEHOLDER
            } else {
                text.as_str()
            };
            out.push(json!({ "text": text }));
        }
        UserContent::Blocks(blocks) => {
            for (index, block) in blocks.iter().enumerate() {
                match block {
                    UserBlock::Text(text) => out.extend(text_block(&text.text)),
                    UserBlock::Image(image) => out.push(image_block(image)?),
                }
                out.extend(marked(Some(index)));
            }
            if out.is_empty() {
                out.push(json!({ "text": PLACEHOLDER }));
            }
        }
    }
    out.extend(marked(None));
    Ok(out)
}

fn assistant_blocks(blocks: &[AssistantBlock], claude: bool) -> Vec<Value> {
    blocks
        .iter()
        .filter_map(|block| match block {
            AssistantBlock::Text(text) => text_block(&text.text),
            AssistantBlock::Thinking(thinking) if thinking.redacted == Some(true) => thinking
                .thinking_signature
                .as_deref()
                .and_then(redacted_content)
                .map(|data| json!({ "reasoningContent": { "redactedContent": data } })),
            AssistantBlock::Thinking(thinking) => {
                let signature = thinking
                    .thinking_signature
                    .as_deref()
                    .filter(|signature| !signature.trim().is_empty());
                match (claude, signature) {
                    _ if thinking.thinking.trim().is_empty() => None,
                    (true, None) => text_block(&thinking.thinking),
                    (true, Some(signature)) => Some(json!({
                        "reasoningContent": {
                            "reasoningText": { "text": thinking.thinking, "signature": signature },
                        },
                    })),
                    (false, _) => Some(json!({
                        "reasoningContent": { "reasoningText": { "text": thinking.thinking } },
                    })),
                }
            }
            AssistantBlock::ToolCall(call) => Some(json!({
                "toolUse": {
                    "toolUseId": tool_id(&call.id),
                    "name": call.name,
                    "input": Value::Object(call.arguments.clone()),
                },
            })),
        })
        .collect()
}

fn redacted_content(signature: &str) -> Option<String> {
    let mut data: String = signature
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect();
    if data.is_empty() || data.len() % 4 == 1 {
        return None;
    }
    if !data.contains('=') {
        while !data.len().is_multiple_of(4) {
            data.push('=');
        }
    }
    Some(data)
}

fn tool_result(result: &ToolResultMessage) -> Result<Value, String> {
    if result.tool_call_id.is_empty() {
        return Err("amazon-bedrock: tool result missing toolCallId".to_owned());
    }
    let mut content = Vec::new();
    for block in &result.content {
        match block {
            UserBlock::Text(text) => content.extend(text_block(&text.text)),
            UserBlock::Image(image) => content.push(image_block(image)?),
        }
    }
    if content.is_empty() {
        content.push(json!({ "text": PLACEHOLDER }));
    }
    let status = if result.is_error { "error" } else { "success" };
    Ok(json!({
        "toolResult": {
            "toolUseId": tool_id(&result.tool_call_id),
            "content": content,
            "status": status,
        },
    }))
}

fn image_block(image: &ImageContent) -> Result<Value, String> {
    let format = match image.mime_type.as_str() {
        "image/jpeg" | "image/jpg" => "jpeg",
        "image/png" => "png",
        "image/gif" => "gif",
        "image/webp" => "webp",
        other => return Err(format!("amazon-bedrock: unknown image type {other:?}")),
    };
    Ok(json!({
        "image": { "format": format, "source": { "bytes": image.data } },
    }))
}

fn tool_id(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(MAX_TOOL_ID_LEN)
        .collect()
}

fn tool_spec(tool: &Tool) -> Value {
    json!({
        "toolSpec": {
            "name": tool.name,
            "description": tool.description,
            "inputSchema": { "json": tool.parameters },
        },
    })
}
