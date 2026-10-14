//! Request shaping for the Anthropic Messages API: transcript to wire messages, tools, thinking,
//! cache breakpoints and beta features, as pi-ai's Anthropic adapter builds them.

use serde_json::{Value, json};

use crate::ai::simple_options::{adjust_max_tokens_for_thinking, clamp_max_tokens_to_context};
use crate::ai::thinking::level_name;
use crate::ai::transform::transform_messages_sourced;
use crate::ai::types::{
    AssistantBlock, CacheRetention, ImageContent, Message, Model, ModelThinkingLevel,
    SimpleStreamOptions, SystemMessage, ThinkingLevel, Tool, ToolResultMessage, UserBlock,
    UserContent,
};
use crate::ai::utils::text::{system_content_text, system_message_text};
use crate::ai::utils::transcript::{get_current_tools, get_initial_system_message};

const FINE_GRAINED_TOOL_STREAMING_BETA: &str = "fine-grained-tool-streaming-2025-05-14";
const INTERLEAVED_THINKING_BETA: &str = "interleaved-thinking-2025-05-14";
const SERVER_SIDE_FALLBACK_BETA: &str = "server-side-fallback-2026-07-01";
const MID_CONVERSATION_OUTPUT_CONFIG_BETA: &str = "mid-conversation-output-config-2026-07-01";
const THINKING_BINDING_CONTROLS_BETA: &str = "thinking-binding-controls-2026-08-01";
const INLINE_TOOLS_BETA: &str = "inline-tools-2026-09-15";
const CLAUDE_CODE_BETAS: [&str; 2] = ["claude-code-20250219", "oauth-2025-04-20"];
const CLAUDE_CODE_IDENTITY: &str = "You are Claude Code, Anthropic's official CLI for Claude.";
const CLAUDE_CODE_TOOLS: [&str; 17] = [
    "Read",
    "Write",
    "Edit",
    "Bash",
    "Grep",
    "Glob",
    "AskUserQuestion",
    "EnterPlanMode",
    "ExitPlanMode",
    "KillShell",
    "NotebookEdit",
    "Skill",
    "Task",
    "TaskOutput",
    "TodoWrite",
    "WebFetch",
    "WebSearch",
];

pub enum Reasoning {
    Adaptive(String),
    Budget(u64),
}

pub struct Built {
    pub body: Value,
    pub betas: Vec<String>,
    pub provider_thinking_level: Option<String>,
}

pub fn flag(model: &Model, name: &str, default: bool) -> bool {
    model
        .compat
        .as_ref()
        .and_then(|compat| compat.get(name))
        .and_then(Value::as_bool)
        .unwrap_or(default)
}

pub fn build(
    model: &Model,
    messages: &[Message],
    oauth: bool,
    options: &SimpleStreamOptions,
) -> Result<Built, String> {
    let normalize =
        |id: &str, _: &Model, _: &crate::ai::types::AssistantMessage| normalize_tool_call_id(id);
    let (transformed, sources): (Vec<Message>, Vec<Option<usize>>) =
        transform_messages_sourced(messages, model, Some(&normalize))
            .into_iter()
            .unzip();
    let initial = get_initial_system_message(messages);
    let system_text = initial.map(system_message_text).unwrap_or_default();
    let offset = usize::from(initial.is_some());
    let conversation = transformed.get(offset..).unwrap_or_default();
    let sources = sources.get(offset..).unwrap_or_default();
    let cache = cache_control(model, options.cache_retention);
    let targets = cache_targets(messages, &options.cache_points)?;
    let tail = if targets.is_empty() {
        cache.as_ref()
    } else {
        None
    };
    let eager = flag(model, "supportsEagerToolInputStreaming", true);
    let initial_tools = initial
        .and_then(|system| system.tools_added.clone())
        .unwrap_or_default();
    let native = flag(model, "supportsMidConvoSystemMessages", false)
        && flag(model, "supportsMidConvoToolChanges", false)
        && !initial_tools.is_empty();
    let tools = if native {
        initial_tools
    } else {
        get_current_tools(messages)
    };
    let mid_effort = flag(model, "supportsMidConvoEffort", false);
    let (max_tokens, reasoning) = plan(model, messages, options);
    let active_effort = match &reasoning {
        Some(Reasoning::Adaptive(effort)) => effort.clone(),
        _ => "high".to_owned(),
    };
    let (converted, levels) = convert(
        conversation,
        Marks {
            cache: cache.as_ref(),
            targets: &targets,
            tail,
            sources,
        },
        oauth,
        flag(model, "allowEmptySignature", false),
        mid_effort.then_some(model.provider.as_str()),
        native,
        eager,
    );
    let messages_json = if mid_effort {
        insert_thinking_levels(converted, &levels, &active_effort)
    } else {
        converted
    };
    let mut body = json!({
        "model": model.id,
        "messages": messages_json,
        "max_tokens": max_tokens,
        "stream": true,
    });
    let mut system = Vec::new();
    if oauth {
        system.push(text_block(CLAUDE_CODE_IDENTITY, cache.as_ref()));
    }
    if !system_text.is_empty() {
        system.push(text_block(&system_text, cache.as_ref()));
    }
    if !system.is_empty() {
        body["system"] = Value::Array(system);
    }
    if !tools.is_empty() {
        let tool_cache = cache
            .as_ref()
            .filter(|_| flag(model, "supportsCacheControlOnTools", true));
        let mut definitions = convert_tools(&tools, oauth, eager, tool_cache);
        if native {
            definitions.push(deferred_placeholder());
        }
        body["tools"] = Value::Array(definitions);
    }
    if mid_effort {
        body["thinking"] = json!({
            "type": "adaptive",
            "display": "summarized",
            "block_binding": {"prefix_mismatch_behavior": "drop_block"},
        });
        body["output_config"] = json!({"effort": "high"});
    } else if model.reasoning {
        match &reasoning {
            Some(Reasoning::Adaptive(effort)) if flag(model, "forceAdaptiveThinking", false) => {
                body["thinking"] = json!({"type": "adaptive", "display": "summarized"});
                body["output_config"] = json!({"effort": effort});
            }
            Some(Reasoning::Budget(tokens)) => {
                body["thinking"] = json!({
                    "type": "enabled",
                    "budget_tokens": tokens,
                    "display": "summarized",
                });
            }
            None if !off_is_null(model) => {
                body["thinking"] = json!({"type": "disabled"});
            }
            _ => {}
        }
    }
    if let Some(user_id) = options
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("user_id"))
        .and_then(Value::as_str)
    {
        body["metadata"] = json!({"user_id": user_id});
    }
    let fallbacks = fallback_models(model);
    if !fallbacks.is_empty() {
        body["fallbacks"] = Value::Array(fallbacks.clone());
    }
    let fine_grained = !tools.is_empty() && !flag(model, "supportsEagerToolInputStreaming", true);
    let interleaved = model.reasoning
        && matches!(reasoning, Some(Reasoning::Budget(_)))
        && !flag(model, "forceAdaptiveThinking", false);
    let betas = betas(
        model,
        options,
        &[
            (oauth, CLAUDE_CODE_BETAS.as_slice()),
            (fine_grained, &[FINE_GRAINED_TOOL_STREAMING_BETA]),
            (interleaved, &[INTERLEAVED_THINKING_BETA]),
            (!fallbacks.is_empty(), &[SERVER_SIDE_FALLBACK_BETA]),
            (native, &[INLINE_TOOLS_BETA]),
            (
                mid_effort,
                &[
                    MID_CONVERSATION_OUTPUT_CONFIG_BETA,
                    THINKING_BINDING_CONTROLS_BETA,
                ],
            ),
        ],
    );
    if !targets.is_empty() && cache.is_some() {
        keep_last_prefix_mark(&mut body);
    }
    Ok(Built {
        body,
        betas,
        provider_thinking_level: mid_effort.then_some(active_effort),
    })
}

pub fn from_claude_code_name(name: &str, tools: &[Tool]) -> String {
    let lower = name.to_lowercase();
    tools
        .iter()
        .find(|tool| tool.name.to_lowercase() == lower)
        .map_or_else(|| name.to_owned(), |tool| tool.name.clone())
}

fn to_claude_code_name(name: &str) -> String {
    CLAUDE_CODE_TOOLS
        .iter()
        .find(|tool| tool.eq_ignore_ascii_case(name))
        .map_or_else(|| name.to_owned(), |tool| (*tool).to_owned())
}

fn normalize_tool_call_id(id: &str) -> String {
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

const MAX_CACHE_POINTS: usize = 2;

fn cache_targets(
    messages: &[Message],
    points: &[crate::ai::types::CachePoint],
) -> Result<Vec<(usize, Option<usize>)>, String> {
    if points.len() > MAX_CACHE_POINTS {
        return Err(format!(
            "at most {MAX_CACHE_POINTS} cache points: with the prefix mark that is Go's three"
        ));
    }
    let mut targets: Vec<(usize, Option<usize>)> = Vec::new();
    for point in points {
        let Some(message) = messages.get(point.message) else {
            return Err(format!(
                "cache point message {} is out of range",
                point.message
            ));
        };
        let valid = match message {
            Message::User(user) => match (&user.content, point.block) {
                (UserContent::Text(text), block) => {
                    block.unwrap_or(0) == 0 && !text.trim().is_empty()
                }
                (UserContent::Blocks(blocks), Some(block)) => {
                    blocks.get(block).is_some_and(user_block_kept)
                }
                (UserContent::Blocks(blocks), None) => blocks.iter().any(user_block_kept),
            },
            Message::ToolResult(_) => matches!(point.block, None | Some(0)),
            Message::System(_) | Message::Assistant(_) => false,
        };
        if !valid {
            return Err(format!(
                "cache point {}:{:?} names no markable block",
                point.message, point.block
            ));
        }
        let target = (point.message, point.block);
        if targets.contains(&target) {
            return Err(format!(
                "cache point {}:{:?} is listed twice",
                point.message, point.block
            ));
        }
        targets.push(target);
    }
    Ok(targets)
}

fn marks_for(targets: &[(usize, Option<usize>)], source: Option<usize>) -> Vec<Option<usize>> {
    targets
        .iter()
        .filter(|(at, _)| Some(*at) == source)
        .map(|(_, block)| *block)
        .collect()
}

fn user_block_kept(block: &UserBlock) -> bool {
    match block {
        UserBlock::Text(text) => !text.text.trim().is_empty(),
        UserBlock::Image(_) => true,
    }
}

fn keep_last_prefix_mark(body: &mut Value) {
    let system_marked = body["system"].as_array().is_some_and(|blocks| {
        blocks
            .iter()
            .any(|block| block.get("cache_control").is_some())
    });
    if !system_marked {
        return;
    }
    if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) {
        for tool in tools {
            if let Some(object) = tool.as_object_mut() {
                object.remove("cache_control");
            }
        }
    }
    if let Some(blocks) = body["system"].as_array_mut() {
        let last = blocks
            .iter()
            .rposition(|block| block.get("cache_control").is_some());
        for (index, block) in blocks.iter_mut().enumerate() {
            if Some(index) != last
                && let Some(object) = block.as_object_mut()
            {
                object.remove("cache_control");
            }
        }
    }
}

fn cache_control(model: &Model, retention: Option<CacheRetention>) -> Option<Value> {
    let env_long = (std::env::var("PI_CACHE_RETENTION").as_deref() == Ok("long"))
        .then_some(CacheRetention::Long);
    match retention.or(env_long).unwrap_or(CacheRetention::Short) {
        CacheRetention::None => None,
        CacheRetention::Long if flag(model, "supportsLongCacheRetention", true) => {
            Some(json!({"type": "ephemeral", "ttl": "1h"}))
        }
        CacheRetention::Short | CacheRetention::Long => Some(json!({"type": "ephemeral"})),
    }
}

fn off_is_null(model: &Model) -> bool {
    model
        .thinking_level_map
        .as_ref()
        .and_then(|map| map.get("off"))
        == Some(&Value::Null)
}

fn plan(
    model: &Model,
    messages: &[Message],
    options: &SimpleStreamOptions,
) -> (u64, Option<Reasoning>) {
    let Some(level) = options.reasoning else {
        return (options.max_tokens.unwrap_or(model.max_tokens), None);
    };
    if flag(model, "forceAdaptiveThinking", false) {
        return (
            options.max_tokens.unwrap_or(model.max_tokens),
            Some(Reasoning::Adaptive(effort(model, level))),
        );
    }
    let adjusted =
        adjust_max_tokens_for_thinking(options.max_tokens, model.max_tokens, level, None);
    let max_tokens = clamp_max_tokens_to_context(model, messages, adjusted.max_tokens);
    let budget = adjusted
        .thinking_budget
        .min(max_tokens.saturating_sub(1024));
    (
        max_tokens,
        Some(Reasoning::Budget(if budget == 0 { 1024 } else { budget })),
    )
}

fn effort(model: &Model, level: ThinkingLevel) -> String {
    let mapped = model
        .thinking_level_map
        .as_ref()
        .and_then(|map| map.get(level_name(ModelThinkingLevel::from(level))))
        .and_then(Value::as_str);
    match (mapped, level) {
        (Some(effort), _) => effort.to_owned(),
        (None, ThinkingLevel::Minimal | ThinkingLevel::Low) => "low".to_owned(),
        (None, ThinkingLevel::Medium) => "medium".to_owned(),
        (None, _) => "high".to_owned(),
    }
}

fn fallback_models(model: &Model) -> Vec<Value> {
    model
        .compat
        .as_ref()
        .and_then(|compat| compat.get("allowedFallbackModels"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|fallback| fallback.get("model"))
        .map(|name| json!({"model": name}))
        .collect()
}

fn betas(
    model: &Model,
    options: &SimpleStreamOptions,
    features: &[(bool, &[&str])],
) -> Vec<String> {
    let mut configured = None;
    for headers in [model.headers.as_ref(), options.headers.as_ref()]
        .into_iter()
        .flatten()
    {
        for (name, value) in headers {
            if name.eq_ignore_ascii_case("anthropic-beta") {
                configured = Some(value.as_str());
            }
        }
    }
    let list: Vec<String> = match configured {
        Some(list) => list
            .split(',')
            .map(str::trim)
            .filter(|f| !f.is_empty())
            .map(str::to_owned)
            .collect(),
        None => features
            .iter()
            .filter(|(enabled, _)| *enabled)
            .flat_map(|(_, names)| names.iter().map(|name| (*name).to_owned()))
            .collect(),
    };
    let mut unique: Vec<String> = Vec::new();
    for feature in list {
        if !unique.contains(&feature) {
            unique.push(feature);
        }
    }
    unique
}

fn deferred_placeholder() -> Value {
    json!({
        "name": "__pi_deferred_placeholder__",
        "description": "Reserved placeholder. Never available. Never call this.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
        "defer_loading": true,
    })
}

fn text_block(text: &str, cache: Option<&Value>) -> Value {
    let mut block = json!({"type": "text", "text": text});
    if let Some(cache) = cache {
        block["cache_control"] = cache.clone();
    }
    block
}

fn image_block(image: &ImageContent) -> Value {
    json!({
        "type": "image",
        "source": {"type": "base64", "media_type": image.mime_type, "data": image.data},
    })
}

fn render_system_update(system: &SystemMessage) -> String {
    let mut parts = Vec::new();
    let text = system_content_text(&system.content);
    if !text.is_empty() {
        parts.push(text);
    }
    for (name, value) in system.sections.iter().flat_map(|sections| &sections.0) {
        parts.push(match value {
            None => format!("Removed system prompt section \"{name}\"."),
            Some(value) => format!("Updated system prompt section \"{name}\":\n\n{value}"),
        });
    }
    parts.join("\n\n")
}

struct Marks<'a> {
    cache: Option<&'a Value>,
    targets: &'a [(usize, Option<usize>)],
    tail: Option<&'a Value>,
    sources: &'a [Option<usize>],
}

fn convert(
    messages: &[Message],
    marks: Marks<'_>,
    oauth: bool,
    empty_signature: bool,
    managed_provider: Option<&str>,
    native: bool,
    eager: bool,
) -> (Vec<Value>, Vec<(usize, String)>) {
    let Marks {
        cache,
        targets,
        tail,
        sources,
    } = marks;
    let mut params: Vec<Value> = Vec::new();
    let mut levels: Vec<(usize, String)> = Vec::new();
    let mut pending: Vec<Value> = Vec::new();
    let mut index = 0;
    while let Some(message) = messages.get(index) {
        match message {
            Message::System(system) => {
                let text = render_system_update(system);
                let mut blocks = Vec::new();
                if !text.is_empty() {
                    blocks.push(json!({"type": "text", "text": text}));
                }
                if native {
                    let added = system.tools_added.as_deref().unwrap_or_default();
                    for removed in system.tools_removed.as_deref().unwrap_or_default() {
                        if added.iter().any(|tool| tool.name == removed.name) {
                            continue;
                        }
                        let name = if oauth {
                            to_claude_code_name(&removed.name)
                        } else {
                            removed.name.clone()
                        };
                        blocks.push(json!({
                            "type": "tool_removal",
                            "tool": {"type": "tool_reference", "name": name},
                        }));
                    }
                    for definition in convert_tools(added, oauth, eager, None) {
                        blocks.push(json!({
                            "type": "tool_addition",
                            "tool": {"type": "tool_definition", "definition": definition},
                        }));
                    }
                }
                if !blocks.is_empty() {
                    pending.push(json!({"role": "system", "content": blocks}));
                }
            }
            Message::User(user) => {
                let marks = marks_for(targets, sources[index]);
                match &user.content {
                    UserContent::Text(text) => {
                        if !text.trim().is_empty() {
                            let content = match cache {
                                Some(cache) if !marks.is_empty() => {
                                    json!([{"type": "text", "text": text, "cache_control": cache}])
                                }
                                _ => json!(text),
                            };
                            params.push(json!({"role": "user", "content": content}));
                        }
                    }
                    UserContent::Blocks(blocks) => {
                        let mut kept: Vec<(usize, Value)> = blocks
                            .iter()
                            .enumerate()
                            .filter_map(|(at, block)| match block {
                                UserBlock::Text(text) => (!text.text.trim().is_empty())
                                    .then(|| (at, json!({"type": "text", "text": text.text}))),
                                UserBlock::Image(image) => Some((at, image_block(image))),
                            })
                            .collect();
                        if let Some(cache) = cache {
                            for mark in &marks {
                                let at = match mark {
                                    Some(block) => kept.iter().position(|(at, _)| *at == *block),
                                    None => kept.len().checked_sub(1),
                                };
                                if let Some(at) = at {
                                    kept[at].1["cache_control"] = cache.clone();
                                }
                            }
                        }
                        let blocks: Vec<Value> = kept.into_iter().map(|(_, block)| block).collect();
                        if !blocks.is_empty() {
                            params.push(json!({"role": "user", "content": blocks}));
                        }
                    }
                }
            }
            Message::Assistant(assistant) => {
                params.append(&mut pending);
                let blocks = assistant_blocks(&assistant.content, oauth, empty_signature);
                if !blocks.is_empty() {
                    let managed = managed_provider.is_some_and(|provider| {
                        assistant.api == "anthropic-messages" && assistant.provider == provider
                    });
                    if let Some(level) = assistant
                        .provider_thinking_level
                        .as_deref()
                        .filter(|level| managed && is_effort(level))
                    {
                        levels.push((params.len(), level.to_owned()));
                    }
                    params.push(json!({"role": "assistant", "content": blocks}));
                }
            }
            Message::ToolResult(_) => {
                let mut results = Vec::new();
                while let Some(Message::ToolResult(result)) = messages.get(index) {
                    let mut value = tool_result(result);
                    if let Some(cache) = cache {
                        for mark in marks_for(targets, sources[index]) {
                            if matches!(mark, None | Some(0)) {
                                value["cache_control"] = cache.clone();
                            }
                        }
                    }
                    results.push(value);
                    index += 1;
                }
                params.push(json!({"role": "user", "content": results}));
                continue;
            }
        }
        index += 1;
    }
    params.append(&mut pending);
    if let (Some(tail), Some(last)) = (tail, params.last_mut()) {
        mark_last(last, tail);
    }
    (params, levels)
}

fn is_effort(level: &str) -> bool {
    matches!(level, "low" | "medium" | "high" | "xhigh" | "max")
}

fn insert_thinking_levels(
    params: Vec<Value>,
    levels: &[(usize, String)],
    active: &str,
) -> Vec<Value> {
    let mut out = Vec::with_capacity(params.len() + levels.len() + 1);
    for (index, message) in params.into_iter().enumerate() {
        if let Some((_, level)) = levels.iter().find(|(at, _)| *at == index) {
            out.push(json!({"role": "system", "content": [], "output_config": {"effort": level}}));
        }
        out.push(message);
    }
    out.push(json!({"role": "system", "content": [], "output_config": {"effort": active}}));
    out
}

fn mark_last(message: &mut Value, cache: &Value) {
    if !matches!(message["role"].as_str(), Some("user" | "system")) {
        return;
    }
    let content = &mut message["content"];
    if let Some(text) = content.as_str().map(str::to_owned) {
        *content = json!([{"type": "text", "text": text, "cache_control": cache}]);
    } else if let Some(block) = content.as_array_mut().and_then(|blocks| blocks.last_mut())
        && matches!(
            block["type"].as_str(),
            Some("text" | "image" | "tool_result" | "tool_addition" | "tool_removal")
        )
    {
        block["cache_control"] = cache.clone();
    }
}

fn assistant_blocks(blocks: &[AssistantBlock], oauth: bool, empty_signature: bool) -> Vec<Value> {
    let mut out = Vec::new();
    for block in blocks {
        match block {
            AssistantBlock::Text(text) => {
                if !text.text.trim().is_empty() {
                    out.push(json!({"type": "text", "text": text.text}));
                }
            }
            AssistantBlock::Thinking(thinking) if thinking.redacted == Some(true) => {
                out.push(json!({
                    "type": "redacted_thinking",
                    "data": thinking.thinking_signature.clone().unwrap_or_default(),
                }));
            }
            AssistantBlock::Thinking(thinking) => {
                let signature = thinking
                    .thinking_signature
                    .as_deref()
                    .filter(|signature| !signature.trim().is_empty());
                match signature {
                    _ if thinking.thinking.trim().is_empty() && signature.is_none() => {}
                    Some(signature) => out.push(json!({
                        "type": "thinking",
                        "thinking": thinking.thinking,
                        "signature": signature,
                    })),
                    None if empty_signature => out.push(json!({
                        "type": "thinking",
                        "thinking": thinking.thinking,
                        "signature": "",
                    })),
                    None => out.push(json!({"type": "text", "text": thinking.thinking})),
                }
            }
            AssistantBlock::ToolCall(call) => {
                let name = if oauth {
                    to_claude_code_name(&call.name)
                } else {
                    call.name.clone()
                };
                out.push(json!({
                    "type": "tool_use",
                    "id": call.id,
                    "name": name,
                    "input": call.arguments,
                }));
            }
        }
    }
    out
}

fn tool_result(result: &ToolResultMessage) -> Value {
    json!({
        "type": "tool_result",
        "tool_use_id": result.tool_call_id,
        "content": result_content(&result.content, result.is_error),
        "is_error": result.is_error,
    })
}

const OMITTED_ERROR_IMAGE: &str =
    "(image omitted: this tool result is an error, and Anthropic requires text-only content)";

fn result_content(content: &[UserBlock], is_error: bool) -> Value {
    if !content
        .iter()
        .any(|block| matches!(block, UserBlock::Image(_)))
    {
        let text: Vec<&str> = content
            .iter()
            .filter_map(|block| match block {
                UserBlock::Text(text) => Some(text.text.as_str()),
                UserBlock::Image(_) => None,
            })
            .collect();
        return Value::String(text.join("\n"));
    }
    let mut blocks: Vec<Value> = content
        .iter()
        .map(|block| match block {
            UserBlock::Text(text) => json!({"type": "text", "text": text.text}),
            UserBlock::Image(image) if !is_error => image_block(image),
            UserBlock::Image(_) => json!({"type": "text", "text": OMITTED_ERROR_IMAGE}),
        })
        .collect();
    if !blocks.iter().any(|block| block["type"] == "text") {
        blocks.insert(0, json!({"type": "text", "text": "(see attached image)"}));
    }
    Value::Array(blocks)
}

fn convert_tools(tools: &[Tool], oauth: bool, eager: bool, cache: Option<&Value>) -> Vec<Value> {
    let count = tools.len();
    tools
        .iter()
        .enumerate()
        .map(|(index, tool)| {
            let name = if oauth {
                to_claude_code_name(&tool.name)
            } else {
                tool.name.clone()
            };
            let schema = &tool.parameters;
            let mut value = json!({
                "name": name,
                "description": tool.description,
                "input_schema": {
                    "type": "object",
                    "properties": schema.get("properties").cloned().unwrap_or_else(|| json!({})),
                    "required": schema.get("required").cloned().unwrap_or_else(|| json!([])),
                },
            });
            if eager {
                value["eager_input_streaming"] = json!(true);
            }
            if let Some(cache) = cache.filter(|_| index + 1 == count) {
                value["cache_control"] = cache.clone();
            }
            value
        })
        .collect()
}
