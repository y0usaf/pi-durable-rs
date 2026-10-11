use serde_json::{Value, json};

use crate::ai::simple_options::{ThinkingTokens, adjust_max_tokens_for_thinking};
use crate::ai::types::{
    AssistantBlock, ImageContent, Message, Model, SimpleStreamOptions, Tool, ToolResultMessage,
    UserBlock, UserContent,
};

const EMPTY_PLACEHOLDER: &str = "(empty)";
const INTERLEAVED_BETA: &str = "interleaved-thinking-2025-05-14";
const MAX_TOOL_ID_LEN: usize = 64;

pub fn build(
    model: &Model,
    messages: &[Message],
    system: Option<&str>,
    tools: &[Tool],
    options: &SimpleStreamOptions,
) -> Result<Value, String> {
    let thinking = thinking(model, options);
    let max_tokens = thinking.as_ref().map_or_else(
        || options.max_tokens.unwrap_or(model.max_tokens),
        |tokens| tokens.max_tokens,
    );
    let mut body = json!({
        "messages": conversation(messages)?,
        "inferenceConfig": { "maxTokens": max_tokens },
    });
    if let Some(text) = system.filter(|text| !text.trim().is_empty()) {
        body["system"] = json!([{ "text": text }]);
    }
    if !tools.is_empty() {
        let specs: Vec<Value> = tools.iter().map(tool_spec).collect();
        body["toolConfig"] = json!({ "tools": specs });
    }
    if let Some(tokens) = thinking {
        body["additionalModelRequestFields"] = json!({
            "thinking": {
                "type": "enabled",
                "budget_tokens": tokens.thinking_budget,
                "display": "summarized",
            },
            "anthropic_beta": [INTERLEAVED_BETA],
        });
    }
    Ok(body)
}

fn thinking(model: &Model, options: &SimpleStreamOptions) -> Option<ThinkingTokens> {
    let level = options.reasoning?;
    let claude = model.id.to_ascii_lowercase().contains("claude");
    (model.reasoning && claude)
        .then(|| adjust_max_tokens_for_thinking(options.max_tokens, model.max_tokens, level, None))
}

fn conversation(messages: &[Message]) -> Result<Vec<Value>, String> {
    let mut out = Vec::new();
    let mut results = Vec::new();
    for message in messages {
        if let Message::ToolResult(result) = message {
            results.push(tool_result(result)?);
            continue;
        }
        flush_results(&mut out, &mut results);
        match message {
            Message::User(user) => push_message(&mut out, "user", user_blocks(&user.content)?),
            Message::Assistant(assistant) => {
                push_message(&mut out, "assistant", assistant_blocks(&assistant.content))
            }
            Message::System(_) | Message::ToolResult(_) => {}
        }
    }
    flush_results(&mut out, &mut results);
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

fn user_blocks(content: &UserContent) -> Result<Vec<Value>, String> {
    match content {
        UserContent::Text(text) => Ok(text_block(text).into_iter().collect()),
        UserContent::Blocks(blocks) => {
            let mut out = Vec::new();
            for block in blocks {
                match block {
                    UserBlock::Text(text) => out.extend(text_block(&text.text)),
                    UserBlock::Image(image) => out.push(image_block(image)?),
                }
            }
            Ok(out)
        }
    }
}

fn assistant_blocks(blocks: &[AssistantBlock]) -> Vec<Value> {
    blocks
        .iter()
        .filter_map(|block| match block {
            AssistantBlock::Text(text) => text_block(&text.text),
            AssistantBlock::Thinking(thinking) if thinking.redacted == Some(true) => thinking
                .thinking_signature
                .as_deref()
                .filter(|data| !data.is_empty())
                .map(|data| json!({ "reasoningContent": { "redactedContent": data } })),
            AssistantBlock::Thinking(thinking) => match thinking.thinking_signature.as_deref() {
                _ if thinking.thinking.trim().is_empty() => None,
                Some(signature) if !signature.trim().is_empty() => Some(json!({
                    "reasoningContent": {
                        "reasoningText": { "text": thinking.thinking, "signature": signature },
                    },
                })),
                _ => text_block(&thinking.thinking),
            },
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
        content.push(json!({ "text": EMPTY_PLACEHOLDER }));
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
