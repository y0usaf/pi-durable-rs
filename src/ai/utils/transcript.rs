use super::text::system_content_text;
use crate::ai::types::{
    CachePoint, Context, Message, Sections, SystemContent, SystemMessage, SystemRole, Tool,
    TranscriptContext,
};

pub fn to_tool_declaration(tool: &Tool) -> Tool {
    Tool {
        name: tool.name.clone(),
        description: tool.description.clone(),
        parameters: tool.parameters.clone(),
        constrained_sampling: tool.constrained_sampling.clone(),
    }
}

pub fn declarations_equal(left: &Tool, right: &Tool) -> bool {
    let left = serde_json::to_string(&to_tool_declaration(left));
    let right = serde_json::to_string(&to_tool_declaration(right));
    matches!((left, right), (Ok(left), Ok(right)) if left == right)
}

fn upsert_tool(tools: &mut Vec<Tool>, tool: &Tool) {
    match tools.iter_mut().find(|slot| slot.name == tool.name) {
        Some(slot) => *slot = tool.clone(),
        None => tools.push(tool.clone()),
    }
}

fn system_messages(messages: &[Message]) -> impl Iterator<Item = &SystemMessage> {
    messages.iter().filter_map(|message| match message {
        Message::System(system) => Some(system),
        Message::User(_) | Message::Assistant(_) | Message::ToolResult(_) => None,
    })
}

/// Build the leading system message for a prompt and tool set. Returns None when both
/// are empty, so an empty transcript stays empty.
pub fn create_initial_system_message(
    system_prompt: Option<&str>,
    tools: Option<&[Tool]>,
) -> Option<SystemMessage> {
    let system_prompt = system_prompt.filter(|prompt| !prompt.is_empty());
    let tools = tools.filter(|tools| !tools.is_empty());
    if system_prompt.is_none() && tools.is_none() {
        return None;
    }
    Some(SystemMessage {
        role: SystemRole,
        content: SystemContent::Text(system_prompt.unwrap_or_default().to_owned()),
        sections: None,
        tools_added: tools.map(<[Tool]>::to_vec),
        tools_removed: None,
        timestamp: 0,
    })
}

/// Fold a request's system prompt and tools into a leading system message.
pub fn normalize_context(context: &Context) -> TranscriptContext {
    let initial =
        create_initial_system_message(context.system_prompt.as_deref(), context.tools.as_deref());
    TranscriptContext {
        messages: initial
            .map(Message::System)
            .into_iter()
            .chain(context.messages.iter().cloned())
            .collect(),
    }
}

/// The leading system message, if the transcript starts with one.
pub fn get_initial_system_message(messages: &[Message]) -> Option<&SystemMessage> {
    match messages.first() {
        Some(Message::System(system)) => Some(system),
        Some(Message::User(_) | Message::Assistant(_) | Message::ToolResult(_)) | None => None,
    }
}

/// Drop the leading system message for APIs that carry the prompt outside the list.
pub fn without_initial_system_message(messages: &[Message]) -> &[Message] {
    if get_initial_system_message(messages).is_some() {
        &messages[1..]
    } else {
        messages
    }
}

/// Tools available after applying every system message's additions and removals in order.
pub fn get_current_tools(messages: &[Message]) -> Vec<Tool> {
    let mut tools: Vec<Tool> = Vec::new();
    for system in system_messages(messages) {
        for removed in system.tools_removed.iter().flatten() {
            tools.retain(|tool| tool.name != removed.name);
        }
        for added in system.tools_added.iter().flatten() {
            upsert_tool(&mut tools, added);
        }
    }
    tools
}

/// Replay every system message into one leading system message with the current prompt,
/// sections and tools. Content is appended, sections are patched by name (null removes),
/// and tools come from get_current_tools.
pub fn get_current_system_message(messages: &[Message]) -> Option<SystemMessage> {
    let mut timestamp = None;
    let mut content = Vec::new();
    let mut sections: Vec<(String, String)> = Vec::new();
    for system in system_messages(messages) {
        timestamp.get_or_insert(system.timestamp);
        let text = system_content_text(&system.content);
        if !text.is_empty() {
            content.push(text);
        }
        for (name, value) in system.sections.iter().flat_map(|patch| patch.0.iter()) {
            let position = sections.iter().position(|(existing, _)| existing == name);
            match (value, position) {
                (None, Some(index)) => {
                    sections.remove(index);
                }
                (None, None) => {}
                (Some(value), Some(index)) => sections[index].1 = value.clone(),
                (Some(value), None) => sections.push((name.clone(), value.clone())),
            }
        }
    }
    let timestamp = timestamp?;
    let tools = get_current_tools(messages);
    Some(SystemMessage {
        role: SystemRole,
        content: SystemContent::Text(content.join("\n\n")),
        sections: (!sections.is_empty()).then(|| {
            Sections(
                sections
                    .into_iter()
                    .map(|(name, value)| (name, Some(value)))
                    .collect(),
            )
        }),
        tools_added: (!tools.is_empty()).then_some(tools),
        tools_removed: None,
        timestamp,
    })
}

/// Rebuild the transcript for APIs without mid-conversation system messages: the replayed
/// system message leads and every later system message is dropped.
pub fn collapse_system_messages(context: &TranscriptContext) -> TranscriptContext {
    let head = get_current_system_message(&context.messages);
    let rest = context
        .messages
        .iter()
        .filter(|message| !matches!(message, Message::System(_)))
        .cloned();
    TranscriptContext {
        messages: head.map(Message::System).into_iter().chain(rest).collect(),
    }
}

/// Keep system messages in place when the model accepts them; otherwise collapse them.
pub fn resolve_transcript(
    context: &TranscriptContext,
    supports_mid_convo_system_messages: bool,
) -> TranscriptContext {
    if supports_mid_convo_system_messages {
        context.clone()
    } else {
        collapse_system_messages(context)
    }
}

/// Every tool definition the transcript declares, in first-declaration order, each with
/// its latest definition.
pub fn get_declared_tools(messages: &[Message]) -> Vec<Tool> {
    let mut definitions: Vec<Tool> = Vec::new();
    for system in system_messages(messages) {
        for tool in system.tools_added.iter().flatten() {
            upsert_tool(&mut definitions, tool);
        }
    }
    definitions
}

/// Whether tool history has a removal or a repeated name, which an addition-only
/// transport cannot replay.
pub fn has_non_additive_tool_changes(messages: &[Message]) -> bool {
    let mut declared: Vec<&str> = Vec::new();
    for system in system_messages(messages) {
        if system
            .tools_removed
            .as_ref()
            .is_some_and(|removed| !removed.is_empty())
        {
            return true;
        }
        for tool in system.tools_added.iter().flatten() {
            if declared.contains(&tool.name.as_str()) {
                return true;
            }
            declared.push(&tool.name);
        }
    }
    false
}

/// Tool declarations split between the top-level request field and in-place additions.
#[derive(Debug, Clone, PartialEq)]
pub struct TranscriptTools {
    /// Tools sent in the top-level request field.
    pub request_tools: Vec<Tool>,
    /// Whether later system messages carry their own tool additions. When false,
    /// request_tools already holds the complete current tool set.
    pub anchors_additions: bool,
}

/// Choose the top-level tools. Transports that can anchor additions at a system message
/// keep the initial tools at the top, but only when no tool was removed or redeclared.
pub fn resolve_transcript_tools(
    messages: &[Message],
    supports_tool_additions: bool,
) -> TranscriptTools {
    let anchors_additions = supports_tool_additions && !has_non_additive_tool_changes(messages);
    let request_tools = if anchors_additions {
        get_initial_system_message(messages)
            .and_then(|system| system.tools_added.clone())
            .unwrap_or_default()
    } else {
        get_current_tools(messages)
    };
    TranscriptTools {
        request_tools,
        anchors_additions,
    }
}

pub fn cache_points(messages: &[Message]) -> Vec<CachePoint> {
    let first = messages
        .iter()
        .position(|message| matches!(message, Message::User(_)));
    let last = messages
        .iter()
        .rposition(|message| matches!(message, Message::User(_) | Message::ToolResult(_)));
    let mut points = Vec::new();
    if let Some(first) = first {
        points.push(CachePoint {
            message: first,
            block: None,
        });
    }
    if let Some(last) = last.filter(|last| Some(*last) != first) {
        points.push(CachePoint {
            message: last,
            block: None,
        });
    }
    points
}
