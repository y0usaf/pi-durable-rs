use crate::ai::types::{Message, Tool};

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

pub fn get_current_tools(messages: &[Message]) -> Vec<Tool> {
    let mut tools: Vec<Tool> = Vec::new();
    for message in messages {
        let Message::System(system) = message else {
            continue;
        };
        for removed in system.tools_removed.iter().flatten() {
            tools.retain(|tool| tool.name != removed.name);
        }
        for added in system.tools_added.iter().flatten() {
            match tools.iter_mut().find(|tool| tool.name == added.name) {
                Some(slot) => *slot = added.clone(),
                None => tools.push(added.clone()),
            }
        }
    }
    tools
}
