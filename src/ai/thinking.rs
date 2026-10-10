use crate::ai::types::{Model, ModelThinkingLevel, ThinkingLevel};
use crate::chord::json::JsonValue;

const LEVELS: [ModelThinkingLevel; 7] = [
    ModelThinkingLevel::Off,
    ModelThinkingLevel::Minimal,
    ModelThinkingLevel::Low,
    ModelThinkingLevel::Medium,
    ModelThinkingLevel::High,
    ModelThinkingLevel::Xhigh,
    ModelThinkingLevel::Max,
];

/// The key a level has in a model's thinking_level_map and sampling_params_by_thinking_level.
pub fn level_name(level: ModelThinkingLevel) -> &'static str {
    match level {
        ModelThinkingLevel::Off => "off",
        ModelThinkingLevel::Minimal => "minimal",
        ModelThinkingLevel::Low => "low",
        ModelThinkingLevel::Medium => "medium",
        ModelThinkingLevel::High => "high",
        ModelThinkingLevel::Xhigh => "xhigh",
        ModelThinkingLevel::Max => "max",
    }
}

impl From<ThinkingLevel> for ModelThinkingLevel {
    fn from(level: ThinkingLevel) -> Self {
        match level {
            ThinkingLevel::Minimal => ModelThinkingLevel::Minimal,
            ThinkingLevel::Low => ModelThinkingLevel::Low,
            ThinkingLevel::Medium => ModelThinkingLevel::Medium,
            ThinkingLevel::High => ModelThinkingLevel::High,
            ThinkingLevel::Xhigh => ModelThinkingLevel::Xhigh,
            ThinkingLevel::Max => ModelThinkingLevel::Max,
        }
    }
}

/// Levels a model accepts. A null entry in thinking_level_map removes a level; xhigh and
/// max are offered only when the map names them. Non-reasoning models accept only off.
pub fn supported_thinking_levels(model: &Model) -> Vec<ModelThinkingLevel> {
    if !model.reasoning {
        return vec![ModelThinkingLevel::Off];
    }
    LEVELS
        .into_iter()
        .filter(|&level| {
            let mapped = model
                .thinking_level_map
                .as_ref()
                .and_then(|map| map.get(level_name(level)));
            match level {
                _ if mapped == Some(&JsonValue::Null) => false,
                ModelThinkingLevel::Xhigh | ModelThinkingLevel::Max => mapped.is_some(),
                _ => true,
            }
        })
        .collect()
}

/// Move a requested level to the nearest supported one: upward first, then downward.
pub fn clamp_thinking_level(model: &Model, level: ModelThinkingLevel) -> ModelThinkingLevel {
    let available = supported_thinking_levels(model);
    if available.contains(&level) {
        return level;
    }
    let index = level as usize;
    (index..LEVELS.len())
        .chain((0..index).rev())
        .map(|candidate| LEVELS[candidate])
        .find(|candidate| available.contains(candidate))
        .unwrap_or_else(|| {
            available
                .first()
                .copied()
                .unwrap_or(ModelThinkingLevel::Off)
        })
}
