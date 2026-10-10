use crate::ai::thinking::clamp_thinking_level;
use crate::ai::types::{Message, Model, ModelThinkingLevel, ThinkingLevel};
use crate::ai::utils::estimate::estimate_context_tokens;
use crate::chord::json::{JsonObject, JsonValue};

const CONTEXT_SAFETY_TOKENS: i128 = 4096;
const MIN_MAX_TOKENS: u64 = 1;

/// Tokens always left for the answer when a thinking budget shares the response ceiling.
pub const MIN_ANSWER_TOKENS: u64 = 1024;

/// Per-level thinking budgets in tokens. A missing level uses the pi-ai default.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ThinkingBudgets {
    pub minimal: Option<u64>,
    pub low: Option<u64>,
    pub medium: Option<u64>,
    pub high: Option<u64>,
}

/// Result of [adjust_max_tokens_for_thinking].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThinkingTokens {
    pub max_tokens: u64,
    pub thinking_budget: u64,
}

/// Cap a response ceiling so the context window keeps room for the safety margin.
pub fn clamp_max_tokens_to_context(model: &Model, messages: &[Message], max_tokens: u64) -> u64 {
    if model.context_window == 0 {
        return max_tokens.max(MIN_MAX_TOKENS);
    }
    let used = i128::from(estimate_context_tokens(messages));
    let available = i128::from(model.context_window) - used - CONTEXT_SAFETY_TOKENS;
    let available =
        u64::try_from(available.max(i128::from(MIN_MAX_TOKENS))).unwrap_or(MIN_MAX_TOKENS);
    max_tokens.min(available)
}

/// Merge sampling parameters: model defaults, then the level's overrides, then the
/// request's. Returns None when no source has any.
pub fn resolve_sampling_params(
    model: &Model,
    level: ModelThinkingLevel,
    request: Option<&JsonObject>,
) -> Option<JsonObject> {
    let effective = clamp_thinking_level(model, level);
    let by_level = model
        .sampling_params_by_thinking_level
        .as_ref()
        .and_then(|map| map.get(crate::ai::thinking::level_name(effective)))
        .and_then(JsonValue::as_object);
    let sources = [model.sampling_params.as_ref(), by_level, request];
    if sources.iter().all(Option::is_none) {
        return None;
    }
    let mut merged = JsonObject::new();
    for source in sources.into_iter().flatten() {
        for (key, value) in source {
            merged.insert(key.clone(), value.clone());
        }
    }
    Some(merged)
}

fn default_budget(level: ThinkingLevel) -> u64 {
    match level {
        ThinkingLevel::Minimal => 1024,
        ThinkingLevel::Low => 2048,
        ThinkingLevel::Medium => 8192,
        ThinkingLevel::High | ThinkingLevel::Xhigh | ThinkingLevel::Max => 16384,
    }
}

/// Thinking budget for a level. xhigh and max use the high budget.
pub fn thinking_budget_for_level(level: ThinkingLevel, custom: Option<&ThinkingBudgets>) -> u64 {
    let custom = match level {
        ThinkingLevel::Minimal => custom.and_then(|budgets| budgets.minimal),
        ThinkingLevel::Low => custom.and_then(|budgets| budgets.low),
        ThinkingLevel::Medium => custom.and_then(|budgets| budgets.medium),
        ThinkingLevel::High | ThinkingLevel::Xhigh | ThinkingLevel::Max => {
            custom.and_then(|budgets| budgets.high)
        }
    };
    custom.unwrap_or_else(|| default_budget(level))
}

/// Cap a thinking budget so at least MIN_ANSWER_TOKENS remain under the ceiling.
pub fn clamp_thinking_budget_to_answer_room(thinking_budget: u64, ceiling: u64) -> u64 {
    thinking_budget.min(ceiling.saturating_sub(MIN_ANSWER_TOKENS))
}

/// Split a response ceiling between answer and thinking. With no caller cap the model
/// cap applies; the thinking budget is trimmed when it would fill the whole ceiling.
pub fn adjust_max_tokens_for_thinking(
    base_max_tokens: Option<u64>,
    model_max_tokens: u64,
    level: ThinkingLevel,
    custom: Option<&ThinkingBudgets>,
) -> ThinkingTokens {
    let mut thinking_budget = thinking_budget_for_level(level, custom);
    let max_tokens = match base_max_tokens {
        None => model_max_tokens,
        Some(base) => base.saturating_add(thinking_budget).min(model_max_tokens),
    };
    if max_tokens <= thinking_budget {
        thinking_budget = clamp_thinking_budget_to_answer_room(thinking_budget, max_tokens);
    }
    ThinkingTokens {
        max_tokens,
        thinking_budget,
    }
}
