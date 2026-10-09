use std::sync::OnceLock;

use regex::Regex;

use super::retry::{js_matches, js_pattern};
use crate::ai::types::{AssistantMessage, StopReason};

const OVERFLOW_PATTERNS: &[&str] = &[
    r"prompt (?:is )?too long",
    r"prompt exceeds max length",
    r"request_too_large",
    r"input is too long for requested model",
    r"exceeds the context window",
    r"exceeds (?:the )?(?:model's )?maximum context length(?: of [0-9,]+ tokens?|\s*\([0-9,]+\))",
    r"input token count.*exceeds the maximum",
    r"maximum prompt length is [0-9]+",
    r"reduce the length of the messages",
    r"maximum context length is [0-9]+ tokens",
    r"exceeds (?:the )?maximum allowed input length of [0-9,]+ tokens?",
    r"input \([0-9]+ tokens\) is longer than the model's context length \([0-9]+ tokens\)",
    r"exceeds the limit of [0-9]+",
    r"exceeds the available context size",
    r"greater than the context length",
    r"context window exceeds limit",
    r"exceeded model token limit",
    r"too large for model with [0-9]+ maximum context length",
    r"prompt has [0-9,]+ tokens?, but the configured context size is [0-9,]+ tokens?",
    r"model_context_window_exceeded",
    r"prompt too long; exceeded (?:max )?context length",
    r"range of input length should be",
    r"context[_ ]length[_ ]exceeded",
    r"too many tokens",
    r"token limit exceeded",
];

const CEREBRAS_BODYLESS_OVERFLOW_PATTERN: &str = r"^4(?:00|13)\s*(?:status code)?\s*\(no body\)";

const NON_OVERFLOW_PATTERNS: &[&str] = &[
    r"^(Throttling error|Service unavailable):",
    "rate limit",
    "too many requests",
];

fn overflow_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| js_pattern(OVERFLOW_PATTERNS))
}

fn cerebras_bodyless_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| js_pattern(&[CEREBRAS_BODYLESS_OVERFLOW_PATTERN]))
}

fn non_overflow_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| js_pattern(NON_OVERFLOW_PATTERNS))
}

pub fn is_context_overflow(message: &AssistantMessage, context_window: Option<u64>) -> bool {
    if message.stop_reason == StopReason::Error
        && let Some(error_message) = message
            .error_message
            .as_deref()
            .filter(|text| !text.is_empty())
        && !js_matches(non_overflow_pattern(), error_message)
    {
        if js_matches(overflow_pattern(), error_message) {
            return true;
        }
        if message.provider == "cerebras" && js_matches(cerebras_bodyless_pattern(), error_message)
        {
            return true;
        }
    }
    let input_tokens = message.usage.input + message.usage.cache_read;
    match context_window.filter(|window| *window != 0) {
        Some(window) if message.stop_reason == StopReason::Stop => input_tokens > window,
        Some(window) if message.stop_reason == StopReason::Length && message.usage.output == 0 => {
            input_tokens as f64 >= window as f64 * 0.99
        }
        _ => false,
    }
}
