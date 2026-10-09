use std::sync::OnceLock;

use regex::Regex;

const JS_DOT: &str = r"[^\r\n\x{2028}\x{2029}]";

use crate::ai::types::{AssistantMessage, StopReason};

pub const DEFAULT_MAX_AGENT_RETRY_DELAY_MS: u64 = 60_000;

const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

const NON_RETRYABLE_PROVIDER_LIMIT_ERROR_PATTERNS: &[&str] = &[
    "GoUsageLimitError",
    "FreeUsageLimitError",
    "Monthly usage limit reached",
    "available balance",
    "insufficient_quota",
    "out of budget",
    "quota exceeded",
    "billing",
    "subscription_sharing_usage_limit_exceeded",
];

const RETRYABLE_PROVIDER_ERROR_PATTERNS: &[&str] = &[
    "overloaded",
    "server_busy",
    "servers are currently busy",
    "currently experiencing high demand",
    "model is at capacity",
    "rate.?limit",
    "too many requests",
    "429",
    "500",
    "502",
    "503",
    "504",
    "520",
    "524",
    "service.?unavailable",
    "server.?error",
    "internal.?error",
    "provider.?returned.?error",
    "exceeded request buffer limit while retrying upstream",
    "network.?error",
    "connection.?error",
    "connection.?refused",
    "connection.?lost",
    "other side closed",
    "fetch failed",
    "getaddrinfo",
    "ENOTFOUND",
    "EAI_AGAIN",
    "upstream.?connect",
    "reset before headers",
    "socket hang up",
    "socket connection was closed",
    "timed? out",
    "timeout",
    "terminated",
    "websocket.?closed",
    "websocket.?error",
    "ended without",
    "stream ended before message_stop",
    "stream ended before a terminal response event",
    "http2 request did not get a response",
    "pending stream has been canceled",
    "retry delay",
    "you can retry your request",
    "try your request again",
    "please retry your request",
    "ResourceExhausted",
    "subscription_sharing_usage_unavailable",
    "subscription_sharing_user_unavailable",
];

pub fn js_pattern(patterns: &[&str]) -> Regex {
    let joined = patterns.join("|").to_ascii_lowercase().replace('.', JS_DOT);
    Regex::new(&joined).expect("static provider pattern compiles")
}

pub fn js_matches(pattern: &Regex, text: &str) -> bool {
    pattern.is_match(&text.to_ascii_lowercase())
}

fn non_retryable_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| js_pattern(NON_RETRYABLE_PROVIDER_LIMIT_ERROR_PATTERNS))
}

fn retryable_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| js_pattern(RETRYABLE_PROVIDER_ERROR_PATTERNS))
}

pub fn retry_delay_ms(base_delay_ms: u64, max_agent_delay_ms: Option<u64>, attempt: u64) -> u64 {
    let exponent = attempt.saturating_sub(1).min(i32::MAX as u64) as i32;
    let delay = base_delay_ms as f64 * 2f64.powi(exponent);
    let safe_delay = if delay.fract() == 0.0 && delay.abs() <= MAX_SAFE_INTEGER {
        delay as u64
    } else {
        MAX_SAFE_INTEGER as u64
    };
    safe_delay.min(max_agent_delay_ms.unwrap_or(DEFAULT_MAX_AGENT_RETRY_DELAY_MS))
}

pub fn is_retryable_assistant_error(message: &AssistantMessage) -> bool {
    if message.stop_reason != StopReason::Error {
        return false;
    }
    let Some(error_message) = message
        .error_message
        .as_deref()
        .filter(|text| !text.is_empty())
    else {
        return false;
    };
    if js_matches(non_retryable_pattern(), error_message) {
        return false;
    }
    js_matches(retryable_pattern(), error_message)
}
