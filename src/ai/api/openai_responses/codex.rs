use std::collections::BTreeMap;

use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use regex::Regex;
use serde_json::Value;

use crate::ai::http::{HttpError, Request, Response, RetryPolicy, send};
use crate::ai::oauth::openai_codex::{ORIGINATOR, account_id_from_token};
use crate::cancel::CancelToken;

const DEFAULT_BASE_URL: &str = "https://chatgpt.com/backend-api";
const ENDPOINT_SUFFIX: &str = "/codex/responses";
const ACCOUNT_HEADER: &str = "chatgpt-account-id";
const RAW_BLOCK_MAX: usize = 128 * 1024;
pub const DEFAULT_INSTRUCTIONS: &str = "You are a helpful assistant.";
pub const HEADERS: [(&str, &str); 3] = [
    ("OpenAI-Beta", "responses=experimental"),
    ("originator", ORIGINATOR),
    ("accept", "text/event-stream"),
];

pub fn endpoint(base_url: &str) -> String {
    let trimmed = base_url.trim();
    let base = if trimmed.is_empty() {
        DEFAULT_BASE_URL
    } else {
        trimmed
    }
    .trim_end_matches('/');
    if base.ends_with(ENDPOINT_SUFFIX) {
        base.to_owned()
    } else if base.ends_with("/codex") {
        format!("{base}/responses")
    } else {
        format!("{base}{ENDPOINT_SUFFIX}")
    }
}

pub fn account_id(
    explicit: Option<&BTreeMap<String, String>>,
    api_key: Option<&str>,
) -> Result<String, String> {
    explicit
        .and_then(|headers| {
            headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(ACCOUNT_HEADER))
                .map(|(_, value)| value)
        })
        .filter(|id| !id.is_empty())
        .cloned()
        .or_else(|| api_key.and_then(account_id_from_token))
        .ok_or_else(|| "openai-codex-responses: failed to extract accountId from token".to_owned())
}

pub fn zstd_raw_frame(data: &[u8]) -> Vec<u8> {
    let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd];
    let size = data.len() as u64;
    if size < 1 << 8 {
        frame.extend([0x20, size as u8]);
    } else if size < (1 << 16) + (1 << 8) {
        frame.push(0x60);
        frame.extend(((size - (1 << 8)) as u16).to_le_bytes());
    } else if size <= u64::from(u32::MAX) {
        frame.push(0xa0);
        frame.extend((size as u32).to_le_bytes());
    } else {
        frame.push(0xe0);
        frame.extend(size.to_le_bytes());
    }
    let mut offset = 0;
    loop {
        let block = RAW_BLOCK_MAX.min(data.len() - offset);
        let last = offset + block == data.len();
        let header = ((block as u32) << 3) | u32::from(last);
        frame.extend(&header.to_le_bytes()[..3]);
        frame.extend(&data[offset..offset + block]);
        offset += block;
        if last {
            break;
        }
    }
    frame
}

fn terminal_rate_limit() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"(?i)GoUsageLimitError|FreeUsageLimitError|Monthly usage limit reached|available balance|insufficient_quota|out of budget|quota exceeded|billing")
            .expect("static pattern is valid")
    })
}

fn transient_message() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"(?i)rate.?limit|overloaded|service.?unavailable|upstream.?connect|connection.?refused")
            .expect("static pattern is valid")
    })
}

fn retryable_status(status: u16, text: &str) -> bool {
    (status != 429 || !terminal_rate_limit().is_match(text))
        && (matches!(status, 429 | 500 | 502 | 503 | 504) || transient_message().is_match(text))
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim())
}

fn retry_delay(
    headers: &[(String, String)],
    attempt: u32,
    max: Option<Duration>,
) -> Result<Duration, String> {
    let mut delay_ms = header(headers, "retry-after-ms")
        .and_then(|value| {
            if value.is_empty() {
                Some(0.0)
            } else {
                value.parse::<f64>().ok()
            }
        })
        .filter(|ms| ms.is_finite());
    if delay_ms.is_none() {
        delay_ms = header(headers, "retry-after")
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|seconds| seconds.is_finite())
            .map(|seconds| seconds * 1000.0);
    }
    let Some(delay_ms) = delay_ms else {
        return Ok(Duration::from_secs_f64(2f64.powi(attempt as i32)));
    };
    let delay_ms = delay_ms.max(0.0);
    if let Some(max) = max {
        let max_ms = max.as_millis() as f64;
        if delay_ms > max_ms {
            return Err(format!(
                "Server requested {}s retry delay (max: {}s)",
                (delay_ms / 1000.0).ceil(),
                (max_ms / 1000.0).ceil()
            ));
        }
    }
    Ok(Duration::from_secs_f64(delay_ms / 1000.0))
}

pub fn send_with_retries(
    request: &Request,
    cancel: Option<&CancelToken>,
    policy: RetryPolicy,
) -> Result<Response, String> {
    let max = policy.max_delay.filter(|delay| !delay.is_zero());
    let mut attempt = 0u32;
    loop {
        let delay = match send(request, cancel) {
            Ok(response) if response.ok() => return Ok(response),
            Ok(response) => {
                let status = response.status;
                let headers = response.headers.clone();
                let text = response.text().map_err(|error| error.to_string())?;
                if attempt >= policy.max_retries || !retryable_status(status, &text) {
                    return Err(http_error(status, &text));
                }
                retry_delay(&headers, attempt, max)?
            }
            Err(HttpError::Cancelled) => return Err(HttpError::Cancelled.to_string()),
            Err(error) => {
                if attempt >= policy.max_retries {
                    return Err(error.to_string());
                }
                Duration::from_secs_f64(2f64.powi(attempt as i32))
            }
        };
        attempt += 1;
        if let Some(token) = cancel {
            if token.wait_timeout(delay) {
                return Err(HttpError::Cancelled.to_string());
            }
        } else {
            std::thread::sleep(delay);
        }
    }
}

pub fn http_error(status: u16, raw: &str) -> String {
    let fallback = if raw.is_empty() {
        status_text(status).to_owned()
    } else {
        raw.to_owned()
    };
    let Ok(parsed) = serde_json::from_str::<Value>(raw) else {
        return fallback;
    };
    let error = &parsed["error"];
    let code = error["code"]
        .as_str()
        .filter(|code| !code.is_empty())
        .or_else(|| error["type"].as_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let usage_limit = status == 429
        || [
            "usage_limit_reached",
            "usage_not_included",
            "rate_limit_exceeded",
        ]
        .iter()
        .any(|needle| code.contains(needle));
    if usage_limit {
        let plan = error["plan_type"]
            .as_str()
            .filter(|plan| !plan.is_empty())
            .map(|plan| format!(" ({} plan)", plan.to_ascii_lowercase()))
            .unwrap_or_default();
        let when = error["resets_at"].as_f64().map(|resets_at| {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0.0, |elapsed| elapsed.as_millis() as f64);
            let minutes = ((resets_at * 1000.0 - now_ms) / 60_000.0 + 0.5)
                .floor()
                .max(0.0);
            format!(" Try again in ~{minutes:.0} min.")
        });
        return format!(
            "You have hit your ChatGPT usage limit{plan}.{}",
            when.unwrap_or_default()
        );
    }
    error["message"]
        .as_str()
        .filter(|message| !message.is_empty())
        .map_or(fallback, str::to_owned)
}

fn status_text(status: u16) -> &'static str {
    match status {
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "",
    }
}

pub fn stream_error(event: &Value) -> String {
    let field = |name: &str| {
        event[name]
            .as_str()
            .or_else(|| event["error"][name].as_str())
            .filter(|value| !value.is_empty())
    };
    let detail = field("message")
        .or_else(|| field("code"))
        .map_or_else(|| event.to_string(), str::to_owned);
    format!("Codex error: {detail}")
}

pub fn failed_message(response: &Value) -> String {
    response["error"]["message"]
        .as_str()
        .filter(|message| !message.is_empty())
        .unwrap_or("Codex response failed")
        .to_owned()
}
