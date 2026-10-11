//! OpenAI Responses API: a streamed POST to {base}/responses. Covers transcript items, reasoning
//! items, function calls, usage, retries and provider errors. Not covered: custom and grammar tools,
//! tool search, service tier, tool choice, the WebSocket transport and Copilot headers.

mod codex;
mod events;
mod request;

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::ai::error_body::format_provider_error;
use crate::ai::event_stream::AssistantMessageEventStream;
use crate::ai::http::{Request, RetryPolicy, send_retrying};
use crate::ai::registry::Api;
use crate::ai::simple_options::clamp_max_tokens_to_context;
use crate::ai::stream::spawn;
use crate::ai::thinking::clamp_thinking_level;
use crate::ai::types::{
    AssistantMessage, AssistantMessageEvent, CacheRetention, Model, ModelThinkingLevel,
    SimpleStreamOptions, TranscriptContext,
};
use crate::ai::utils::transcript::resolve_transcript;

use codex::{HEADERS, account_id, endpoint, zstd_raw_frame};
use request::{Compat, Turn};

pub fn api() -> Arc<dyn Api> {
    Arc::new(OpenAiResponses)
}

struct OpenAiResponses;

impl Api for OpenAiResponses {
    fn stream_simple(
        &self,
        model: &Model,
        context: TranscriptContext,
        options: SimpleStreamOptions,
    ) -> AssistantMessageEventStream {
        let signal = options.signal.clone();
        let run_model = model.clone();
        spawn(model, signal.as_ref(), move |out, output| {
            run(&run_model, &context, &options, out, output)
        })
    }
}

fn run(
    model: &Model,
    context: &TranscriptContext,
    options: &SimpleStreamOptions,
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
) -> Result<(), String> {
    if model.base_url.is_empty() {
        return Err(format!("No base URL for model {}", model.id));
    }
    let compat = Compat::of(model);
    let context = resolve_transcript(context, compat.mid_convo_system);
    let api_key = options.api_key.as_deref().filter(|key| !key.is_empty());
    let has_auth_header =
        has_header(options, "authorization") || has_header(options, "cf-aig-authorization");
    if api_key.is_none() && !has_auth_header {
        return Err(format!("No API key for provider: {}", model.provider));
    }
    let retention = cache_retention(options);
    let turn = turn(model, &context, options, retention, api_key);
    let body = request::build(model, &context, &compat, &turn)?;
    let url = if compat.codex {
        endpoint(&model.base_url)
    } else {
        format!("{}/responses", model.base_url.trim_end_matches('/'))
    };
    let account = if compat.codex {
        Some(account_id(options.headers.as_ref(), api_key)?)
    } else {
        None
    };
    let mut request = Request::new("POST", url)
        .json(&body)
        .timeout(options.timeout_ms.map(Duration::from_millis));
    if compat.codex {
        request.body = zstd_raw_frame(&request.body);
        request = request.header("content-encoding", "zstd");
    }
    for (name, value) in headers(
        model,
        options,
        retention,
        &compat,
        api_key,
        account.as_deref(),
    ) {
        request = request.header(&name, value);
    }
    let response = send_retrying(&request, options.signal.as_ref(), retry_policy(options))
        .map_err(|error| error.to_string())?;
    if !response.ok() {
        let status = response.status;
        let text = response.text().map_err(|error| error.to_string())?;
        return Err(status_error(&model.provider, status, &text));
    }
    out.push(AssistantMessageEvent::Start {
        partial: output.clone(),
    });
    events::process(
        response.into_reader(),
        model,
        options.signal.as_ref(),
        out,
        output,
    )
}

fn turn(
    model: &Model,
    context: &TranscriptContext,
    options: &SimpleStreamOptions,
    retention: CacheRetention,
    api_key: Option<&str>,
) -> Turn {
    let cache_key = match options.session_id.as_deref() {
        Some(session) if retention != CacheRetention::None => {
            Some(session.chars().take(64).collect::<String>())
        }
        _ => None,
    };
    let max_tokens = clamp_max_tokens_to_context(
        model,
        &context.messages,
        options.max_tokens.unwrap_or(model.max_tokens),
    );
    let effort = options
        .reasoning
        .map(|level| clamp_thinking_level(model, level.into()))
        .filter(|level| *level != ModelThinkingLevel::Off);
    let chatgpt_sign_in = model.provider == "openai"
        && model.base_url == "https://api.openai.com/v1"
        && api_key.is_some_and(|key| !key.starts_with("sk-"));
    Turn {
        cache_key,
        retention,
        max_tokens,
        effort,
        chatgpt_sign_in,
    }
}

fn cache_retention(options: &SimpleStreamOptions) -> CacheRetention {
    options.cache_retention.unwrap_or_else(|| {
        if std::env::var("PI_CACHE_RETENTION").is_ok_and(|value| value == "long") {
            CacheRetention::Long
        } else {
            CacheRetention::Short
        }
    })
}

fn retry_policy(options: &SimpleStreamOptions) -> RetryPolicy {
    let default = RetryPolicy::default();
    RetryPolicy {
        max_retries: options.max_retries.map_or(default.max_retries, |retries| {
            u32::try_from(retries).unwrap_or(u32::MAX)
        }),
        max_delay: options
            .max_retry_delay_ms
            .map(Duration::from_millis)
            .or(default.max_delay),
    }
}

fn has_header(options: &SimpleStreamOptions, name: &str) -> bool {
    options.headers.as_ref().is_some_and(|headers| {
        headers
            .iter()
            .any(|(key, value)| key.eq_ignore_ascii_case(name) && !value.trim().is_empty())
    })
}

fn headers(
    model: &Model,
    options: &SimpleStreamOptions,
    retention: CacheRetention,
    compat: &Compat,
    api_key: Option<&str>,
    account: Option<&str>,
) -> Vec<(String, String)> {
    let mut headers = Vec::new();
    if let Some(key) = api_key {
        set_header(&mut headers, "authorization", format!("Bearer {key}"));
    }
    for (name, value) in model.headers.iter().flatten() {
        set_header(&mut headers, name, value.clone());
    }
    let session = options
        .session_id
        .as_deref()
        .filter(|session| !session.is_empty() && retention != CacheRetention::None);
    if let Some(session) = session {
        if compat.codex {
            let clamped: String = session.chars().take(64).collect();
            set_header(&mut headers, "session-id", clamped.clone());
            set_header(&mut headers, "x-client-request-id", clamped);
        } else if compat.openrouter_session() {
            set_header(&mut headers, "x-session-id", session.to_owned());
        } else {
            set_header(&mut headers, "session_id", session.to_owned());
            set_header(&mut headers, "x-client-request-id", session.to_owned());
        }
    }
    for (name, value) in options.headers.iter().flatten() {
        set_header(&mut headers, name, value.clone());
    }
    if let Some(account) = account {
        if let Some(key) = api_key {
            set_header(&mut headers, "authorization", format!("Bearer {key}"));
        }
        set_header(&mut headers, "chatgpt-account-id", account.to_owned());
        for (name, value) in HEADERS.iter().filter(|(name, _)| *name != "accept") {
            set_header(&mut headers, name, value.to_string());
        }
    }
    headers
}

fn set_header(headers: &mut Vec<(String, String)>, name: &str, value: String) {
    match headers
        .iter_mut()
        .find(|(known, _)| known.eq_ignore_ascii_case(name))
    {
        Some(slot) => slot.1 = value,
        None => headers.push((name.to_owned(), value)),
    }
}

fn status_error(provider: &str, status: u16, text: &str) -> String {
    let prefix = if provider == "openai" {
        "OpenAI API error".to_owned()
    } else {
        format!("{provider} API error")
    };
    let body: Option<Value> = serde_json::from_str::<Value>(text.trim())
        .ok()
        .filter(|body| body.as_object().is_some_and(|object| !object.is_empty()));
    let detail = body
        .as_ref()
        .and_then(|body| body["error"]["message"].as_str())
        .filter(|detail| !detail.is_empty());
    let message = match (detail, &body) {
        (Some(detail), _) => format!("{status} {detail}"),
        (None, Some(body)) => format!("{status} {body}"),
        (None, None) => format!("{status} status code (no body)"),
    };
    let body_text = body.as_ref().map(Value::to_string);
    format_provider_error(Some(&prefix), Some(status), &message, body_text.as_deref())
}
