//! The `anthropic-messages` provider: the Anthropic Messages API over HTTP with SSE streaming,
//! API-key and Claude Code OAuth auth, and the request shaping of pi-ai's Anthropic adapter.

mod request;
mod stream;

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::ai::error_body::format_provider_error;
use crate::ai::event_stream::AssistantMessageEventStream;
use crate::ai::http::{self, Request, RetryPolicy};
use crate::ai::oauth::anthropic::is_oauth_token;
use crate::ai::registry::Api;
use crate::ai::stream::spawn;
use crate::ai::types::{
    AssistantMessage, AssistantMessageEvent, Model, SimpleStreamOptions, TranscriptContext,
};
use crate::ai::utils::transcript::{get_current_tools, resolve_transcript};

const API_VERSION: &str = "2023-06-01";
const CLAUDE_CODE_VERSION: &str = "2.1.280";
const DEFAULT_MAX_RETRY_DELAY_MS: u64 = 60_000;
const AUTH_HEADERS: [&str; 3] = ["authorization", "x-api-key", "cf-aig-authorization"];

struct Anthropic;

pub fn api() -> Arc<dyn Api> {
    Arc::new(Anthropic)
}

impl Api for Anthropic {
    fn stream_simple(
        &self,
        model: &Model,
        context: TranscriptContext,
        options: SimpleStreamOptions,
    ) -> AssistantMessageEventStream {
        let signal = options.signal.clone();
        let owned = model.clone();
        spawn(model, signal.as_ref(), move |out, output| {
            run(&owned, context, options, out, output)
        })
    }
}

fn run(
    model: &Model,
    context: TranscriptContext,
    options: SimpleStreamOptions,
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
) -> Result<(), String> {
    let transcript = resolve_transcript(
        &context,
        request::flag(model, "supportsMidConvoSystemMessages", false),
    );
    let api_key = options.api_key.as_deref().filter(|key| !key.is_empty());
    let oauth = api_key.is_some_and(is_oauth_token);
    let mut headers = headers(model, &options, api_key, oauth)?;
    let built = request::build(model, &transcript.messages, oauth, &options);
    if built.provider_thinking_level.is_some() {
        output.provider_thinking_level = built.provider_thinking_level.clone();
    }
    if !built.betas.is_empty() {
        set_header(&mut headers, "anthropic-beta", &built.betas.join(","));
    }
    let base = options.base_url.as_deref().unwrap_or(&model.base_url);
    let mut request = Request::new(
        "POST",
        format!("{}/v1/messages?beta=true", base.trim_end_matches('/')),
    )
    .json(&built.body)
    .timeout(options.timeout_ms.map(Duration::from_millis));
    for (name, value) in &headers {
        request = request.header(name, value);
    }
    let policy = RetryPolicy {
        max_retries: u32::try_from(options.max_retries.unwrap_or(2)).unwrap_or(u32::MAX),
        max_delay: Some(Duration::from_millis(
            options
                .max_retry_delay_ms
                .unwrap_or(DEFAULT_MAX_RETRY_DELAY_MS),
        )),
    };
    let response = http::send_retrying(&request, options.signal.as_ref(), policy)
        .map_err(|error| error.to_string())?;
    if !response.ok() {
        let status = response.status;
        let text = response.text().map_err(|error| error.to_string())?;
        let message = serde_json::from_str::<Value>(&text)
            .ok()
            .and_then(|body| body["error"]["message"].as_str().map(str::to_owned))
            .unwrap_or_else(|| text.clone());
        return Err(format_provider_error(
            None,
            Some(status),
            &message,
            Some(&text),
        ));
    }
    out.push(AssistantMessageEvent::Start {
        partial: output.clone(),
    });
    let tools = get_current_tools(&transcript.messages);
    stream::read(
        out,
        output,
        response.into_reader(),
        model,
        oauth,
        &tools,
        options.signal.as_ref(),
    )
}

fn headers(
    model: &Model,
    options: &SimpleStreamOptions,
    api_key: Option<&str>,
    oauth: bool,
) -> Result<Vec<(String, String)>, String> {
    let header_auth = [model.headers.as_ref(), options.headers.as_ref()]
        .into_iter()
        .flatten()
        .flatten()
        .any(|(name, value)| {
            AUTH_HEADERS
                .iter()
                .any(|auth| name.eq_ignore_ascii_case(auth))
                && !value.trim().is_empty()
        });
    if api_key.is_none() && !header_auth {
        return Err(format!("No API key for provider: {}", model.provider));
    }
    let mut headers = Vec::new();
    set_header(&mut headers, "accept", "application/json");
    set_header(
        &mut headers,
        "anthropic-dangerous-direct-browser-access",
        "true",
    );
    set_header(&mut headers, "anthropic-version", API_VERSION);
    if let Some(key) = api_key {
        if oauth {
            set_header(&mut headers, "authorization", &format!("Bearer {key}"));
            set_header(
                &mut headers,
                "user-agent",
                &format!("claude-cli/{CLAUDE_CODE_VERSION}"),
            );
            set_header(&mut headers, "x-app", "cli");
        } else {
            set_header(&mut headers, "x-api-key", key);
        }
    }
    for source in [model.headers.as_ref(), options.headers.as_ref()]
        .into_iter()
        .flatten()
    {
        for (name, value) in source {
            if !name.eq_ignore_ascii_case("anthropic-beta") {
                set_header(&mut headers, name, value);
            }
        }
    }
    Ok(headers)
}

fn set_header(headers: &mut Vec<(String, String)>, name: &str, value: &str) {
    match headers
        .iter_mut()
        .find(|(known, _)| known.eq_ignore_ascii_case(name))
    {
        Some(slot) => slot.1 = value.to_owned(),
        None => headers.push((name.to_owned(), value.to_owned())),
    }
}
