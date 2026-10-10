//! OpenAI-compatible Chat Completions provider (`openai-completions`).

mod compat;
mod request;
mod stream;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use self::compat::Compat;
use self::request::Settings;
use crate::ai::error_body::format_provider_error;
use crate::ai::event_stream::AssistantMessageEventStream;
use crate::ai::http::{Request, RetryPolicy, send_retrying};
use crate::ai::registry::Api;
use crate::ai::stream::spawn;
use crate::ai::thinking::clamp_thinking_level;
use crate::ai::types::{
    AssistantMessage, CacheRetention, Model, ModelThinkingLevel, SimpleStreamOptions,
    ThinkingLevel, TranscriptContext,
};
use crate::cancel::CancelToken;

struct OpenAiCompletions;

impl Api for OpenAiCompletions {
    fn stream_simple(
        &self,
        model: &Model,
        context: TranscriptContext,
        options: SimpleStreamOptions,
    ) -> AssistantMessageEventStream {
        let signal = options.signal.clone();
        let model = model.clone();
        spawn(&model.clone(), signal.as_ref(), move |out, output| {
            run(out, output, &model, context, options)
        })
    }
}

pub fn api() -> Arc<dyn Api> {
    Arc::new(OpenAiCompletions)
}

fn run(
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
    model: &Model,
    context: TranscriptContext,
    options: SimpleStreamOptions,
) -> Result<(), String> {
    let base_url = options
        .base_url
        .clone()
        .filter(|url| !url.is_empty())
        .unwrap_or_else(|| model.base_url.clone());
    if base_url.is_empty() {
        return Err(format!(
            "No base URL for model {}/{}",
            model.provider, model.id
        ));
    }
    let compat = Compat::resolve(model, &base_url)?;
    let headers = headers(model, &options)?;
    let effort = options
        .reasoning
        .map(model_level)
        .map(|level| clamp_thinking_level(model, level))
        .filter(|level| *level != ModelThinkingLevel::Off);
    let settings = Settings {
        base_url: &base_url,
        effort,
        max_tokens: options.max_tokens.unwrap_or(model.max_tokens),
        retention: options.cache_retention.unwrap_or(CacheRetention::Short),
        session_id: options.session_id.as_deref(),
    };
    let body = request::build(model, &compat, &context, &settings);
    let mut request = Request::new(
        "POST",
        format!("{}/chat/completions", base_url.trim_end_matches('/')),
    )
    .json(&Value::Object(body))
    .timeout(options.timeout_ms.map(Duration::from_millis));
    for (name, value) in headers {
        request = request.header(&name, value);
    }
    let policy = retry_policy(&options);
    let cancel: Option<&CancelToken> = options.signal.as_ref();
    let response = send_retrying(&request, cancel, policy).map_err(|error| error.to_string())?;
    if !response.ok() {
        let status = response.status;
        let body = response.text().map_err(|error| error.to_string())?;
        return Err(format_provider_error(None, Some(status), "", Some(&body)));
    }
    stream::consume(out, output, model, &compat, cancel, response.into_reader())
}

fn model_level(level: ThinkingLevel) -> ModelThinkingLevel {
    match level {
        ThinkingLevel::Minimal => ModelThinkingLevel::Minimal,
        ThinkingLevel::Low => ModelThinkingLevel::Low,
        ThinkingLevel::Medium => ModelThinkingLevel::Medium,
        ThinkingLevel::High => ModelThinkingLevel::High,
        ThinkingLevel::Xhigh => ModelThinkingLevel::Xhigh,
        ThinkingLevel::Max => ModelThinkingLevel::Max,
    }
}

fn headers(
    model: &Model,
    options: &SimpleStreamOptions,
) -> Result<BTreeMap<String, String>, String> {
    let mut headers = BTreeMap::new();
    let overrides = options.headers.clone().unwrap_or_default();
    let has_authorization = model
        .headers
        .iter()
        .flatten()
        .chain(overrides.iter())
        .any(|(name, _)| name.eq_ignore_ascii_case("authorization"));
    match &options.api_key {
        Some(key) => {
            headers.insert("authorization".to_owned(), format!("Bearer {key}"));
        }
        None if has_authorization => {}
        None => return Err(format!("No API key for provider: {}", model.provider)),
    }
    headers.extend(model.headers.clone().unwrap_or_default());
    headers.extend(overrides);
    Ok(headers)
}

fn retry_policy(options: &SimpleStreamOptions) -> RetryPolicy {
    let default = RetryPolicy::default();
    RetryPolicy {
        max_retries: options.max_retries.map_or(default.max_retries, |count| {
            u32::try_from(count).unwrap_or(u32::MAX)
        }),
        max_delay: options
            .max_retry_delay_ms
            .map(Duration::from_millis)
            .or(default.max_delay),
    }
}
