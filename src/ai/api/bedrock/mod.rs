mod config;
mod eventstream;
mod request;
mod sigv4;
mod stream;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde_json::Value;

use crate::ai::error_body::format_provider_error;
use crate::ai::event_stream::AssistantMessageEventStream;
use crate::ai::http::{self, Request, RetryPolicy};
use crate::ai::registry::Api;
use crate::ai::stream::spawn;
use crate::ai::types::{
    AssistantMessage, AssistantMessageEvent, Model, SimpleStreamOptions, TranscriptContext,
};
use crate::ai::utils::text::system_message_text;
use crate::ai::utils::transcript::{
    collapse_system_messages, get_current_tools, get_initial_system_message,
};

use config::{env, profile_region};

const DEFAULT_MAX_RETRY_DELAY_MS: u64 = 60_000;

struct Bedrock;

pub fn api() -> Arc<dyn Api> {
    Arc::new(Bedrock)
}

impl Api for Bedrock {
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

enum Auth {
    Bearer(String),
    Sigv4 {
        access_key: String,
        secret_key: String,
        session_token: Option<String>,
    },
}

fn run(
    model: &Model,
    context: TranscriptContext,
    options: SimpleStreamOptions,
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
) -> Result<(), String> {
    let auth = auth(&options)?;
    let transcript = collapse_system_messages(&context);
    let messages = &transcript.messages;
    let system = get_initial_system_message(messages)
        .map(system_message_text)
        .filter(|text| !text.is_empty());
    let tools = get_current_tools(messages);
    let built = request::build(model, messages, system.as_deref(), &tools, &options)?;
    let body = serde_json::to_vec(&built).map_err(|error| error.to_string())?;

    let configured = options
        .base_url
        .as_deref()
        .or(Some(model.base_url.as_str()))
        .filter(|url| !url.is_empty());
    let region = region(model, configured)?;
    let endpoint = match configured {
        Some(url)
            if endpoint_region(url).is_none()
                || (env("AWS_REGION").is_none()
                    && env("AWS_DEFAULT_REGION").is_none()
                    && env("AWS_PROFILE").is_none()) =>
        {
            url.trim_end_matches('/').to_owned()
        }
        _ => format!("https://bedrock-runtime.{region}.amazonaws.com"),
    };
    let path = format!(
        "/model/{}/converse-stream",
        sigv4::uri_encode(&model.id, false)
    );
    let url = format!("{endpoint}{path}");
    let host = authority(&url).to_owned();
    let amz_date = sigv4::amz_date(SystemTime::now());

    let mut request = Request::new("POST", url)
        .header("content-type", "application/json")
        .timeout(options.timeout_ms.map(Duration::from_millis));
    request.body = body;
    for source in [model.headers.as_ref(), options.headers.as_ref()]
        .into_iter()
        .flatten()
    {
        for (name, value) in source {
            request = request.header(name, value);
        }
    }
    request = match auth {
        Auth::Bearer(token) => request.header("authorization", format!("Bearer {token}")),
        Auth::Sigv4 {
            access_key,
            secret_key,
            session_token,
        } => {
            let mut signed = vec![
                ("content-type".to_owned(), "application/json".to_owned()),
                ("host".to_owned(), host),
                ("x-amz-date".to_owned(), amz_date.clone()),
            ];
            if let Some(token) = &session_token {
                signed.push(("x-amz-security-token".to_owned(), token.clone()));
            }
            let signing = sigv4::Signing {
                access_key: &access_key,
                secret_key: &secret_key,
                region: &region,
                service: "bedrock",
            };
            let authorization = sigv4::authorization(
                &signing,
                &amz_date,
                "POST",
                &sigv4::uri_encode(&path, true),
                &signed,
                &request.body,
            );
            let request = request.header("x-amz-date", &amz_date);
            let request = match &session_token {
                Some(token) => request.header("x-amz-security-token", token),
                None => request,
            };
            request.header("authorization", authorization)
        }
    };

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
            .and_then(|body| body["message"].as_str().map(str::to_owned))
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
    stream::read(
        out,
        output,
        response.into_reader(),
        model,
        options.signal.as_ref(),
    )
}

fn auth(options: &SimpleStreamOptions) -> Result<Auth, String> {
    if env("AWS_BEDROCK_SKIP_AUTH").as_deref() == Some("1") {
        return Ok(Auth::Sigv4 {
            access_key: "dummy-access-key".to_owned(),
            secret_key: "dummy-secret-key".to_owned(),
            session_token: None,
        });
    }
    if let Some(token) = options
        .api_key
        .clone()
        .filter(|key| !key.is_empty())
        .or_else(|| env("AWS_BEARER_TOKEN_BEDROCK"))
    {
        return Ok(Auth::Bearer(token));
    }
    let credentials = config::credentials()?;
    Ok(Auth::Sigv4 {
        access_key: credentials.access_key,
        secret_key: credentials.secret_key,
        session_token: credentials.session_token,
    })
}

fn region(model: &Model, endpoint: Option<&str>) -> Result<String, String> {
    if let Some(region) = arn_region(&model.id) {
        return Ok(region);
    }
    if let Some(region) = env("AWS_REGION").or_else(|| env("AWS_DEFAULT_REGION")) {
        return Ok(region);
    }
    if let Some(profile) = env("AWS_PROFILE") {
        return profile_region(&profile)?
            .ok_or_else(|| format!("amazon-bedrock: profile {profile} sets no region"));
    }
    Ok(endpoint
        .and_then(endpoint_region)
        .unwrap_or_else(|| "us-east-1".to_owned()))
}

fn arn_region(id: &str) -> Option<String> {
    let parts: Vec<&str> = id.split(':').collect();
    (parts.len() >= 6 && parts[0] == "arn" && parts[2] == "bedrock" && !parts[3].is_empty())
        .then(|| parts[3].to_owned())
}

fn endpoint_region(endpoint: &str) -> Option<String> {
    let host = authority(endpoint).split(':').next()?;
    host.strip_prefix("bedrock-runtime.")?
        .strip_suffix(".amazonaws.com")
        .map(str::to_owned)
}

fn authority(url: &str) -> &str {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    rest.split('/').next().unwrap_or(rest)
}
