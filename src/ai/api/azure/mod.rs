use std::sync::Arc;

use serde_json::{Map, Value};

use super::openai_responses::{Route, run};
use crate::ai::event_stream::AssistantMessageEventStream;
use crate::ai::registry::Api;
use crate::ai::stream::spawn;
use crate::ai::types::{Model, SimpleStreamOptions, TranscriptContext};

const DEFAULT_API_VERSION: &str = "v1";
const AZURE_HOST_SUFFIXES: [&str; 3] = [
    ".openai.azure.com",
    ".cognitiveservices.azure.com",
    ".ai.azure.com",
];

pub fn api() -> Arc<dyn Api> {
    Arc::new(Azure)
}

struct Azure;

impl Api for Azure {
    fn stream_simple(
        &self,
        model: &Model,
        context: TranscriptContext,
        options: SimpleStreamOptions,
    ) -> AssistantMessageEventStream {
        let signal = options.signal.clone();
        let run_model = model.clone();
        spawn(model, signal.as_ref(), move |out, output| {
            let (wire, route, options) = prepare(&run_model, options)?;
            run(&wire, Some(&route), &context, &options, out, output)
        })
    }
}

fn prepare(
    model: &Model,
    mut options: SimpleStreamOptions,
) -> Result<(Model, Route, SimpleStreamOptions), String> {
    let api_key = options
        .api_key
        .clone()
        .filter(|key| !key.is_empty())
        .or_else(|| env_value("AZURE_OPENAI_API_KEY"))
        .ok_or("azure-openai-responses: AZURE_OPENAI_API_KEY is required")?;
    options.api_key = Some(api_key);
    let base = base_url(model, &options)?;
    let version =
        env_value("AZURE_OPENAI_API_VERSION").unwrap_or_else(|| DEFAULT_API_VERSION.to_owned());
    let route = Route {
        url: format!("{base}/responses?api-version={}", query_escape(&version)),
        key_header: "api-key",
    };
    let mut wire = model.clone();
    wire.id = deployment_name(&model.id);
    let compat = wire.compat.get_or_insert_with(|| Value::Object(Map::new()));
    if let Some(compat) = compat.as_object_mut() {
        compat
            .entry("supportsStrictMode")
            .or_insert(Value::Bool(true));
    }
    Ok((wire, route, options))
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn deployment_name(model_id: &str) -> String {
    let map = env_value("AZURE_OPENAI_DEPLOYMENT_NAME_MAP").unwrap_or_default();
    let mut mapped = None;
    for entry in map.split(',') {
        let parts: Vec<&str> = entry.trim().splitn(3, '=').collect();
        if parts.len() < 2 || parts[0].is_empty() || parts[1].is_empty() {
            continue;
        }
        if parts[0].trim() == model_id {
            mapped = Some(parts[1].trim().to_owned());
        }
    }
    mapped
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| model_id.to_owned())
}

fn base_url(model: &Model, options: &SimpleStreamOptions) -> Result<String, String> {
    let configured = options
        .base_url
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(str::to_owned)
        .or_else(|| env_value("AZURE_OPENAI_BASE_URL"));
    let from_resource = || {
        env_value("AZURE_OPENAI_RESOURCE_NAME")
            .map(|name| format!("https://{name}.openai.azure.com/openai/v1"))
    };
    let base = configured
        .or_else(from_resource)
        .or_else(|| (!model.base_url.is_empty()).then(|| model.base_url.clone()))
        .ok_or(
            "Azure OpenAI base URL is required. Set AZURE_OPENAI_BASE_URL or AZURE_OPENAI_RESOURCE_NAME, or pass azureBaseUrl, azureResourceName, or model.baseUrl.",
        )?;
    normalize_base(&base)
}

fn normalize_base(base: &str) -> Result<String, String> {
    let invalid = || format!("Invalid Azure OpenAI base URL: {base}");
    let trimmed = base.trim().trim_end_matches('/');
    let (scheme, rest) = trimmed.split_once("://").ok_or_else(invalid)?;
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let (authority, path) = rest.split_at(authority_end);
    let host = authority.split(':').next().unwrap_or_default();
    if scheme.is_empty() || host.is_empty() {
        return Err(invalid());
    }
    let (path, query) = match path.find('?') {
        Some(index) => path.split_at(index),
        None => (path, ""),
    };
    let path = path.trim_end_matches('/');
    let azure = AZURE_HOST_SUFFIXES
        .iter()
        .any(|suffix| host.ends_with(suffix));
    if azure && matches!(path, "" | "/openai" | "/openai/v1/responses") {
        return Ok(format!("{scheme}://{authority}/openai/v1"));
    }
    Ok(format!("{scheme}://{authority}{path}{query}"))
}

fn query_escape(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                char::from(byte).to_string()
            }
            b' ' => "+".to_owned(),
            _ => format!("%{byte:02X}"),
        })
        .collect()
}
