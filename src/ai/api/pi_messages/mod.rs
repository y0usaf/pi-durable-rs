mod events;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value, json};

use crate::ai::event_stream::{ABORTED_TEXT, AssistantMessageEventStream, now_ms};
use crate::ai::http::{Request, Response, send};
use crate::ai::registry::Api;
use crate::ai::sse::SseReader;
use crate::ai::stream::spawn;
use crate::ai::types::{
    AssistantMessage, AssistantMessageDiagnostic, CacheRetention, DiagnosticError, Model,
    SimpleStreamOptions, ThinkingLevel, TranscriptContext,
};
use crate::chord::json::JsonObject;

use events::Converter;

const ERROR_BODY_DETAIL_UNITS: usize = 8192;

pub type OnResponse = Arc<dyn Fn(&ResponseInfo) -> Result<(), String> + Send + Sync>;

#[derive(Clone, Default)]
pub struct Config {
    pub base_url: Option<String>,
    pub api_key: Option<String>,
    pub debug: bool,
    pub temperature: Option<f64>,
    pub tool_choice: Option<Value>,
    pub on_response: Option<OnResponse>,
}

pub struct ResponseInfo {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
}

pub fn api() -> Arc<dyn Api> {
    with_config(Config::default())
}

pub fn with_config(config: Config) -> Arc<dyn Api> {
    Arc::new(PiMessages { config })
}

pub fn radius(base_url: impl Into<String>, api_key: impl Into<String>) -> Arc<dyn Api> {
    with_config(Config {
        base_url: Some(base_url.into()),
        api_key: Some(api_key.into()),
        ..Config::default()
    })
}

struct PiMessages {
    config: Config,
}

impl Api for PiMessages {
    fn stream_simple(
        &self,
        model: &Model,
        context: TranscriptContext,
        options: SimpleStreamOptions,
    ) -> AssistantMessageEventStream {
        let signal = options.signal.clone();
        let run_model = model.clone();
        let config = self.config.clone();
        spawn(model, signal.as_ref(), move |out, output| {
            run(&run_model, &config, &context, &options, out, output)
        })
    }
}

fn run(
    model: &Model,
    config: &Config,
    context: &TranscriptContext,
    options: &SimpleStreamOptions,
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
) -> Result<(), String> {
    let api_key = options
        .api_key
        .as_deref()
        .filter(|key| !key.is_empty())
        .or(config.api_key.as_deref().filter(|key| !key.is_empty()))
        .ok_or_else(|| format!("No API key provided for provider {:?}", model.provider))?;
    let base_url = config
        .base_url
        .as_deref()
        .filter(|url| !url.is_empty())
        .unwrap_or(model.base_url.as_str());
    if base_url.is_empty() {
        return Err(format!("No base URL for model {}", model.id));
    }
    let body = json!({
        "model": model.id,
        "context": { "messages": context.messages },
        "options": Value::Object(payload_options(options, config)),
    });
    let mut url = format!("{}/messages", base_url.trim_end_matches('/'));
    if config.debug {
        url.push_str("?debug=1");
    }
    let mut headers: Vec<(String, String)> = Vec::new();
    set_header(&mut headers, "authorization", format!("Bearer {api_key}"));
    set_header(&mut headers, "accept", "text/event-stream".to_owned());
    for (name, value) in model.headers.iter().flatten() {
        set_header(&mut headers, name, value.clone());
    }
    for (name, value) in options.headers.iter().flatten() {
        set_header(&mut headers, name, value.clone());
    }
    let mut request = Request::new("POST", url.clone())
        .json(&body)
        .timeout(options.timeout_ms.map(Duration::from_millis));
    for (name, value) in &headers {
        request = request.header(name, value.clone());
    }

    let response = send(&request, options.signal.as_ref()).map_err(|error| error.to_string())?;
    if let Some(on_response) = &config.on_response {
        on_response(&response_info(&response))?;
    }
    if !response.ok() {
        let status = response.status;
        let text = response.text().map_err(|error| error.to_string())?;
        let cancelled = options
            .signal
            .as_ref()
            .is_some_and(|signal| signal.is_cancelled());
        return Err(response_error(
            model, &url, status, &text, cancelled, output,
        ));
    }

    let mut converter = Converter::default();
    for event in SseReader::new(response.into_reader()) {
        if options
            .signal
            .as_ref()
            .is_some_and(|signal| signal.is_cancelled())
        {
            return Err(ABORTED_TEXT.to_owned());
        }
        let event = event.map_err(|error| {
            crate::ai::http::io_error(&error, options.signal.as_ref()).to_string()
        })?;
        let data = event.data.trim();
        if data.is_empty() || data == "[DONE]" {
            continue;
        }
        let value: Value = serde_json::from_str(data)
            .map_err(|_| format!("invalid pi-messages event JSON: {data}"))?;
        if !truthy(&value) {
            continue;
        }
        if converter.apply(out, output, &value)? {
            return Ok(());
        }
    }
    Err(format!(
        "{} pi-messages stream ended before a done or error event",
        model.provider
    ))
}

fn response_info(response: &Response) -> ResponseInfo {
    let mut headers = BTreeMap::new();
    for (name, value) in &response.headers {
        let key = name.to_ascii_lowercase();
        let joined = match headers.get(&key) {
            Some(previous) if key != "set-cookie" => format!("{previous}, {value}"),
            _ => value.clone(),
        };
        headers.insert(key, joined);
    }
    ResponseInfo {
        status: response.status,
        headers,
    }
}

fn payload_options(options: &SimpleStreamOptions, config: &Config) -> Map<String, Value> {
    let mut fields = Map::new();
    if let Some(temperature) = config.temperature {
        fields.insert("temperature".to_owned(), json!(temperature));
    }
    if let Some(max_tokens) = options.max_tokens.filter(|tokens| *tokens > 0) {
        fields.insert("maxTokens".to_owned(), json!(max_tokens));
    }
    if let Some(level) = options.reasoning {
        fields.insert("reasoning".to_owned(), json!(thinking_name(level)));
    }
    if let Some(retention) = cache_retention_field(options) {
        fields.insert("cacheRetention".to_owned(), json!(retention));
    }
    if let Some(session) = options
        .session_id
        .as_deref()
        .filter(|session| !session.is_empty())
    {
        fields.insert("sessionId".to_owned(), json!(session));
    }
    if let Some(choice) = config
        .tool_choice
        .as_ref()
        .filter(|choice| !choice.is_null())
    {
        fields.insert("toolChoice".to_owned(), choice.clone());
    }
    fields
}

fn thinking_name(level: ThinkingLevel) -> &'static str {
    match level {
        ThinkingLevel::Minimal => "minimal",
        ThinkingLevel::Low => "low",
        ThinkingLevel::Medium => "medium",
        ThinkingLevel::High => "high",
        ThinkingLevel::Xhigh => "xhigh",
        ThinkingLevel::Max => "max",
    }
}

fn cache_retention_field(options: &SimpleStreamOptions) -> Option<&'static str> {
    match options.cache_retention {
        Some(CacheRetention::None) => Some("none"),
        Some(CacheRetention::Short) => Some("short"),
        Some(CacheRetention::Long) => Some("long"),
        None if std::env::var("PI_CACHE_RETENTION").is_ok_and(|value| value == "long") => {
            Some("long")
        }
        None => None,
    }
}

fn response_error(
    model: &Model,
    url: &str,
    status: u16,
    body: &str,
    cancelled: bool,
    output: &mut AssistantMessage,
) -> String {
    let error = serde_json::from_str::<Value>(body.trim())
        .ok()
        .and_then(|parsed| {
            parsed
                .get("error")
                .filter(|error| error.is_object())
                .cloned()
        });
    let message = error
        .as_ref()
        .and_then(|error| error["message"].as_str())
        .map(str::to_owned);
    let code = error
        .as_ref()
        .and_then(|error| error["code"].as_str())
        .filter(|code| !code.is_empty())
        .map(str::to_owned);
    let suffix = message.clone().unwrap_or_else(|| body.to_owned());
    let text = match &code {
        Some(code) => format!("{status}: {suffix} ({code})"),
        None => format!("{status}: {suffix}"),
    };
    if cancelled {
        return text;
    }
    let mut details = JsonObject::new();
    details.insert("version".to_owned(), json!(1));
    details.insert("provider".to_owned(), json!(model.provider));
    details.insert("model".to_owned(), json!(model.id));
    details.insert("url".to_owned(), json!(url));
    details.insert("status".to_owned(), json!(status));
    details.insert("timestampMs".to_owned(), json!(now_ms()));
    match error {
        Some(error) => details.insert("error".to_owned(), error),
        None => details.insert(
            "body".to_owned(),
            json!(truncate_units(body, ERROR_BODY_DETAIL_UNITS)),
        ),
    };
    output.diagnostics = Some(vec![AssistantMessageDiagnostic {
        kind: "pi_messages_response_failure".to_owned(),
        timestamp: now_ms(),
        error: Some(DiagnosticError {
            name: Some("PiMessagesResponseError".to_owned()),
            message: text.clone(),
            stack: None,
            code: code.map(Value::String),
        }),
        details: Some(details),
    }]);
    text
}

fn truncate_units(text: &str, units: usize) -> String {
    let encoded: Vec<u16> = text.encode_utf16().collect();
    if encoded.len() <= units {
        return text.to_owned();
    }
    format!("{}…", String::from_utf16_lossy(&encoded[..units]))
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
        Value::String(text) => !text.is_empty(),
        _ => true,
    }
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
