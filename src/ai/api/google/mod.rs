mod adc;
mod request;
mod stream;
mod vertex;

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::ai::error_body::format_provider_error;
use crate::ai::event_stream::AssistantMessageEventStream;
use crate::ai::http::{Request, RetryPolicy, send_retrying};
use crate::ai::registry::Api;
use crate::ai::stream::spawn;
use crate::ai::types::{
    AssistantMessage, AssistantMessageEvent, Model, SimpleStreamOptions, TranscriptContext,
};
use crate::ai::utils::text::system_message_text;
use crate::ai::utils::transcript::{
    collapse_system_messages, get_current_tools, get_initial_system_message,
    without_initial_system_message,
};

const DEFAULT_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta";

pub fn api() -> Arc<dyn Api> {
    Arc::new(Google { vertex: false })
}

pub fn vertex_api() -> Arc<dyn Api> {
    Arc::new(Google { vertex: true })
}

struct Google {
    vertex: bool,
}

impl Api for Google {
    fn stream_simple(
        &self,
        model: &Model,
        context: TranscriptContext,
        options: SimpleStreamOptions,
    ) -> AssistantMessageEventStream {
        let signal = options.signal.clone();
        let run_model = model.clone();
        let vertex = self.vertex;
        spawn(model, signal.as_ref(), move |out, output| {
            run(&run_model, &context, &options, vertex, out, output)
        })
    }
}

struct Target {
    base_url: String,
    headers: Vec<(String, String)>,
}

fn run(
    model: &Model,
    context: &TranscriptContext,
    options: &SimpleStreamOptions,
    vertex: bool,
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
) -> Result<(), String> {
    let context = collapse_system_messages(context);
    let messages = context.messages.as_slice();
    let system_prompt = get_initial_system_message(messages)
        .map(system_message_text)
        .filter(|text| !text.is_empty());
    let contents = request::contents(model, without_initial_system_message(messages));
    let tools = get_current_tools(messages);
    let body = request::build(model, contents, system_prompt, &tools, options)?;
    let target = if vertex {
        vertex::target(model, options)?
    } else {
        generative_target(model, options)
    };
    let url = format!(
        "{}/models/{}:streamGenerateContent?alt=sse",
        target.base_url.trim_end_matches('/'),
        model.id.strip_prefix("models/").unwrap_or(&model.id)
    );
    let mut request = Request::new("POST", url)
        .json(&body)
        .timeout(options.timeout_ms.map(Duration::from_millis));
    for (name, value) in target.headers {
        request = request.header(&name, value);
    }
    for (name, value) in model.headers.iter().flatten() {
        request = request.header(name, value.clone());
    }
    for (name, value) in options.headers.iter().flatten() {
        request = request.header(name, value.clone());
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
    stream::read(
        out,
        output,
        response.into_reader(),
        model,
        vertex,
        options.signal.as_ref(),
    )
}

fn generative_target(model: &Model, options: &SimpleStreamOptions) -> Target {
    let base_url = options
        .base_url
        .as_deref()
        .filter(|url| !url.is_empty())
        .or(Some(model.base_url.as_str()).filter(|url| !url.is_empty()))
        .unwrap_or(DEFAULT_BASE_URL)
        .to_owned();
    let headers = options
        .api_key
        .as_deref()
        .filter(|key| !key.is_empty())
        .map(|key| ("x-goog-api-key".to_owned(), key.to_owned()))
        .into_iter()
        .collect();
    Target { base_url, headers }
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

fn status_error(provider: &str, status: u16, text: &str) -> String {
    let prefix = format!("{provider} API error");
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
