use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, Hasher, RandomState};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::ai::event_stream::AssistantMessageEventStream;
use crate::ai::models::{Models, ModelsError};
use crate::ai::types::{
    AssistantBlock, AssistantMessage, AssistantMessageEvent, CacheRetention, Cost, DeferredHandle,
    DoneReason, ErrorReason, Message, Model, SimpleStreamOptions, StopReason, SystemMessage,
    TextContent, TextType, ThinkingContent, ThinkingType, ToolCall, ToolCallType,
    TranscriptContext, Usage, UserBlock, UserContent,
};
use crate::ai::utils::js;
use crate::ai::utils::text::{system_message_text, utf16_len};
use crate::cancel::CancelToken;

const DEFAULT_PROVIDER: &str = "faux";
const DEFAULT_MODEL_ID: &str = "faux-1";
const DEFAULT_MODEL_NAME: &str = "Faux Model";
const DEFAULT_MIN_TOKEN_SIZE: u64 = 3;
const DEFAULT_MAX_TOKEN_SIZE: u64 = 5;
const DEFAULT_CONTEXT_WINDOW: u64 = 128_000;
const DEFAULT_MAX_TOKENS: u64 = 16_384;
const NO_MORE_RESPONSES: &str = "No more faux responses queued";

#[derive(Debug, Clone, Default)]
pub struct FauxModelDefinition {
    pub id: String,
    pub name: Option<String>,
    pub reasoning: bool,
    pub context_window: Option<u64>,
    pub max_tokens: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct FauxTokenSize {
    pub min: Option<u64>,
    pub max: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct FauxDeferredOptions {
    pub pending_fetches: Option<u64>,
    pub poll_after_ms: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct FauxOptions {
    pub api: Option<String>,
    pub provider: Option<String>,
    pub models: Vec<FauxModelDefinition>,
    pub deferred: FauxDeferredOptions,
    pub tokens_per_second: Option<f64>,
    pub token_size: FauxTokenSize,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FauxState {
    pub call_count: u64,
    pub deferred_fetch_count: u64,
    pub cancelled_deferred: Vec<DeferredHandle>,
}

pub type ResponseFactory = Arc<
    dyn Fn(
            &TranscriptContext,
            Option<&SimpleStreamOptions>,
            &FauxState,
            &Model,
        ) -> Result<AssistantMessage, String>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub enum FauxResponse {
    Message(Box<AssistantMessage>),
    Factory(ResponseFactory),
}

struct Deferred {
    handle: DeferredHandle,
    response: Arc<FauxResponse>,
    context: TranscriptContext,
    options: SimpleStreamOptions,
    model: Model,
    pending_fetches: u64,
    cancelled: bool,
    final_message: Option<AssistantMessage>,
}

#[derive(Default)]
struct Inner {
    pending: VecDeque<Arc<FauxResponse>>,
    state: FauxState,
    prompt_cache: HashMap<String, Vec<u16>>,
    deferred: HashMap<String, Deferred>,
}

struct Core {
    api: String,
    provider: String,
    min_token: u64,
    max_token: u64,
    tokens_per_second: Option<f64>,
    deferred_pending: u64,
    poll_after_ms: Option<u64>,
    inner: Mutex<Inner>,
}

impl Core {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

pub struct Faux {
    core: Arc<Core>,
    models: Vec<Model>,
}

impl Faux {
    pub fn new(options: FauxOptions) -> Self {
        let api = options.api.unwrap_or_else(|| random_id("faux"));
        let provider = options
            .provider
            .unwrap_or_else(|| DEFAULT_PROVIDER.to_owned());
        let min_token = options
            .token_size
            .min
            .unwrap_or(DEFAULT_MIN_TOKEN_SIZE)
            .min(options.token_size.max.unwrap_or(DEFAULT_MAX_TOKEN_SIZE))
            .max(1);
        let max_token = options
            .token_size
            .max
            .unwrap_or(DEFAULT_MAX_TOKEN_SIZE)
            .max(min_token);
        let definitions = if options.models.is_empty() {
            vec![FauxModelDefinition {
                id: DEFAULT_MODEL_ID.to_owned(),
                name: Some(DEFAULT_MODEL_NAME.to_owned()),
                ..FauxModelDefinition::default()
            }]
        } else {
            options.models
        };
        let models = definitions
            .into_iter()
            .map(|definition| {
                let mut model = Model::new(
                    &provider,
                    &definition.id,
                    &api,
                    definition.context_window.unwrap_or(DEFAULT_CONTEXT_WINDOW),
                    definition.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
                    definition.reasoning,
                );
                model.name = definition.name.unwrap_or_else(|| definition.id.clone());
                model
            })
            .collect();
        let core = Core {
            api,
            provider,
            min_token,
            max_token,
            tokens_per_second: options.tokens_per_second,
            deferred_pending: options.deferred.pending_fetches.unwrap_or(0),
            poll_after_ms: options.deferred.poll_after_ms,
            inner: Mutex::new(Inner::default()),
        };
        Self {
            core: Arc::new(core),
            models,
        }
    }

    pub fn default_model(&self) -> Model {
        self.models[0].clone()
    }

    pub fn set_responses(&self, responses: Vec<FauxResponse>) {
        self.core.lock().pending = responses.into_iter().map(Arc::new).collect();
    }

    pub fn state(&self) -> FauxState {
        self.core.lock().state.clone()
    }

    pub fn stream(
        &self,
        model: &Model,
        context: TranscriptContext,
        options: SimpleStreamOptions,
    ) -> AssistantMessageEventStream {
        let out = AssistantMessageEventStream::new();
        if let Some(signal) = &options.signal {
            out.bind_cancel(signal);
        }
        let response = {
            let mut inner = self.core.lock();
            inner.state.call_count += 1;
            inner.pending.pop_front()
        };
        let core = Arc::clone(&self.core);
        let model = model.clone();
        let sink = out.clone();
        thread::spawn(move || run_stream(&core, &model, response, context, options, &sink));
        out
    }

    pub fn fetch_stream(
        &self,
        model: &Model,
        handle: &DeferredHandle,
        signal: &CancelToken,
    ) -> AssistantMessageEventStream {
        let out = AssistantMessageEventStream::new();
        out.bind_cancel(signal);
        self.core.lock().state.deferred_fetch_count += 1;
        let core = Arc::clone(&self.core);
        let model = model.clone();
        let handle = handle.clone();
        let signal = signal.clone();
        let sink = out.clone();
        thread::spawn(move || run_fetch(&core, &model, &handle, &signal, &sink));
        out
    }
}

impl Models for Faux {
    fn get_model(&self, provider: &str, id: &str) -> Option<Model> {
        if provider != self.core.provider {
            return None;
        }
        self.models.iter().find(|model| model.id == id).cloned()
    }

    fn stream_simple(
        &self,
        model: &Model,
        context: TranscriptContext,
        options: SimpleStreamOptions,
    ) -> AssistantMessageEventStream {
        self.stream(model, context, options)
    }

    fn fetch_deferred(
        &self,
        model: &Model,
        handle: &DeferredHandle,
        signal: &CancelToken,
    ) -> Result<AssistantMessage, ModelsError> {
        self.fetch_stream(model, handle, signal)
            .result()
            .ok_or_else(|| ModelsError("faux deferred stream ended without a message".to_owned()))
    }

    fn cancel_deferred(
        &self,
        _model: &Model,
        handle: &DeferredHandle,
        _signal: &CancelToken,
    ) -> Result<(), ModelsError> {
        let mut inner = self.core.lock();
        inner.state.cancelled_deferred.push(handle.clone());
        if let Some(entry) = inner.deferred.get_mut(&handle.id) {
            entry.cancelled = true;
        }
        Ok(())
    }
}

fn run_stream(
    core: &Core,
    model: &Model,
    response: Option<Arc<FauxResponse>>,
    context: TranscriptContext,
    options: SimpleStreamOptions,
    out: &AssistantMessageEventStream,
) {
    if let Err(error) = produce(core, model, response, &context, &options, out) {
        fail(core, model, &error, out);
    }
}

fn produce(
    core: &Core,
    model: &Model,
    response: Option<Arc<FauxResponse>>,
    context: &TranscriptContext,
    options: &SimpleStreamOptions,
    out: &AssistantMessageEventStream,
) -> Result<(), String> {
    let Some(response) = response else {
        let message = with_usage(
            core,
            error_message(core, &model.id, NO_MORE_RESPONSES),
            context,
            Some(options),
        );
        out.push(AssistantMessageEvent::Error {
            reason: ErrorReason::Error,
            error: message.clone(),
        });
        out.end(Some(message));
        return Ok(());
    };
    if options.deferred {
        let handle = DeferredHandle {
            provider: model.provider.clone(),
            model_id: model.id.clone(),
            api: model.api.clone(),
            id: random_id("deferred"),
            expires_at: None,
            poll_after_ms: core.poll_after_ms,
            data: None,
        };
        core.lock().deferred.insert(
            handle.id.clone(),
            Deferred {
                handle: handle.clone(),
                response,
                context: context.clone(),
                options: options.clone(),
                model: model.clone(),
                pending_fetches: core.deferred_pending,
                cancelled: false,
                final_message: None,
            },
        );
        let message = deferred_message(model, &handle);
        return stream_with_deltas(core, out, &message, options.signal.as_ref());
    }
    let message = resolve(core, &response, context, Some(options), model)?;
    stream_with_deltas(core, out, &message, options.signal.as_ref())
}

fn run_fetch(
    core: &Core,
    model: &Model,
    handle: &DeferredHandle,
    signal: &CancelToken,
    out: &AssistantMessageEventStream,
) {
    if let Err(error) = produce_fetch(core, model, handle, signal, out) {
        fail(core, model, &error, out);
    }
}

enum Next {
    Pending(AssistantMessage),
    Final(AssistantMessage),
    Resolve(
        Arc<FauxResponse>,
        TranscriptContext,
        SimpleStreamOptions,
        Box<Model>,
    ),
}

fn produce_fetch(
    core: &Core,
    model: &Model,
    handle: &DeferredHandle,
    signal: &CancelToken,
    out: &AssistantMessageEventStream,
) -> Result<(), String> {
    let next = {
        let mut inner = core.lock();
        let entry = inner
            .deferred
            .get_mut(&handle.id)
            .filter(|entry| same_handle(&entry.handle, handle))
            .ok_or_else(|| format!("Unknown faux deferred response: {}", handle.id))?;
        if entry.cancelled {
            return Err(format!(
                "Faux deferred response was cancelled: {}",
                handle.id
            ));
        }
        if entry.pending_fetches > 0 {
            entry.pending_fetches -= 1;
            Next::Pending(deferred_message(model, &entry.handle))
        } else if let Some(message) = &entry.final_message {
            Next::Final(message.clone())
        } else {
            Next::Resolve(
                Arc::clone(&entry.response),
                entry.context.clone(),
                entry.options.clone(),
                Box::new(entry.model.clone()),
            )
        }
    };
    let message = match next {
        Next::Pending(message) | Next::Final(message) => message,
        Next::Resolve(response, context, mut options, entry_model) => {
            options.deferred = false;
            options.signal = None;
            let message = resolve(core, &response, &context, Some(&options), &entry_model)
                .unwrap_or_else(|error| error_message(core, &entry_model.id, &error));
            if let Some(entry) = core.lock().deferred.get_mut(&handle.id) {
                entry.final_message = Some(message.clone());
            }
            message
        }
    };
    stream_with_deltas(core, out, &message, Some(signal))
}

fn same_handle(entry: &DeferredHandle, handle: &DeferredHandle) -> bool {
    entry.provider == handle.provider
        && entry.model_id == handle.model_id
        && entry.api == handle.api
}

fn resolve(
    core: &Core,
    response: &FauxResponse,
    context: &TranscriptContext,
    options: Option<&SimpleStreamOptions>,
    model: &Model,
) -> Result<AssistantMessage, String> {
    let mut resolved = match response {
        FauxResponse::Message(message) => (**message).clone(),
        FauxResponse::Factory(factory) => {
            let state = core.lock().state.clone();
            factory(context, options, &state, model)?
        }
    };
    resolved.api = core.api.clone();
    resolved.provider = core.provider.clone();
    resolved.model = model.id.clone();
    Ok(with_usage(core, resolved, context, options))
}

fn fail(core: &Core, model: &Model, error: &str, out: &AssistantMessageEventStream) {
    let message = error_message(core, &model.id, error);
    out.push(AssistantMessageEvent::Error {
        reason: ErrorReason::Error,
        error: message.clone(),
    });
    out.end(Some(message));
}

fn with_usage(
    core: &Core,
    mut message: AssistantMessage,
    context: &TranscriptContext,
    options: Option<&SimpleStreamOptions>,
) -> AssistantMessage {
    let prompt = prompt_units(context);
    let prompt_length = prompt.len() as u64;
    let prompt_tokens = tokens_of(prompt_length);
    let output = tokens_of(utf16_len(&assistant_text(&message.content)) as u64);
    let mut input = prompt_tokens;
    let mut cache_read = 0;
    let mut cache_write = 0;
    if let Some(options) = options
        && let Some(session) = options.session_id.as_deref().filter(|id| !id.is_empty())
        && options.cache_retention != Some(CacheRetention::None)
    {
        let mut inner = core.lock();
        match inner.prompt_cache.get(session) {
            Some(previous) => {
                let cached = common_prefix(previous, &prompt);
                cache_read = tokens_of(cached);
                cache_write = tokens_of(prompt_length - cached);
                input = prompt_tokens.saturating_sub(cache_read);
            }
            None => cache_write = prompt_tokens,
        }
        inner.prompt_cache.insert(session.to_owned(), prompt);
    }
    message.usage = Usage {
        input,
        output,
        cache_read,
        cache_write,
        cache_write_1h: None,
        reasoning: None,
        total_tokens: input + output + cache_read + cache_write,
        cost: Cost {
            input: 0.0,
            output: 0.0,
            cache_read: 0.0,
            cache_write: 0.0,
            total: 0.0,
        },
    };
    message
}

fn tokens_of(units: u64) -> u64 {
    units.div_ceil(4)
}

fn prompt_units(context: &TranscriptContext) -> Vec<u16> {
    let mut text = String::new();
    for (index, message) in context.messages.iter().enumerate() {
        if index > 0 {
            text.push_str("\n\n");
        }
        text.push_str(role_name(message));
        text.push(':');
        push_message_text(&mut text, message);
    }
    let mut units = Vec::with_capacity(text.len());
    units.extend(text.encode_utf16());
    units
}

fn common_prefix(previous: &[u16], current: &[u16]) -> u64 {
    previous
        .iter()
        .zip(current)
        .take_while(|(a, b)| a == b)
        .count() as u64
}

fn role_name(message: &Message) -> &'static str {
    match message {
        Message::System(_) => "system",
        Message::User(_) => "user",
        Message::Assistant(_) => "assistant",
        Message::ToolResult(_) => "toolResult",
    }
}

fn push_message_text(out: &mut String, message: &Message) {
    match message {
        Message::System(system) => out.push_str(&system_text(system)),
        Message::User(user) => match &user.content {
            UserContent::Text(text) => out.push_str(text),
            UserContent::Blocks(blocks) => {
                for (index, block) in blocks.iter().enumerate() {
                    if index > 0 {
                        out.push('\n');
                    }
                    push_block_text(out, block);
                }
            }
        },
        Message::Assistant(assistant) => push_assistant_text(out, &assistant.content),
        Message::ToolResult(result) => {
            out.push_str(&result.tool_name);
            for block in &result.content {
                out.push('\n');
                push_block_text(out, block);
            }
        }
    }
}

fn system_text(system: &SystemMessage) -> String {
    let mut parts = vec![system_message_text(system)];
    if let Some(removed) = &system.tools_removed {
        parts.extend(
            removed
                .iter()
                .map(|tool| format!("tool-:{}", json_text(tool))),
        );
    }
    if let Some(added) = &system.tools_added {
        parts.extend(
            added
                .iter()
                .map(|tool| format!("tool+:{}", json_text(tool))),
        );
    }
    parts
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn json_text<T: Serialize>(value: &T) -> String {
    js::stringify(&serde_json::to_value(value).expect("typed records serialize to JSON"))
}

fn push_block_text(out: &mut String, block: &UserBlock) {
    match block {
        UserBlock::Text(text) => out.push_str(&text.text),
        UserBlock::Image(image) => {
            out.push_str(&format!(
                "[image:{}:{}]",
                image.mime_type,
                utf16_len(&image.data)
            ));
        }
    }
}

fn assistant_text(content: &[AssistantBlock]) -> String {
    let mut out = String::new();
    push_assistant_text(&mut out, content);
    out
}

fn push_assistant_text(out: &mut String, content: &[AssistantBlock]) {
    for (index, block) in content.iter().enumerate() {
        if index > 0 {
            out.push('\n');
        }
        match block {
            AssistantBlock::Text(text) => out.push_str(&text.text),
            AssistantBlock::Thinking(thinking) => out.push_str(&thinking.thinking),
            AssistantBlock::ToolCall(call) => {
                out.push_str(&call.name);
                out.push(':');
                out.push_str(&js::stringify(&Value::Object(call.arguments.clone())));
            }
        }
    }
}

fn stream_with_deltas(
    core: &Core,
    out: &AssistantMessageEventStream,
    message: &AssistantMessage,
    signal: Option<&CancelToken>,
) -> Result<(), String> {
    let mut partial = AssistantMessage {
        content: Vec::new(),
        stop_reason: StopReason::Pending,
        ..message.clone()
    };
    if is_aborted(signal) {
        out.abort(&partial);
        return Ok(());
    }
    out.push(AssistantMessageEvent::Start {
        partial: partial.clone(),
    });
    for (index, block) in message.content.iter().enumerate() {
        if is_aborted(signal) {
            out.abort(&partial);
            return Ok(());
        }
        let content_index = index as u64;
        match block {
            AssistantBlock::Thinking(block) => {
                partial
                    .content
                    .push(AssistantBlock::Thinking(ThinkingContent {
                        kind: ThinkingType,
                        thinking: String::new(),
                        thinking_signature: None,
                        redacted: None,
                    }));
                out.push(AssistantMessageEvent::ThinkingStart {
                    content_index,
                    partial: partial.clone(),
                });
                for chunk in split_by_token_size(&block.thinking, core) {
                    pause(core, &chunk);
                    if is_aborted(signal) {
                        out.abort(&partial);
                        return Ok(());
                    }
                    if let Some(AssistantBlock::Thinking(current)) = partial.content.get_mut(index)
                    {
                        current.thinking.push_str(&chunk);
                    }
                    out.push(AssistantMessageEvent::ThinkingDelta {
                        content_index,
                        delta: chunk,
                        partial: partial.clone(),
                    });
                }
                out.push(AssistantMessageEvent::ThinkingEnd {
                    content_index,
                    content: block.thinking.clone(),
                    partial: partial.clone(),
                });
            }
            AssistantBlock::Text(block) => {
                partial.content.push(AssistantBlock::Text(TextContent {
                    kind: TextType,
                    text: String::new(),
                    text_signature: None,
                }));
                out.push(AssistantMessageEvent::TextStart {
                    content_index,
                    partial: partial.clone(),
                });
                for chunk in split_by_token_size(&block.text, core) {
                    pause(core, &chunk);
                    if is_aborted(signal) {
                        out.abort(&partial);
                        return Ok(());
                    }
                    if let Some(AssistantBlock::Text(current)) = partial.content.get_mut(index) {
                        current.text.push_str(&chunk);
                    }
                    out.push(AssistantMessageEvent::TextDelta {
                        content_index,
                        delta: chunk,
                        partial: partial.clone(),
                    });
                }
                out.push(AssistantMessageEvent::TextEnd {
                    content_index,
                    content: block.text.clone(),
                    partial: partial.clone(),
                });
            }
            AssistantBlock::ToolCall(call) => {
                partial.content.push(AssistantBlock::ToolCall(ToolCall {
                    kind: ToolCallType,
                    id: call.id.clone(),
                    name: call.name.clone(),
                    arguments: Map::new(),
                    thought_signature: None,
                    namespace: None,
                }));
                out.push(AssistantMessageEvent::ToolcallStart {
                    content_index,
                    partial: partial.clone(),
                });
                let encoded = js::stringify(&Value::Object(call.arguments.clone()));
                for chunk in split_by_token_size(&encoded, core) {
                    pause(core, &chunk);
                    if is_aborted(signal) {
                        out.abort(&partial);
                        return Ok(());
                    }
                    out.push(AssistantMessageEvent::ToolcallDelta {
                        content_index,
                        delta: chunk,
                        partial: partial.clone(),
                    });
                }
                if let Some(AssistantBlock::ToolCall(current)) = partial.content.get_mut(index) {
                    current.arguments = call.arguments.clone();
                }
                out.push(AssistantMessageEvent::ToolcallEnd {
                    content_index,
                    tool_call: call.clone(),
                    partial: partial.clone(),
                });
            }
        }
    }
    if matches!(message.stop_reason, StopReason::Pending) {
        return Err("Faux response ended without a stop reason".to_owned());
    }
    match message.stop_reason {
        StopReason::Error => out.push(AssistantMessageEvent::Error {
            reason: ErrorReason::Error,
            error: message.clone(),
        }),
        StopReason::Aborted => out.push(AssistantMessageEvent::Error {
            reason: ErrorReason::Aborted,
            error: message.clone(),
        }),
        _ => out.push(AssistantMessageEvent::Done {
            reason: done_reason(&message.stop_reason),
            message: message.clone(),
        }),
    }
    out.end(Some(message.clone()));
    Ok(())
}

fn done_reason(stop_reason: &StopReason) -> DoneReason {
    match stop_reason {
        StopReason::Length => DoneReason::Length,
        StopReason::ToolUse => DoneReason::ToolUse,
        StopReason::Deferred => DoneReason::Deferred,
        _ => DoneReason::Stop,
    }
}

fn is_aborted(signal: Option<&CancelToken>) -> bool {
    signal.is_some_and(CancelToken::is_cancelled)
}

fn pause(core: &Core, chunk: &str) {
    let Some(tokens_per_second) = core.tokens_per_second.filter(|rate| *rate > 0.0) else {
        return;
    };
    let delay_ms = tokens_of(utf16_len(chunk) as u64) as f64 / tokens_per_second * 1000.0;
    let whole_ms = if delay_ms < 1.0 {
        1
    } else {
        delay_ms.floor() as u64
    };
    thread::sleep(Duration::from_millis(whole_ms));
}

fn split_by_token_size(text: &str, core: &Core) -> Vec<String> {
    let units: Vec<u16> = text.encode_utf16().collect();
    let mut chunks = Vec::new();
    let mut index = 0;
    while index < units.len() {
        let token_size = core.min_token + random_below(core.max_token - core.min_token + 1);
        let size = (token_size * 4).max(1) as usize;
        let end = (index + size).min(units.len());
        chunks.push(String::from_utf16_lossy(&units[index..end]));
        index += size;
    }
    if chunks.is_empty() {
        vec![String::new()]
    } else {
        chunks
    }
}

fn random_below(bound: u64) -> u64 {
    RandomState::new().build_hasher().finish() % bound
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

fn random_id(prefix: &str) -> String {
    format!("{prefix}:{}:{:x}", now_ms(), random_below(1 << 53))
}

fn base_message(api: &str, provider: &str, model: &str, stop_reason: &str) -> AssistantMessage {
    serde_json::from_value(json!({
        "role": "assistant",
        "content": [],
        "api": api,
        "provider": provider,
        "model": model,
        "usage": {
            "input": 0,
            "output": 0,
            "cacheRead": 0,
            "cacheWrite": 0,
            "totalTokens": 0,
            "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}
        },
        "stopReason": stop_reason,
        "timestamp": now_ms(),
    }))
    .expect("faux base message matches the AssistantMessage shape")
}

fn error_message(core: &Core, model_id: &str, text: &str) -> AssistantMessage {
    let mut message = base_message(&core.api, &core.provider, model_id, "error");
    message.error_message = Some(text.to_owned());
    message
}

fn deferred_message(model: &Model, handle: &DeferredHandle) -> AssistantMessage {
    let mut message = base_message(&model.api, &model.provider, &model.id, "deferred");
    message.deferred = Some(handle.clone());
    message
}
