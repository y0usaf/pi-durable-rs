use std::panic::{AssertUnwindSafe, catch_unwind};
use std::thread;

use super::event_stream::{ABORTED_TEXT, AssistantMessageEventStream};
use super::types::{
    AssistantMessage, AssistantMessageEvent, AssistantRole, Cost, DoneReason, ErrorReason, Model,
    StopReason, Usage,
};
use crate::cancel::CancelToken;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

pub fn new_output(model: &Model) -> AssistantMessage {
    AssistantMessage {
        role: AssistantRole,
        content: Vec::new(),
        api: model.api.clone(),
        provider: model.provider.clone(),
        model: model.id.clone(),
        response_model: None,
        response_id: None,
        provider_thinking_level: None,
        thinking_level: None,
        diagnostics: None,
        usage: Usage {
            input: 0,
            output: 0,
            cache_read: 0,
            cache_write: 0,
            cache_write_1h: None,
            reasoning: None,
            total_tokens: 0,
            cost: Cost {
                input: 0.0,
                output: 0.0,
                cache_read: 0.0,
                cache_write: 0.0,
                total: 0.0,
            },
        },
        stop_reason: StopReason::Pending,
        deferred: None,
        error_message: None,
        raw_stop_reason: None,
        end_turn: None,
        timestamp: now_ms(),
        duration_ms: None,
    }
}

pub fn fail(
    out: &AssistantMessageEventStream,
    mut output: AssistantMessage,
    signal: Option<&CancelToken>,
    message: String,
) {
    if out.is_done() {
        return;
    }
    let aborted = signal.is_some_and(CancelToken::is_cancelled);
    output.stop_reason = if aborted {
        StopReason::Aborted
    } else {
        StopReason::Error
    };
    output.error_message = Some(if aborted {
        ABORTED_TEXT.to_owned()
    } else {
        message
    });
    out.push(AssistantMessageEvent::Error {
        reason: if aborted {
            ErrorReason::Aborted
        } else {
            ErrorReason::Error
        },
        error: output,
    });
    out.end(None);
}

pub fn error_stream(model: &Model, message: String) -> AssistantMessageEventStream {
    let out = AssistantMessageEventStream::new();
    fail(&out, new_output(model), None, message);
    out
}

pub fn spawn<F>(model: &Model, signal: Option<&CancelToken>, run: F) -> AssistantMessageEventStream
where
    F: FnOnce(&AssistantMessageEventStream, &mut AssistantMessage) -> Result<(), String>
        + Send
        + 'static,
{
    let out = AssistantMessageEventStream::new();
    let mut output = new_output(model);
    if let Some(signal) = signal {
        out.bind_cancel_with(signal, output.clone());
    }
    let signal = signal.cloned();
    let sink = out.clone();
    thread::spawn(move || {
        let result = catch_unwind(AssertUnwindSafe(|| run(&sink, &mut output)));
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(_) => Err("provider stream panicked".to_owned()),
        };
        let failure = match outcome {
            Err(message) => Some(message),
            Ok(()) if signal.as_ref().is_some_and(CancelToken::is_cancelled) => {
                Some(ABORTED_TEXT.to_owned())
            }
            Ok(()) => match output.stop_reason {
                StopReason::Pending => Some("stream ended without a stop reason".to_owned()),
                StopReason::Error | StopReason::Aborted => Some(
                    output
                        .error_message
                        .clone()
                        .unwrap_or_else(|| "An unknown error occurred".to_owned()),
                ),
                _ => None,
            },
        };
        match failure {
            Some(message) => fail(&sink, output, signal.as_ref(), message),
            None => {
                let reason = match output.stop_reason {
                    StopReason::Length => DoneReason::Length,
                    StopReason::ToolUse => DoneReason::ToolUse,
                    StopReason::Deferred => DoneReason::Deferred,
                    _ => DoneReason::Stop,
                };
                sink.push(AssistantMessageEvent::Done {
                    reason,
                    message: output,
                });
                sink.end(None);
            }
        }
    });
    out
}
