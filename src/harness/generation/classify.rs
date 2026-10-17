use crate::ai::event_stream::{Next, now_ms};
use crate::ai::stream::new_output;
use crate::ai::types::{
    AssistantBlock, AssistantMessage, AssistantMessageEvent, Message, Model, SimpleStreamOptions,
    StopReason, ToolCall,
};
use crate::ai::utils::overflow::is_context_overflow;
use crate::ai::utils::retry::{is_retryable_assistant_error, retry_delay_ms};
use crate::cancel::CancelToken;
use crate::errors::{Error, Result};
use crate::harness::compaction::{create_compaction, select_cut};
use crate::harness::context::ContextView;
use crate::harness::generation::{
    GenerationCheckpoint, PartialHook, Runtime, append_assistant, edit_live, fail_run, queue_modes,
    read_live, write_live,
};
use crate::harness::inbox::prepare_boundary;
use crate::harness::live::{DeferredStatus, GenerationStatus, RetryStatus};
use crate::harness::types::{CompactionReason, HookApi, ModelRef};
use crate::ids::{EntryId, TaskId};
use crate::tasks::{JoinPolicy, NextTaskState, TaskOutcome, TaskOutcomeError};
use crate::types::SubmissionSettlement;
use std::sync::Arc;

use std::time::{Duration, Instant};

pub const DEFAULT_POLL_AFTER_MS: u64 = 5000;

pub struct Request {
    pub attempt: u64,
    pub compacted: Option<TaskId>,
    pub model: ModelRef,
    pub cutoff: EntryId,
    pub view: Option<Arc<ContextView>>,
    pub poll_at: Option<u64>,
}

fn into_partial(event: AssistantMessageEvent) -> Option<AssistantMessage> {
    match event {
        AssistantMessageEvent::Start { partial }
        | AssistantMessageEvent::TextStart { partial, .. }
        | AssistantMessageEvent::TextDelta { partial, .. }
        | AssistantMessageEvent::TextEnd { partial, .. }
        | AssistantMessageEvent::ThinkingStart { partial, .. }
        | AssistantMessageEvent::ThinkingDelta { partial, .. }
        | AssistantMessageEvent::ThinkingEnd { partial, .. }
        | AssistantMessageEvent::ToolcallStart { partial, .. }
        | AssistantMessageEvent::ToolcallDelta { partial, .. }
        | AssistantMessageEvent::ToolcallEnd { partial, .. } => Some(partial),
        AssistantMessageEvent::Done { .. } | AssistantMessageEvent::Error { .. } => None,
    }
}

pub fn stream_response(
    runtime: Runtime<'_>,
    model: &Model,
    messages: &[Message],
    options: SimpleStreamOptions,
    attempt: u64,
    cancel: &CancelToken,
) -> Result<AssistantMessage> {
    let mut sinks: Vec<PartialHook> = Vec::new();
    runtime.hooks().each(&mut |hooks| {
        if let Some(sink) = hooks.on_partial.as_ref() {
            sinks.push(Arc::clone(sink));
        }
        Ok(())
    })?;
    let api = HookApi {
        conversation_id: runtime.conversation_id(),
    };
    let result = stream_events(
        runtime, model, messages, options, attempt, cancel, &sinks, &api,
    );
    if result.is_err() || runtime.signal().is_cancelled() {
        for sink in &sinks {
            sink(&api, None);
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn stream_events(
    runtime: Runtime<'_>,
    model: &Model,
    messages: &[Message],
    mut options: SimpleStreamOptions,
    attempt: u64,
    cancel: &CancelToken,
    sinks: &[PartialHook],
    api: &HookApi,
) -> Result<AssistantMessage> {
    let progress = runtime.settings().progress;
    let interval = Duration::from_millis(progress.partial_interval_ms);
    let stall = Duration::from_millis(progress.stall_ms);
    let stop = CancelToken::new();
    let _link = options.signal.replace(stop.clone()).map(|signal| {
        let stop = stop.clone();
        signal.subscribe(move || stop.cancel())
    });
    let stream = runtime.models().stream_simple(
        model,
        crate::ai::types::TranscriptContext {
            messages: messages.to_vec(),
        },
        options,
    );
    let mut latest: Option<Arc<AssistantMessage>> = None;
    let mut pending: Option<Arc<AssistantMessage>> = None;
    let mut due: Option<Instant> = None;
    let mut committed = Instant::now()
        .checked_sub(interval)
        .unwrap_or_else(Instant::now);
    let mut heard = Instant::now();
    loop {
        let quiet = heard + stall;
        let wake = due.map_or(quiet, |at| at.min(quiet));
        match stream.next_event_timeout(wake.saturating_duration_since(Instant::now())) {
            Next::Event(event) => {
                heard = Instant::now();
                let Some(partial) = into_partial(event) else {
                    continue;
                };
                if partial.content.is_empty() {
                    continue;
                }
                let partial = Arc::new(partial);
                for sink in sinks {
                    sink(api, Some(Arc::clone(&partial)));
                }
                latest = Some(Arc::clone(&partial));
                pending = Some(partial);
                due.get_or_insert(committed + interval);
            }
            Next::Timeout if Instant::now() >= quiet => {
                stop.cancel();
                let mut message = latest.map_or_else(|| new_output(model), Arc::unwrap_or_clone);
                message.stop_reason = StopReason::Error;
                message.error_message =
                    Some(format!("Request timed out: no stream event in {stall:?}"));
                message.timestamp = now_ms();
                return Ok(message);
            }
            Next::Timeout => {
                due = None;
                if let Some(partial) = pending.take() {
                    commit_partial(&runtime, attempt, partial, cancel)?;
                    committed = Instant::now();
                }
            }
            Next::End => break,
        }
    }
    stream
        .result()
        .ok_or_else(|| Error::Invalid("stream ended without a final message".to_owned()))
}

fn commit_partial(
    runtime: &Runtime<'_>,
    attempt: u64,
    snapshot: Arc<AssistantMessage>,
    cancel: &CancelToken,
) -> Result<()> {
    let snapshot = Arc::try_unwrap(snapshot).unwrap_or_else(|shared| (*shared).clone());
    let conversation_id = runtime.conversation_id();
    runtime.commit(
        Box::new(move |tx, _task| {
            edit_live(tx, conversation_id, |live| {
                live.generation
                    .get_or_insert(GenerationStatus {
                        attempt,
                        message: None,
                        retry: None,
                        deferred: None,
                    })
                    .message = Some(snapshot);
            })?;
            Ok(None)
        }),
        cancel,
    )?;
    Ok(())
}

pub fn classify(
    runtime: Runtime<'_>,
    request: &Request,
    message: AssistantMessage,
    cancel: &CancelToken,
) -> Result<()> {
    if runtime.signal().is_cancelled() {
        return Err(Error::Aborted);
    }
    let conversation_id = runtime.conversation_id();
    let task_id = crate::harness::tool::erase(runtime.task_id());
    if message.stop_reason == StopReason::Deferred
        && let Some(handle) = message.deferred.clone()
    {
        let after = handle.poll_after_ms.unwrap_or(DEFAULT_POLL_AFTER_MS);
        let poll_at = (runtime.now() + after).max(request.poll_at.map_or(0, |at| at + 1));
        let attempt = request.attempt;
        let compacted = request.compacted;
        let model = request.model.clone();
        let cutoff = request.cutoff;
        runtime.commit(
            Box::new(move |tx, _task| {
                edit_live(tx, conversation_id, |live| {
                    live.generation = Some(GenerationStatus {
                        attempt,
                        message: None,
                        retry: None,
                        deferred: Some(DeferredStatus { poll_at }),
                    });
                })?;
                Ok(Some(NextTaskState::Running {
                    checkpoint: GenerationCheckpoint::Poll {
                        attempt,
                        compacted,
                        model,
                        cutoff,
                        handle,
                        poll_at,
                    },
                }))
            }),
            cancel,
        )?;
        return Ok(());
    }
    let api = HookApi {
        conversation_id: runtime.conversation_id(),
    };
    runtime.hooks().each(&mut |hooks| {
        if let Some(hook) = hooks.after_response.as_ref() {
            hook(&api, &message)?;
        }
        Ok(())
    })?;
    let calls: Vec<ToolCall> = message
        .content
        .iter()
        .filter_map(|block| match block {
            AssistantBlock::ToolCall(call) => Some(call.clone()),
            _ => None,
        })
        .collect();
    if message.stop_reason == StopReason::ToolUse && !calls.is_empty() {
        return crate::harness::generation::rounds::start_tool_round(
            runtime, request, message, calls, cancel,
        );
    }
    if matches!(
        message.stop_reason,
        StopReason::Stop | StopReason::Length | StopReason::ToolUse
    ) {
        return crate::harness::generation::rounds::answer(runtime, message, cancel);
    }
    let settings = runtime.settings().clone();
    let overflow = message.stop_reason == StopReason::Error && is_context_overflow(&message, None);
    if overflow && request.compacted.is_none() && settings.compaction.enabled {
        let view = runtime.context(Some(request.cutoff), cancel)?;
        if select_cut(&view, settings.compaction.keep_recent_tokens).is_some() {
            let text = message
                .error_message
                .clone()
                .unwrap_or_else(|| "Context overflow".to_owned());
            let attempt = request.attempt;
            runtime.commit(
                Box::new(move |tx, _task| {
                    append_assistant(tx, conversation_id, &message)?;
                    edit_live(tx, conversation_id, |live| live.generation = None)?;
                    let child = create_compaction(
                        tx,
                        conversation_id,
                        CompactionReason::Overflow,
                        Some(task_id),
                    )?;
                    Ok(Some(NextTaskState::Waiting {
                        checkpoint: GenerationCheckpoint::Prepare {
                            attempt,
                            compacted: Some(child),
                            overflow: Some(text),
                        },
                        on: vec![child],
                        policy: JoinPolicy::AllSettled,
                    }))
                }),
                cancel,
            )?;
            return Ok(());
        }
    }
    let policy = settings.retry;
    let retry = message.stop_reason == StopReason::Error
        && !overflow
        && is_retryable_assistant_error(&message)
        && policy.enabled
        && request.attempt <= policy.max_retries;
    let until = if retry {
        runtime.now()
            + retry_delay_ms(
                policy.base_delay_ms,
                policy.max_agent_delay_ms,
                request.attempt,
            )
    } else {
        0
    };
    let attempt = request.attempt;
    let compacted = request.compacted;
    let modes = queue_modes(&settings);
    let now = runtime.now();
    runtime.commit(
        Box::new(move |tx, _task| {
            let mut boundary = prepare_boundary(tx, conversation_id, modes)?;
            append_assistant(tx, conversation_id, &message)?;
            if retry {
                edit_live(tx, conversation_id, |live| {
                    live.generation = Some(GenerationStatus {
                        attempt,
                        message: None,
                        retry: Some(RetryStatus {
                            at: until,
                            error: message.error_message.clone().unwrap_or_default(),
                        }),
                        deferred: None,
                    });
                })?;
                return Ok(Some(NextTaskState::Running {
                    checkpoint: GenerationCheckpoint::Retry {
                        attempt,
                        compacted,
                        until,
                    },
                }));
            }
            let text = message.error_message.clone().unwrap_or_else(|| {
                format!(
                    "Model response ended with stop reason {}",
                    stop_reason_text(message.stop_reason)
                )
            });
            let mut live = read_live(tx, conversation_id)?;
            fail_run(
                tx,
                &mut boundary,
                &mut live,
                task_id,
                SubmissionSettlement::Unanswered {
                    reason: "model_error".to_owned(),
                    detail: Some(serde_json::Value::String(text.clone())),
                },
                now,
            )?;
            write_live(tx, conversation_id, &live)?;
            Ok(Some(NextTaskState::Terminal {
                outcome: TaskOutcome::Failed {
                    error: TaskOutcomeError {
                        message: text,
                        detail: Some(serde_json::json!({ "reason": "model_error" })),
                    },
                    result: None,
                },
            }))
        }),
        cancel,
    )
}

fn stop_reason_text(reason: StopReason) -> &'static str {
    match reason {
        StopReason::Pending => "pending",
        StopReason::Stop => "stop",
        StopReason::Length => "length",
        StopReason::ToolUse => "toolUse",
        StopReason::Error => "error",
        StopReason::Aborted => "aborted",
        StopReason::Deferred => "deferred",
    }
}
