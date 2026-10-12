use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::ai::types::{
    AssistantMessage, DeferredHandle, Message, ModelThinkingLevel, SimpleStreamOptions, StopReason,
    ThinkingLevel,
};
use crate::cancel::CancelToken;
use crate::entries::assistant_entry;
use crate::errors::{Error, Result};
use crate::harness::compaction::{create_compaction, estimate_context, select_cut};
use crate::harness::context::ContextView;
use crate::harness::inbox::QueueModes;
use crate::harness::live::live_doc;
use crate::harness::live::{GenerationStatus, LiveState, RunState};
use crate::harness::prompt::{plan_system_entries, render_sections, replay_sections};
use crate::harness::provider::ensure_provider_session_id;
use crate::harness::runtime::{HarnessPhaseHandler, HarnessTaskDefinition, HarnessTaskRuntime};
use crate::harness::tool::{ToolTaskInput, erase, harness_error, tool_task};
use crate::harness::types::{
    CompactionPolicy, CompactionReason, ConversationStreamOptions, Deferred, HookApi, HookTask,
    ModelRef, PromptInput, Settings,
};
use crate::harness::usage::{UsageBucket, record_usage};
use crate::harness::util::{conversation_doc, edit_doc, read_draft, write_draft};
use crate::ids::{ConversationId, EntryId, SubmissionId, TaskId};
use crate::session::types::{TaskCreate, TaskOptions, Tx};
use crate::tasks::{NextTaskState, RunningTask, TaskOutcome, TaskOutcomeError, TaskOwnership};
use crate::types::{EntryDraft, EntryQuery, EntryRecord, ScanOrder, SubmissionSettlement};

pub mod classify;
pub mod rounds;

pub type GenerationInput = serde_json::Map<String, serde_json::Value>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "lowercase")]
pub enum GenerationCheckpoint {
    Prepare {
        attempt: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        compacted: Option<TaskId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        overflow: Option<String>,
    },
    Request {
        attempt: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        compacted: Option<TaskId>,
        model: ModelRef,
        thinking_level: ModelThinkingLevel,
        stream_options: ConversationStreamOptions,
        cutoff: EntryId,
    },
    Retry {
        attempt: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        compacted: Option<TaskId>,
        until: u64,
    },
    Poll {
        attempt: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        compacted: Option<TaskId>,
        model: ModelRef,
        cutoff: EntryId,
        handle: DeferredHandle,
        poll_at: u64,
    },
    Tools {
        assistant: EntryId,
        tools: Vec<TaskId>,
        pending: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerationResult {
    pub entry_id: EntryId,
}

pub type Generation =
    HarnessTaskDefinition<GenerationInput, GenerationCheckpoint, GenerationResult, GenerationHooks>;

pub type Runtime<'a> = &'a (
        dyn HarnessTaskRuntime<
    GenerationInput,
    GenerationCheckpoint,
    GenerationResult,
    GenerationHooks,
> + Sync
    );

pub type BeforeRequestHook =
    Box<dyn Fn(&HookApi, &[Message]) -> Result<Option<Vec<Message>>> + Send + Sync>;
pub type AfterResponseHook = Box<dyn Fn(&HookApi, &AssistantMessage) -> Result<()> + Send + Sync>;
pub type OnYieldHook = Box<
    dyn Fn(&HookApi, &AssistantMessage) -> Result<Option<crate::harness::types::UserInput>>
        + Send
        + Sync,
>;
pub type AfterToolsHook = Box<dyn Fn(&HookApi, EntryId, &[EntryId]) -> Result<()> + Send + Sync>;
pub type PartialHook = Arc<dyn Fn(&HookApi, Option<Arc<AssistantMessage>>) + Send + Sync>;

#[derive(Default)]
pub struct GenerationHooks {
    pub before_request: Option<BeforeRequestHook>,
    pub after_response: Option<AfterResponseHook>,
    pub on_yield: Option<OnYieldHook>,
    pub after_tools: Option<AfterToolsHook>,
    pub on_partial: Option<PartialHook>,
}

pub const GENERATION_KIND: &str = "pi.generation";
pub const GENERATION_HOOK: HookTask<GenerationHooks> = HookTask::new(GENERATION_KIND);
pub const GENERATION_VERSION: u64 = 1;

fn initial_checkpoint() -> GenerationCheckpoint {
    GenerationCheckpoint::Prepare {
        attempt: 1,
        compacted: None,
        overflow: None,
    }
}

pub fn create_generation(tx: &mut dyn Tx, conversation_id: ConversationId) -> Result<TaskId> {
    tx.create_task(
        TaskCreate {
            kind: GENERATION_KIND.to_owned(),
            version: GENERATION_VERSION,
            input: serde_json::json!({}),
            checkpoint: serde_json::to_value(initial_checkpoint())?,
        },
        TaskOptions {
            ownership: TaskOwnership::Conversation,
            conversation_id: Some(conversation_id),
            background: None,
        },
    )
}

pub fn create_tool_task(
    tx: &mut dyn Tx,
    owner: TaskId,
    assistant: EntryId,
    call_id: &str,
) -> Result<TaskId> {
    let definition = tool_task();
    let input = ToolTaskInput {
        assistant,
        call_id: call_id.to_owned(),
    };
    tx.create_task(
        TaskCreate {
            kind: definition.name.clone(),
            version: definition.version,
            input: serde_json::to_value(&input)?,
            checkpoint: serde_json::to_value((definition.initial)(&input))?,
        },
        TaskOptions {
            ownership: TaskOwnership::Task { task_id: owner },
            conversation_id: None,
            background: None,
        },
    )
}

pub fn definition() -> Generation {
    let mut phases: BTreeMap<
        String,
        HarnessPhaseHandler<
            GenerationInput,
            GenerationCheckpoint,
            GenerationResult,
            GenerationHooks,
        >,
    > = BTreeMap::new();
    phases.insert("prepare".to_owned(), Box::new(prepare));
    phases.insert("request".to_owned(), Box::new(request));
    phases.insert("retry".to_owned(), Box::new(retry));
    phases.insert("poll".to_owned(), Box::new(poll));
    phases.insert("tools".to_owned(), Box::new(rounds::tools_phase));
    HarnessTaskDefinition {
        name: GENERATION_KIND.to_owned(),
        version: GENERATION_VERSION,
        initial: Box::new(|_| initial_checkpoint()),
        phases,
        abort: Box::new(abort),
    }
}

pub fn read_live(tx: &mut dyn Tx, conversation_id: ConversationId) -> Result<LiveState> {
    read_draft(tx, &live_doc()?, conversation_id)
}

pub fn write_live(
    tx: &mut dyn Tx,
    conversation_id: ConversationId,
    live: &LiveState,
) -> Result<()> {
    write_draft(tx, &live_doc()?, conversation_id, live)
}

pub fn edit_live<R>(
    tx: &mut dyn Tx,
    conversation_id: ConversationId,
    edit: impl FnOnce(&mut LiveState) -> R,
) -> Result<R> {
    edit_doc(tx, &live_doc()?, conversation_id, edit)
}

pub fn snapshot_live(runtime: Runtime<'_>, cancel: &CancelToken) -> Result<LiveState> {
    let token = live_doc()?;
    let doc = conversation_doc(token.definition(), runtime.conversation_id());
    let value = runtime.snapshot(doc, cancel)?.unwrap_or_default();
    Ok(serde_json::from_value(serde_json::Value::Object(value))?)
}

pub fn start_run(
    tx: &mut dyn Tx,
    conversation_id: ConversationId,
    live: &mut LiveState,
    inputs: Vec<SubmissionId>,
) -> Result<()> {
    live.run = Some(RunState {
        task_id: create_generation(tx, conversation_id)?,
        inputs,
    });
    Ok(())
}

pub fn hand_over(live: &mut LiveState, from: TaskId, to: TaskId) {
    if let Some(run) = live.run.as_mut().filter(|run| run.task_id == from) {
        run.task_id = to;
    }
}

pub fn end_run(
    tx: &mut dyn Tx,
    live: &mut LiveState,
    task_id: TaskId,
    settlement: SubmissionSettlement,
) -> Result<()> {
    if let Some(inputs) = live.end_run(task_id) {
        for id in inputs {
            tx.settle_submission(id, settlement.clone())?;
        }
    }
    Ok(())
}

pub fn queue_modes(settings: &Settings) -> QueueModes {
    QueueModes {
        steering_mode: settings.steering_mode,
        follow_up_mode: settings.follow_up_mode,
    }
}

pub fn append_assistant(
    tx: &mut dyn Tx,
    conversation_id: ConversationId,
    message: &AssistantMessage,
) -> Result<EntryRecord> {
    let key = format!("{}/{}", message.provider, message.model);
    record_usage(
        tx,
        conversation_id,
        UsageBucket::Models,
        &key,
        &message.usage,
    )?;
    tx.append_entry(
        conversation_id,
        EntryDraft {
            kind: assistant_entry().kind().to_owned(),
            model: Some(vec![Message::Assistant(Arc::new(message.clone()))]),
            data: None,
            head: None,
            edits: None,
        },
    )
}

pub fn convert_partial(
    tx: &mut dyn Tx,
    live: &LiveState,
    conversation_id: ConversationId,
) -> Result<()> {
    let Some(partial) = live
        .generation
        .as_ref()
        .and_then(|status| status.message.as_ref())
    else {
        return Ok(());
    };
    let aborted = AssistantMessage {
        stop_reason: StopReason::Aborted,
        ..partial.clone()
    };
    append_assistant(tx, conversation_id, &aborted)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Threshold {
    Blocking,
    Background,
}

pub fn threshold_compaction(
    view: &ContextView,
    planned: &[EntryDraft],
    context_window: u64,
    policy: &CompactionPolicy,
) -> Option<Threshold> {
    if !policy.enabled || context_window == 0 {
        return None;
    }
    let planned: Vec<Message> = planned
        .iter()
        .flat_map(|entry| entry.model.clone().unwrap_or_default())
        .collect();
    let tokens = estimate_context(view, &planned) as i128;
    let blocking = context_window as i128 - policy.reserve_tokens as i128;
    let background = blocking - policy.background_tokens as i128;
    let over = if tokens > blocking {
        Some(Threshold::Blocking)
    } else if policy.background_tokens > 0 && tokens > background {
        Some(Threshold::Background)
    } else {
        None
    };
    over.filter(|_| select_cut(view, policy.keep_recent_tokens).is_some())
}

pub fn reasoning(level: ModelThinkingLevel) -> Option<ThinkingLevel> {
    Some(match level {
        ModelThinkingLevel::Off => return None,
        ModelThinkingLevel::Minimal => ThinkingLevel::Minimal,
        ModelThinkingLevel::Low => ThinkingLevel::Low,
        ModelThinkingLevel::Medium => ThinkingLevel::Medium,
        ModelThinkingLevel::High => ThinkingLevel::High,
        ModelThinkingLevel::Xhigh => ThinkingLevel::Xhigh,
        ModelThinkingLevel::Max => ThinkingLevel::Max,
    })
}

pub fn simple_options(
    stream: &ConversationStreamOptions,
    level: ModelThinkingLevel,
    signal: &CancelToken,
    session_id: String,
) -> SimpleStreamOptions {
    SimpleStreamOptions {
        signal: Some(signal.clone()),
        api_key: None,
        base_url: None,
        session_id: Some(session_id),
        reasoning: reasoning(level),
        cache_retention: stream.cache_retention,
        deferred: match stream.deferred {
            None => false,
            Some(Deferred::Enabled(enabled)) => enabled,
            Some(Deferred::Window { .. }) => true,
        },
        max_tokens: None,
        transport: stream.transport,
        timeout_ms: stream.timeout_ms,
        max_retries: stream.max_retries,
        max_retry_delay_ms: stream.max_retry_delay_ms,
        headers: stream.headers.clone(),
        cache_points: Vec::new(),
        metadata: stream.metadata.clone(),
    }
}

fn mismatch(phase: &str) -> Error {
    Error::Invalid(format!(
        "generation phase handler {phase} got another checkpoint"
    ))
}

fn settle(
    runtime: Runtime<'_>,
    cancel: &CancelToken,
    settlement: SubmissionSettlement,
    outcome: TaskOutcome<GenerationResult>,
) -> Result<()> {
    let conversation_id = runtime.conversation_id();
    let task_id = runtime.task_id();
    runtime.commit(
        Box::new(move |tx, _task| {
            let mut live = read_live(tx, conversation_id)?;
            end_run(tx, &mut live, erase(task_id), settlement)?;
            write_live(tx, conversation_id, &live)?;
            Ok(Some(NextTaskState::Terminal { outcome }))
        }),
        cancel,
    )
}

fn failed(message: String, reason: &str) -> TaskOutcome<GenerationResult> {
    TaskOutcome::Failed {
        error: TaskOutcomeError {
            message,
            detail: Some(serde_json::json!({ "reason": reason })),
        },
        result: None,
    }
}

fn fail_no_model(
    runtime: Runtime<'_>,
    model: Option<&ModelRef>,
    cancel: &CancelToken,
) -> Result<()> {
    let message = match model {
        None => "No model is configured".to_owned(),
        Some(model) => format!(
            "Model {}/{} is not available",
            model.provider, model.model_id
        ),
    };
    settle(
        runtime,
        cancel,
        SubmissionSettlement::Unanswered {
            reason: "no_model".to_owned(),
            detail: None,
        },
        failed(message, "no_model"),
    )
}

fn fail_model_error(runtime: Runtime<'_>, text: String, cancel: &CancelToken) -> Result<()> {
    settle(
        runtime,
        cancel,
        SubmissionSettlement::Unanswered {
            reason: "model_error".to_owned(),
            detail: Some(serde_json::Value::String(text.clone())),
        },
        failed(text, "model_error"),
    )
}

fn prepare(
    task: RunningTask<GenerationInput, GenerationCheckpoint, GenerationResult>,
    runtime: Runtime<'_>,
    cancel: &CancelToken,
) -> Result<()> {
    let GenerationCheckpoint::Prepare {
        attempt,
        compacted,
        overflow,
    } = task.checkpoint().clone()
    else {
        return Err(mismatch("prepare"));
    };
    let conversation_id = runtime.conversation_id();
    let agent = runtime.agent(cancel)?;
    let settings = runtime.settings().clone();
    let Some(model_ref) = agent.model.clone() else {
        return fail_no_model(runtime, None, cancel);
    };
    let Some(resolved) = runtime
        .models()
        .get_model(&model_ref.provider, &model_ref.model_id)
    else {
        return fail_no_model(runtime, Some(&model_ref), cancel);
    };
    if let (Some(compacted), Some(overflow)) = (compacted.as_ref(), overflow.as_ref()) {
        let outcomes = runtime.outcomes(std::slice::from_ref(compacted), cancel)?;
        if !matches!(outcomes.first(), Some(TaskOutcome::Completed { .. })) {
            return fail_model_error(runtime, overflow.clone(), cancel);
        }
    }
    let view = runtime.context(None, cancel)?;
    let shown = replay_sections(&view.messages);
    let env = match runtime.env(cancel) {
        Ok(env) => env,
        Err(error) => {
            if cancel.is_cancelled() {
                return Err(error);
            }
            runtime.report(error);
            None
        }
    };
    let tools: Vec<crate::ai::types::Tool> = agent
        .tools
        .iter()
        .map(|registration| registration.tool.clone())
        .collect();
    let shown_map: BTreeMap<String, String> = shown.iter().cloned().collect();
    let input = PromptInput {
        conversation_id,
        agent: &agent,
        env: env.as_deref(),
        shown: &shown_map,
        read: runtime,
    };
    let desired = render_sections(
        &agent.sections,
        &input,
        &shown,
        &mut |error| runtime.report(Error::Invalid(error.to_string())),
        cancel,
    )?;
    let entries = plan_system_entries(&view, &desired, &tools, runtime.now());
    let threshold = if compacted.is_none() {
        threshold_compaction(
            &view,
            &entries,
            resolved.context_window,
            &settings.compaction,
        )
    } else {
        None
    };
    let task_id = erase(runtime.task_id());
    if threshold == Some(Threshold::Blocking) {
        runtime.commit(
            Box::new(move |tx, _task| {
                let child = create_compaction(
                    tx,
                    conversation_id,
                    CompactionReason::Threshold,
                    Some(task_id),
                )?;
                Ok(Some(NextTaskState::Waiting {
                    checkpoint: GenerationCheckpoint::Prepare {
                        attempt,
                        compacted: Some(child),
                        overflow: None,
                    },
                    on: vec![child],
                    policy: crate::tasks::JoinPolicy::AllSettled,
                }))
            }),
            cancel,
        )?;
        return Ok(());
    }
    let thinking_level = agent.thinking_level;
    let stream_options = settings.stream.clone();
    runtime.commit(
        Box::new(move |tx, _task| {
            let mut cutoff = tx
                .scan_entries(
                    &EntryQuery {
                        conversation_id,
                        min_entry_id: None,
                        max_entry_id: None,
                        order: Some(ScanOrder::Descending),
                    },
                    1,
                    None,
                )?
                .items
                .first()
                .map(|entry| entry.id);
            for entry in entries {
                cutoff = Some(tx.append_entry(conversation_id, entry)?.id);
            }
            let Some(cutoff) = cutoff else {
                return Err(Error::Invalid(format!(
                    "Conversation {conversation_id} has no entries to send"
                )));
            };
            if threshold == Some(Threshold::Background) {
                let live = read_live(tx, conversation_id)?;
                if live.compactions.is_none() {
                    create_compaction(tx, conversation_id, CompactionReason::Threshold, None)?;
                }
            }
            Ok(Some(NextTaskState::Running {
                checkpoint: GenerationCheckpoint::Request {
                    attempt,
                    compacted,
                    model: model_ref,
                    thinking_level,
                    stream_options,
                    cutoff,
                },
            }))
        }),
        cancel,
    )
}

fn request(
    task: RunningTask<GenerationInput, GenerationCheckpoint, GenerationResult>,
    runtime: Runtime<'_>,
    cancel: &CancelToken,
) -> Result<()> {
    let GenerationCheckpoint::Request {
        attempt,
        compacted,
        model: model_ref,
        thinking_level,
        stream_options,
        cutoff,
    } = task.checkpoint().clone()
    else {
        return Err(mismatch("request"));
    };
    let conversation_id = runtime.conversation_id();
    runtime.commit(
        Box::new(move |tx, _task| {
            let live = read_live(tx, conversation_id)?;
            convert_partial(tx, &live, conversation_id)?;
            let mut live = live;
            live.generation = Some(GenerationStatus {
                attempt,
                message: None,
                retry: None,
                deferred: None,
            });
            write_live(tx, conversation_id, &live)?;
            Ok(None)
        }),
        cancel,
    )?;
    let Some(model) = runtime
        .models()
        .get_model(&model_ref.provider, &model_ref.model_id)
    else {
        return fail_no_model(runtime, Some(&model_ref), cancel);
    };
    let view = runtime.context(Some(cutoff), cancel)?;
    let mut replaced: Option<Vec<Message>> = None;
    let api = HookApi {
        conversation_id: runtime.conversation_id(),
    };
    runtime.hooks().each(&mut |hooks| {
        if let Some(hook) = hooks.before_request.as_ref()
            && let Some(next) = hook(&api, replaced.as_deref().unwrap_or(&view.messages))?
        {
            replaced = Some(next);
        }
        Ok(())
    })?;
    let messages = replaced.as_deref().unwrap_or(&view.messages);
    let session_id = ensure_provider_session_id(runtime, cancel)?;
    let options = simple_options(
        &stream_options,
        thinking_level,
        runtime.signal(),
        session_id,
    );
    let message = classify::stream_response(runtime, &model, messages, options, attempt, cancel)?;
    let request = classify::Request {
        attempt,
        compacted,
        model: model_ref,
        cutoff,
        view: Some(view),
        poll_at: None,
    };
    classify::classify(runtime, &request, message, cancel)
}

fn retry(
    task: RunningTask<GenerationInput, GenerationCheckpoint, GenerationResult>,
    runtime: Runtime<'_>,
    cancel: &CancelToken,
) -> Result<()> {
    let GenerationCheckpoint::Retry {
        attempt,
        compacted,
        until,
    } = task.checkpoint().clone()
    else {
        return Err(mismatch("retry"));
    };
    runtime.sleep(until, cancel)?;
    let conversation_id = runtime.conversation_id();
    runtime.commit(
        Box::new(move |tx, _task| {
            let mut live = read_live(tx, conversation_id)?;
            live.generation = Some(GenerationStatus {
                attempt: attempt + 1,
                message: None,
                retry: None,
                deferred: None,
            });
            write_live(tx, conversation_id, &live)?;
            Ok(Some(NextTaskState::Running {
                checkpoint: GenerationCheckpoint::Prepare {
                    attempt: attempt + 1,
                    compacted,
                    overflow: None,
                },
            }))
        }),
        cancel,
    )
}

fn poll(
    task: RunningTask<GenerationInput, GenerationCheckpoint, GenerationResult>,
    runtime: Runtime<'_>,
    cancel: &CancelToken,
) -> Result<()> {
    let GenerationCheckpoint::Poll {
        attempt,
        compacted,
        model: model_ref,
        cutoff,
        handle,
        poll_at,
    } = task.checkpoint().clone()
    else {
        return Err(mismatch("poll"));
    };
    let Some(model) = runtime
        .models()
        .get_model(&model_ref.provider, &model_ref.model_id)
    else {
        return fail_no_model(runtime, Some(&model_ref), cancel);
    };
    runtime.sleep(poll_at, cancel)?;
    let message = runtime
        .models()
        .fetch_deferred(&model, &handle, runtime.signal())
        .map_err(|error| Error::Invalid(error.0))?;
    let request = classify::Request {
        attempt,
        compacted,
        model: model_ref,
        cutoff,
        view: None,
        poll_at: Some(poll_at),
    };
    classify::classify(runtime, &request, message, cancel)
}

fn abort(
    task: RunningTask<GenerationInput, GenerationCheckpoint, GenerationResult>,
    runtime: Runtime<'_>,
    cancel: &CancelToken,
) -> Result<()> {
    let checkpoint = task.checkpoint().clone();
    if let GenerationCheckpoint::Poll { model, handle, .. } = &checkpoint
        && let Some(model) = runtime.models().get_model(&model.provider, &model.model_id)
        && let Err(error) = runtime
            .models()
            .cancel_deferred(&model, handle, runtime.signal())
    {
        runtime.report(Error::Invalid(error.0));
    }
    let conversation_id = runtime.conversation_id();
    let unstarted = match &checkpoint {
        GenerationCheckpoint::Tools {
            assistant, pending, ..
        } => rounds::read_calls(runtime, *assistant, pending, cancel)?,
        _ => Vec::new(),
    };
    let task_id = erase(runtime.task_id());
    let now = runtime.now();
    runtime.commit(
        Box::new(move |tx, _task| {
            let mut live = read_live(tx, conversation_id)?;
            convert_partial(tx, &live, conversation_id)?;
            for call in &unstarted {
                let result = harness_error("aborted", &format!("Tool {} was aborted", call.name));
                crate::harness::tool::append_tool_result(
                    tx,
                    conversation_id,
                    call,
                    &result,
                    now,
                    None,
                )?;
            }
            end_run(
                tx,
                &mut live,
                task_id,
                SubmissionSettlement::Unanswered {
                    reason: "aborted".to_owned(),
                    detail: None,
                },
            )?;
            write_live(tx, conversation_id, &live)?;
            Ok(Some(NextTaskState::Terminal {
                outcome: TaskOutcome::Aborted {
                    reason: None,
                    result: None,
                },
            }))
        }),
        cancel,
    )
}
