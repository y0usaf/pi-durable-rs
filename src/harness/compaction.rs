use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::ai::types::{
    AssistantBlock, AssistantMessage, CacheRetention, Message, StopReason, SystemContent,
    SystemMessage, SystemRole, TextContent, TextType, TranscriptContext, UserBlock, UserContent,
    UserMessage, UserRole,
};
use crate::ai::utils::estimate::{calculate_context_tokens, estimate_message_tokens};
use crate::ai::utils::js;
use crate::ai::utils::retry::{is_retryable_assistant_error, retry_delay_ms};
use crate::ai::utils::text::utf16_len;
use crate::cancel::CancelToken;
use crate::entries::{CompactionData, compaction_entry};
use crate::errors::{Error, Result};
use crate::harness::context::{ContextView, order_tool_results};
use crate::harness::generation::{edit_live, queue_modes, simple_options};
use crate::harness::inbox::{WriteDraft, WriteHead};
use crate::harness::live::{CompactionStatus, RetryStatus};
use crate::harness::provider::ensure_provider_session_id;
use crate::harness::runtime::{HarnessPhaseHandler, HarnessTaskDefinition, HarnessTaskRuntime};
use crate::harness::submissions::{SubmissionDraft, admit_submission};
use crate::harness::tool::erase;
use crate::harness::types::{ConversationStreamOptions, HookApi, HookTask, ModelRef};
use crate::harness::usage::{UsageBucket, record_usage};
use crate::ids::{ConversationId, EntryId, SubmissionId, TaskId};
use crate::session::types::{TaskCreate, TaskOptions, Tx};
use crate::tasks::{NextTaskState, RunningTask, TaskOutcome, TaskOutcomeError, TaskOwnership};
use crate::types::{CompactionReason, EntryDraft, EntryHead, EntryRecord, JsonObject};

pub const COMPACTION_KIND: &str = "pi.compaction";
pub const COMPACTION_HOOK: HookTask<CompactionHooks> = HookTask::new(COMPACTION_KIND);
pub const COMPACTION_VERSION: u64 = 1;

const TOOL_RESULT_MAX_CHARS: usize = 2000;

const SUMMARY_PREFIX: &str = "The conversation history before this point was compacted into the following summary:\n\n<summary>\n";
const SUMMARY_SUFFIX: &str = "\n</summary>";

const SUMMARIZATION_SYSTEM_PROMPT: &str = r#"You are a context summarization assistant. Your task is to read a conversation between a user and an AI assistant, then produce a structured summary following the exact format specified.

Do NOT continue the conversation. Do NOT respond to any questions in the conversation. ONLY output the structured summary."#;

const SUMMARIZATION_PROMPT: &str = r#"The messages above are a conversation to summarize. Create a structured context checkpoint summary that another LLM will use to continue the work. If the conversation starts with an earlier summary, preserve its information and fold the newer messages into it.

Use this EXACT format:

## Goal
[What is the user trying to accomplish? Can be multiple items if the session covers different tasks.]

## Constraints & Preferences
- [Any constraints, preferences, or requirements mentioned by user]
- [Or "(none)" if none were mentioned]

## Progress
### Done
- [x] [Completed tasks/changes]

### In Progress
- [ ] [Current work]

### Blocked
- [Issues preventing progress, if any]

## Key Decisions
- **[Decision]**: [Brief rationale]

## Next Steps
1. [Ordered list of what should happen next]

## Critical Context
- [Any data, examples, or references needed to continue]
- [Or "(none)" if not applicable]

Keep each section concise. Preserve exact file paths, function names, and error messages."#;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionInput {
    pub reason: CompactionReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SummaryRequest {
    pub attempt: u64,
    pub model: ModelRef,
    pub thinking_level: crate::ai::types::ModelThinkingLevel,
    pub stream_options: ConversationStreamOptions,
    pub max_tokens: u64,
    pub tail: EntryId,
    pub first_kept: EntryId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "lowercase")]
pub enum CompactionCheckpoint {
    Select,
    Summarize {
        #[serde(flatten)]
        request: SummaryRequest,
    },
    Retry {
        until: u64,
        #[serde(flatten)]
        request: SummaryRequest,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_id: Option<EntryId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<SubmissionId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionDecision {
    Decline,
    Summary(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompactionSource {
    pub reason: CompactionReason,
    pub entries: Vec<EntryRecord>,
    pub messages: Vec<Message>,
    pub first_kept: EntryId,
    pub instructions: Option<String>,
}

pub type BeforeCompact =
    Box<dyn Fn(&HookApi, &CompactionSource) -> Result<Option<CompactionDecision>> + Send + Sync>;

#[derive(Default)]
pub struct CompactionHooks {
    pub before_compact: Option<BeforeCompact>,
}

pub type Compaction =
    HarnessTaskDefinition<CompactionInput, CompactionCheckpoint, CompactionResult, CompactionHooks>;

type Runtime<'a> = &'a (
        dyn HarnessTaskRuntime<
    CompactionInput,
    CompactionCheckpoint,
    CompactionResult,
    CompactionHooks,
> + Sync
    );

type Task = RunningTask<CompactionInput, CompactionCheckpoint, CompactionResult>;

type Next = NextTaskState<CompactionCheckpoint, CompactionResult>;

pub fn definition() -> Compaction {
    let mut phases: BTreeMap<
        String,
        HarnessPhaseHandler<
            CompactionInput,
            CompactionCheckpoint,
            CompactionResult,
            CompactionHooks,
        >,
    > = BTreeMap::new();
    phases.insert("select".to_owned(), Box::new(select));
    phases.insert("summarize".to_owned(), Box::new(summarize));
    phases.insert("retry".to_owned(), Box::new(retry));
    HarnessTaskDefinition {
        name: COMPACTION_KIND.to_owned(),
        version: COMPACTION_VERSION,
        initial: Box::new(|_| CompactionCheckpoint::Select),
        phases,
        abort: Box::new(abort),
    }
}

pub fn create_compaction(
    tx: &mut dyn Tx,
    conversation_id: ConversationId,
    reason: CompactionReason,
    owner: Option<TaskId>,
) -> Result<TaskId> {
    create_compaction_with(
        tx,
        conversation_id,
        CompactionInput {
            reason,
            instructions: None,
        },
        owner,
    )
}

pub fn create_compaction_with(
    tx: &mut dyn Tx,
    conversation_id: ConversationId,
    input: CompactionInput,
    owner: Option<TaskId>,
) -> Result<TaskId> {
    let reason = input.reason;
    let background = owner.is_none() && reason != CompactionReason::Manual;
    let ownership = match owner {
        None => TaskOwnership::Conversation,
        Some(owner_id) => TaskOwnership::Task { task_id: owner_id },
    };
    let task_id = tx.create_task(
        TaskCreate {
            kind: COMPACTION_KIND.to_owned(),
            version: COMPACTION_VERSION,
            input: serde_json::to_value(&input)?,
            checkpoint: serde_json::to_value(CompactionCheckpoint::Select)?,
        },
        TaskOptions {
            ownership,
            conversation_id: Some(conversation_id),
            background: Some(background),
        },
    )?;
    let status = CompactionStatus {
        task_id,
        reason,
        blocking: owner.is_some(),
        attempt: 1,
        retry: None,
    };
    edit_live(tx, conversation_id, |live| {
        live.add_compaction_status(status)
    })?;
    Ok(task_id)
}

fn select(task: Task, runtime: Runtime<'_>, cancel: &CancelToken) -> Result<()> {
    let agent = runtime.agent(cancel)?;
    let settings = runtime.settings().clone();
    let policy = settings.compaction;
    let found = agent.model.as_ref().and_then(|reference| {
        runtime
            .models()
            .get_model(&reference.provider, &reference.model_id)
            .map(|model| (reference.clone(), model))
    });
    let Some((model_ref, model)) = found else {
        return fail_no_model(runtime, agent.model.as_ref(), cancel);
    };
    let view = runtime.context(None, cancel)?;
    let Some(cut) = select_cut(&view, policy.keep_recent_tokens) else {
        return complete(runtime, cancel);
    };
    let first_kept = view.entries[cut].id;
    let source = CompactionSource {
        reason: task.input.reason,
        entries: view.entries[..cut]
            .iter()
            .map(|entry| (**entry).clone())
            .collect(),
        messages: summarized_messages(&view, cut),
        first_kept,
        instructions: task.input.instructions.clone(),
    };
    let mut decision = None;
    let api = HookApi {
        conversation_id: runtime.conversation_id(),
    };
    runtime.hooks().each(&mut |hooks| {
        if decision.is_none()
            && let Some(hook) = hooks.before_compact.as_ref()
        {
            decision = hook(&api, &source)?;
        }
        Ok(())
    })?;
    match decision {
        Some(CompactionDecision::Decline) => return complete(runtime, cancel),
        Some(CompactionDecision::Summary(summary)) => {
            return place(runtime, first_kept, summary, cancel);
        }
        None => {}
    }
    let request = SummaryRequest {
        attempt: 1,
        model: model_ref,
        thinking_level: agent.thinking_level,
        stream_options: settings.stream,
        max_tokens: summary_max_tokens(policy.reserve_tokens, model.max_tokens),
        tail: view
            .entries
            .iter()
            .map(|entry| entry.id)
            .fold(first_kept, Ord::max),
        first_kept,
    };
    runtime.commit(
        Box::new(move |_tx, _task| {
            Ok(Some(NextTaskState::Running {
                checkpoint: CompactionCheckpoint::Summarize { request },
            }))
        }),
        cancel,
    )
}

fn summarize(task: Task, runtime: Runtime<'_>, cancel: &CancelToken) -> Result<()> {
    let CompactionCheckpoint::Summarize { request } = task.checkpoint().clone() else {
        return Err(mismatch("summarize"));
    };
    let Some(model) = runtime
        .models()
        .get_model(&request.model.provider, &request.model.model_id)
    else {
        return fail_no_model(runtime, Some(&request.model), cancel);
    };
    let view = runtime.context(Some(request.tail), cancel)?;
    let cut = view
        .entries
        .iter()
        .position(|entry| entry.id == request.first_kept)
        .ok_or_else(|| {
            Error::Invalid(format!(
                "Entry {} is not in the context at {}",
                request.first_kept, request.tail
            ))
        })?;
    let now = runtime.now();
    let messages = vec![
        Message::System(SystemMessage {
            role: SystemRole,
            content: SystemContent::Text(SUMMARIZATION_SYSTEM_PROMPT.to_owned()),
            sections: None,
            tools_added: None,
            tools_removed: None,
            timestamp: now,
        }),
        Message::User(user_text_message(
            summary_prompt(
                &summarized_messages(&view, cut),
                task.input.instructions.as_deref(),
            ),
            now,
        )),
    ];
    let mut options = simple_options(
        &request.stream_options,
        request.thinking_level,
        runtime.signal(),
        ensure_provider_session_id(runtime, cancel)?,
    );
    options.deferred = false;
    options.cache_retention = Some(CacheRetention::None);
    options.max_tokens = Some(request.max_tokens);
    let message = runtime
        .models()
        .complete_simple(&model, TranscriptContext { messages }, options)
        .map_err(|error| Error::Invalid(error.0))?;
    if runtime.signal().is_cancelled() {
        return Err(Error::Aborted);
    }
    let summary = summary_text(&message);
    let policy = runtime.settings().retry;
    let retry = message.stop_reason == StopReason::Error
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
    let conversation_id = runtime.conversation_id();
    let task_id = erase(runtime.task_id());
    let modes = queue_modes(runtime.settings());
    let first_kept = request.first_kept;
    let usage_key = format!("{}/{}", message.provider, message.model);
    runtime.commit(
        Box::new(move |tx, current| {
            record_usage(
                tx,
                conversation_id,
                UsageBucket::Models,
                &usage_key,
                &message.usage,
            )?;
            if let Some(summary) = summary {
                return place_summary(tx, &current, now, modes, first_kept, &summary).map(Some);
            }
            if retry {
                let error = message.error_message.clone().unwrap_or_default();
                edit_live(tx, conversation_id, |live| {
                    if let Some(status) = live.compaction_status(task_id) {
                        status.retry = Some(RetryStatus { at: until, error });
                    }
                })?;
                return Ok(Some(NextTaskState::Running {
                    checkpoint: CompactionCheckpoint::Retry { until, request },
                }));
            }
            remove_status(tx, conversation_id, task_id)?;
            Ok(Some(NextTaskState::Terminal {
                outcome: TaskOutcome::Failed {
                    error: TaskOutcomeError {
                        message: summary_failure(&message),
                        detail: Some(json!({ "reason": "model_error" })),
                    },
                    result: None,
                },
            }))
        }),
        cancel,
    )
}

fn retry(task: Task, runtime: Runtime<'_>, cancel: &CancelToken) -> Result<()> {
    let CompactionCheckpoint::Retry { until, request } = task.checkpoint().clone() else {
        return Err(mismatch("retry"));
    };
    runtime.sleep(until, cancel)?;
    let conversation_id = runtime.conversation_id();
    let task_id = erase(runtime.task_id());
    let attempt = request.attempt + 1;
    runtime.commit(
        Box::new(move |tx, _task| {
            edit_live(tx, conversation_id, |live| {
                if let Some(status) = live.compaction_status(task_id) {
                    status.attempt = attempt;
                    status.retry = None;
                }
            })?;
            Ok(Some(NextTaskState::Running {
                checkpoint: CompactionCheckpoint::Summarize {
                    request: SummaryRequest { attempt, ..request },
                },
            }))
        }),
        cancel,
    )
}

fn abort(_task: Task, runtime: Runtime<'_>, cancel: &CancelToken) -> Result<()> {
    let conversation_id = runtime.conversation_id();
    let task_id = erase(runtime.task_id());
    runtime.commit(
        Box::new(move |tx, _task| {
            remove_status(tx, conversation_id, task_id)?;
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

fn place(
    runtime: Runtime<'_>,
    first_kept: EntryId,
    summary: String,
    cancel: &CancelToken,
) -> Result<()> {
    let now = runtime.now();
    let modes = queue_modes(runtime.settings());
    runtime.commit(
        Box::new(move |tx, current| {
            place_summary(tx, &current, now, modes, first_kept, &summary).map(Some)
        }),
        cancel,
    )
}

fn place_summary(
    tx: &mut dyn Tx,
    current: &Task,
    now: u64,
    modes: crate::harness::inbox::QueueModes,
    first_kept: EntryId,
    summary: &str,
) -> Result<Next> {
    let task_id = erase(current.id);
    let conversation_id = current.conversation_id;
    remove_status(tx, conversation_id, task_id)?;
    let model = vec![Message::User(user_text_message(
        format!("{SUMMARY_PREFIX}{summary}{SUMMARY_SUFFIX}"),
        now,
    ))];
    let data = serde_json::to_value(CompactionData {
        reason: current.input.reason,
    })?;
    let kind = compaction_entry().kind().to_owned();
    let result = match current.owner {
        None => CompactionResult {
            entry_id: None,
            submission_id: Some(admit_submission(
                tx,
                conversation_id,
                SubmissionDraft::Write {
                    request_id: Some(format!("compaction:{task_id}")),
                    entry: WriteDraft {
                        kind,
                        model: Some(model),
                        data: Some(data),
                        head: Some(WriteHead::Entry(first_kept)),
                        edits: None,
                    },
                },
                now,
                modes,
            )?),
        },
        Some(_) => {
            let entry = tx.append_entry(
                conversation_id,
                EntryDraft {
                    kind,
                    model: Some(model),
                    data: Some(data),
                    head: Some(EntryHead::Entry(first_kept)),
                    edits: None,
                },
            )?;
            CompactionResult {
                entry_id: Some(entry.id),
                submission_id: None,
            }
        }
    };
    Ok(NextTaskState::Terminal {
        outcome: TaskOutcome::Completed { result },
    })
}

fn complete(runtime: Runtime<'_>, cancel: &CancelToken) -> Result<()> {
    let conversation_id = runtime.conversation_id();
    let task_id = erase(runtime.task_id());
    runtime.commit(
        Box::new(move |tx, _task| {
            remove_status(tx, conversation_id, task_id)?;
            Ok(Some(NextTaskState::Terminal {
                outcome: TaskOutcome::Completed {
                    result: CompactionResult::default(),
                },
            }))
        }),
        cancel,
    )
}

fn fail_no_model(
    runtime: Runtime<'_>,
    reference: Option<&ModelRef>,
    cancel: &CancelToken,
) -> Result<()> {
    let message = match reference {
        None => "No model is configured".to_owned(),
        Some(reference) => format!(
            "Model {}/{} is not available",
            reference.provider, reference.model_id
        ),
    };
    let conversation_id = runtime.conversation_id();
    let task_id = erase(runtime.task_id());
    runtime.commit(
        Box::new(move |tx, _task| {
            remove_status(tx, conversation_id, task_id)?;
            Ok(Some(NextTaskState::Terminal {
                outcome: TaskOutcome::Failed {
                    error: TaskOutcomeError {
                        message,
                        detail: Some(json!({ "reason": "no_model" })),
                    },
                    result: None,
                },
            }))
        }),
        cancel,
    )
}

fn remove_status(tx: &mut dyn Tx, conversation_id: ConversationId, task_id: TaskId) -> Result<()> {
    edit_live(tx, conversation_id, |live| {
        live.remove_compaction_status(task_id)
    })
}

fn mismatch(phase: &str) -> Error {
    Error::Invalid(format!(
        "Compaction checkpoint does not match phase {phase}"
    ))
}

fn user_text_message(text: String, timestamp: u64) -> Arc<UserMessage> {
    Arc::new(UserMessage {
        role: UserRole,
        content: UserContent::Blocks(vec![UserBlock::Text(TextContent {
            kind: TextType,
            text,
            text_signature: None,
        })]),
        timestamp,
    })
}

fn summary_max_tokens(reserve_tokens: u64, model_max_tokens: u64) -> u64 {
    let budget = (0.8 * reserve_tokens as f64).floor() as u64;
    if model_max_tokens > 0 {
        budget.min(model_max_tokens)
    } else {
        budget
    }
}

pub fn estimate_context(view: &ContextView, extra: &[Message]) -> u64 {
    let after = view.head.as_ref().map(|head| head.id);
    let measured: Option<&AssistantMessage> = (0..view.entries.len())
        .rev()
        .filter(|&index| after.is_none_or(|after| view.entries[index].id > after))
        .find_map(|index| {
            view.contributions[index]
                .iter()
                .rev()
                .find_map(|message| match message {
                    Message::Assistant(assistant)
                        if calculate_context_tokens(&assistant.usage) > 0 =>
                    {
                        Some(assistant.as_ref())
                    }
                    _ => None,
                })
        });
    let (from, mut tokens) = match measured {
        None => (0, 0),
        Some(measured) => {
            let position = view
                .messages
                .iter()
                .rposition(
                    |message| matches!(message, Message::Assistant(a) if a.as_ref() == measured),
                )
                .map_or(0, |index| index + 1);
            (position, calculate_context_tokens(&measured.usage))
        }
    };
    for message in view.messages.iter().skip(from).chain(extra) {
        tokens += estimate_message_tokens(message);
    }
    tokens
}

pub fn select_cut(view: &ContextView, keep_recent_tokens: u64) -> Option<usize> {
    let contributions = &view.contributions;
    let start = usize::from(view.head.is_some());
    let candidates: Vec<usize> = (start..contributions.len())
        .filter(|&index| is_candidate(contributions, index))
        .collect();
    let mut kept = 0;
    let mut cut = None;
    for index in (start..contributions.len()).rev() {
        kept += contributions[index]
            .iter()
            .map(estimate_message_tokens)
            .sum::<u64>();
        if kept < keep_recent_tokens {
            continue;
        }
        cut = candidates
            .iter()
            .copied()
            .find(|&candidate| candidate >= index)
            .or(candidates.last().copied());
        break;
    }
    let cut = cut?;
    (start..cut)
        .any(|index| !contributions[index].is_empty())
        .then_some(cut)
}

fn is_candidate(contributions: &[Arc<[Message]>], index: usize) -> bool {
    match contributions[index].first() {
        Some(Message::Assistant(_)) => return true,
        Some(Message::User(_)) => {}
        _ => return false,
    }
    let calls: Vec<String> = contributions[..index]
        .iter()
        .rev()
        .find_map(|messages| {
            messages.iter().rev().find_map(|message| match message {
                Message::Assistant(assistant) => Some(
                    assistant
                        .content
                        .iter()
                        .filter_map(|content| match content {
                            AssistantBlock::ToolCall(call) => Some(call.id.clone()),
                            _ => None,
                        })
                        .collect(),
                ),
                _ => None,
            })
        })
        .unwrap_or_default();
    if calls.is_empty() {
        return true;
    }
    let calls: HashSet<String> = calls.into_iter().collect();
    for (after, messages) in contributions.iter().enumerate().skip(index) {
        for (position, message) in messages.iter().enumerate() {
            match message {
                Message::Assistant(_) if after > index || position > 0 => return true,
                Message::ToolResult(result) if calls.contains(&result.tool_call_id) => {
                    return false;
                }
                _ => {}
            }
        }
    }
    true
}

pub fn summarized_messages(view: &ContextView, cut: usize) -> Vec<Message> {
    let flat: Vec<Message> = view.contributions[..cut]
        .iter()
        .flat_map(|c| c.iter())
        .cloned()
        .collect();
    order_tool_results(&flat)
}

fn summary_text(message: &AssistantMessage) -> Option<String> {
    if message.stop_reason != StopReason::Stop || has_tool_call(message) {
        return None;
    }
    let text = message
        .content
        .iter()
        .filter_map(|block| match block {
            AssistantBlock::Text(text) => Some(text.text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    let text = js::trim(&text);
    (!text.is_empty()).then(|| text.to_owned())
}

fn has_tool_call(message: &AssistantMessage) -> bool {
    message
        .content
        .iter()
        .any(|block| matches!(block, AssistantBlock::ToolCall(_)))
}

fn summary_failure(message: &AssistantMessage) -> String {
    match message.stop_reason {
        StopReason::Error | StopReason::Aborted => {
            let fallback = if message.stop_reason == StopReason::Error {
                "error"
            } else {
                "aborted"
            };
            format!(
                "Summarization failed: {}",
                message
                    .error_message
                    .clone()
                    .unwrap_or_else(|| fallback.to_owned())
            )
        }
        StopReason::Length => {
            "Summarization hit the token limit; the summary is incomplete".to_owned()
        }
        _ if has_tool_call(message) => "Summarization attempted to call a tool".to_owned(),
        _ => "Summarization produced no text".to_owned(),
    }
}

fn summary_prompt(messages: &[Message], instructions: Option<&str>) -> String {
    let focus = instructions
        .map(|text| format!("\n\nAdditional focus: {text}"))
        .unwrap_or_default();
    format!(
        "<conversation>\n{}\n</conversation>\n\n{SUMMARIZATION_PROMPT}{focus}",
        serialize_conversation(messages)
    )
}

pub fn serialize_conversation(messages: &[Message]) -> String {
    let mut parts: Vec<String> = Vec::new();
    for message in messages {
        match message {
            Message::User(user) => {
                let text = user_content_text(&user.content);
                if !text.is_empty() {
                    parts.push(format!("[User]: {text}"));
                }
            }
            Message::Assistant(assistant) => {
                let thinking: Vec<&str> = assistant
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        AssistantBlock::Thinking(thinking) => Some(thinking.thinking.as_str()),
                        _ => None,
                    })
                    .collect();
                let text: Vec<&str> = assistant
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        AssistantBlock::Text(text) => Some(text.text.as_str()),
                        _ => None,
                    })
                    .collect();
                let calls: Vec<String> = assistant
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        AssistantBlock::ToolCall(call) => Some(format!(
                            "{}({})",
                            call.name,
                            arguments_text(&call.arguments)
                        )),
                        _ => None,
                    })
                    .collect();
                if !thinking.is_empty() {
                    parts.push(format!("[Assistant thinking]: {}", thinking.join("\n")));
                }
                if !text.is_empty() {
                    parts.push(format!("[Assistant]: {}", text.join("\n")));
                }
                if !calls.is_empty() {
                    parts.push(format!("[Assistant tool calls]: {}", calls.join("; ")));
                }
            }
            Message::ToolResult(result) => {
                let text = blocks_text(&result.content);
                if !text.is_empty() {
                    parts.push(format!(
                        "[Tool result]: {}",
                        truncate(&text, TOOL_RESULT_MAX_CHARS)
                    ));
                }
            }
            Message::System(_) => {}
        }
    }
    parts.join("\n\n")
}

fn arguments_text(arguments: &JsonObject) -> String {
    js::keys(arguments)
        .into_iter()
        .map(|key| {
            format!(
                "{key}={}",
                js::stringify(arguments.get(key).unwrap_or(&serde_json::Value::Null))
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn user_content_text(content: &UserContent) -> String {
    match content {
        UserContent::Text(text) => text.clone(),
        UserContent::Blocks(blocks) => blocks_text(blocks),
    }
}

fn blocks_text(blocks: &[UserBlock]) -> String {
    blocks
        .iter()
        .filter_map(|block| match block {
            UserBlock::Text(text) => Some(text.text.as_str()),
            UserBlock::Image(_) => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn truncate(text: &str, max_chars: usize) -> String {
    let length = utf16_len(text);
    if length <= max_chars {
        return text.to_owned();
    }
    let units: Vec<u16> = text.encode_utf16().collect();
    format!(
        "{}\n\n[... {} more characters truncated]",
        String::from_utf16_lossy(&units[..max_chars]),
        length - max_chars
    )
}
