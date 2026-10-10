use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ai::models::Models;
use crate::ai::types::{
    AssistantBlock, Message, TextContent, TextType, ToolCall, ToolResultMessage, ToolResultRole,
    UserBlock,
};
use crate::ai::utils::text::utf16_len;
use crate::ai::utils::validation::validate_tool_arguments;
use crate::cancel::CancelToken;
use crate::chord::delta::overlap;
use crate::entries::{assistant_entry, tool_result_entry};
use crate::env::ExecutionEnv;
use crate::errors::{Error, Result};
use crate::harness::agent::Agent;
use crate::harness::live::{LiveState, SlotStatus, ToolSlot, live_doc};
use crate::harness::output::{
    OutputBuffer, OutputChunk, OutputLimits, Retain, ShellOutputSkip, bound_output,
};
use crate::harness::progress::{Progress, Waiter};
use crate::harness::registry::RegistrySnapshot;
use crate::harness::runtime::{HarnessPhaseHandler, HarnessTaskDefinition, HarnessTaskRuntime};
use crate::harness::types::{
    HookTask, Replay, ToolControl, ToolExecutionApi, ToolExecutionResult, ToolRegistration,
};
use crate::harness::usage::{UsageBucket, record_usage};
use crate::harness::util::{edit_doc, read_draft, write_draft};
use crate::ids::{ConversationId, EntryId, TaskId};
use crate::session::types::{DocRef, DocumentObserver, DocumentReader, Tx};
use crate::tasks::{NextTaskState, RunningTask, TaskOutcome, TaskOutcomeError};
use crate::truncate::{DEFAULT_MAX_BYTES, DEFAULT_MAX_LINES, utf8_byte_length};
use crate::turn::pause;
use crate::types::{
    AnyTaskRecord, EntryDraft, EntryRecord, JsonObject, JsonValue, ToolDiagnostic,
    ToolDiagnosticSeverity,
};

pub type ToolRuntime<'a> = dyn HarnessTaskRuntime<ToolTaskInput, ToolTaskCheckpoint, ToolTaskResult, ToolHooks>
    + Sync
    + 'a;

const OVERLAP_SCAN: usize = 65_536;
const OVERLAP_PROBE: usize = 64;
const OVERLAP_CANDIDATES: usize = 8;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolTaskInput {
    pub assistant: EntryId,
    pub call_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "lowercase")]
pub enum ToolTaskCheckpoint {
    Call,
    Execute {
        arguments: JsonObject,
        replay: Replay,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolTaskResult {
    pub entry_id: EntryId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control: Option<ToolControl>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BeforeToolDecision {
    pub arguments: Option<JsonObject>,
    pub block: Option<String>,
}

pub type BeforeTool = Arc<
    dyn Fn(&ToolCall, &ToolRuntime<'_>, &CancelToken) -> Result<Option<BeforeToolDecision>>
        + Send
        + Sync,
>;

pub type AfterTool = Arc<
    dyn Fn(
            &ToolCall,
            ToolExecutionResult,
            &ToolRuntime<'_>,
            &CancelToken,
        ) -> Result<Option<ToolExecutionResult>>
        + Send
        + Sync,
>;

#[derive(Clone, Default)]
pub struct ToolHooks {
    pub before_tool: Option<BeforeTool>,
    pub after_tool: Option<AfterTool>,
}

pub const TOOL_KIND: &str = "pi.tool";
pub const TOOL_HOOK: HookTask<ToolHooks> = HookTask::new(TOOL_KIND);

pub fn tool_task()
-> HarnessTaskDefinition<ToolTaskInput, ToolTaskCheckpoint, ToolTaskResult, ToolHooks> {
    let call: HarnessPhaseHandler<ToolTaskInput, ToolTaskCheckpoint, ToolTaskResult, ToolHooks> =
        Box::new(call_phase);
    let execute: HarnessPhaseHandler<ToolTaskInput, ToolTaskCheckpoint, ToolTaskResult, ToolHooks> =
        Box::new(execute_phase);
    HarnessTaskDefinition {
        name: TOOL_KIND.to_owned(),
        version: 1,
        initial: Box::new(|_| ToolTaskCheckpoint::Call),
        phases: BTreeMap::from([("call".to_owned(), call), ("execute".to_owned(), execute)]),
        abort: Box::new(abort_phase),
    }
}

enum Ending {
    Completed,
    Aborted,
    Failed { message: String },
}

fn call_phase(
    task: RunningTask<ToolTaskInput, ToolTaskCheckpoint, ToolTaskResult>,
    rt: &ToolRuntime<'_>,
    cancel: &CancelToken,
) -> Result<()> {
    let call = read_call(rt, &task.input, cancel)?;
    let agent = rt.agent(cancel)?;
    let Some(tool) = find_tool(&agent, &call) else {
        let error = harness_error(
            "tool_unavailable",
            &format!("Tool {} is not available", call.name),
        );
        return settle(rt, &call, Ending::Completed, move |_| error, cancel, None);
    };
    let checked = match prepare(tool, call.arguments.clone()) {
        Ok(args) => validate(tool, &call, args),
        Err(message) => Err(message),
    };
    let mut args = match checked {
        Ok(args) => args,
        Err(message) => {
            return settle(
                rt,
                &call,
                Ending::Completed,
                move |_| invalid(&message),
                cancel,
                None,
            );
        }
    };
    let mut block: Option<String> = None;
    rt.hooks().each(&mut |hook: &ToolHooks| {
        if block.is_some() {
            return Ok(());
        }
        let Some(before) = &hook.before_tool else {
            return Ok(());
        };
        let current = ToolCall {
            arguments: args.clone(),
            ..call.clone()
        };
        match before(&current, rt, cancel) {
            Ok(Some(BeforeToolDecision {
                block: Some(reason),
                ..
            })) => block = Some(reason),
            Ok(Some(BeforeToolDecision {
                arguments: Some(replaced),
                ..
            })) => args = replaced,
            Ok(_) => {}
            Err(error) => {
                if rt.signal().is_cancelled() {
                    return Err(error);
                }
                block = Some(error.to_string());
            }
        }
        Ok(())
    })?;
    if let Some(reason) = block {
        let blocked = harness_error("blocked", &format!("Tool call blocked: {reason}"));
        return settle(rt, &call, Ending::Completed, move |_| blocked, cancel, None);
    }
    let final_args = match validate(tool, &call, args) {
        Ok(args) => args,
        Err(message) => {
            return settle(
                rt,
                &call,
                Ending::Completed,
                move |_| invalid(&message),
                cancel,
                None,
            );
        }
    };
    let replay = tool.replay.unwrap_or(Replay::Unsafe);
    let intent = final_args.clone();
    let task_id = rt.task_id();
    let conversation = rt.conversation_id();
    rt.commit(
        Box::new(move |tx: &mut dyn Tx, _task| {
            set_slot_status(tx, conversation, task_id, SlotStatus::Running)?;
            Ok(Some(NextTaskState::Running {
                checkpoint: ToolTaskCheckpoint::Execute {
                    arguments: intent,
                    replay,
                },
            }))
        }),
        cancel,
    )?;
    run(rt, &call, tool, final_args, cancel)
}

fn execute_phase(
    task: RunningTask<ToolTaskInput, ToolTaskCheckpoint, ToolTaskResult>,
    rt: &ToolRuntime<'_>,
    cancel: &CancelToken,
) -> Result<()> {
    let ToolTaskCheckpoint::Execute { arguments, replay } = task.checkpoint().clone() else {
        return Err(Error::Invalid(format!(
            "Task {} has no execute intent",
            task.id
        )));
    };
    let call = read_call(rt, &task.input, cancel)?;
    let agent = rt.agent(cancel)?;
    match find_tool(&agent, &call) {
        Some(tool) if replay == Replay::Safe && tool.replay == Some(Replay::Safe) => {
            let task_id = rt.task_id();
            let conversation = rt.conversation_id();
            rt.commit(
                Box::new(move |tx: &mut dyn Tx, _task| {
                    edit_doc(
                        tx,
                        &live_doc()?,
                        conversation,
                        move |live: &mut LiveState| {
                            if let Some(slot) = live.tool_slot(erase(task_id)) {
                                slot.clear_progress();
                            }
                        },
                    )?;
                    Ok(None)
                }),
                cancel,
            )?;
            run(rt, &call, tool, arguments, cancel)
        }
        _ => {
            let message = format!(
                "Tool {} was interrupted and may have partially run",
                call.name
            );
            let text = message.clone();
            settle(
                rt,
                &call,
                Ending::Failed { message },
                move |slot| from_slot(slot, "interrupted", &text),
                cancel,
                None,
            )
        }
    }
}

fn abort_phase(
    task: RunningTask<ToolTaskInput, ToolTaskCheckpoint, ToolTaskResult>,
    rt: &ToolRuntime<'_>,
    cancel: &CancelToken,
) -> Result<()> {
    let call = read_call(rt, &task.input, cancel)?;
    let message = format!("Tool {} was aborted", call.name);
    settle(
        rt,
        &call,
        Ending::Aborted,
        move |slot| from_slot(slot, "aborted", &message),
        cancel,
        None,
    )
}

fn find_tool<'g>(agent: &'g Agent, call: &ToolCall) -> Option<&'g ToolRegistration> {
    agent.tools.iter().find(|each| each.tool.name == call.name)
}

fn read_call(
    rt: &ToolRuntime<'_>,
    input: &ToolTaskInput,
    cancel: &CancelToken,
) -> Result<ToolCall> {
    let entry = rt
        .entry(input.assistant, cancel)?
        .filter(|entry| assistant_entry().is(Some(entry)));
    let call = entry
        .and_then(|entry| entry.model)
        .and_then(|model| model.into_iter().next())
        .and_then(|message| match message {
            Message::Assistant(message) => message.content.iter().find_map(|block| match block {
                AssistantBlock::ToolCall(call) if call.id == input.call_id => Some(call.clone()),
                _ => None,
            }),
            _ => None,
        });
    call.ok_or_else(|| {
        Error::Invalid(format!(
            "Entry {} has no tool call {}",
            input.assistant, input.call_id
        ))
    })
}

fn prepare(tool: &ToolRegistration, args: JsonObject) -> std::result::Result<JsonObject, String> {
    let Some(prepare) = &tool.prepare_arguments else {
        return Ok(args);
    };
    match prepare(JsonValue::Object(args)) {
        Err(error) => Err(error.to_string()),
        Ok(Value::Object(args)) => Ok(args),
        Ok(repaired) => Err(format!(
            "Validation failed for tool \"{}\":\n  - root: must be object\n\nReceived arguments:\n{}",
            tool.tool.name,
            crate::ai::utils::js::stringify_pretty(&repaired)
        )),
    }
}

fn validate(
    tool: &ToolRegistration,
    call: &ToolCall,
    args: JsonObject,
) -> std::result::Result<JsonObject, String> {
    let candidate = ToolCall {
        arguments: args,
        ..call.clone()
    };
    match validate_tool_arguments(&tool.tool, &candidate) {
        Ok(Value::Object(args)) => Ok(args),
        Ok(_) => Err("Tool arguments must be an object".to_owned()),
        Err(error) => Err(error.to_string()),
    }
}

struct Reported {
    output: OutputBuffer,
    diagnostics: Vec<ToolDiagnostic>,
    details: Option<JsonValue>,
}

#[derive(Default)]
struct Written {
    text: String,
    details: Option<JsonValue>,
    diagnostics: usize,
}

struct Shared {
    limits: OutputLimits,
    reported: Mutex<Reported>,
    written: Mutex<Written>,
    settled: AtomicBool,
}

struct Api<'s> {
    rt: &'s ToolRuntime<'s>,
    shared: &'s Shared,
    progress: &'s Progress<'s>,
    call_id: String,
    env: Option<Arc<dyn ExecutionEnv>>,
}

fn run(
    rt: &ToolRuntime<'_>,
    call: &ToolCall,
    tool: &ToolRegistration,
    args: JsonObject,
    cancel: &CancelToken,
) -> Result<()> {
    let spec = tool.output_limits.unwrap_or_default();
    let limits = OutputLimits {
        max_bytes: spec.max_bytes.unwrap_or(DEFAULT_MAX_BYTES),
        max_lines: spec.max_lines.unwrap_or(DEFAULT_MAX_LINES),
        retain: spec.retain.unwrap_or(Retain::Head),
    };
    let shared = Shared {
        limits,
        reported: Mutex::new(Reported {
            output: OutputBuffer::new(limits),
            diagnostics: Vec::new(),
            details: None,
        }),
        written: Mutex::new(Written::default()),
        settled: AtomicBool::new(false),
    };
    let interval = Duration::from_millis(rt.settings().progress.output_interval_ms);
    let write = || publish(rt, &shared, cancel);
    let report = |error: Error| {
        if !rt.signal().is_cancelled() {
            rt.report(error);
        }
    };
    let progress = Progress::new(&write, &report, interval);
    let ((executed, duration_ms), waiters) = progress.run(|| {
        let env = match rt.env(cancel) {
            Ok(env) => env,
            Err(error) => return (Err(error), None),
        };
        let api = Api {
            rt,
            shared: &shared,
            progress: &progress,
            call_id: call.id.clone(),
            env,
        };
        let started = Instant::now();
        let outcome = (tool.execute)(JsonValue::Object(args), &api, cancel);
        (outcome, Some(elapsed_ms(started)))
    });
    let (result, ending) = match executed {
        Ok(result) => (result, Ending::Completed),
        Err(error) if rt.signal().is_cancelled() => {
            notify(waiters, Some(error.to_string()));
            return Err(error);
        }
        Err(error) => (
            ToolExecutionResult {
                is_error: Some(true),
                diagnostics: Some(vec![tool_diagnostic("tool_error", &error.to_string())]),
                ..ToolExecutionResult::default()
            },
            Ending::Failed {
                message: format!("Tool {} threw", call.name),
            },
        ),
    };
    shared.settled.store(true, Ordering::SeqCst);
    lock(&shared.reported).output.end();
    let outcome = finalize(rt, call, &shared, result, ending, cancel, duration_ms);
    notify(waiters, outcome.as_ref().err().map(ToString::to_string));
    outcome
}

fn finalize(
    rt: &ToolRuntime<'_>,
    call: &ToolCall,
    shared: &Shared,
    result: ToolExecutionResult,
    ending: Ending,
    cancel: &CancelToken,
    duration_ms: Option<u64>,
) -> Result<()> {
    let settled = final_result(rt, call, shared, result, cancel)?;
    settle(rt, call, ending, move |_| settled, cancel, duration_ms)
}

fn notify(waiters: Vec<Waiter>, failure: Option<String>) {
    for waiter in waiters {
        let _ = waiter.send(match &failure {
            None => Ok(()),
            Some(text) => Err(text.clone()),
        });
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    (started.elapsed().as_secs_f64() * 1000.0).round() as u64
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn utf16_tail(text: &str, from: usize) -> String {
    let units: Vec<u16> = text.encode_utf16().collect();
    String::from_utf16_lossy(units.get(from..).unwrap_or(&[]))
}

fn publish(rt: &ToolRuntime<'_>, shared: &Shared, cancel: &CancelToken) -> Result<usize> {
    let mut written = lock(&shared.written);
    let (snapshot, details, added, count) = {
        let mut reported = lock(&shared.reported);
        let snapshot = reported.output.snapshot();
        let count = reported.diagnostics.len();
        let added = reported
            .diagnostics
            .get(written.diagnostics..)
            .unwrap_or(&[])
            .to_vec();
        (snapshot, reported.details.clone(), added, count)
    };
    let details_changed = details != written.details;
    let mut bytes = 0;
    if snapshot.text != written.text {
        let shared_units = if snapshot.text.starts_with(&written.text) {
            utf16_len(&written.text)
        } else {
            overlap(
                &written.text,
                &snapshot.text,
                OVERLAP_SCAN,
                OVERLAP_PROBE,
                OVERLAP_CANDIDATES,
            )
        };
        bytes += utf8_byte_length(&utf16_tail(&snapshot.text, shared_units));
    }
    if details_changed {
        bytes += serde_json::to_string(details.as_ref().unwrap_or(&Value::Null))
            .map_or(0, |text| text.len());
    }
    if !added.is_empty() {
        bytes += serde_json::to_string(&added).map_or(0, |text| text.len());
    }
    let task_id = rt.task_id();
    let conversation = rt.conversation_id();
    let text = snapshot.text.clone();
    let slot_details = if details_changed {
        details.clone()
    } else {
        None
    };
    let (dropped_bytes, dropped_lines) = (snapshot.dropped_bytes, snapshot.dropped_lines);
    let to_write = added.clone();
    rt.commit(
        Box::new(move |tx: &mut dyn Tx, _task| {
            edit_doc(
                tx,
                &live_doc()?,
                conversation,
                move |live: &mut LiveState| {
                    let Some(slot) = live.tool_slot(erase(task_id)) else {
                        return;
                    };
                    if slot.output.as_deref().unwrap_or("") != text {
                        slot.output = Some(text);
                    }
                    if dropped_bytes > 0 {
                        slot.dropped_bytes = Some(dropped_bytes as u64);
                    }
                    if dropped_lines > 0 {
                        slot.dropped_lines = Some(dropped_lines as u64);
                    }
                    if let Some(details) = slot_details {
                        slot.details = Some(details);
                    }
                    if !to_write.is_empty() {
                        slot.diagnostics
                            .get_or_insert_with(Vec::new)
                            .extend(to_write);
                    }
                },
            )?;
            Ok(None)
        }),
        cancel,
    )?;
    written.text = snapshot.text;
    written.details = details;
    written.diagnostics = count;
    Ok(bytes)
}

fn final_result(
    rt: &ToolRuntime<'_>,
    call: &ToolCall,
    shared: &Shared,
    result: ToolExecutionResult,
    cancel: &CancelToken,
) -> Result<ToolExecutionResult> {
    let (retained, reported_details, reported_diagnostics) = {
        let mut reported = lock(&shared.reported);
        let retained = match result.content {
            None => Some(reported.output.snapshot()),
            Some(_) => None,
        };
        (
            retained,
            reported.details.clone(),
            reported.diagnostics.clone(),
        )
    };
    let content: Vec<UserBlock> = match &retained {
        None => result.content.clone().unwrap_or_default(),
        Some(retained) if retained.text.is_empty() => Vec::new(),
        Some(retained) => vec![text_block(retained.text.clone())],
    };
    let mut fin = ToolExecutionResult {
        content: Some(content.clone()),
        details: result.details.clone().or(reported_details),
        diagnostics: Some(
            reported_diagnostics
                .into_iter()
                .chain(result.diagnostics.clone().unwrap_or_default())
                .collect(),
        ),
        ..result
    };
    rt.hooks().each(&mut |hook: &ToolHooks| {
        let Some(after) = &hook.after_tool else {
            return Ok(());
        };
        if let Some(next) = after(call, fin.clone(), rt, cancel)? {
            fin = next;
        }
        Ok(())
    })?;
    let limits = shared.limits;
    let mut harness = Vec::new();
    if let Some(retained) = &retained
        && retained.dropped_bytes > 0
        && fin.content.as_ref() == Some(&content)
    {
        harness.push(truncated(
            retained.dropped_lines,
            retained.dropped_bytes,
            Some(limits.retain),
        ));
    }
    let bounded = bound_content(fin.content.take().unwrap_or_default(), limits);
    if bounded.dropped_bytes > 0 {
        harness.push(truncated(
            bounded.dropped_lines,
            bounded.dropped_bytes,
            Some(limits.retain),
        ));
    }
    let mut diagnostics = fin.diagnostics.take().unwrap_or_default();
    diagnostics.extend(harness);
    Ok(ToolExecutionResult {
        content: Some(bounded.content),
        diagnostics: Some(diagnostics),
        ..fin
    })
}

fn settle(
    rt: &ToolRuntime<'_>,
    call: &ToolCall,
    ending: Ending,
    build: impl FnOnce(Option<&ToolSlot>) -> ToolExecutionResult,
    cancel: &CancelToken,
    duration_ms: Option<u64>,
) -> Result<()> {
    let task_id = rt.task_id();
    let conversation = rt.conversation_id();
    let timestamp = rt.now();
    rt.commit(
        Box::new(move |tx: &mut dyn Tx, _task| {
            let token = live_doc()?;
            let mut live: LiveState = read_draft(tx, &token, conversation)?;
            let slot = live.tool_slot(erase(task_id)).map(|slot| slot.clone());
            let result = build(slot.as_ref());
            let entry =
                append_tool_result(tx, conversation, call, &result, timestamp, duration_ms)?;
            let entry_id = entry.id;
            let finished = live
                .tool_slot(erase(task_id))
                .map(|slot| slot.finish(Some(entry_id)))
                .is_some();
            if finished {
                write_draft(tx, &token, conversation, &live)?;
            }
            let outcome = match ending {
                Ending::Completed => TaskOutcome::Completed {
                    result: ToolTaskResult {
                        entry_id,
                        control: result.control,
                    },
                },
                Ending::Aborted => TaskOutcome::Aborted {
                    reason: None,
                    result: Some(ToolTaskResult {
                        entry_id,
                        control: None,
                    }),
                },
                Ending::Failed { message } => TaskOutcome::Failed {
                    error: TaskOutcomeError {
                        message,
                        detail: None,
                    },
                    result: Some(ToolTaskResult {
                        entry_id,
                        control: None,
                    }),
                },
            };
            Ok(Some(NextTaskState::Terminal { outcome }))
        }),
        cancel,
    )
}

fn set_slot_status(
    tx: &mut dyn Tx,
    conversation: ConversationId,
    task_id: TaskId<ToolTaskResult>,
    status: SlotStatus,
) -> Result<()> {
    edit_doc(
        tx,
        &live_doc()?,
        conversation,
        move |live: &mut LiveState| {
            if let Some(slot) = live.tool_slot(erase(task_id)) {
                slot.status = status;
            }
        },
    )
}

pub fn erase<R>(id: TaskId<R>) -> TaskId {
    TaskId::new(id.get())
}

fn from_slot(slot: Option<&ToolSlot>, code: &str, message: &str) -> ToolExecutionResult {
    let mut diagnostics = slot
        .and_then(|slot| slot.diagnostics.clone())
        .unwrap_or_default();
    let dropped_bytes = slot.and_then(|slot| slot.dropped_bytes).unwrap_or(0);
    if dropped_bytes > 0 {
        let dropped_lines = slot.and_then(|slot| slot.dropped_lines).unwrap_or(0);
        diagnostics.push(truncated(
            dropped_lines as usize,
            dropped_bytes as usize,
            None,
        ));
    }
    diagnostics.push(tool_diagnostic(code, message));
    ToolExecutionResult {
        content: Some(
            slot.and_then(|slot| slot.output.clone())
                .filter(|text| !text.is_empty())
                .map(text_block)
                .into_iter()
                .collect(),
        ),
        is_error: Some(true),
        details: slot.and_then(|slot| slot.details.clone()),
        diagnostics: Some(diagnostics),
        ..ToolExecutionResult::default()
    }
}

pub fn harness_error(code: &str, message: &str) -> ToolExecutionResult {
    ToolExecutionResult {
        content: Some(Vec::new()),
        is_error: Some(true),
        diagnostics: Some(vec![tool_diagnostic(code, message)]),
        ..ToolExecutionResult::default()
    }
}

fn invalid(message: &str) -> ToolExecutionResult {
    harness_error("invalid_arguments", message)
}

fn tool_diagnostic(code: &str, message: &str) -> ToolDiagnostic {
    ToolDiagnostic {
        severity: ToolDiagnosticSeverity::Error,
        code: Some(code.to_owned()),
        message: message.to_owned(),
    }
}

fn truncated(dropped_lines: usize, dropped_bytes: usize, retain: Option<Retain>) -> ToolDiagnostic {
    let kept = match retain {
        None => "",
        Some(Retain::Head) => " to its beginning",
        Some(Retain::Tail) => " to its end",
    };
    ToolDiagnostic {
        severity: ToolDiagnosticSeverity::Warn,
        code: Some("truncated".to_owned()),
        message: format!(
            "Output truncated{kept}: {dropped_lines} lines, {dropped_bytes} bytes dropped"
        ),
    }
}

fn text_block(text: String) -> UserBlock {
    UserBlock::Text(TextContent {
        kind: TextType,
        text,
        text_signature: None,
    })
}

fn block_text(block: &UserBlock) -> Option<&str> {
    match block {
        UserBlock::Text(text) => Some(&text.text),
        UserBlock::Image(_) => None,
    }
}

struct Bounded {
    content: Vec<UserBlock>,
    dropped_bytes: usize,
    dropped_lines: usize,
}

fn bound_content(content: Vec<UserBlock>, limits: OutputLimits) -> Bounded {
    let texts: Vec<usize> = content
        .iter()
        .enumerate()
        .filter(|(_, block)| block_text(block).is_some())
        .map(|(index, _)| index)
        .collect();
    let joined: String = texts
        .iter()
        .filter_map(|&index| block_text(&content[index]))
        .collect();
    let bounded = bound_output(&joined, limits);
    if bounded.dropped_bytes == 0 {
        return Bounded {
            content,
            dropped_bytes: 0,
            dropped_lines: 0,
        };
    }
    let keep = match limits.retain {
        Retain::Head => texts.first(),
        Retain::Tail => texts.last(),
    };
    let content = content
        .into_iter()
        .enumerate()
        .filter_map(|(index, block)| match block_text(&block) {
            None => Some(block),
            Some(_) if Some(&index) == keep => Some(text_block(bounded.text.clone())),
            Some(_) => None,
        })
        .collect();
    Bounded {
        content,
        dropped_bytes: bounded.dropped_bytes,
        dropped_lines: bounded.dropped_lines,
    }
}

#[derive(Serialize)]
struct ToolResultData {
    diagnostics: Vec<ToolDiagnostic>,
}

pub fn append_tool_result(
    tx: &mut dyn Tx,
    conversation: ConversationId,
    call: &ToolCall,
    result: &ToolExecutionResult,
    timestamp: u64,
    duration_ms: Option<u64>,
) -> Result<EntryRecord> {
    let diagnostics = result.diagnostics.clone().unwrap_or_default();
    let mut content = result.content.clone().unwrap_or_default();
    if !diagnostics.is_empty() {
        content.push(text_block(render_diagnostics(&diagnostics)));
    }
    if let Some(usage) = &result.usage {
        record_usage(tx, conversation, UsageBucket::Tools, &call.name, usage)?;
    }
    let message = ToolResultMessage {
        role: ToolResultRole,
        tool_call_id: call.id.clone(),
        tool_name: call.name.clone(),
        content,
        details: result.details.clone(),
        usage: result.usage.clone(),
        nested_calls: None,
        is_error: result.is_error.unwrap_or(false),
        duration_ms,
        timestamp,
    };
    let data = serde_json::to_value(ToolResultData { diagnostics })?;
    tx.append_entry(
        conversation,
        EntryDraft {
            kind: tool_result_entry().kind().to_owned(),
            model: Some(vec![Message::ToolResult(Arc::new(message))]),
            data: Some(data),
            head: None,
            edits: None,
        },
    )
}

fn render_diagnostics(diagnostics: &[ToolDiagnostic]) -> String {
    let lines: Vec<String> = diagnostics
        .iter()
        .map(|diagnostic| {
            format!(
                "[{}] {}",
                severity_text(diagnostic.severity),
                diagnostic.message
            )
        })
        .collect();
    format!("<harness>\n{}\n</harness>", lines.join("\n"))
}

fn severity_text(severity: ToolDiagnosticSeverity) -> &'static str {
    match severity {
        ToolDiagnosticSeverity::Info => "info",
        ToolDiagnosticSeverity::Warn => "warn",
        ToolDiagnosticSeverity::Error => "error",
    }
}

impl Api<'_> {
    fn live(&self) -> Result<()> {
        if self.shared.settled.load(Ordering::SeqCst) {
            return Err(Error::Invalid(format!(
                "Tool call {} has settled",
                self.call_id
            )));
        }
        Ok(())
    }
}

impl DocumentObserver for Api<'_> {
    fn watch_doc(
        &self,
        doc: DocRef,
        cancel: &CancelToken,
    ) -> Result<Option<crate::session::types::DocumentWatch>> {
        self.rt.watch_doc(doc, cancel)
    }
}

impl DocumentReader for Api<'_> {
    fn snapshot(&self, doc: DocRef, cancel: &CancelToken) -> Result<Option<JsonObject>> {
        self.rt.snapshot(doc, cancel)
    }

    fn snapshot_as_of(
        &self,
        doc: DocRef,
        at: EntryId,
        cancel: &CancelToken,
    ) -> Result<Option<JsonObject>> {
        self.rt.snapshot_as_of(doc, at, cancel)
    }
}

impl ToolExecutionApi for Api<'_> {
    fn task_id(&self) -> TaskId {
        erase(self.rt.task_id())
    }

    fn conversation_id(&self) -> ConversationId {
        self.rt.conversation_id()
    }

    fn call_id(&self) -> &str {
        &self.call_id
    }

    fn registry(&self) -> Arc<RegistrySnapshot> {
        self.rt.registry()
    }

    fn agent(&self, cancel: &CancelToken) -> Result<Agent> {
        self.rt.agent(cancel)
    }

    fn models(&self) -> &dyn Models {
        self.rt.models()
    }

    fn env(&self) -> Option<&dyn ExecutionEnv> {
        self.env.as_deref()
    }

    fn conversation(
        &self,
        id: ConversationId,
        cancel: &CancelToken,
    ) -> Result<Option<Box<dyn crate::harness::types::ConversationHandle>>> {
        self.rt.conversation(id, cancel)
    }

    fn output(&self, chunk: OutputChunk<'_>, skipped: Option<ShellOutputSkip>) -> Result<()> {
        self.live()?;
        let changed = lock(&self.shared.reported).output.push(chunk, skipped)?;
        if changed {
            self.progress.mark();
        }
        Ok(())
    }

    fn diagnostic(&self, diagnostic: ToolDiagnostic) -> Result<()> {
        self.live()?;
        lock(&self.shared.reported).diagnostics.push(diagnostic);
        self.progress.mark();
        Ok(())
    }

    fn details(&self, value: JsonValue, cancel: &CancelToken) -> Result<()> {
        self.live()?;
        if cancel.is_cancelled() {
            return Err(Error::Aborted);
        }
        lock(&self.shared.reported).details = Some(value);
        match pause(|| self.progress.mark_and_wait().recv()) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(message)) => Err(Error::StorageRejected {
                message,
                cause: None,
            }),
            Err(_) => Err(Error::Aborted),
        }
    }

    fn commit(
        &self,
        change: &mut dyn FnMut(&mut dyn Tx) -> Result<()>,
        cancel: &CancelToken,
    ) -> Result<()> {
        self.rt.commit(
            Box::new(move |tx: &mut dyn Tx, _task| {
                change(tx)?;
                Ok(None)
            }),
            cancel,
        )
    }

    fn memo(&self, name: &str, cancel: &CancelToken) -> Result<Option<JsonValue>> {
        self.rt.memo(name, cancel)
    }

    fn memo_or_set(
        &self,
        name: &str,
        candidate: JsonValue,
        cancel: &CancelToken,
    ) -> Result<JsonValue> {
        self.rt.memo_or_set(name, candidate, cancel)
    }

    fn get_task(&self, id: TaskId, cancel: &CancelToken) -> Result<Option<AnyTaskRecord>> {
        self.rt.get_task(id, cancel)
    }
}
