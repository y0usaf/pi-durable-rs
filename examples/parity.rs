use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pi_durable::ai::providers::faux::{Faux, FauxOptions, FauxResponse};
use pi_durable::cancel::CancelToken;
use pi_durable::harness::agent::{AgentChange, Change};
use pi_durable::harness::harness::Harness;
use pi_durable::harness::live::live_doc;
use pi_durable::harness::registry::Registry;
use pi_durable::harness::submissions::{SubmissionDraft, WhenBusy};
use pi_durable::harness::types::{
    CompactionOverrides, Extension, HarnessOptions, HarnessSettings, ModelRef, RetryOverrides,
    RootOptions, ToolExecutionResult, ToolRegistration,
};
use pi_durable::ids::TaskId;
use pi_durable::storage::memory::MemoryStorage;
use pi_durable::storage::sqlite::SqliteStorage;
use pi_durable::types::Storage;
use pi_durable::types::{EntryQuery, ScanOrder};
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn usage() -> Value {
    json!({"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 0,
        "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}})
}

fn assistant(case: &Value) -> Result<FauxResponse> {
    let mut message = json!({"role": "assistant", "content": case["content"], "api": "faux", "provider": "faux",
        "model": "faux-1", "usage": usage(), "stopReason": case["stopReason"], "timestamp": now_ms()});
    if let Some(error) = case.get("errorMessage") {
        message["errorMessage"] = error.clone();
    }
    Ok(FauxResponse::Message(Box::new(serde_json::from_value(
        message,
    )?)))
}

fn responses(list: &Value) -> Result<Vec<FauxResponse>> {
    list.as_array()
        .ok_or("responses")?
        .iter()
        .map(assistant)
        .collect()
}

fn tool(
    name: &str,
    description: &str,
    parameters: Value,
    execute: pi_durable::harness::types::ToolExecute,
) -> ToolRegistration {
    ToolRegistration {
        tool: pi_durable::ai::types::Tool {
            name: name.to_owned(),
            description: description.to_owned(),
            parameters,
            constrained_sampling: None,
        },
        replay: None,
        execution_mode: None,
        output_limits: None,
        prepare_arguments: None,
        execute,
    }
}

fn echo() -> ToolRegistration {
    let parameters =
        json!({"type": "object", "required": ["text"], "properties": {"text": {"type": "string"}}});
    tool(
        "echo",
        "echo tool",
        parameters,
        Arc::new(|args, _api, _cancel| {
            let text = args["text"].as_str().unwrap_or_default().to_owned();
            Ok(ToolExecutionResult {
                content: Some(vec![serde_json::from_value(
                    json!({"type": "text", "text": format!("echoed:{text}")}),
                )?]),
                ..Default::default()
            })
        }),
    )
}

fn sleep() -> ToolRegistration {
    let parameters =
        json!({"type": "object", "required": ["ms"], "properties": {"ms": {"type": "number"}}});
    tool(
        "sleep",
        "sleep tool",
        parameters,
        Arc::new(|args, _api, _cancel| {
            let ms = args["ms"].as_u64().unwrap_or_default();
            pi_durable::turn::pause(|| std::thread::sleep(Duration::from_millis(ms)));
            Ok(ToolExecutionResult {
                content: Some(vec![serde_json::from_value(
                    json!({"type": "text", "text": format!("slept:{ms}")}),
                )?]),
                ..Default::default()
            })
        }),
    )
}

fn settings(case: &Value) -> Result<HarnessSettings> {
    let mut out = HarnessSettings::default();
    if let Some(retry) = case["settings"].get("retry") {
        out.retry = Some(RetryOverrides {
            enabled: retry["enabled"].as_bool(),
            max_retries: retry["maxRetries"].as_u64(),
            base_delay_ms: retry["baseDelayMs"].as_u64(),
            max_agent_delay_ms: None,
        });
    }
    if let Some(compaction) = case["settings"].get("compaction") {
        out.compaction = Some(CompactionOverrides {
            enabled: compaction["enabled"].as_bool(),
            reserve_tokens: compaction["reserveTokens"].as_u64(),
            keep_recent_tokens: compaction["keepRecentTokens"].as_u64(),
            background_tokens: None,
        });
    }
    Ok(out)
}

fn when_busy(text: Option<&str>) -> Result<WhenBusy> {
    Ok(match text {
        None => WhenBusy::default(),
        Some("steer") => WhenBusy::Steer,
        Some("followUp") => WhenBusy::FollowUp,
        Some("reject") => WhenBusy::Reject,
        Some(other) => return Err(format!("unknown whenBusy {other}").into()),
    })
}

fn wait_until(mut ready: impl FnMut() -> Result<bool>) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready()? {
        if Instant::now() > deadline {
            return Err("driver: wait timed out".into());
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    Ok(())
}

fn open_storage(kind: &str, db: &str, case: &str, phase: &str) -> Result<Arc<dyn Storage>> {
    if kind == "memory" {
        return Ok(Arc::new(MemoryStorage::new()));
    }
    let path = format!("{db}/{case}.db");
    if phase != "resume" {
        let _ = std::fs::remove_file(&path);
    }
    Ok(Arc::new(SqliteStorage::open(&path, &Default::default())?))
}

fn run_case(case: &Value, kind: &str, db: &str, phase: &str) -> Result<Value> {
    let cancel = CancelToken::new();
    let name = case["name"].as_str().ok_or("name")?;
    let resuming = phase == "resume";
    let faux = Arc::new(Faux::new(FauxOptions {
        tokens_per_second: case["settings"]["tokensPerSecond"].as_f64(),
        ..Default::default()
    }));
    let scripted = if resuming {
        &case["resumeResponses"]
    } else {
        &case["responses"]
    };
    faux.set_responses(responses(scripted)?);
    let mut tools = Vec::new();
    let wanted: Vec<&str> = case["tools"]
        .as_array()
        .map(|t| t.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    if wanted.contains(&"echo") {
        tools.push(echo());
    }
    if wanted.contains(&"sleep") {
        tools.push(sleep());
    }
    let registry = Arc::new(Registry::default());
    registry.install(Extension {
        name: "tools".to_owned(),
        tools,
        sections: vec![],
        wraps: vec![],
        hooks: vec![],
        tasks: vec![],
    })?;
    let reports = Arc::new(Mutex::new(Vec::<String>::new()));
    let sink = Arc::clone(&reports);
    let harness = Harness::open(
        open_storage(kind, db, name, phase)?,
        HarnessOptions {
            models: faux,
            registry,
            settings: Some(settings(case)?),
            conversation_created: None,
            env: None,
            now: Some(Arc::new(|| 1000)),
            on_report: Some(Arc::new(move |error| {
                sink.lock().expect("reports").push(error.to_string())
            })),
        },
    )?;
    let root = harness.root(
        RootOptions {
            agent: Some(AgentChange {
                model: Change::Set(ModelRef {
                    provider: "faux".to_owned(),
                    model_id: "faux-1".to_owned(),
                }),
                ..Default::default()
            }),
            init: None,
        },
        &cancel,
    )?;
    let token = live_doc()?;
    let root_id = root.id();
    let live_now = || -> Result<Value> {
        Ok(serde_json::to_value(harness.session().snapshot_as(
            &token,
            Some(root_id.0),
            None,
            &cancel,
        )?)?)
    };
    let mut step_results: Vec<Value> = Vec::new();
    let mut compact_ids: Vec<u64> = Vec::new();
    let steps = case["steps"].as_array().ok_or("steps")?;
    let crash_at = steps.iter().position(|s| s["do"] == "crashPoint");
    let start = if resuming {
        crash_at.map_or(steps.len(), |i| i + 1)
    } else {
        0
    };
    for step in &steps[start..] {
        match step["do"].as_str().ok_or("do")? {
            "submit" => {
                let draft = SubmissionDraft::Input {
                    request_id: step["requestId"].as_str().map(str::to_owned),
                    content: serde_json::from_value(step["text"].clone())?,
                    when_busy: when_busy(step["whenBusy"].as_str())?,
                };
                match root.submit(draft, &cancel) {
                    Ok(handle) => {
                        step_results.push(json!({"submit": serde_json::to_value(handle.id)?}))
                    }
                    Err(error) => step_results.push(json!({"err": error.to_string()})),
                }
            }
            "idle" => harness.wait_for_idle(&cancel)?,
            "sleepMs" => std::thread::sleep(Duration::from_millis(
                step["ms"].as_u64().unwrap_or_default(),
            )),
            "waitRun" => wait_until(|| Ok(!live_now()?["run"].is_null()))?,
            "waitTool" => wait_until(|| {
                Ok(live_now()?["tools"]
                    .as_array()
                    .is_some_and(|t| t.iter().any(|s| s["status"] == "running")))
            })?,
            "abortRun" => root.abort(false, &cancel)?,
            "compact" => {
                let task = root.compact(step["instructions"].as_str(), &cancel)?;
                let id = serde_json::to_value(task)?.as_u64().unwrap_or_default();
                compact_ids.push(id);
                step_results.push(json!({"compact": id}));
            }
            "crashPoint" => {
                if phase == "run" {
                    println!("READY");
                    std::io::stdout().flush()?;
                    loop {
                        std::thread::sleep(Duration::from_secs(3600));
                    }
                }
            }
            other => return Err(format!("driver: unknown step {other}").into()),
        }
    }
    harness.wait_for_idle(&cancel)?;
    let page = root.entries(
        EntryQuery {
            conversation_id: root.id(),
            min_entry_id: None,
            max_entry_id: None,
            order: Some(ScanOrder::Ascending),
        },
        1000,
        None,
        &cancel,
    )?;
    let reports = reports.lock().expect("reports").clone();
    let mut entries = serde_json::to_value(&page.items)?;
    if let Some(list) = entries.as_array_mut() {
        list.sort_by_key(|entry| entry["id"].as_u64().unwrap_or_default());
    }
    let mut ids: std::collections::BTreeSet<u64> = entries
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry["byTaskId"].as_u64())
        .collect();
    ids.extend(compact_ids);
    let mut tasks = Vec::new();
    for id in ids {
        tasks.push(serde_json::to_value(
            harness.get_task(TaskId::new(id), &cancel)?,
        )?);
    }
    harness.close(&cancel)?;
    Ok(json!({"steps": step_results, "entries": entries, "reports": reports, "tasks": tasks}))
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let [_, file, out_dir, kind, db, rest @ ..] = args.as_slice() else {
        return Err(
            "usage: parity <scenarios.json> <out-dir> <memory|sqlite> <db-dir> [case] [run|resume]"
                .into(),
        );
    };
    let only = rest.first().map(String::as_str).filter(|s| !s.is_empty());
    let phase = rest.get(1).map_or("full", String::as_str);
    let cases: Vec<Value> = serde_json::from_str(&std::fs::read_to_string(file)?)?;
    std::fs::create_dir_all(out_dir)?;
    std::fs::create_dir_all(db)?;
    for case in &cases {
        let name = case["name"].as_str().ok_or("name")?;
        if only.is_some_and(|o| o != name) {
            continue;
        }
        let is_crash = case["crash"].as_bool() == Some(true);
        if (is_crash && phase == "full") || (!is_crash && phase != "full") {
            continue;
        }
        let out = run_case(case, kind, db, phase)?;
        std::fs::write(
            format!("{out_dir}/{name}.json"),
            serde_json::to_string_pretty(&out)?,
        )?;
        println!(
            "rs {name} {phase} entries {} tasks {} reports {}",
            out["entries"].as_array().map_or(0, Vec::len),
            out["tasks"].as_array().map_or(0, Vec::len),
            out["reports"].as_array().map_or(0, Vec::len)
        );
    }
    Ok(())
}
