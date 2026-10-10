//! A durable agent on a real provider: `agent PROVIDER/MODEL PROMPT [--cancel-after-ms N] [--db FILE]`.
//!
//! Keys come from the provider's environment variable (ANTHROPIC_API_KEY, AI_GATEWAY_API_KEY, OPENAI_API_KEY).
//! The model id is everything after the first slash, e.g. `vercel-ai-gateway/anthropic/claude-sonnet-5.5`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use pi_durable::ai::registry::Providers;
use pi_durable::cancel::CancelToken;
use pi_durable::harness::agent::{AgentChange, Change};
use pi_durable::harness::harness::Harness;
use pi_durable::harness::registry::Registry;
use pi_durable::harness::submissions::{SubmissionDraft, WhenBusy};
use pi_durable::harness::types::{
    Extension, HarnessOptions, ModelRef, RootOptions, ToolExecutionResult, ToolRegistration,
};
use pi_durable::storage::memory::MemoryStorage;
use pi_durable::storage::sqlite::SqliteStorage;
use pi_durable::types::{EntryQuery, ScanOrder, Storage};
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn add_tool() -> ToolRegistration {
    ToolRegistration {
        tool: serde_json::from_value(json!({
            "name": "add",
            "description": "Add two numbers",
            "parameters": {"type": "object", "required": ["a", "b"],
                "properties": {"a": {"type": "number"}, "b": {"type": "number"}}}
        }))
        .expect("tool schema"),
        replay: None,
        execution_mode: None,
        output_limits: None,
        prepare_arguments: None,
        execute: Arc::new(|arguments, _api, _cancel| {
            let sum =
                arguments["a"].as_f64().unwrap_or(0.0) + arguments["b"].as_f64().unwrap_or(0.0);
            Ok(ToolExecutionResult {
                content: Some(vec![serde_json::from_value(
                    json!({"type": "text", "text": sum.to_string()}),
                )?]),
                ..Default::default()
            })
        }),
    }
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|at| args.get(at + 1))
        .cloned()
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (Some(model), Some(prompt)) = (args.first(), args.get(1)) else {
        return Err("usage: agent PROVIDER/MODEL PROMPT [--cancel-after-ms N] [--db FILE]".into());
    };
    let (provider, model_id) = model
        .split_once('/')
        .ok_or("model must be PROVIDER/MODEL")?;
    let cancel_after = flag(&args, "--cancel-after-ms")
        .map(|ms| ms.parse::<u64>())
        .transpose()?;
    let storage: Arc<dyn Storage> = match flag(&args, "--db") {
        Some(path) => Arc::new(SqliteStorage::open(&path, &Default::default())?),
        None => Arc::new(MemoryStorage::new()),
    };

    let cancel = CancelToken::new();
    let registry = Arc::new(Registry::default());
    registry.install(Extension {
        name: "demo".to_owned(),
        tools: vec![add_tool()],
        sections: vec![],
        wraps: vec![],
        hooks: vec![],
        tasks: vec![],
    })?;
    let harness = Harness::open(
        storage,
        HarnessOptions {
            models: Arc::new(Providers::builtin()),
            registry,
            settings: None,
            conversation_created: None,
            now: None,
            env: None,
            on_report: Some(Arc::new(|error| eprintln!("report: {error}"))),
        },
    )?;
    let root = harness.root(
        RootOptions {
            agent: Some(AgentChange {
                model: Change::Set(ModelRef {
                    provider: provider.to_owned(),
                    model_id: model_id.to_owned(),
                }),
                ..Default::default()
            }),
            init: None,
        },
        &cancel,
    )?;
    let started = Instant::now();
    root.submit(
        SubmissionDraft::Input {
            request_id: None,
            content: serde_json::from_value(json!(prompt))?,
            when_busy: WhenBusy::default(),
        },
        &cancel,
    )?;
    if let Some(ms) = cancel_after {
        std::thread::sleep(Duration::from_millis(ms));
        let abort_started = Instant::now();
        root.abort(false, &cancel)?;
        harness.wait_for_idle(&cancel)?;
        println!("aborted_in_ms {}", abort_started.elapsed().as_millis());
    } else {
        harness.wait_for_idle(&cancel)?;
    }
    println!("elapsed_ms {}", started.elapsed().as_millis());

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
    for entry in serde_json::to_value(&page.items)?
        .as_array()
        .into_iter()
        .flatten()
    {
        for message in entry["model"].as_array().into_iter().flatten() {
            summarize(message);
        }
    }
    harness.close(&cancel)?;
    Ok(())
}

fn summarize(message: &Value) {
    match message["role"].as_str().unwrap_or("?") {
        "assistant" => {
            for block in message["content"].as_array().into_iter().flatten() {
                match block["type"].as_str() {
                    Some("text") => println!("assistant: {}", block["text"].as_str().unwrap_or("")),
                    Some("toolCall") => println!(
                        "assistant tool call: {} {}",
                        block["name"], block["arguments"]
                    ),
                    Some("thinking") => println!(
                        "assistant thinking: {} chars",
                        block["thinking"].as_str().map_or(0, str::len)
                    ),
                    _ => {}
                }
            }
            println!(
                "  stop {} in {} out {} cost {}",
                message["stopReason"],
                message["usage"]["input"],
                message["usage"]["output"],
                message["usage"]["cost"]["total"]
            );
            if let Some(error) = message["errorMessage"].as_str() {
                println!("  error: {error}");
            }
        }
        "toolResult" => println!(
            "tool result: {}",
            message["content"][0]["text"].as_str().unwrap_or("")
        ),
        "user" => println!("user: {}", message["content"]),
        _ => {}
    }
}
