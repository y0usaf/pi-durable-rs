//! A durable agent with a scripted faux provider and one tool: the smallest end-to-end use of the harness.

use std::sync::Arc;

use pi_durable::ai::providers::faux::{Faux, FauxOptions, FauxResponse};
use pi_durable::cancel::CancelToken;
use pi_durable::harness::agent::{AgentChange, Change};
use pi_durable::harness::harness::Harness;
use pi_durable::harness::registry::Registry;
use pi_durable::harness::submissions::{SubmissionDraft, WhenBusy};
use pi_durable::harness::types::{
    Extension, HarnessOptions, ModelRef, RootOptions, ToolExecutionResult, ToolRegistration,
};
use pi_durable::storage::memory::MemoryStorage;
use pi_durable::types::{EntryQuery, ScanOrder};
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn scripted(content: Value, stop_reason: &str) -> Result<FauxResponse> {
    let message = json!({
        "role": "assistant", "content": content, "api": "faux", "provider": "faux", "model": "faux-1",
        "usage": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 0,
            "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}},
        "stopReason": stop_reason, "timestamp": 0
    });
    Ok(FauxResponse::Message(Box::new(serde_json::from_value(
        message,
    )?)))
}

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

fn main() -> Result<()> {
    let cancel = CancelToken::new();
    let faux = Arc::new(Faux::new(FauxOptions::default()));
    faux.set_responses(vec![
        scripted(json!([{"type": "toolCall", "id": "c1", "name": "add", "arguments": {"a": 2, "b": 40}}]), "toolUse")?,
        scripted(json!([{"type": "text", "text": "2 + 40 = 42"}]), "stop")?,
    ]);
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
        Arc::new(MemoryStorage::new()),
        HarnessOptions {
            models: faux,
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
                    provider: "faux".to_owned(),
                    model_id: "faux-1".to_owned(),
                }),
                ..Default::default()
            }),
            init: None,
        },
        &cancel,
    )?;
    root.submit(
        SubmissionDraft::Input {
            request_id: None,
            content: serde_json::from_value(json!("What is 2 + 40?"))?,
            when_busy: WhenBusy::default(),
        },
        &cancel,
    )?;
    harness.wait_for_idle(&cancel)?;
    let page = root.entries(
        EntryQuery {
            conversation_id: root.id(),
            min_entry_id: None,
            max_entry_id: None,
            order: Some(ScanOrder::Ascending),
        },
        100,
        None,
        &cancel,
    )?;
    for entry in serde_json::to_value(&page.items)?
        .as_array()
        .into_iter()
        .flatten()
    {
        println!("{}", entry["kind"].as_str().unwrap_or("?"));
    }
    harness.close(&cancel)?;
    Ok(())
}
