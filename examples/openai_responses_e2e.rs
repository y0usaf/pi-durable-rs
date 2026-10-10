//! End-to-end check of the OpenAI Responses provider: a std-only fake /responses server on loopback, the real
//! harness, a tool-using turn, a retried 429 and a cancel in the middle of a stream. Exits non-zero on any mismatch.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pi_durable::ai::registry::Providers;
use pi_durable::ai::types::{Model, ModelCost};
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

#[derive(Default)]
struct Seen {
    requests: AtomicUsize,
    rate_limited: AtomicBool,
    slow_closed: AtomicBool,
    keys: Mutex<Vec<String>>,
}

fn read_request(stream: &mut TcpStream) -> Result<(Vec<(String, String)>, Value)> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let mut headers = Vec::new();
    loop {
        line.clear();
        reader.read_line(&mut line)?;
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_owned()));
        }
    }
    let length = headers
        .iter()
        .find(|(name, _)| name == "content-length")
        .and_then(|(_, value)| value.parse::<usize>().ok())
        .ok_or("no content-length")?;
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    Ok((headers, serde_json::from_slice(&body)?))
}

fn event(stream: &mut TcpStream, name: &str, data: Value) -> std::io::Result<()> {
    let chunk = format!("event: {name}\ndata: {data}\n\n");
    write!(stream, "{:x}\r\n{chunk}\r\n", chunk.len())?;
    stream.flush()
}

fn serve(mut stream: TcpStream, seen: &Seen) -> Result<()> {
    let (headers, body) = read_request(&mut stream)?;
    seen.requests.fetch_add(1, Ordering::SeqCst);
    let key = headers.iter().find(|(name, _)| name == "authorization");
    seen.keys
        .lock()
        .unwrap()
        .push(key.map(|(_, value)| value.clone()).unwrap_or_default());
    let transcript = body.to_string();
    if transcript.contains("RATELIMIT") && !seen.rate_limited.swap(true, Ordering::SeqCst) {
        let reply =
            json!({"error": {"message": "slow down", "type": "rate_limit_error"}}).to_string();
        write!(
            stream,
            "HTTP/1.1 429 Too Many Requests\r\nretry-after-ms: 20\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{reply}",
            reply.len()
        )?;
        return Ok(());
    }
    let model = "gpt-test";
    if transcript.contains("SLOW") {
        start(&mut stream)?;
        event(
            &mut stream,
            "response.output_item.added",
            json!({"type": "response.output_item.added", "output_index": 0,
            "item": {"type": "message", "id": "msg_1", "role": "assistant", "content": [], "status": "in_progress"}}),
        )?;
        for _ in 0..600 {
            let sent = event(
                &mut stream,
                "response.output_text.delta",
                json!({"type": "response.output_text.delta",
                "output_index": 0, "content_index": 0, "delta": "tick "}),
            );
            if sent.is_err() {
                seen.slow_closed.store(true, Ordering::SeqCst);
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        return Ok(());
    }
    start(&mut stream)?;
    if transcript.contains("function_call_output") {
        let text = "the sum is 42";
        event(
            &mut stream,
            "response.output_item.added",
            json!({"type": "response.output_item.added", "output_index": 0,
            "item": {"type": "message", "id": "msg_2", "role": "assistant", "content": [], "status": "in_progress"}}),
        )?;
        event(
            &mut stream,
            "response.output_text.delta",
            json!({"type": "response.output_text.delta", "output_index": 0,
            "content_index": 0, "delta": text}),
        )?;
        let item = json!({"type": "message", "id": "msg_2", "role": "assistant", "status": "completed",
            "content": [{"type": "output_text", "text": text, "annotations": []}]});
        event(
            &mut stream,
            "response.output_item.done",
            json!({"type": "response.output_item.done", "output_index": 0, "item": item}),
        )?;
        finish(&mut stream, model, "completed", vec![item])?;
    } else {
        let added = json!({"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "add", "arguments": "", "status": "in_progress"});
        event(
            &mut stream,
            "response.output_item.added",
            json!({"type": "response.output_item.added", "output_index": 0, "item": added}),
        )?;
        for part in ["{\"a\": 2,", " \"b\": 40}"] {
            event(
                &mut stream,
                "response.function_call_arguments.delta",
                json!({"type": "response.function_call_arguments.delta",
                "output_index": 0, "item_id": "fc_1", "delta": part}),
            )?;
        }
        let arguments = "{\"a\": 2, \"b\": 40}";
        event(
            &mut stream,
            "response.function_call_arguments.done",
            json!({"type": "response.function_call_arguments.done",
            "output_index": 0, "item_id": "fc_1", "arguments": arguments}),
        )?;
        let item = json!({"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "add", "arguments": arguments, "status": "completed"});
        event(
            &mut stream,
            "response.output_item.done",
            json!({"type": "response.output_item.done", "output_index": 0, "item": item}),
        )?;
        finish(&mut stream, model, "completed", vec![item])?;
    }
    Ok(())
}

fn start(stream: &mut TcpStream) -> std::io::Result<()> {
    stream.write_all(
        b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n",
    )?;
    event(
        stream,
        "response.created",
        json!({"type": "response.created",
        "response": {"id": "resp_1", "object": "response", "status": "in_progress", "output": []}}),
    )
}

fn finish(
    stream: &mut TcpStream,
    model: &str,
    status: &str,
    output: Vec<Value>,
) -> std::io::Result<()> {
    let response = json!({"id": "resp_1", "object": "response", "model": model, "status": status, "output": output,
        "usage": {"input_tokens": 1000, "output_tokens": 30, "total_tokens": 1030,
            "input_tokens_details": {"cached_tokens": 0}, "output_tokens_details": {"reasoning_tokens": 0}}});
    event(
        stream,
        "response.completed",
        json!({"type": "response.completed", "response": response}),
    )?;
    stream.write_all(b"0\r\n\r\n")
}

fn add_tool() -> ToolRegistration {
    ToolRegistration {
        tool: serde_json::from_value(json!({
            "name": "add", "description": "Add two numbers",
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

fn messages(
    root: &pi_durable::harness::harness::Conversation,
    cancel: &CancelToken,
) -> Result<Vec<Value>> {
    let page = root.entries(
        EntryQuery {
            conversation_id: root.id(),
            min_entry_id: None,
            max_entry_id: None,
            order: Some(ScanOrder::Ascending),
        },
        1000,
        None,
        cancel,
    )?;
    let entries = serde_json::to_value(&page.items)?;
    Ok(entries
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|entry| entry["model"].as_array().cloned().unwrap_or_default())
        .collect())
}

fn submit(
    root: &pi_durable::harness::harness::Conversation,
    text: &str,
    cancel: &CancelToken,
) -> Result<()> {
    root.submit(
        SubmissionDraft::Input {
            request_id: None,
            content: serde_json::from_value(json!(text))?,
            when_busy: WhenBusy::default(),
        },
        cancel,
    )?;
    Ok(())
}

fn main() -> Result<()> {
    let seen = Arc::new(Seen::default());
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    let server_seen = Arc::clone(&seen);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let seen = Arc::clone(&server_seen);
            std::thread::spawn(move || {
                if let Err(error) = serve(stream, &seen) {
                    eprintln!("fake server: {error}");
                }
            });
        }
    });

    let mut model = Model::new(
        "openai",
        "gpt-test",
        "openai-responses",
        200_000,
        8192,
        false,
    );
    model.base_url = format!("http://127.0.0.1:{port}");
    model.cost = Some(ModelCost {
        input: 3.0,
        output: 15.0,
        cache_read: 0.3,
        cache_write: 3.75,
        tiers: Vec::new(),
    });
    let mut providers = Providers::builtin();
    providers.add_models([model]);
    providers.set_auth(Arc::new(|_| {
        Ok(pi_durable::ai::registry::Auth {
            api_key: Some("sk-test".to_owned()),
            ..Default::default()
        })
    }));

    let registry = Arc::new(Registry::default());
    registry.install(Extension {
        name: "demo".to_owned(),
        tools: vec![add_tool()],
        sections: vec![],
        wraps: vec![],
        hooks: vec![],
        tasks: vec![],
    })?;
    let cancel = CancelToken::new();
    let harness = Harness::open(
        Arc::new(MemoryStorage::new()),
        HarnessOptions {
            models: Arc::new(providers),
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
                    provider: "openai".to_owned(),
                    model_id: "gpt-test".to_owned(),
                }),
                ..Default::default()
            }),
            init: None,
        },
        &cancel,
    )?;

    submit(&root, "RATELIMIT then add 2 and 40", &cancel)?;
    harness.wait_for_idle(&cancel)?;
    let all = messages(&root, &cancel)?;
    let assistants: Vec<&Value> = all
        .iter()
        .filter(|message| message["role"] == "assistant")
        .collect();
    assert_eq!(assistants.len(), 2, "two assistant messages: {all:#?}");
    assert_eq!(
        assistants[0]["stopReason"], "toolUse",
        "{:#?}",
        assistants[0]
    );
    assert_eq!(
        assistants[0]["content"][0]["arguments"],
        json!({"a": 2, "b": 40})
    );
    assert_eq!(assistants[1]["stopReason"], "stop", "{:#?}", assistants[1]);
    assert_eq!(assistants[1]["content"][0]["text"], "the sum is 42");
    assert!(all.iter().any(|message| message["role"] == "toolResult" && message["content"][0]["text"] == "42"));
    assert_eq!(assistants[0]["usage"]["input"], 1000);
    assert!(
        assistants[0]["usage"]["cost"]["total"]
            .as_f64()
            .unwrap_or(0.0)
            > 0.0,
        "cost is computed"
    );
    assert!(
        seen.rate_limited.load(Ordering::SeqCst),
        "the 429 was served"
    );
    assert_eq!(
        seen.requests.load(Ordering::SeqCst),
        3,
        "one retry plus two turns"
    );
    assert!(
        seen.keys
            .lock()
            .unwrap()
            .iter()
            .all(|key| key.contains("sk-test")),
        "the resolved key was sent"
    );
    println!("tool turn ok");

    submit(&root, "SLOW count forever", &cancel)?;
    std::thread::sleep(Duration::from_millis(700));
    let aborted = Instant::now();
    root.abort(false, &cancel)?;
    harness.wait_for_idle(&cancel)?;
    let took = aborted.elapsed();
    assert!(took < Duration::from_secs(1), "abort took {took:?}");
    let all = messages(&root, &cancel)?;
    let last = all
        .iter()
        .rev()
        .find(|message| message["role"] == "assistant")
        .ok_or("no assistant")?;
    assert_eq!(last["stopReason"], "aborted", "{last:#?}");
    let deadline = Instant::now() + Duration::from_secs(1);
    while !seen.slow_closed.load(Ordering::SeqCst) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        seen.slow_closed.load(Ordering::SeqCst),
        "the server saw the connection close"
    );
    println!("cancel ok in {took:?}");
    harness.close(&cancel)?;
    Ok(())
}
