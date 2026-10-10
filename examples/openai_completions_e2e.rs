//! End-to-end check of the Chat Completions provider: a std-only fake chat.completions server on loopback,
//! the real harness, a tool-using turn, a retried 429 and a cancel in the middle of a stream. Exits non-zero on
//! any mismatch.

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

fn data(stream: &mut TcpStream, value: &Value) -> std::io::Result<()> {
    let chunk = format!("data: {value}\n\n");
    write!(stream, "{:x}\r\n{chunk}\r\n", chunk.len())?;
    stream.flush()
}

fn start(stream: &mut TcpStream) -> std::io::Result<()> {
    stream.write_all(
        b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n",
    )
}

fn finish_stream(stream: &mut TcpStream) -> std::io::Result<()> {
    data(stream, &json!("[DONE]"))?;
    stream.write_all(b"0\r\n\r\n")
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
    if transcript.contains("SLOW") {
        start(&mut stream)?;
        for _ in 0..600 {
            let sent = data(
                &mut stream,
                &json!({"choices": [{"index": 0, "delta": {"content": "tick "}}]}),
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
    if transcript.contains("\"role\":\"tool\"") {
        data(
            &mut stream,
            &json!({"id": "c2", "choices": [{"index": 0, "delta": {"role": "assistant", "content": "the sum is 42"}}]}),
        )?;
        data(
            &mut stream,
            &json!({"id": "c2", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}),
        )?;
        data(
            &mut stream,
            &json!({"id": "c2", "choices": [], "usage": {"prompt_tokens": 1000, "completion_tokens": 12, "prompt_tokens_details": {"cached_tokens": 0}}}),
        )?;
    } else {
        data(
            &mut stream,
            &json!({"id": "c1", "choices": [{"index": 0, "delta": {"role": "assistant", "tool_calls": [{"index": 0, "id": "call_1", "type": "function", "function": {"name": "add", "arguments": ""}}]}}]}),
        )?;
        for part in ["{\"a\": 2,", " \"b\": 40}"] {
            data(
                &mut stream,
                &json!({"id": "c1", "choices": [{"index": 0, "delta": {"tool_calls": [{"index": 0, "function": {"arguments": part}}]}}]}),
            )?;
        }
        data(
            &mut stream,
            &json!({"id": "c1", "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]}),
        )?;
        data(
            &mut stream,
            &json!({"id": "c1", "choices": [], "usage": {"prompt_tokens": 1000, "completion_tokens": 30}}),
        )?;
    }
    finish_stream(&mut stream)?;
    Ok(())
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
        "fake",
        "fake-test",
        "openai-completions",
        200_000,
        8192,
        false,
    );
    model.base_url = format!("http://127.0.0.1:{port}/v1");
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
                    provider: "fake".to_owned(),
                    model_id: "fake-test".to_owned(),
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
    assert_eq!(assistants[0]["content"][0]["id"], "call_1");
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
            .all(|key| key == "Bearer sk-test"),
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
