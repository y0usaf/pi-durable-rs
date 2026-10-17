use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use pi_durable::ai::cost::calculate_cost;
use pi_durable::ai::error_body::truncate_error_text;
use pi_durable::ai::json_parse::{parse_json_with_repair, parse_streaming_json};
use pi_durable::ai::models::Models;
use pi_durable::ai::registry::Providers;
use pi_durable::ai::simple_options::clamp_max_tokens_to_context;
use pi_durable::ai::thinking::{clamp_thinking_level, supported_thinking_levels};
use pi_durable::ai::types::{
    AssistantMessage, Context, Message, Model, SimpleStreamOptions, Usage,
};
use pi_durable::ai::utils::estimate::{calculate_context_tokens, estimate_message_tokens};
use pi_durable::ai::utils::overflow::is_context_overflow;
use pi_durable::ai::utils::retry::{is_retryable_assistant_error, retry_delay_ms};
use pi_durable::ai::utils::transcript::{
    get_current_tools, has_non_additive_tool_changes, normalize_context,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, String>;

fn parse<T: DeserializeOwned>(value: &Value, what: &str) -> Result<T> {
    serde_json::from_value(value.clone()).map_err(|error| format!("{what}: {error}"))
}

fn optional<T: DeserializeOwned>(value: &Value, what: &str) -> Result<Option<T>> {
    if value.is_null() {
        Ok(None)
    } else {
        parse(value, what).map(Some)
    }
}

fn read(path: &Path) -> Result<Value> {
    let text = fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    serde_json::from_str(&text).map_err(|error| format!("{}: {error}", path.display()))
}

fn options(value: &Value) -> Result<SimpleStreamOptions> {
    let text = |key: &str| value[key].as_str().map(str::to_owned);
    Ok(SimpleStreamOptions {
        api_key: text("apiKey"),
        session_id: text("sessionId"),
        reasoning: optional(&value["reasoning"], "reasoning")?,
        cache_retention: optional(&value["cacheRetention"], "cacheRetention")?,
        deferred: value["deferred"].as_bool().unwrap_or(false),
        max_tokens: value["maxTokens"].as_u64(),
        transport: optional(&value["transport"], "transport")?,
        timeout_ms: value["timeoutMs"].as_u64(),
        max_retries: value["maxRetries"].as_u64(),
        max_retry_delay_ms: value["maxRetryDelayMs"].as_u64(),
        headers: optional::<BTreeMap<String, String>>(&value["headers"], "headers")?,
        metadata: optional(&value["metadata"], "metadata")?,
        temperature: value["temperature"].as_f64(),
        tool_choice: text("toolChoice"),
        ..SimpleStreamOptions::default()
    })
}

fn case(providers: &Providers, scenario: &Value, port: u64) -> Result<(Vec<Value>, Value)> {
    let mut model = scenario["model"].clone();
    let base = model["baseUrl"]
        .as_str()
        .unwrap_or_default()
        .replace("PORT", &port.to_string());
    model["baseUrl"] = Value::String(base);
    let model: Model = parse(&model, "model")?;
    let context: Context = parse(&scenario["context"], "context")?;
    let stream = providers.stream_simple(
        &model,
        normalize_context(&context),
        options(&scenario["options"])?,
    );
    let mut events = Vec::new();
    while let Some(event) = stream.next_event() {
        events.push(serde_json::to_value(event).map_err(|error| error.to_string())?);
    }
    let message = match stream.result() {
        Some(message) => serde_json::to_value(message).map_err(|error| error.to_string())?,
        None => Value::Null,
    };
    Ok((events, message))
}

fn e1(dir: &Path, ports: &Path) -> Result<()> {
    let ports = read(ports)?;
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .map_err(|error| error.to_string())?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    files.sort();
    let providers = Providers::builtin();
    for file in files {
        let scenario = read(&file)?;
        let name = scenario["name"].as_str().unwrap_or_default().to_owned();
        let port = ports["rs"][&name]
            .as_u64()
            .ok_or(format!("no port for {name}"))?;
        let line = match case(&providers, &scenario, port) {
            Ok((events, message)) => {
                json!({"name": name, "events": events, "message": message, "error": null})
            }
            Err(error) => json!({"name": name, "events": [], "message": null, "error": error}),
        };
        println!("{line}");
    }
    Ok(())
}

fn arg<T: DeserializeOwned>(args: &[Value], index: usize) -> Result<T> {
    parse(
        args.get(index).unwrap_or(&Value::Null),
        &format!("arg {index}"),
    )
}

fn value<T: serde::Serialize>(result: T) -> Result<Value> {
    serde_json::to_value(result).map_err(|error| error.to_string())
}

fn call(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "parse_streaming_json" => value(parse_streaming_json(args[0].as_str())),
        "parse_json_with_repair" => {
            parse_json_with_repair(&arg::<String>(args, 0)?).map_err(|error| error.to_string())
        }
        "truncate_error_text" => {
            value(truncate_error_text(&arg::<String>(args, 0)?, arg(args, 1)?))
        }
        "calculate_cost" => {
            let mut usage: Usage = arg(args, 1)?;
            calculate_cost(&arg::<Model>(args, 0)?, &mut usage);
            value(usage.cost)
        }
        "is_context_overflow" => value(is_context_overflow(
            &arg::<AssistantMessage>(args, 0)?,
            arg(args, 1)?,
        )),
        "is_retryable_assistant_error" => value(is_retryable_assistant_error(&arg::<
            AssistantMessage,
        >(args, 0)?)),
        "calculate_context_tokens" => value(calculate_context_tokens(&arg::<Usage>(args, 0)?)),
        "estimate_message_tokens" => value(estimate_message_tokens(&arg::<Message>(args, 0)?)),
        "supported_thinking_levels" => value(supported_thinking_levels(&arg::<Model>(args, 0)?)),
        "clamp_thinking_level" => {
            value(clamp_thinking_level(&arg::<Model>(args, 0)?, arg(args, 1)?))
        }
        "retry_delay_ms" => {
            let policy = args.first().cloned().unwrap_or_default();
            let base = policy["baseDelayMs"].as_u64().ok_or("arg 0: baseDelayMs")?;
            value(retry_delay_ms(
                base,
                policy["maxAgentDelayMs"].as_u64(),
                arg(args, 1)?,
            ))
        }
        "normalize_context" => {
            value(json!({"messages": normalize_context(&arg::<Context>(args, 0)?).messages}))
        }
        "get_current_tools" => value(get_current_tools(&arg::<Vec<Message>>(args, 0)?)),
        "has_non_additive_tool_changes" => {
            value(has_non_additive_tool_changes(&arg::<Vec<Message>>(
                args, 0,
            )?))
        }
        "clamp_max_tokens_to_context" => {
            let context = args.get(1).cloned().unwrap_or_default();
            let messages: Vec<Message> = parse(&context["messages"], "arg 1")?;
            value(clamp_max_tokens_to_context(
                &arg::<Model>(args, 0)?,
                &messages,
                arg(args, 2)?,
            ))
        }
        _ => Err(format!("unported: {name}")),
    }
}

fn e2(path: &Path) -> Result<()> {
    let rows = read(path)?;
    for row in rows.as_array().ok_or("fns.json is not an array")? {
        let name = row["rs"].as_str().unwrap_or_default();
        for (index, args) in row["cases"].as_array().ok_or("cases")?.iter().enumerate() {
            let args = args.as_array().cloned().unwrap_or_default();
            let out = match call(name, &args) {
                Ok(value) => json!({"ok": true, "value": value}),
                Err(error) => json!({"ok": false, "kind": "Error", "error": error}),
            };
            println!("{}", json!({"fn": row["fn"], "case": index, "out": out}));
        }
    }
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["e1", dir, ports] => e1(Path::new(dir), Path::new(ports)),
        ["e2", fns] => e2(Path::new(fns)),
        _ => Err("usage: parity_ai e1 <scenarios-dir> <ports.json> | e2 <fns.json>".to_owned()),
    };
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
