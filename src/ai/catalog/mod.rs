//! Built-in chat model catalogs, generated from PiG's `models_generated.go` by
//! `scripts/gen-catalog.py`. Each function deserializes its JSON and panics on
//! a malformed entry, naming the model id, because the data is fixed at build time.

use crate::ai::types::Model;

#[cfg(any(feature = "anthropic", feature = "openai"))]
fn parse(file: &str, json: &str) -> Vec<Model> {
    let values: Vec<serde_json::Value> = serde_json::from_str(json)
        .unwrap_or_else(|error| panic!("catalog {file}: not a JSON array: {error}"));
    values
        .into_iter()
        .map(|value| {
            let id = value
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<no id>")
                .to_owned();
            serde_json::from_value(value)
                .unwrap_or_else(|error| panic!("catalog {file}, model {id}: {error}"))
        })
        .collect()
}

#[cfg(feature = "anthropic")]
pub fn anthropic() -> Vec<Model> {
    parse("anthropic.json", include_str!("anthropic.json"))
}

#[cfg(feature = "vercel-gateway")]
pub fn vercel_gateway() -> Vec<Model> {
    parse(
        "vercel-ai-gateway.json",
        include_str!("vercel-ai-gateway.json"),
    )
}

#[cfg(feature = "openai")]
pub fn openai() -> Vec<Model> {
    parse("openai.json", include_str!("openai.json"))
}

#[cfg(feature = "openai")]
pub fn openai_codex() -> Vec<Model> {
    parse("openai-codex.json", include_str!("openai-codex.json"))
}
