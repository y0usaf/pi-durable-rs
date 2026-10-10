pub fn env_api_key(provider: &str) -> Option<String> {
    let names: &[&str] = match provider {
        "anthropic" => &["ANTHROPIC_OAUTH_TOKEN", "ANTHROPIC_API_KEY"],
        "vercel-ai-gateway" => &["AI_GATEWAY_API_KEY"],
        "openai" => &["OPENAI_API_KEY"],
        _ => &[],
    };
    names
        .iter()
        .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
}
