use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::ai::http::{Request, send};
use crate::ai::oauth::pkce::Pkce;
use crate::ai::oauth::{Credentials, form_encode, now_ms, query_param};
use crate::cancel::CancelToken;

const TOKEN_TIMEOUT: Duration = Duration::from_secs(30);
const EXPIRY_MARGIN_MS: i64 = 5 * 60 * 1000;

/// Where the Claude OAuth flow talks to. `Default` is the production set; an app can override
/// `callback_host` (from its own config) and tests point the URLs at a local server.
#[derive(Debug, Clone)]
pub struct Endpoints {
    pub client_id: String,
    pub authorize_url: String,
    pub token_url: String,
    pub scopes: String,
    pub copy_code_redirect_uri: String,
    pub callback_host: String,
    pub callback_port: u16,
    pub callback_path: String,
    pub redirect_host: String,
}

impl Default for Endpoints {
    fn default() -> Self {
        Self {
            client_id: "9d1c250a-e61b-44d9-88ed-5944d1962f5e".to_owned(),
            authorize_url: "https://claude.ai/oauth/authorize".to_owned(),
            token_url: "https://platform.claude.com/v1/oauth/token".to_owned(),
            scopes: "org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload".to_owned(),
            copy_code_redirect_uri: "https://platform.claude.com/oauth/code/callback".to_owned(),
            callback_host: "127.0.0.1".to_owned(),
            callback_port: 53692,
            callback_path: "/callback".to_owned(),
            redirect_host: "localhost".to_owned(),
        }
    }
}

impl Endpoints {
    /// The redirect URI for the loopback flow on `port`, as the browser sees it.
    pub fn loopback_redirect_uri(&self, port: u16) -> String {
        format!("http://{}:{port}{}", self.redirect_host, self.callback_path)
    }

    /// The URL the user opens. `redirect_uri` is the loopback URI or `copy_code_redirect_uri`;
    /// the state is the PKCE verifier, as in pi-ai.
    pub fn authorize_link(&self, redirect_uri: &str, pkce: &Pkce) -> String {
        let params = [
            ("code", "true"),
            ("client_id", self.client_id.as_str()),
            ("response_type", "code"),
            ("redirect_uri", redirect_uri),
            ("scope", self.scopes.as_str()),
            ("code_challenge", pkce.challenge.as_str()),
            ("code_challenge_method", "S256"),
            ("state", pkce.verifier.as_str()),
        ];
        let query: Vec<String> = params
            .iter()
            .map(|(key, value)| format!("{}={}", form_encode(key), form_encode(value)))
            .collect();
        format!("{}?{}", self.authorize_url, query.join("&"))
    }
}

/// Fields of a pasted or redirected authorization response.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Parsed {
    pub code: Option<String>,
    pub state: Option<String>,
}

/// Reads `code`/`state` from a full redirect URL, a query string, `code#state`, or a bare code.
pub fn parse_authorization_input(input: &str) -> Parsed {
    let value = input.trim();
    if value.is_empty() {
        return Parsed::default();
    }
    if let Some(query) = absolute_url_query(value) {
        return Parsed {
            code: query_param(query, "code"),
            state: query_param(query, "state"),
        };
    }
    if value.contains('#') {
        let mut parts = value.split('#');
        return Parsed {
            code: parts.next().map(str::to_owned),
            state: parts.next().map(str::to_owned),
        };
    }
    if value.contains("code=") {
        let query = value.strip_prefix('?').unwrap_or(value);
        return Parsed {
            code: query_param(query, "code"),
            state: query_param(query, "state"),
        };
    }
    Parsed {
        code: Some(value.to_owned()),
        state: None,
    }
}

fn absolute_url_query(value: &str) -> Option<&str> {
    let (scheme, rest) = value.split_once(':')?;
    let mut chars = scheme.chars();
    let valid_scheme = chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    if !valid_scheme {
        return None;
    }
    let without_fragment = rest.split('#').next().unwrap_or(rest);
    Some(
        without_fragment
            .split_once('?')
            .map_or("", |(_, query)| query),
    )
}

/// Turns manually pasted input into `(code, state)`. A state that is present and differs from
/// `verifier` is refused, as pi-ai does.
pub fn resolve_manual_input(input: &str, verifier: &str) -> Result<(String, String), String> {
    let parsed = parse_authorization_input(input);
    if parsed
        .state
        .as_deref()
        .is_some_and(|state| !state.is_empty() && state != verifier)
    {
        return Err("OAuth state mismatch".to_owned());
    }
    let code = parsed
        .code
        .filter(|code| !code.is_empty())
        .ok_or_else(|| "Missing authorization code".to_owned())?;
    let state = parsed.state.unwrap_or_else(|| verifier.to_owned());
    Ok((code, state))
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: String,
    expires_in: i64,
}

impl TokenResponse {
    fn into_credentials(self, now: i64) -> Credentials {
        Credentials {
            refresh: self.refresh_token,
            access: self.access_token,
            expires_ms: now + self.expires_in * 1000 - EXPIRY_MARGIN_MS,
        }
    }
}

/// Exchanges an authorization code for tokens. `redirect_uri` must be the one sent to the
/// authorize URL.
pub fn exchange_code(
    endpoints: &Endpoints,
    code: &str,
    state: &str,
    verifier: &str,
    redirect_uri: &str,
    cancel: Option<&CancelToken>,
) -> Result<Credentials, String> {
    let body = json!({
        "grant_type": "authorization_code",
        "client_id": endpoints.client_id,
        "code": code,
        "state": state,
        "redirect_uri": redirect_uri,
        "code_verifier": verifier,
    });
    let url = &endpoints.token_url;
    let text = post_json(url, &body, cancel).map_err(|details| {
        format!("Token exchange request failed. url={url}; redirect_uri={redirect_uri}; response_type=authorization_code; details={details}")
    })?;
    let data: TokenResponse = serde_json::from_str(&text).map_err(|error| {
        format!("Token exchange returned invalid JSON. url={url}; body={text}; details=SyntaxError: {error}")
    })?;
    Ok(data.into_credentials(now_ms()))
}

/// Trades a refresh token for a new access token and refresh token.
pub fn refresh(
    endpoints: &Endpoints,
    refresh_token: &str,
    cancel: Option<&CancelToken>,
) -> Result<Credentials, String> {
    let body = json!({
        "grant_type": "refresh_token",
        "client_id": endpoints.client_id,
        "refresh_token": refresh_token,
    });
    let url = &endpoints.token_url;
    let text = post_json(url, &body, cancel).map_err(|details| {
        format!("Anthropic token refresh request failed. url={url}; details={details}")
    })?;
    let data: TokenResponse = serde_json::from_str(&text).map_err(|error| {
        format!("Anthropic token refresh returned invalid JSON. url={url}; body={text}; details=SyntaxError: {error}")
    })?;
    Ok(data.into_credentials(now_ms()))
}

/// Whether an API key is an OAuth access token (sent as a bearer token, not `x-api-key`).
pub fn is_oauth_token(key: &str) -> bool {
    key.contains("sk-ant-oat")
}

fn post_json(url: &str, body: &Value, cancel: Option<&CancelToken>) -> Result<String, String> {
    let request = Request::new("POST", url)
        .header("Accept", "application/json")
        .json(body)
        .timeout(Some(TOKEN_TIMEOUT));
    let response = send(&request, cancel).map_err(|error| format!("Error: {error}"))?;
    let status = response.status;
    let ok = response.ok();
    let text = response.text().map_err(|error| format!("Error: {error}"))?;
    if !ok {
        return Err(format!(
            "HTTP request failed. status={status}; url={url}; body={text}"
        ));
    }
    Ok(text)
}
