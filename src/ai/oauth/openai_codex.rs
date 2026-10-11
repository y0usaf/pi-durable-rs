use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;

use crate::ai::http::{Request, send};
use crate::ai::oauth::pkce::Pkce;
use crate::ai::oauth::{Credentials, form_encode, now_ms};
use crate::cancel::CancelToken;

pub const ORIGINATOR: &str = "pig";
const TOKEN_TIMEOUT: Duration = Duration::from_secs(30);
const EXPIRY_MARGIN_MS: i64 = 5 * 60 * 1000;
const JWT_CLAIM: &str = "https://api.openai.com/auth";

#[derive(Debug, Clone)]
pub struct Endpoints {
    pub client_id: String,
    pub authorize_url: String,
    pub token_url: String,
    pub redirect_uri: String,
    pub scope: String,
}

impl Default for Endpoints {
    fn default() -> Self {
        Self {
            client_id: "app_EMoamEEZ73f0CkXaXp7hrann".to_owned(),
            authorize_url: "https://auth.openai.com/oauth/authorize".to_owned(),
            token_url: "https://auth.openai.com/oauth/token".to_owned(),
            redirect_uri: "http://localhost:1455/auth/callback".to_owned(),
            scope: "openid profile email offline_access".to_owned(),
        }
    }
}

impl Endpoints {
    pub fn authorize_link(&self, pkce: &Pkce, state: &str) -> String {
        let params = [
            ("response_type", "code"),
            ("client_id", self.client_id.as_str()),
            ("redirect_uri", self.redirect_uri.as_str()),
            ("scope", self.scope.as_str()),
            ("code_challenge", pkce.challenge.as_str()),
            ("code_challenge_method", "S256"),
            ("state", state),
            ("id_token_add_organizations", "true"),
            ("codex_cli_simplified_flow", "true"),
            ("originator", ORIGINATOR),
        ];
        let query: Vec<String> = params
            .iter()
            .map(|(key, value)| format!("{}={}", form_encode(key), form_encode(value)))
            .collect();
        format!("{}?{}", self.authorize_url, query.join("&"))
    }
}

pub fn new_state() -> Result<String, String> {
    use ring::rand::{SecureRandom, SystemRandom};
    let mut bytes = [0u8; 16];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| "could not read random bytes for the OAuth state".to_owned())?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: String,
    expires_in: f64,
}

fn credentials(text: &str, operation: &str) -> Result<Credentials, String> {
    let data: TokenResponse = serde_json::from_str(text).map_err(|error| {
        format!("OpenAI Codex token {operation} returned invalid JSON: {error}; body={text}")
    })?;
    if data.access_token.is_empty() || data.refresh_token.is_empty() {
        return Err(format!(
            "OpenAI Codex token {operation} response missing fields"
        ));
    }
    Ok(Credentials {
        refresh: data.refresh_token,
        access: data.access_token,
        expires_ms: now_ms() + (data.expires_in * 1000.0) as i64 - EXPIRY_MARGIN_MS,
    })
}

fn post_form(
    url: &str,
    pairs: &[(&str, &str)],
    operation: &str,
    cancel: Option<&CancelToken>,
) -> Result<String, String> {
    let body: Vec<String> = pairs
        .iter()
        .map(|(key, value)| format!("{}={}", form_encode(key), form_encode(value)))
        .collect();
    let mut request = Request::new("POST", url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Accept", "application/json")
        .timeout(Some(TOKEN_TIMEOUT));
    request.body = body.join("&").into_bytes();
    let response = send(&request, cancel).map_err(|error| format!("Error: {error}"))?;
    let status = response.status;
    let ok = response.ok();
    let text = response.text().map_err(|error| format!("Error: {error}"))?;
    if !ok {
        return Err(format!(
            "OpenAI Codex token {operation} failed ({status}): {text}"
        ));
    }
    Ok(text)
}

pub fn exchange_code(
    endpoints: &Endpoints,
    code: &str,
    verifier: &str,
    cancel: Option<&CancelToken>,
) -> Result<Credentials, String> {
    let text = post_form(
        &endpoints.token_url,
        &[
            ("grant_type", "authorization_code"),
            ("client_id", endpoints.client_id.as_str()),
            ("code", code),
            ("code_verifier", verifier),
            ("redirect_uri", endpoints.redirect_uri.as_str()),
        ],
        "exchange",
        cancel,
    )?;
    credentials(&text, "exchange")
}

pub fn refresh(
    endpoints: &Endpoints,
    refresh_token: &str,
    cancel: Option<&CancelToken>,
) -> Result<Credentials, String> {
    let text = post_form(
        &endpoints.token_url,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", endpoints.client_id.as_str()),
        ],
        "refresh",
        cancel,
    )?;
    credentials(&text, "refresh")
}

pub fn account_id_from_token(access: &str) -> Option<String> {
    let payload = access
        .split('.')
        .nth(1)
        .filter(|_| access.matches('.').count() == 2)?;
    let claims: Value = serde_json::from_slice(&base64url_decode(payload)?).ok()?;
    claims[JWT_CLAIM]["chatgpt_account_id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

fn base64url_decode(input: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut buffer: u32 = 0;
    let mut bits = 0;
    for byte in input.bytes() {
        if byte == b'=' {
            break;
        }
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' | b'+' => 62,
            b'_' | b'/' => 63,
            _ => return None,
        };
        buffer = (buffer << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
            buffer &= (1 << bits) - 1;
        }
    }
    Some(out)
}
