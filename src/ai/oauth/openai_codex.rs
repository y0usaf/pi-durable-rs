use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::ai::http::{Request, send};
use crate::ai::oauth::anthropic::parse_authorization_input;
use crate::ai::oauth::callback::Listener;
use crate::ai::oauth::pkce::Pkce;
use crate::ai::oauth::{Credentials, form_encode, now_ms};
use crate::cancel::CancelToken;

pub const ORIGINATOR: &str = "pig";
const TOKEN_TIMEOUT: Duration = Duration::from_secs(30);
const JWT_CLAIM: &str = "https://api.openai.com/auth";
const CALLBACK_PATH: &str = "/auth/callback";
const DEVICE_CODE_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const MIN_INTERVAL_MS: u64 = 1000;
const SLOW_DOWN_STEP_MS: u64 = 5000;
const DEVICE_NOT_ENABLED: &str = "OpenAI Codex device code login is not enabled for this server. Use browser login or verify the server URL.";
const DEVICE_CANCELLED: &str = "Login cancelled";
const DEVICE_TIMED_OUT: &str = "Device flow timed out";
const DEVICE_SLOW_DOWN_TIMED_OUT: &str = "Device flow timed out after one or more slow_down responses. This is often caused by clock drift in WSL or VM environments. Please sync or restart the VM clock and try again.";

#[derive(Debug, Clone)]
pub struct Endpoints {
    pub client_id: String,
    pub authorize_url: String,
    pub token_url: String,
    pub redirect_uri: String,
    pub scope: String,
    pub device_user_code_url: String,
    pub device_token_url: String,
    pub device_verification_uri: String,
    pub device_redirect_uri: String,
}

impl Default for Endpoints {
    fn default() -> Self {
        Self {
            client_id: "app_EMoamEEZ73f0CkXaXp7hrann".to_owned(),
            authorize_url: "https://auth.openai.com/oauth/authorize".to_owned(),
            token_url: "https://auth.openai.com/oauth/token".to_owned(),
            redirect_uri: "http://localhost:1455/auth/callback".to_owned(),
            scope: "openid profile email offline_access".to_owned(),
            device_user_code_url: "https://auth.openai.com/api/accounts/deviceauth/usercode"
                .to_owned(),
            device_token_url: "https://auth.openai.com/api/accounts/deviceauth/token".to_owned(),
            device_verification_uri: "https://auth.openai.com/codex/device".to_owned(),
            device_redirect_uri: "https://auth.openai.com/deviceauth/callback".to_owned(),
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
    if account_id_from_token(&data.access_token).is_none() {
        return Err("Failed to extract accountId from token".to_owned());
    }
    Ok(Credentials {
        refresh: data.refresh_token,
        access: data.access_token,
        expires_ms: now_ms() + (data.expires_in * 1000.0) as i64,
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

pub fn code_from_manual_input(input: &str, state: &str) -> Result<String, String> {
    let parsed = parse_authorization_input(input);
    if parsed
        .state
        .as_deref()
        .is_some_and(|value| !value.is_empty() && value != state)
    {
        return Err("State mismatch".to_owned());
    }
    parsed
        .code
        .filter(|code| !code.is_empty())
        .ok_or_else(|| "Missing authorization code".to_owned())
}

pub fn login_browser(
    endpoints: &Endpoints,
    pkce: &Pkce,
    listener: Listener,
    state: &str,
    cancel: &CancelToken,
) -> Result<Credentials, String> {
    let code = listener.wait_code("OpenAI", CALLBACK_PATH, state, cancel)?;
    exchange_code(endpoints, &code, &pkce.verifier, Some(cancel))
}

#[derive(Deserialize)]
struct UserCodeResponse {
    device_auth_id: String,
    user_code: String,
    interval: Value,
}

#[derive(Deserialize)]
struct DeviceAuthorization {
    authorization_code: String,
    code_verifier: String,
}

pub struct DeviceCode {
    pub device_auth_id: String,
    pub user_code: String,
    pub verification_uri: String,
    pub interval_seconds: f64,
}

pub enum DevicePoll<T> {
    Pending,
    SlowDown,
    Failed(String),
    Complete(T),
}

fn body_suffix(text: &str) -> String {
    if text.is_empty() {
        String::new()
    } else {
        format!(": {text}")
    }
}

fn post_json_status(
    url: &str,
    body: &Value,
    cancel: Option<&CancelToken>,
) -> Result<(u16, String), String> {
    let request = Request::new("POST", url)
        .json(body)
        .timeout(Some(TOKEN_TIMEOUT));
    let response = send(&request, cancel).map_err(|error| format!("Error: {error}"))?;
    let status = response.status;
    let text = response.text().map_err(|error| format!("Error: {error}"))?;
    Ok((status, text))
}

fn parse_interval(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                Some(0.0)
            } else {
                trimmed
                    .parse::<f64>()
                    .ok()
                    .filter(|seconds| seconds.is_finite())
            }
        }
        _ => None,
    }
}

fn error_code(text: &str) -> String {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return String::new();
    };
    match &value["error"] {
        Value::String(code) => code.clone(),
        Value::Object(object) => object
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        _ => String::new(),
    }
}

pub fn start_device_login(
    endpoints: &Endpoints,
    cancel: Option<&CancelToken>,
) -> Result<DeviceCode, String> {
    let body = json!({ "client_id": endpoints.client_id });
    let (status, text) = post_json_status(&endpoints.device_user_code_url, &body, cancel)?;
    if status == 404 {
        return Err(DEVICE_NOT_ENABLED.to_owned());
    }
    if status != 200 {
        return Err(format!(
            "OpenAI Codex device code request failed with status {status}{}",
            body_suffix(&text)
        ));
    }
    let invalid = || format!("Invalid OpenAI Codex device code response: {text}");
    let data: UserCodeResponse = serde_json::from_str(&text).map_err(|_| invalid())?;
    let interval_seconds = parse_interval(&data.interval)
        .filter(|seconds| *seconds >= 0.0)
        .filter(|_| !data.device_auth_id.is_empty() && !data.user_code.is_empty())
        .ok_or_else(invalid)?;
    Ok(DeviceCode {
        device_auth_id: data.device_auth_id,
        user_code: data.user_code,
        verification_uri: endpoints.device_verification_uri.clone(),
        interval_seconds,
    })
}

fn poll_device(
    endpoints: &Endpoints,
    device: &DeviceCode,
    cancel: &CancelToken,
) -> Result<DevicePoll<DeviceAuthorization>, String> {
    let body = json!({
        "device_auth_id": device.device_auth_id,
        "user_code": device.user_code,
    });
    let (status, text) = post_json_status(&endpoints.device_token_url, &body, Some(cancel))?;
    if status == 200 {
        return Ok(match serde_json::from_str::<DeviceAuthorization>(&text) {
            Ok(auth) if !auth.authorization_code.is_empty() && !auth.code_verifier.is_empty() => {
                DevicePoll::Complete(auth)
            }
            _ => DevicePoll::Failed(format!(
                "Invalid OpenAI Codex device auth token response: {text}"
            )),
        });
    }
    if status == 403 || status == 404 {
        return Ok(DevicePoll::Pending);
    }
    Ok(match error_code(&text).as_str() {
        "deviceauth_authorization_pending" => DevicePoll::Pending,
        "slow_down" => DevicePoll::SlowDown,
        _ => DevicePoll::Failed(format!(
            "OpenAI Codex device auth failed with status {status}{}",
            body_suffix(&text)
        )),
    })
}

fn poll_flow<T>(
    interval_seconds: f64,
    cancel: &CancelToken,
    mut poll: impl FnMut() -> Result<DevicePoll<T>, String>,
) -> Result<T, String> {
    let deadline = Instant::now() + DEVICE_CODE_TIMEOUT;
    let mut interval_ms = ((interval_seconds * 1000.0).floor() as u64).max(MIN_INTERVAL_MS);
    let mut slow_downs = 0;
    while Instant::now() < deadline {
        if cancel.is_cancelled() {
            return Err(DEVICE_CANCELLED.to_owned());
        }
        match poll()? {
            DevicePoll::Complete(value) => return Ok(value),
            DevicePoll::Failed(message) => return Err(message),
            DevicePoll::SlowDown => {
                slow_downs += 1;
                interval_ms += SLOW_DOWN_STEP_MS;
            }
            DevicePoll::Pending => {}
        }
        let remaining_ms = deadline
            .saturating_duration_since(Instant::now())
            .as_millis() as u64;
        if remaining_ms == 0 {
            break;
        }
        if cancel.wait_timeout(Duration::from_millis(interval_ms.min(remaining_ms))) {
            return Err(DEVICE_CANCELLED.to_owned());
        }
    }
    Err(if slow_downs > 0 {
        DEVICE_SLOW_DOWN_TIMED_OUT
    } else {
        DEVICE_TIMED_OUT
    }
    .to_owned())
}

pub fn login_device(
    endpoints: &Endpoints,
    cancel: &CancelToken,
    on_code: impl FnOnce(&DeviceCode),
) -> Result<Credentials, String> {
    let device = start_device_login(endpoints, Some(cancel))?;
    on_code(&device);
    let authorization = poll_flow(device.interval_seconds, cancel, || {
        poll_device(endpoints, &device, cancel)
    })?;
    let device_endpoints = Endpoints {
        redirect_uri: endpoints.device_redirect_uri.clone(),
        ..endpoints.clone()
    };
    exchange_code(
        &device_endpoints,
        &authorization.authorization_code,
        &authorization.code_verifier,
        Some(cancel),
    )
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
