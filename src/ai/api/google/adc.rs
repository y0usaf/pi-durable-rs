use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ring::rand::SystemRandom;
use ring::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use serde_json::{Value, json};

use crate::ai::http::{Request, send};

const SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const DEFAULT_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
const JWT_BEARER: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";
const TOKEN_EXCHANGE: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
const ACCESS_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:access_token";
const LIFETIME_SECS: u64 = 3600;
const EXPIRY_SLACK: Duration = Duration::from_secs(10);
const GCLOUD_ADC: &str = ".config/gcloud/application_default_credentials.json";
const FORM_TYPE: (&str, &str) = ("content-type", "application/x-www-form-urlencoded");
const JSON_TYPE: (&str, &str) = ("content-type", "application/json");
const STANDARD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const URL_SAFE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

struct Token {
    value: String,
    lifetime: Duration,
}

static CACHE: LazyLock<Mutex<HashMap<PathBuf, (String, Instant)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn access_token() -> Result<String, String> {
    let path = credentials_path()?;
    if let Some(token) = cached(&path) {
        return Ok(token);
    }
    let text = std::fs::read_to_string(&path).map_err(|error| format!("Vertex ADC: {error}"))?;
    let file: Value =
        serde_json::from_str(&text).map_err(|error| format!("parse credentials file: {error}"))?;
    let token = fetch(&file)?;
    remember(path, &token);
    Ok(token.value)
}

fn credentials_path() -> Result<PathBuf, String> {
    if let Some(path) = env_value("GOOGLE_APPLICATION_CREDENTIALS") {
        return Ok(PathBuf::from(path));
    }
    let home = env_value("HOME").ok_or("Vertex ADC: set GOOGLE_APPLICATION_CREDENTIALS or HOME")?;
    let path = Path::new(&home).join(GCLOUD_ADC);
    if path.is_file() {
        Ok(path)
    } else {
        Err("Vertex ADC: set GOOGLE_APPLICATION_CREDENTIALS or run gcloud auth application-default login".to_owned())
    }
}

fn env_value(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn cached(path: &Path) -> Option<String> {
    let cache = CACHE.lock().ok()?;
    let (token, expires) = cache.get(path)?;
    (Instant::now() < *expires).then(|| token.clone())
}

fn remember(path: PathBuf, token: &Token) {
    let Some(lifetime) = token.lifetime.checked_sub(EXPIRY_SLACK) else {
        return;
    };
    if let Ok(mut cache) = CACHE.lock() {
        cache.insert(path, (token.value.clone(), Instant::now() + lifetime));
    }
}

fn fetch(file: &Value) -> Result<Token, String> {
    let kind = match &file["type"] {
        Value::Null => "service_account",
        Value::String(kind) if kind.is_empty() => "service_account",
        Value::String(kind) => kind.as_str(),
        other => return Err(format!("parse credentials file: type is {other}")),
    };
    match kind {
        "service_account" => service_account(file),
        "authorized_user" => authorized_user(file),
        "external_account" => external_account(file),
        "external_account_authorized_user" => external_account_authorized_user(file),
        "impersonated_service_account" => impersonated_service_account(file),
        other => Err(format!(
            "Vertex ADC: unsupported credentials type {other:?}"
        )),
    }
}

fn service_account(file: &Value) -> Result<Token, String> {
    let email = field(file, "client_email")?;
    let key = field(file, "private_key")?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs();
    let header = b64_encode(
        json!({"alg": "RS256", "typ": "JWT"}).to_string().as_bytes(),
        URL_SAFE,
        false,
    );
    let claims = b64_encode(
        json!({
            "iss": email,
            "scope": SCOPE,
            "aud": token_uri(file),
            "iat": now,
            "exp": now + LIFETIME_SECS,
        })
        .to_string()
        .as_bytes(),
        URL_SAFE,
        false,
    );
    let input = format!("{header}.{claims}");
    let pair = RsaKeyPair::from_pkcs8(&pem_der(key)?)
        .map_err(|error| format!("Vertex ADC: private key: {error}"))?;
    let mut signature = vec![0; pair.public().modulus_len()];
    pair.sign(
        &RSA_PKCS1_SHA256,
        &SystemRandom::new(),
        input.as_bytes(),
        &mut signature,
    )
    .map_err(|error| format!("Vertex ADC: sign: {error}"))?;
    let assertion = format!("{input}.{}", b64_encode(&signature, URL_SAFE, false));
    let body = form(&[
        ("grant_type", JWT_BEARER),
        ("assertion", assertion.as_str()),
    ]);
    token(&post(token_uri(file), &[FORM_TYPE], body)?, "access_token")
}

fn authorized_user(file: &Value) -> Result<Token, String> {
    let body = form(&[
        ("grant_type", "refresh_token"),
        ("client_id", field(file, "client_id")?),
        ("client_secret", field(file, "client_secret")?),
        ("refresh_token", field(file, "refresh_token")?),
    ]);
    token(&post(token_uri(file), &[FORM_TYPE], body)?, "access_token")
}

fn external_account_authorized_user(file: &Value) -> Result<Token, String> {
    let id = field(file, "client_id")?;
    let secret = field(file, "client_secret")?;
    let basic = format!(
        "Basic {}",
        b64_encode(format!("{id}:{secret}").as_bytes(), STANDARD, true)
    );
    let body = form(&[
        ("grant_type", "refresh_token"),
        ("refresh_token", field(file, "refresh_token")?),
    ]);
    token(
        &post(
            field(file, "token_url")?,
            &[FORM_TYPE, ("authorization", basic.as_str())],
            body,
        )?,
        "access_token",
    )
}

fn external_account(file: &Value) -> Result<Token, String> {
    let subject = subject_token(&file["credential_source"])?;
    let body = form(&[
        ("grant_type", TOKEN_EXCHANGE),
        ("audience", field(file, "audience")?),
        ("requested_token_type", ACCESS_TOKEN_TYPE),
        ("subject_token_type", field(file, "subject_token_type")?),
        ("subject_token", subject.as_str()),
        ("scope", SCOPE),
    ]);
    let sts = token(
        &post(field(file, "token_url")?, &[FORM_TYPE], body)?,
        "access_token",
    )?;
    match file["service_account_impersonation_url"].as_str() {
        Some(url) => impersonate(url, &sts.value),
        None => Ok(sts),
    }
}

fn impersonated_service_account(file: &Value) -> Result<Token, String> {
    if !file["source_credentials"].is_object() {
        return Err(
            "Vertex ADC: impersonated_service_account has no source_credentials".to_owned(),
        );
    }
    let source = fetch(&file["source_credentials"])?;
    impersonate(
        field(file, "service_account_impersonation_url")?,
        &source.value,
    )
}

fn impersonate(url: &str, source: &str) -> Result<Token, String> {
    let bearer = format!("Bearer {source}");
    let body = json!({"lifetime": format!("{LIFETIME_SECS}s"), "scope": [SCOPE]}).to_string();
    token(
        &post(url, &[JSON_TYPE, ("authorization", bearer.as_str())], body)?,
        "accessToken",
    )
}

fn subject_token(source: &Value) -> Result<String, String> {
    let raw = if let Some(path) = source["file"].as_str() {
        std::fs::read_to_string(path)
            .map_err(|error| format!("Vertex ADC subject token: {error}"))?
    } else if let Some(url) = source["url"].as_str() {
        fetch_subject_url(url, &source["headers"])?
    } else {
        return Err(
            "Vertex ADC: credential_source needs file or url; executable sources are unsupported"
                .to_owned(),
        );
    };
    let format = &source["format"];
    match format["type"].as_str().unwrap_or("text") {
        "text" => Ok(raw.trim().to_owned()),
        "json" => {
            let name = field(format, "subject_token_field_name")?;
            let value: Value = serde_json::from_str(&raw)
                .map_err(|error| format!("Vertex ADC subject token: {error}"))?;
            value[name]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("Vertex ADC subject token: no {name} field"))
        }
        other => Err(format!(
            "Vertex ADC: unsupported subject token format {other:?}"
        )),
    }
}

fn fetch_subject_url(url: &str, headers: &Value) -> Result<String, String> {
    let mut request = Request::new("GET", url);
    for (name, value) in headers.as_object().into_iter().flatten() {
        if let Some(value) = value.as_str() {
            request = request.header(name, value);
        }
    }
    let response =
        send(&request, None).map_err(|error| format!("Vertex ADC subject token: {error}"))?;
    let status = response.status;
    let text = response
        .text()
        .map_err(|error| format!("Vertex ADC subject token: {error}"))?;
    if (200..300).contains(&status) {
        Ok(text)
    } else {
        Err(format!("Vertex ADC subject token: HTTP {status}: {text}"))
    }
}

fn post(url: &str, headers: &[(&str, &str)], body: String) -> Result<Value, String> {
    let mut request = Request::new("POST", url);
    for (name, value) in headers {
        request = request.header(name, *value);
    }
    request.body = body.into_bytes();
    let response = send(&request, None).map_err(|error| format!("Vertex ADC token: {error}"))?;
    let status = response.status;
    let text = response
        .text()
        .map_err(|error| format!("Vertex ADC token: {error}"))?;
    if !(200..300).contains(&status) {
        return Err(format!("Vertex ADC token: HTTP {status}: {text}"));
    }
    serde_json::from_str(&text).map_err(|error| format!("Vertex ADC token: {error}"))
}

fn token(body: &Value, name: &str) -> Result<Token, String> {
    let value = body[name]
        .as_str()
        .ok_or_else(|| format!("Vertex ADC token: response has no {name}"))?;
    let lifetime = Duration::from_secs(body["expires_in"].as_u64().unwrap_or(LIFETIME_SECS));
    Ok(Token {
        value: value.to_owned(),
        lifetime,
    })
}

fn field<'a>(file: &'a Value, name: &str) -> Result<&'a str, String> {
    file[name]
        .as_str()
        .ok_or_else(|| format!("Vertex ADC: credentials file has no {name}"))
}

fn token_uri(file: &Value) -> &str {
    file["token_uri"].as_str().unwrap_or(DEFAULT_TOKEN_URI)
}

fn form(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(name, value)| format!("{}={}", percent(name), percent(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn percent(text: &str) -> String {
    text.bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                char::from(byte).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn pem_der(pem: &str) -> Result<Vec<u8>, String> {
    let body: String = pem
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect();
    b64_decode(&body)
}

fn b64_encode(bytes: &[u8], alphabet: &[u8; 64], pad: bool) -> String {
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let mut padded = [0u8; 3];
        padded[..chunk.len()].copy_from_slice(chunk);
        let n = (u32::from(padded[0]) << 16) | (u32::from(padded[1]) << 8) | u32::from(padded[2]);
        for (index, shift) in [18, 12, 6, 0].into_iter().enumerate() {
            if index <= chunk.len() {
                out.push(char::from(alphabet[((n >> shift) & 63) as usize]));
            } else if pad {
                out.push('=');
            }
        }
    }
    out
}

fn b64_decode(text: &str) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut acc: u32 = 0;
    let mut bits = 0;
    for byte in text
        .bytes()
        .filter(|byte| *byte != b'=' && !byte.is_ascii_whitespace())
    {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return Err("Vertex ADC: private key is not base64".to_owned()),
        };
        acc = (acc << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Ok(out)
}
