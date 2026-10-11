use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use ring::rand::SystemRandom;
use ring::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use serde_json::{Value, json};

use crate::ai::http::{Request, send};

const SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const DEFAULT_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
const GRANT_TYPE: &str = "urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer";
const LIFETIME_SECS: u64 = 3600;

pub fn access_token() -> Result<String, String> {
    let path = env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .ok()
        .filter(|path| !path.trim().is_empty())
        .ok_or_else(|| "Vertex ADC: set GOOGLE_APPLICATION_CREDENTIALS".to_owned())?;
    let text = std::fs::read_to_string(&path).map_err(|error| format!("Vertex ADC: {error}"))?;
    let file: Value =
        serde_json::from_str(&text).map_err(|error| format!("parse credentials file: {error}"))?;
    match file["type"].as_str().unwrap_or("service_account") {
        "service_account" => service_account_token(&file),
        other => Err(format!("unsupported credentials type {other:?}")),
    }
}

fn service_account_token(file: &Value) -> Result<String, String> {
    let email = file["client_email"]
        .as_str()
        .ok_or("Vertex ADC: credentials file has no client_email")?;
    let key = file["private_key"]
        .as_str()
        .ok_or("Vertex ADC: credentials file has no private_key")?;
    let token_uri = file["token_uri"].as_str().unwrap_or(DEFAULT_TOKEN_URI);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs();
    let header = b64url(json!({"alg": "RS256", "typ": "JWT"}).to_string().as_bytes());
    let claims = b64url(
        json!({
            "iss": email,
            "scope": SCOPE,
            "aud": token_uri,
            "iat": now,
            "exp": now + LIFETIME_SECS,
        })
        .to_string()
        .as_bytes(),
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
    let assertion = format!("{input}.{}", b64url(&signature));
    let mut request =
        Request::new("POST", token_uri).header("content-type", "application/x-www-form-urlencoded");
    request.body = format!("grant_type={GRANT_TYPE}&assertion={assertion}").into_bytes();
    let response = send(&request, None).map_err(|error| format!("Vertex ADC token: {error}"))?;
    let status = response.status;
    let text = response
        .text()
        .map_err(|error| format!("Vertex ADC token: {error}"))?;
    if !(200..300).contains(&status) {
        return Err(format!("Vertex ADC token: HTTP {status}: {text}"));
    }
    let body: Value =
        serde_json::from_str(&text).map_err(|error| format!("Vertex ADC token: {error}"))?;
    body["access_token"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| "Vertex ADC token: response has no access_token".to_owned())
}

fn pem_der(pem: &str) -> Result<Vec<u8>, String> {
    let body: String = pem
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect();
    b64_decode(&body)
}

fn b64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let mut padded = [0u8; 3];
        padded[..chunk.len()].copy_from_slice(chunk);
        let n = (u32::from(padded[0]) << 16) | (u32::from(padded[1]) << 8) | u32::from(padded[2]);
        for shift in [18, 12, 6, 0].into_iter().take(chunk.len() + 1) {
            out.push(ALPHABET[((n >> shift) & 63) as usize] as char);
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
