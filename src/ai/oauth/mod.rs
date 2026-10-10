pub mod anthropic;
pub mod callback;
pub mod pkce;

use serde::{Deserialize, Serialize};

/// OAuth credentials as the app stores them. `expires_ms` is when the access token
/// should be refreshed (already five minutes before the server's expiry).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credentials {
    pub refresh: String,
    pub access: String,
    pub expires_ms: i64,
}

/// Milliseconds since the Unix epoch.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after 1970")
        .as_millis() as i64
}

/// Encodes like `URLSearchParams`: unreserved bytes stay, space becomes `+`, the rest is `%XX`.
pub fn form_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'*' | b'-' | b'.' | b'_' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Decodes one `application/x-www-form-urlencoded` component. Malformed escapes stay literal.
pub fn form_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'+' {
            out.push(b' ');
        } else if byte == b'%' && index + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or("");
            match u8::from_str_radix(hex, 16) {
                Ok(decoded) => {
                    out.push(decoded);
                    index += 2;
                }
                Err(_) => out.push(byte),
            }
        } else {
            out.push(byte);
        }
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// First value of `name` in a query string (`a=1&b=2`), decoded, like `URLSearchParams.get`.
pub fn query_param(query: &str, name: &str) -> Option<String> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .find_map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (form_decode(key) == name).then(|| form_decode(value))
        })
}
