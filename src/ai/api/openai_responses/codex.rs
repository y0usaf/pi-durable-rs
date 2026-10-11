use std::collections::BTreeMap;

use crate::ai::oauth::openai_codex::{ORIGINATOR, account_id_from_token};

const DEFAULT_BASE_URL: &str = "https://chatgpt.com/backend-api";
const ENDPOINT_SUFFIX: &str = "/codex/responses";
const ACCOUNT_HEADER: &str = "chatgpt-account-id";
const RAW_BLOCK_MAX: usize = 128 * 1024;
pub const DEFAULT_INSTRUCTIONS: &str = "You are a helpful assistant.";
pub const HEADERS: [(&str, &str); 3] = [
    ("OpenAI-Beta", "responses=experimental"),
    ("originator", ORIGINATOR),
    ("accept", "text/event-stream"),
];

pub fn endpoint(base_url: &str) -> String {
    let trimmed = base_url.trim();
    let base = if trimmed.is_empty() {
        DEFAULT_BASE_URL
    } else {
        trimmed
    }
    .trim_end_matches('/');
    if base.ends_with(ENDPOINT_SUFFIX) {
        base.to_owned()
    } else if base.ends_with("/codex") {
        format!("{base}/responses")
    } else {
        format!("{base}{ENDPOINT_SUFFIX}")
    }
}

pub fn account_id(
    explicit: Option<&BTreeMap<String, String>>,
    api_key: Option<&str>,
) -> Result<String, String> {
    explicit
        .and_then(|headers| {
            headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(ACCOUNT_HEADER))
                .map(|(_, value)| value)
        })
        .filter(|id| !id.is_empty())
        .cloned()
        .or_else(|| api_key.and_then(account_id_from_token))
        .ok_or_else(|| "openai-codex-responses: failed to extract accountId from token".to_owned())
}

pub fn zstd_raw_frame(data: &[u8]) -> Vec<u8> {
    let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd];
    let size = data.len() as u64;
    if size < 1 << 8 {
        frame.extend([0x20, size as u8]);
    } else if size < (1 << 16) + (1 << 8) {
        frame.push(0x60);
        frame.extend(((size - (1 << 8)) as u16).to_le_bytes());
    } else if size <= u64::from(u32::MAX) {
        frame.push(0xa0);
        frame.extend((size as u32).to_le_bytes());
    } else {
        frame.push(0xe0);
        frame.extend(size.to_le_bytes());
    }
    let mut offset = 0;
    loop {
        let block = RAW_BLOCK_MAX.min(data.len() - offset);
        let last = offset + block == data.len();
        let header = ((block as u32) << 3) | u32::from(last);
        frame.extend(&header.to_le_bytes()[..3]);
        frame.extend(&data[offset..offset + block]);
        offset += block;
        if last {
            break;
        }
    }
    frame
}
