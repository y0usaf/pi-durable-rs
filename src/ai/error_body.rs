use crate::ai::json_parse::trim_js;

/// Longest provider error body kept in a message, in UTF-16 code units.
pub const MAX_PROVIDER_ERROR_BODY_CHARS: usize = 4000;

/// Cut text to max_units UTF-16 code units and mark the cut, like pi-ai's truncateErrorText.
pub fn truncate_error_text(text: &str, max_units: usize) -> String {
    let units: Vec<u16> = text.encode_utf16().collect();
    if units.len() <= max_units {
        return text.to_owned();
    }
    format!(
        "{}... [truncated {} chars]",
        String::from_utf16_lossy(&units[..max_units]),
        units.len() - max_units
    )
}

/// Build the display string for a failed provider call. The body is the raw response
/// body (or the SDK's parsed error object, serialized by the caller). It is trimmed,
/// capped and shown with the status unless the message already contains it.
/// Output: "<prefix> (<status>): <text>" or "<status>: <text>", where text is the body
/// or, when the message already has it or no status or body exists, the message.
pub fn format_provider_error(
    prefix: Option<&str>,
    status: Option<u16>,
    message: &str,
    body: Option<&str>,
) -> String {
    let body = body
        .map(trim_js)
        .filter(|body| !body.is_empty())
        .map(|body| truncate_error_text(body, MAX_PROVIDER_ERROR_BODY_CHARS));
    let message_carries_body = body.as_deref().is_none_or(|body| message.contains(body));
    match (status, body) {
        (Some(status), Some(body)) if !message_carries_body => match prefix {
            Some(prefix) => format!("{prefix} ({status}): {body}"),
            None => format!("{status}: {body}"),
        },
        (Some(status), _) => match prefix {
            Some(prefix) => format!("{prefix} ({status}): {message}"),
            None => message.to_owned(),
        },
        (None, _) => message.to_owned(),
    }
}
