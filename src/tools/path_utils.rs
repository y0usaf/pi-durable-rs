use crate::cancel::CancelToken;
use crate::env::{ExecutionEnv, FileError};
use unicode_normalization::UnicodeNormalization;

const NARROW_NO_BREAK_SPACE: char = '\u{202f}';

fn is_unicode_space(c: char) -> bool {
    matches!(
        c,
        '\u{a0}' | '\u{2000}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}'
    )
}

fn normalize_tool_path(path: &str) -> String {
    let spaced: String = path
        .chars()
        .map(|c| if is_unicode_space(c) { ' ' } else { c })
        .collect();
    match spaced.strip_prefix('@') {
        Some(rest) => rest.to_owned(),
        None => spaced,
    }
}

pub fn resolve_tool_path(
    env: &dyn ExecutionEnv,
    path: &str,
    cancel: &CancelToken,
) -> Result<String, FileError> {
    env.absolute_path(&normalize_tool_path(path), cancel)
}

fn narrow_before_meridiem(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < chars.len() {
        let is_match = chars[i] == ' '
            && i + 3 < chars.len()
            && chars[i + 3] == '.'
            && matches!(
                [chars[i + 1], chars[i + 2]]
                    .iter()
                    .collect::<String>()
                    .to_ascii_lowercase()
                    .as_str(),
                "am" | "pm"
            );
        if is_match {
            out.push(NARROW_NO_BREAK_SPACE);
            out.push(chars[i + 1]);
            out.push(chars[i + 2]);
            out.push('.');
            i += 4;
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

pub fn resolve_read_tool_path(
    env: &dyn ExecutionEnv,
    path: &str,
    cancel: &CancelToken,
) -> Result<String, FileError> {
    let resolved = resolve_tool_path(env, path, cancel)?;
    let decomposed: String = resolved.nfd().collect();
    let variants = [
        resolved.clone(),
        narrow_before_meridiem(&resolved),
        decomposed.clone(),
        resolved.replace('\'', "\u{2019}"),
        decomposed.replace('\'', "\u{2019}"),
    ];
    let mut seen: Vec<&str> = Vec::new();
    for variant in &variants {
        if seen.contains(&variant.as_str()) {
            continue;
        }
        seen.push(variant);
        if env.exists(variant, cancel)? {
            return Ok(variant.clone());
        }
    }
    Ok(resolved)
}
