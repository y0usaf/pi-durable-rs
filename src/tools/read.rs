use super::image::detect_supported_image_mime_type_of;
use super::path_utils::resolve_read_tool_path;
use super::{Diagnostic, Severity, ToolError, ToolOutput};
use crate::cancel::CancelToken;
use crate::env::{BinaryReader, ExecutionEnv, FileError, FileInfo, RangeDecoder, starts_with_bom};
use crate::truncate::{
    DEFAULT_MAX_BYTES, DEFAULT_MAX_LINES, TextTotals, TruncatedBy, TruncationOptions,
    TruncationResult, format_size, truncate_head_of, utf8_byte_length,
};
use serde::Deserialize;
use serde_json::{Value, json};

const READ_CHUNK: u64 = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ReadArgs {
    pub path: String,
    pub offset: Option<f64>,
    pub limit: Option<f64>,
}

pub fn parameters() -> Value {
    json!({
        "type": "object",
        "required": ["path"],
        "properties": {
            "path": {"type": "string", "description": "Path to the file to read (relative or absolute)"},
            "offset": {"type": "number", "description": "Line number to start reading from (1-indexed)"},
            "limit": {"type": "number", "description": "Maximum number of lines to read"}
        }
    })
}

fn slice_index(value: f64) -> f64 {
    if value.is_nan() { 0.0 } else { value.trunc() }
}

fn safe_integer(value: f64) -> Option<u64> {
    const MAX_SAFE: f64 = 9_007_199_254_740_991.0;
    if value.fract() == 0.0 && value.abs() <= MAX_SAFE {
        Some(value as u64)
    } else {
        None
    }
}

fn character_end(bytes: &[u8], index: usize) -> usize {
    let mut end = index;
    while end > 0 && bytes.get(end).copied().unwrap_or(0) & 0xc0 == 0x80 {
        end -= 1;
    }
    end
}

fn read_head(
    reader: &dyn BinaryReader,
    start: u64,
    end: u64,
    skip_bom: bool,
    cancel: &CancelToken,
) -> Result<String, FileError> {
    let mut decoder = RangeDecoder::new();
    let mut text = String::new();
    let mut newlines = 0;
    let mut position = if skip_bom && start == 0 { 3 } else { start };
    while position < end {
        let bytes = reader.read(position, (end - position).min(READ_CHUNK), cancel)?;
        if bytes.is_empty() {
            break;
        }
        position += bytes.len() as u64;
        let decoded = decoder.decode(&bytes);
        newlines += decoded.matches('\n').count();
        text.push_str(&decoded);
        if newlines >= DEFAULT_MAX_LINES || utf8_byte_length(&text) > DEFAULT_MAX_BYTES + 1 {
            return Ok(text);
        }
    }
    text.push_str(&decoder.finish());
    Ok(text)
}

pub fn execute(
    env: &dyn ExecutionEnv,
    args: &ReadArgs,
    cancel: &CancelToken,
) -> Result<ToolOutput<Option<TruncationResult>>, ToolError> {
    let absolute = resolve_read_tool_path(env, &args.path, cancel)?;
    let mut reader = env.open_binary_reader(&absolute, false, cancel)?;
    let result = read_stable(reader.as_mut(), args, cancel);
    reader.close();
    result
}

fn read_stable(
    reader: &mut dyn BinaryReader,
    args: &ReadArgs,
    cancel: &CancelToken,
) -> Result<ToolOutput<Option<TruncationResult>>, ToolError> {
    let mut attempt = 0;
    loop {
        let before = reader.info(cancel)?;
        let result = read_text(reader, &before, args, cancel)?;
        let after = reader.info(cancel)?;
        if after.size > before.size
            || (after.size == before.size && after.mtime_ms == before.mtime_ms)
        {
            return Ok(result);
        }
        if attempt == 1 {
            return Err(ToolError::new(format!(
                "{} changed while it was read",
                args.path
            )));
        }
        attempt += 1;
    }
}

fn read_text(
    reader: &dyn BinaryReader,
    info: &FileInfo,
    args: &ReadArgs,
    cancel: &CancelToken,
) -> Result<ToolOutput<Option<TruncationResult>>, ToolError> {
    let path = args.path.as_str();
    let mime = detect_supported_image_mime_type_of(info.size, &mut |position, length| {
        reader.read(position, length, cancel)
    })?;
    if let Some(mime) = mime {
        return Ok(ToolOutput {
            text: String::new(),
            diagnostics: vec![Diagnostic {
                severity: Severity::Error,
                code: Some("unsupported_image"),
                message: format!("{path} is an image ({mime}); reading images is not supported"),
            }],
            details: None,
            is_error: true,
        });
    }

    let start_line = match args.offset {
        Some(offset) if offset != 0.0 => f64::max(0.0, offset - 1.0),
        _ => 0.0,
    };
    let start_line_display = start_line + 1.0;
    let slice_start = slice_index(start_line);
    let scan_start = safe_integer(slice_start).unwrap_or(0);
    let requested_end = args
        .limit
        .map(|limit| f64::max(scan_start as f64 + 1.0, slice_index(start_line + limit)));
    let scan_end = requested_end.and_then(safe_integer);
    let mut scan = reader.scan_lines(scan_start, scan_end, cancel)?;
    let total_file_lines = scan.newlines as f64 + 1.0;
    if start_line >= total_file_lines {
        return Err(ToolError::new(format!(
            "Offset {} is beyond end of file ({} lines total)",
            args.offset.unwrap_or(0.0),
            total_file_lines
        )));
    }

    let mut user_limited_lines = None;
    let mut selected_line_count = total_file_lines - slice_start;
    if let Some(limit) = args.limit {
        let end_line = f64::min(start_line + limit, total_file_lines);
        user_limited_lines = Some(end_line - start_line);
        let relative_end = slice_index(end_line);
        let slice_end = if relative_end < 0.0 {
            f64::max(total_file_lines + relative_end, 0.0)
        } else {
            relative_end
        };
        selected_line_count = f64::max(0.0, slice_end - slice_start);
        if selected_line_count > 0.0 && relative_end < 0.0 {
            let end = safe_integer(slice_end).unwrap_or(0);
            scan = reader.scan_lines(scan_start, Some(end), cancel)?;
        }
    }

    let empty = selected_line_count == 0.0;
    let ends_with_newline =
        !empty && scan.last_line_start == scan.end && scan.last_line_start > scan.start;
    let totals = TextTotals {
        lines: if empty || scan.selected_bytes == 0 {
            0
        } else {
            (selected_line_count - if ends_with_newline { 1.0 } else { 0.0 }) as usize
        },
        bytes: if empty {
            0
        } else {
            scan.selected_bytes as usize
        },
    };
    let first_bytes = reader.read(0, 3, cancel)?;
    let head = if empty {
        String::new()
    } else {
        read_head(
            reader,
            scan.start,
            scan.end,
            starts_with_bom(&first_bytes),
            cancel,
        )?
    };

    let truncation = truncate_head_of(&head, totals, TruncationOptions::default());
    let mut diagnostics = Vec::new();
    let mut output = truncation.content.clone();
    let mut details = None;
    if truncation.first_line_exceeds_limit {
        let integral = start_line.fract() == 0.0;
        let first_line = head.split('\n').next().unwrap_or("");
        let line_bytes: &[u8] = if integral { first_line.as_bytes() } else { &[] };
        let line_size = if integral { scan.first_line_bytes } else { 0 };
        let end = character_end(line_bytes, DEFAULT_MAX_BYTES);
        output = String::from_utf8_lossy(&line_bytes[..end.min(line_bytes.len())]).into_owned();
        diagnostics.push(Diagnostic {
            severity: Severity::Warn,
            code: Some("truncated"),
            message: format!(
                "Line {} is {}, exceeds the {} limit; showing its first {}. Use bash: sed -n '{}p' {} | tail -c +{}",
                start_line_display,
                format_size(line_size as usize),
                format_size(DEFAULT_MAX_BYTES),
                format_size(end),
                start_line_display,
                path,
                end + 1
            ),
        });
        details = Some(TruncationResult {
            content: output.clone(),
            output_bytes: end,
            output_lines: 1,
            ..truncation
        });
    } else if truncation.truncated {
        let end_line_display = start_line_display + truncation.output_lines as f64 - 1.0;
        let next_offset = end_line_display + 1.0;
        let limit_text = if truncation.truncated_by == Some(TruncatedBy::Lines) {
            String::new()
        } else {
            format!(" ({} limit)", format_size(DEFAULT_MAX_BYTES))
        };
        diagnostics.push(Diagnostic {
            severity: Severity::Info,
            code: Some("truncated"),
            message: format!(
                "Showing lines {}-{} of {}{}. Use offset={} to continue.",
                start_line_display, end_line_display, total_file_lines, limit_text, next_offset
            ),
        });
        details = Some(truncation);
    } else if let Some(limited) =
        user_limited_lines.filter(|limited| start_line + limited < total_file_lines)
    {
        let remaining = total_file_lines - (start_line + limited);
        let next_offset = start_line + limited + 1.0;
        diagnostics.push(Diagnostic {
            severity: Severity::Info,
            code: None,
            message: format!(
                "{remaining} more lines in file. Use offset={next_offset} to continue."
            ),
        });
    }

    Ok(ToolOutput {
        text: output,
        diagnostics,
        details,
        is_error: false,
    })
}
