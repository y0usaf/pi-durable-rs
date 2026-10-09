pub const DEFAULT_MAX_LINES: usize = 2000;
pub const DEFAULT_MAX_BYTES: usize = 50 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruncatedBy {
    Lines,
    Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncationResult {
    pub content: String,
    pub truncated: bool,
    pub truncated_by: Option<TruncatedBy>,
    pub total_lines: usize,
    pub total_bytes: usize,
    pub output_lines: usize,
    pub output_bytes: usize,
    pub last_line_partial: bool,
    pub first_line_exceeds_limit: bool,
    pub max_lines: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TruncationOptions {
    pub max_lines: Option<usize>,
    pub max_bytes: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextTotals {
    pub lines: usize,
    pub bytes: usize,
}

pub fn utf8_byte_length(content: &str) -> usize {
    content.len()
}

fn split_lines_for_counting(content: &str) -> Vec<&str> {
    if content.is_empty() {
        return Vec::new();
    }
    let mut lines: Vec<&str> = content.split('\n').collect();
    if content.ends_with('\n') {
        lines.pop();
    }
    lines
}

pub fn count_lines(content: &str) -> usize {
    split_lines_for_counting(content).len()
}

pub fn format_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{}KB", to_fixed_one(bytes as f64 / 1024.0))
    } else {
        format!("{}MB", to_fixed_one(bytes as f64 / (1024.0 * 1024.0)))
    }
}

fn to_fixed_one(value: f64) -> String {
    let tenths = (value * 10.0 + 0.5).floor() as u64;
    format!("{}.{}", tenths / 10, tenths % 10)
}

pub fn truncate_head(content: &str, options: TruncationOptions) -> TruncationResult {
    truncate_head_of(
        content,
        TextTotals {
            lines: count_lines(content),
            bytes: utf8_byte_length(content),
        },
        options,
    )
}

pub fn truncate_head_of(
    prefix: &str,
    totals: TextTotals,
    options: TruncationOptions,
) -> TruncationResult {
    let max_lines = options.max_lines.unwrap_or(DEFAULT_MAX_LINES);
    let max_bytes = options.max_bytes.unwrap_or(DEFAULT_MAX_BYTES);
    let total_lines = totals.lines;
    let total_bytes = totals.bytes;

    if total_lines <= max_lines && total_bytes <= max_bytes {
        return TruncationResult {
            content: prefix.to_owned(),
            truncated: false,
            truncated_by: None,
            total_lines,
            total_bytes,
            output_lines: total_lines,
            output_bytes: total_bytes,
            last_line_partial: false,
            first_line_exceeds_limit: false,
            max_lines,
            max_bytes,
        };
    }

    let lines = split_lines_for_counting(prefix);
    let first_line = lines
        .first()
        .copied()
        .expect("truncated prefix must hold its first line");
    if utf8_byte_length(first_line) > max_bytes {
        return TruncationResult {
            content: String::new(),
            truncated: true,
            truncated_by: Some(TruncatedBy::Bytes),
            total_lines,
            total_bytes,
            output_lines: 0,
            output_bytes: 0,
            last_line_partial: false,
            first_line_exceeds_limit: true,
            max_lines,
            max_bytes,
        };
    }

    let mut kept: Vec<&str> = Vec::new();
    let mut output_bytes = 0;
    let mut truncated_by = TruncatedBy::Lines;
    for (index, line) in lines.iter().enumerate().take(max_lines) {
        let line_bytes = utf8_byte_length(line) + usize::from(index > 0);
        if output_bytes + line_bytes > max_bytes {
            truncated_by = TruncatedBy::Bytes;
            break;
        }
        kept.push(line);
        output_bytes += line_bytes;
    }

    if truncated_by != TruncatedBy::Bytes {
        truncated_by = if kept.len() < total_lines {
            TruncatedBy::Lines
        } else {
            TruncatedBy::Bytes
        };
    }

    let content = kept.join("\n");
    TruncationResult {
        output_lines: kept.len(),
        output_bytes: utf8_byte_length(&content),
        content,
        truncated: true,
        truncated_by: Some(truncated_by),
        total_lines,
        total_bytes,
        last_line_partial: false,
        first_line_exceeds_limit: false,
        max_lines,
        max_bytes,
    }
}
