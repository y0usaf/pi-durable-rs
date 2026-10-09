use super::ToolError;
use super::jsdiff::{self, Change};
use serde::Deserialize;
use unicode_normalization::UnicodeNormalization;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Edit {
    pub old_text: String,
    pub new_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedEdits {
    pub base_content: String,
    pub new_content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffString {
    pub diff: String,
    pub first_changed_line: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Match {
    index: usize,
    length: usize,
    fuzzy: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MatchedEdit {
    edit_index: usize,
    match_index: usize,
    match_length: usize,
    new_text: String,
}

fn is_js_whitespace(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n' | '\u{b}' | '\u{c}' | '\r' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

pub fn detect_line_ending(content: &str) -> &'static str {
    match content.find('\n') {
        None => "\n",
        Some(lf) => match content.find("\r\n") {
            Some(crlf) if crlf < lf => "\r\n",
            _ => "\n",
        },
    }
}

pub fn normalize_to_lf(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

pub fn restore_line_endings(text: &str, ending: &str) -> String {
    if ending == "\r\n" {
        text.replace('\n', "\r\n")
    } else {
        text.to_owned()
    }
}

pub fn normalize_for_fuzzy_match(text: &str) -> String {
    let nfkc: String = text.nfkc().collect();
    nfkc.split('\n')
        .map(|line| line.trim_end_matches(is_js_whitespace))
        .collect::<Vec<_>>()
        .join("\n")
        .chars()
        .map(|c| match c {
            '\u{2018}'..='\u{201b}' => '\'',
            '\u{201c}'..='\u{201f}' => '"',
            '\u{2010}'..='\u{2015}' | '\u{2212}' => '-',
            '\u{a0}' | '\u{2002}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
            other => other,
        })
        .collect()
}

pub fn strip_bom(content: &str) -> (&'static str, &str) {
    match content.strip_prefix('\u{feff}') {
        Some(text) => ("\u{feff}", text),
        None => ("", content),
    }
}

fn fuzzy_find_text(content: &str, old_text: &str) -> Option<Match> {
    if let Some(index) = content.find(old_text) {
        return Some(Match {
            index,
            length: old_text.len(),
            fuzzy: false,
        });
    }
    let fuzzy_content = normalize_for_fuzzy_match(content);
    let fuzzy_old = normalize_for_fuzzy_match(old_text);
    fuzzy_content.find(&fuzzy_old).map(|index| Match {
        index,
        length: fuzzy_old.len(),
        fuzzy: true,
    })
}

fn count_occurrences(content: &str, old_text: &str) -> usize {
    let fuzzy_content = normalize_for_fuzzy_match(content);
    let fuzzy_old = normalize_for_fuzzy_match(old_text);
    if fuzzy_old.is_empty() {
        return fuzzy_content.encode_utf16().count().saturating_sub(1);
    }
    fuzzy_content.matches(fuzzy_old.as_str()).count()
}

fn not_found_error(path: &str, edit_index: usize, total: usize) -> ToolError {
    if total == 1 {
        return ToolError::new(format!(
            "Could not find the exact text in {path}. The old text must match exactly including all whitespace and newlines."
        ));
    }
    ToolError::new(format!(
        "Could not find edits[{edit_index}] in {path}. The oldText must match exactly including all whitespace and newlines."
    ))
}

fn duplicate_error(path: &str, edit_index: usize, total: usize, occurrences: usize) -> ToolError {
    if total == 1 {
        return ToolError::new(format!(
            "Found {occurrences} occurrences of the text in {path}. The text must be unique. Please provide more context to make it unique."
        ));
    }
    ToolError::new(format!(
        "Found {occurrences} occurrences of edits[{edit_index}] in {path}. Each oldText must be unique. Please provide more context to make it unique."
    ))
}

fn empty_old_text_error(path: &str, edit_index: usize, total: usize) -> ToolError {
    if total == 1 {
        return ToolError::new(format!("oldText must not be empty in {path}."));
    }
    ToolError::new(format!(
        "edits[{edit_index}].oldText must not be empty in {path}."
    ))
}

fn no_change_error(path: &str, total: usize) -> ToolError {
    if total == 1 {
        return ToolError::new(format!(
            "No changes made to {path}. The replacement produced identical content. This might indicate an issue with special characters or the text not existing as expected."
        ));
    }
    ToolError::new(format!(
        "No changes made to {path}. The replacements produced identical content."
    ))
}

fn split_lines_with_endings(content: &str) -> Vec<&str> {
    content.split_inclusive('\n').collect()
}

fn line_spans(content: &str) -> Vec<(usize, usize)> {
    let mut offset = 0;
    split_lines_with_endings(content)
        .into_iter()
        .map(|line| {
            let span = (offset, offset + line.len());
            offset = span.1;
            span
        })
        .collect()
}

fn replacement_line_range(
    lines: &[(usize, usize)],
    replacement: &MatchedEdit,
) -> Result<(usize, usize), ToolError> {
    let outside = || ToolError::new("Replacement range is outside the base content.");
    let start = replacement.match_index;
    let end = replacement.match_index + replacement.match_length;
    let start_line = lines
        .iter()
        .position(|&(line_start, line_end)| start >= line_start && start < line_end)
        .ok_or_else(outside)?;
    let mut end_line = start_line;
    while end_line < lines.len() && lines[end_line].1 < end {
        end_line += 1;
    }
    if end_line >= lines.len() {
        return Err(outside());
    }
    Ok((start_line, end_line + 1))
}

fn apply_replacements(content: &str, replacements: &[MatchedEdit], offset: usize) -> String {
    let mut result = content.to_owned();
    for replacement in replacements.iter().rev() {
        let index = replacement.match_index - offset;
        result = format!(
            "{}{}{}",
            &result[..index],
            replacement.new_text,
            &result[index + replacement.match_length..]
        );
    }
    result
}

struct Group {
    start_line: usize,
    end_line: usize,
    replacements: Vec<MatchedEdit>,
}

fn apply_replacements_preserving_unchanged_lines(
    original: &str,
    base: &str,
    replacements: &[MatchedEdit],
) -> Result<String, ToolError> {
    let original_lines = split_lines_with_endings(original);
    let base_lines = line_spans(base);
    if original_lines.len() != base_lines.len() {
        return Err(ToolError::new(
            "Cannot preserve unchanged lines because the base content has a different line count.",
        ));
    }

    let mut sorted = replacements.to_vec();
    sorted.sort_by_key(|replacement| replacement.match_index);
    let mut groups: Vec<Group> = Vec::new();
    for replacement in sorted {
        let (start_line, end_line) = replacement_line_range(&base_lines, &replacement)?;
        if let Some(current) = groups
            .last_mut()
            .filter(|current| start_line < current.end_line)
        {
            current.end_line = current.end_line.max(end_line);
            current.replacements.push(replacement);
            continue;
        }
        groups.push(Group {
            start_line,
            end_line,
            replacements: vec![replacement],
        });
    }

    let mut original_index = 0;
    let mut result = String::new();
    for group in groups {
        result.push_str(&original_lines[original_index..group.start_line].concat());
        let group_start = base_lines[group.start_line].0;
        let group_end = base_lines[group.end_line - 1].1;
        result.push_str(&apply_replacements(
            &base[group_start..group_end],
            &group.replacements,
            group_start,
        ));
        original_index = group.end_line;
    }
    result.push_str(&original_lines[original_index..].concat());
    Ok(result)
}

pub fn apply_edits_to_normalized_content(
    normalized: &str,
    edits: &[Edit],
    path: &str,
) -> Result<AppliedEdits, ToolError> {
    let total = edits.len();
    let normalized_edits: Vec<(String, String)> = edits
        .iter()
        .map(|edit| {
            (
                normalize_to_lf(&edit.old_text),
                normalize_to_lf(&edit.new_text),
            )
        })
        .collect();
    for (index, (old_text, _)) in normalized_edits.iter().enumerate() {
        if old_text.is_empty() {
            return Err(empty_old_text_error(path, index, total));
        }
    }

    let used_fuzzy = normalized_edits.iter().any(|(old_text, _)| {
        fuzzy_find_text(normalized, old_text).is_some_and(|found| found.fuzzy)
    });
    let replacement_base = if used_fuzzy {
        normalize_for_fuzzy_match(normalized)
    } else {
        normalized.to_owned()
    };

    let mut matched = Vec::with_capacity(total);
    for (index, (old_text, new_text)) in normalized_edits.iter().enumerate() {
        let found = fuzzy_find_text(&replacement_base, old_text)
            .ok_or_else(|| not_found_error(path, index, total))?;
        let occurrences = count_occurrences(&replacement_base, old_text);
        if occurrences > 1 {
            return Err(duplicate_error(path, index, total, occurrences));
        }
        matched.push(MatchedEdit {
            edit_index: index,
            match_index: found.index,
            match_length: found.length,
            new_text: new_text.clone(),
        });
    }

    matched.sort_by_key(|edit| edit.match_index);
    for pair in matched.windows(2) {
        let (previous, current) = (&pair[0], &pair[1]);
        if previous.match_index + previous.match_length > current.match_index {
            return Err(ToolError::new(format!(
                "edits[{}] and edits[{}] overlap in {path}. Merge them into one edit or target disjoint regions.",
                previous.edit_index, current.edit_index
            )));
        }
    }

    let base_content = normalized.to_owned();
    let new_content = if used_fuzzy {
        apply_replacements_preserving_unchanged_lines(normalized, &replacement_base, &matched)?
    } else {
        apply_replacements(&replacement_base, &matched, 0)
    };
    if base_content == new_content {
        return Err(no_change_error(path, total));
    }
    Ok(AppliedEdits {
        base_content,
        new_content,
    })
}

pub fn generate_unified_patch(path: &str, old: &str, new: &str) -> String {
    jsdiff::create_file_patch(path, old, new, 4)
}

pub fn generate_diff_string(old: &str, new: &str) -> DiffString {
    const CONTEXT: usize = 4;
    let parts: Vec<Change> = jsdiff::diff_lines(old, new);
    let mut output: Vec<String> = Vec::new();

    let old_count = old.split('\n').count();
    let new_count = new.split('\n').count();
    let width = old_count.max(new_count).to_string().len();
    let number = |n: usize| format!("{n:>width$}");
    let blank = " ".repeat(width);

    let mut old_line = 1;
    let mut new_line = 1;
    let mut last_was_change = false;
    let mut first_changed_line = None;

    for (index, part) in parts.iter().enumerate() {
        let mut raw: Vec<&str> = part.value.split('\n').collect();
        if raw.last() == Some(&"") {
            raw.pop();
        }

        if part.added || part.removed {
            first_changed_line.get_or_insert(new_line);
            for line in raw {
                if part.added {
                    output.push(format!("+{} {line}", number(new_line)));
                    new_line += 1;
                } else {
                    output.push(format!("-{} {line}", number(old_line)));
                    old_line += 1;
                }
            }
            last_was_change = true;
        } else {
            let next_is_change = parts
                .get(index + 1)
                .is_some_and(|next| next.added || next.removed);
            let leading = last_was_change;
            let trailing = next_is_change;

            if leading && trailing {
                if raw.len() <= CONTEXT * 2 {
                    for line in &raw {
                        output.push(format!(" {} {line}", number(old_line)));
                        old_line += 1;
                        new_line += 1;
                    }
                } else {
                    let leading_lines = &raw[..CONTEXT];
                    let trailing_lines = &raw[raw.len() - CONTEXT..];
                    let skipped = raw.len() - leading_lines.len() - trailing_lines.len();
                    for line in leading_lines {
                        output.push(format!(" {} {line}", number(old_line)));
                        old_line += 1;
                        new_line += 1;
                    }
                    output.push(format!(" {blank} ..."));
                    old_line += skipped;
                    new_line += skipped;
                    for line in trailing_lines {
                        output.push(format!(" {} {line}", number(old_line)));
                        old_line += 1;
                        new_line += 1;
                    }
                }
            } else if leading {
                let shown = &raw[..CONTEXT.min(raw.len())];
                let skipped = raw.len() - shown.len();
                for line in shown {
                    output.push(format!(" {} {line}", number(old_line)));
                    old_line += 1;
                    new_line += 1;
                }
                if skipped > 0 {
                    output.push(format!(" {blank} ..."));
                    old_line += skipped;
                    new_line += skipped;
                }
            } else if trailing {
                let skipped = raw.len().saturating_sub(CONTEXT);
                if skipped > 0 {
                    output.push(format!(" {blank} ..."));
                    old_line += skipped;
                    new_line += skipped;
                }
                for line in &raw[skipped..] {
                    output.push(format!(" {} {line}", number(old_line)));
                    old_line += 1;
                    new_line += 1;
                }
            } else {
                old_line += raw.len();
                new_line += raw.len();
            }
            last_was_change = false;
        }
    }

    DiffString {
        diff: output.join("\n"),
        first_changed_line,
    }
}
