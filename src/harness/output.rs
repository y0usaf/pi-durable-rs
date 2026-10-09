use std::collections::VecDeque;

use crate::errors::{Error, Result};
use crate::truncate::utf8_byte_length;

const NEWLINE: u8 = b'\n';

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retain {
    Head,
    Tail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputLimits {
    pub max_bytes: usize,
    pub max_lines: usize,
    pub retain: Retain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedOutput {
    pub text: String,
    pub dropped_bytes: usize,
    pub dropped_lines: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputSlice {
    pub text: String,
    pub bytes: usize,
    pub dropped_bytes: usize,
    pub dropped_lines: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShellOutputSkip {
    pub bytes: usize,
    pub newlines: usize,
    pub ends_with_newline: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum OutputChunk<'a> {
    Text(&'a str),
    Bytes(&'a [u8]),
}

pub fn sanitize_output(text: &str) -> String {
    text.chars().filter(|c| !is_invalid_output(*c)).collect()
}

fn is_invalid_output(c: char) -> bool {
    matches!(c, '\u{0}'..='\u{8}' | '\u{b}'..='\u{1f}' | '\u{fff9}'..='\u{fffb}')
}

pub fn bound_output(text: &str, limits: OutputLimits) -> OutputSlice {
    let bytes = text.as_bytes();
    let (from, to) = match limits.retain {
        Retain::Head => head_range(bytes, limits),
        Retain::Tail => tail_range(bytes, limits),
    };
    let kept = &bytes[from..to];
    OutputSlice {
        text: if kept.len() == bytes.len() {
            text.to_owned()
        } else {
            String::from_utf8_lossy(kept).into_owned()
        },
        bytes: kept.len(),
        dropped_bytes: bytes.len() - kept.len(),
        dropped_lines: line_count(bytes) - line_count(kept),
    }
}

fn head_range(bytes: &[u8], limits: OutputLimits) -> (usize, usize) {
    if limits.max_lines == 0 || limits.max_bytes == 0 {
        return (0, 0);
    }
    let mut end = bytes.len();
    let mut lines = 0;
    for (index, _) in bytes
        .iter()
        .enumerate()
        .filter(|(_, byte)| **byte == NEWLINE)
    {
        lines += 1;
        if lines == limits.max_lines {
            end = index + 1;
            break;
        }
    }
    if end > limits.max_bytes {
        end = match bytes[..limits.max_bytes]
            .iter()
            .rposition(|byte| *byte == NEWLINE)
        {
            Some(newline) => newline + 1,
            None => character_end(bytes, limits.max_bytes),
        };
    }
    (0, end)
}

fn tail_range(bytes: &[u8], limits: OutputLimits) -> (usize, usize) {
    let len = bytes.len();
    if limits.max_lines == 0 || limits.max_bytes == 0 {
        return (len, len);
    }
    let last = if bytes.last() == Some(&NEWLINE) {
        len as isize - 2
    } else {
        len as isize - 1
    };
    let mut start = 0;
    let mut lines = 1;
    let mut index = newline_at_or_before(bytes, last);
    while index != -1 {
        if lines == limits.max_lines {
            start = index as usize + 1;
            break;
        }
        lines += 1;
        index = if index == 0 {
            -1
        } else {
            newline_at_or_before(bytes, index - 1)
        };
    }
    if len - start > limits.max_bytes {
        let from = len - limits.max_bytes;
        start = match first_newline_from(bytes, from - 1) {
            Some(newline) if newline + 1 < len => newline + 1,
            _ => character_start(bytes, from),
        };
    }
    (start, len)
}

fn newline_at_or_before(bytes: &[u8], index: isize) -> isize {
    if index < 0 {
        return -1;
    }
    bytes[..=index as usize]
        .iter()
        .rposition(|byte| *byte == NEWLINE)
        .map_or(-1, |found| found as isize)
}

fn first_newline_from(bytes: &[u8], from: usize) -> Option<usize> {
    bytes[from..]
        .iter()
        .position(|byte| *byte == NEWLINE)
        .map(|found| found + from)
}

pub fn character_end(bytes: &[u8], index: usize) -> usize {
    let mut end = index;
    while end > 0 && (byte_at(bytes, end) & 0xc0) == 0x80 {
        end -= 1;
    }
    end
}

fn character_start(bytes: &[u8], index: usize) -> usize {
    let mut start = index;
    while start < bytes.len() && (byte_at(bytes, start) & 0xc0) == 0x80 {
        start += 1;
    }
    start
}

fn byte_at(bytes: &[u8], index: usize) -> u8 {
    bytes.get(index).copied().unwrap_or(0)
}

fn line_count(bytes: &[u8]) -> usize {
    match bytes.last() {
        None => 0,
        Some(last) => count_newlines_in(bytes) + usize::from(*last != NEWLINE),
    }
}

fn count_newlines_in(bytes: &[u8]) -> usize {
    bytes.iter().filter(|byte| **byte == NEWLINE).count()
}

fn count_newlines(text: &str) -> usize {
    text.matches('\n').count()
}

fn lines(newlines: usize, terminated: bool) -> usize {
    newlines + usize::from(!terminated)
}

fn decode_utf8(pending: &mut Vec<u8>, bytes: &[u8], stream: bool) -> String {
    pending.extend_from_slice(bytes);
    let mut out = String::new();
    let mut rest = std::mem::take(pending);
    loop {
        match std::str::from_utf8(&rest) {
            Ok(valid) => {
                out.push_str(valid);
                break;
            }
            Err(error) => {
                let valid_up_to = error.valid_up_to();
                out.push_str(std::str::from_utf8(&rest[..valid_up_to]).unwrap_or(""));
                match error.error_len() {
                    Some(len) => {
                        out.push('\u{fffd}');
                        rest = rest[valid_up_to + len..].to_vec();
                    }
                    None if stream => {
                        *pending = rest[valid_up_to..].to_vec();
                        break;
                    }
                    None => {
                        out.push('\u{fffd}');
                        break;
                    }
                }
            }
        }
    }
    out
}

#[derive(Debug, Clone)]
struct Chunk {
    text: String,
    bytes: usize,
    newlines: usize,
}

#[derive(Debug, Clone)]
pub struct OutputBuffer {
    limits: OutputLimits,
    pending: Vec<u8>,
    started: bool,
    chunks: VecDeque<Chunk>,
    stored_bytes: usize,
    stored_newlines: usize,
    full: bool,
    total_bytes: usize,
    total_newlines: usize,
    ends_with_newline: bool,
}

impl OutputBuffer {
    pub fn new(limits: OutputLimits) -> Self {
        Self {
            limits,
            pending: Vec::new(),
            started: false,
            chunks: VecDeque::new(),
            stored_bytes: 0,
            stored_newlines: 0,
            full: false,
            total_bytes: 0,
            total_newlines: 0,
            ends_with_newline: true,
        }
    }

    pub fn stored_bytes(&self) -> usize {
        self.stored_bytes
    }

    fn flush_decoder(&mut self) -> String {
        decode_utf8(&mut self.pending, &[], false)
    }

    pub fn push(
        &mut self,
        chunk: OutputChunk<'_>,
        skipped: Option<ShellOutputSkip>,
    ) -> Result<bool> {
        let pending = match chunk {
            OutputChunk::Text(_) => self.flush_decoder(),
            OutputChunk::Bytes(_) if skipped.is_some() => self.flush_decoder(),
            OutputChunk::Bytes(_) => String::new(),
        };
        let mut text = match chunk {
            OutputChunk::Text(text) => text.to_owned(),
            OutputChunk::Bytes(bytes) => decode_utf8(&mut self.pending, bytes, true),
        };
        let first = !self.started && pending.is_empty() && skipped.is_none();
        if !pending.is_empty() || !text.is_empty() || skipped.is_some() {
            self.started = true;
        }
        if first
            && matches!(chunk, OutputChunk::Bytes(_))
            && let Some(rest) = text.strip_prefix('\u{feff}')
        {
            text = rest.to_owned();
        }
        match skipped {
            None => Ok(self.accept(&(pending + &text))),
            Some(skip) => {
                if self.limits.retain != Retain::Tail {
                    return Err(Error::Invalid(
                        "Skipped output requires tail retention".to_owned(),
                    ));
                }
                self.accept(&pending);
                self.skip(skip);
                self.accept(&text);
                Ok(true)
            }
        }
    }

    fn skip(&mut self, skipped: ShellOutputSkip) {
        if skipped.bytes == 0 {
            return;
        }
        self.total_bytes += skipped.bytes;
        self.total_newlines += skipped.newlines;
        self.ends_with_newline = skipped.ends_with_newline;
        self.chunks.clear();
        self.stored_bytes = 0;
        self.stored_newlines = 0;
    }

    pub fn end(&mut self) {
        let rest = self.flush_decoder();
        self.accept(&rest);
    }

    fn accept(&mut self, text: &str) -> bool {
        if text.is_empty() {
            return false;
        }
        let bytes = utf8_byte_length(text);
        let newlines = count_newlines(text);
        self.total_bytes += bytes;
        self.total_newlines += newlines;
        self.ends_with_newline = text.ends_with('\n');
        if self.full {
            return true;
        }
        self.chunks.push_back(Chunk {
            text: text.to_owned(),
            bytes,
            newlines,
        });
        self.stored_bytes += bytes;
        self.stored_newlines += newlines;
        match self.limits.retain {
            Retain::Head => {
                self.full = self.stored_bytes > self.limits.max_bytes
                    || self.stored_newlines >= self.limits.max_lines;
            }
            Retain::Tail => {
                while self.chunks.len() > 1 {
                    let first = self.chunks.front().cloned().unwrap_or(Chunk {
                        text: String::new(),
                        bytes: 0,
                        newlines: 0,
                    });
                    let bytes_after = self.stored_bytes - first.bytes;
                    let newlines_after = self.stored_newlines - first.newlines;
                    if bytes_after <= self.limits.max_bytes + 1
                        && newlines_after <= self.limits.max_lines + 1
                    {
                        break;
                    }
                    self.chunks.pop_front();
                    self.stored_bytes = bytes_after;
                    self.stored_newlines = newlines_after;
                }
            }
        }
        true
    }

    pub fn snapshot(&mut self) -> BoundedOutput {
        let stored: String = self
            .chunks
            .iter()
            .map(|chunk| chunk.text.as_str())
            .collect();
        let kept = bound_output(&stored, self.limits);
        let stored_lines = lines(
            self.stored_newlines,
            stored.is_empty() || stored.ends_with('\n'),
        );
        let kept_lines = stored_lines - kept.dropped_lines;
        if self.limits.retain == Retain::Tail || self.chunks.len() > 1 {
            let text = match self.limits.retain {
                Retain::Tail => tail_margin(&stored, self.limits),
                Retain::Head => stored.clone(),
            };
            let bytes = match self.limits.retain {
                Retain::Tail => utf8_byte_length(&text),
                Retain::Head => self.stored_bytes,
            };
            self.chunks = if text.is_empty() {
                VecDeque::new()
            } else {
                VecDeque::from([Chunk {
                    newlines: count_newlines(&text),
                    text,
                    bytes,
                }])
            };
            self.stored_bytes = bytes;
            self.stored_newlines = self.chunks.front().map_or(0, |chunk| chunk.newlines);
        }
        BoundedOutput {
            text: sanitize_output(&kept.text),
            dropped_bytes: self.total_bytes - kept.bytes,
            dropped_lines: lines(self.total_newlines, self.ends_with_newline) - kept_lines,
        }
    }
}

fn tail_margin(text: &str, limits: OutputLimits) -> String {
    let bytes = text.as_bytes();
    let byte_start = if bytes.len() > limits.max_bytes {
        character_end(bytes, bytes.len() - limits.max_bytes - 1)
    } else {
        0
    };
    let mut line_start = 0;
    let mut newlines = 0;
    let mut index = bytes.iter().rposition(|byte| *byte == NEWLINE);
    while let Some(at) = index {
        newlines += 1;
        if newlines > limits.max_lines {
            line_start = at;
            break;
        }
        if at == 0 {
            break;
        }
        index = bytes[..at].iter().rposition(|byte| *byte == NEWLINE);
    }
    let start = byte_start.max(line_start);
    if start == 0 {
        text.to_owned()
    } else {
        String::from_utf8_lossy(&bytes[start..]).into_owned()
    }
}
