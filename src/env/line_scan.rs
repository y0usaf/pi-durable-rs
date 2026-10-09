use super::LineScan;
use super::decode::{RangeDecoder, starts_with_bom};

const NEWLINE: u8 = b'\n';

pub struct LineScanner {
    start_line: u64,
    end_line: u64,
    position: u64,
    newlines: u64,
    line_start: u64,
    start: Option<u64>,
    end: Option<u64>,
    first_line_end: Option<u64>,
    last_line_start: Option<u64>,
    selected_bytes: u64,
    first_line_bytes: u64,
    selection: Option<RangeDecoder>,
    first_line: Option<RangeDecoder>,
    head: Option<Vec<u8>>,
    bom: bool,
}

impl LineScanner {
    pub fn new(start_line: u64, end_line: Option<u64>) -> Option<Self> {
        let end_line = end_line.unwrap_or(u64::MAX);
        if end_line <= start_line {
            return None;
        }
        let mut scanner = Self {
            start_line,
            end_line,
            position: 0,
            newlines: 0,
            line_start: 0,
            start: None,
            end: None,
            first_line_end: None,
            last_line_start: None,
            selected_bytes: 0,
            first_line_bytes: 0,
            selection: None,
            first_line: None,
            head: Some(Vec::new()),
            bom: false,
        };
        if start_line == 0 {
            scanner.begin(0);
        }
        Some(scanner)
    }

    pub fn push(&mut self, mut chunk: &[u8]) {
        if let Some(head) = self.head.as_mut() {
            let take = (3 - head.len()).min(chunk.len());
            head.extend_from_slice(&chunk[..take]);
            if head.len() < 3 {
                return;
            }
            self.release_head();
            chunk = &chunk[take..];
        }
        self.process(chunk);
    }

    pub fn finish(mut self) -> LineScan {
        if self.head.is_some() {
            self.release_head();
        }
        let size = self.position;
        let Some(start) = self.start else {
            return LineScan {
                newlines: self.newlines,
                start: size,
                end: size,
                first_line_end: size,
                last_line_start: size,
                selected_bytes: 0,
                first_line_bytes: 0,
            };
        };
        if self.first_line_end.is_none() {
            self.end_first_line(size);
        }
        if self.end.is_none() {
            self.end_selection(size);
        }
        LineScan {
            newlines: self.newlines,
            start,
            end: self.end.unwrap_or(size),
            first_line_end: self.first_line_end.unwrap_or(size),
            last_line_start: self.last_line_start.unwrap_or(self.line_start),
            selected_bytes: self.selected_bytes,
            first_line_bytes: self.first_line_bytes,
        }
    }

    fn release_head(&mut self) {
        let head = self.head.take().unwrap_or_default();
        self.bom = starts_with_bom(&head);
        self.process(&head);
    }

    fn process(&mut self, chunk: &[u8]) {
        let base = self.position;
        let mut from = 0;
        while let Some(offset) = chunk[from..].iter().position(|&byte| byte == NEWLINE) {
            let index = from + offset;
            self.feed(chunk, base, from, index);
            let line = self.newlines;
            let position = base + index as u64;
            if line == self.start_line {
                self.end_first_line(position);
            }
            if line == self.end_line - 1 {
                self.end_selection(position);
            }
            self.feed(chunk, base, index, index + 1);
            from = index + 1;
            self.newlines += 1;
            self.line_start = position + 1;
            if self.newlines == self.start_line {
                self.begin(self.line_start);
            }
            if self.newlines == self.end_line - 1 {
                self.last_line_start = Some(self.line_start);
            }
        }
        self.feed(chunk, base, from, chunk.len());
        self.position += chunk.len() as u64;
    }

    fn begin(&mut self, start: u64) {
        self.start = Some(start);
        if self.start_line == self.end_line - 1 {
            self.last_line_start = Some(start);
        }
        self.selection = Some(RangeDecoder::new());
        self.first_line = Some(RangeDecoder::new());
    }

    fn end_first_line(&mut self, position: u64) {
        self.first_line_end = Some(position);
        if let Some(mut decoder) = self.first_line.take() {
            self.first_line_bytes += decoder.finish().len() as u64;
        }
    }

    fn end_selection(&mut self, position: u64) {
        self.end = Some(position);
        if let Some(mut decoder) = self.selection.take() {
            self.selected_bytes += decoder.finish().len() as u64;
        }
    }

    fn feed(&mut self, chunk: &[u8], base: u64, from: usize, to: usize) {
        let mut from = from;
        if self.bom && base + (from as u64) < 3 {
            from = to.min((3 - base) as usize);
        }
        if to <= from {
            return;
        }
        let bytes = &chunk[from..to];
        if let Some(decoder) = self.selection.as_mut() {
            self.selected_bytes += decoder.decode(bytes).len() as u64;
        }
        if let Some(decoder) = self.first_line.as_mut() {
            self.first_line_bytes += decoder.decode(bytes).len() as u64;
        }
    }
}
