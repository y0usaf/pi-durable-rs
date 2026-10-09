const REPLACEMENT: char = '\u{fffd}';
const BOM: char = '\u{feff}';

#[derive(Debug, Default)]
pub struct RangeDecoder {
    pending: Vec<u8>,
}

impl RangeDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn decode(&mut self, bytes: &[u8]) -> String {
        self.pending.extend_from_slice(bytes);
        let mut text = String::new();
        loop {
            match std::str::from_utf8(&self.pending) {
                Ok(valid) => {
                    text.push_str(valid);
                    self.pending.clear();
                    break;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    let invalid = error.error_len();
                    text.push_str(&String::from_utf8_lossy(&self.pending[..valid]));
                    match invalid {
                        Some(len) => {
                            text.push(REPLACEMENT);
                            self.pending.drain(..valid + len);
                        }
                        None => {
                            self.pending.drain(..valid);
                            break;
                        }
                    }
                }
            }
        }
        text
    }

    pub fn finish(&mut self) -> String {
        let text = if self.pending.is_empty() {
            String::new()
        } else {
            REPLACEMENT.to_string()
        };
        self.pending.clear();
        text
    }
}

#[derive(Debug, Default)]
pub struct StreamDecoder {
    range: RangeDecoder,
    started: bool,
}

impl StreamDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn decode(&mut self, bytes: &[u8]) -> String {
        let text = self.range.decode(bytes);
        self.strip(text)
    }

    pub fn finish(&mut self) -> String {
        let text = self.range.finish();
        self.strip(text)
    }

    fn strip(&mut self, text: String) -> String {
        if self.started || text.is_empty() {
            return text;
        }
        self.started = true;
        if let Some(rest) = text.strip_prefix(BOM) {
            return rest.to_owned();
        }
        text
    }
}

pub fn starts_with_bom(bytes: &[u8]) -> bool {
    matches!(bytes, [0xef, 0xbb, 0xbf, ..])
}
