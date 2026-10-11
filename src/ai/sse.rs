use std::io::{self, BufRead};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SseEvent {
    pub event: Option<String>,
    pub id: Option<String>,
    pub data: String,
    pub retry_millis: Option<u64>,
}

pub struct SseReader<R> {
    reader: R,
    done: bool,
    retry: Option<u64>,
}

impl<R: BufRead> SseReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            done: false,
            retry: None,
        }
    }

    pub fn retry_delay(&self) -> Option<u64> {
        self.retry
    }
}

impl<R: BufRead> Iterator for SseReader<R> {
    type Item = io::Result<SseEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut event: Option<String> = None;
        let mut id: Option<String> = None;
        let mut data: Vec<String> = Vec::new();
        let mut line = Vec::new();
        loop {
            line.clear();
            match self.reader.read_until(b'\n', &mut line) {
                Err(error) => {
                    self.done = true;
                    return Some(Err(error));
                }
                Ok(0) => {
                    self.done = true;
                    if data.is_empty() && event.is_none() {
                        return None;
                    }
                    return Some(Ok(SseEvent {
                        event,
                        id,
                        data: data.join("\n"),
                        retry_millis: self.retry,
                    }));
                }
                Ok(_) => {}
            }
            while matches!(line.last(), Some(b'\n' | b'\r')) {
                line.pop();
            }
            if line.is_empty() {
                if data.is_empty() && event.is_none() {
                    continue;
                }
                return Some(Ok(SseEvent {
                    event,
                    id,
                    data: data.join("\n"),
                    retry_millis: self.retry,
                }));
            }
            if line[0] == b':' {
                continue;
            }
            let text = String::from_utf8_lossy(&line);
            let (field, value) = match text.split_once(':') {
                Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
                None => (text.as_ref(), ""),
            };
            match field {
                "event" => event = Some(value.to_owned()),
                "data" => data.push(value.to_owned()),
                "id" => id = Some(value.to_owned()),
                "retry" if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) => {
                    self.retry = value.parse::<u64>().ok().or(self.retry);
                }
                _ => {}
            }
        }
    }
}
