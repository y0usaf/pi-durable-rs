use super::{Diagnostic, Severity, ToolError, ToolOutput};
use crate::cancel::CancelToken;
use crate::env::{
    Command, ExecOptions, ExecutionEnv, ExecutionErrorCode, OutputStream, SpillOptions,
};
use crate::truncate::{DEFAULT_MAX_BYTES, DEFAULT_MAX_LINES};
use serde::Deserialize;
use serde_json::{Value, json};

const MAX_TIMEOUT_SECONDS: f64 = 2_147_483_647.0 / 1000.0;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct BashArgs {
    pub command: String,
    pub timeout: Option<f64>,
}

pub fn parameters() -> Value {
    json!({
        "type": "object",
        "required": ["command"],
        "properties": {
            "command": {"type": "string", "description": "Bash command to execute"},
            "timeout": {"type": "number", "description": "Timeout in seconds (optional, no default timeout)"}
        }
    })
}

fn validate_timeout(timeout: Option<f64>) -> Result<(), ToolError> {
    let Some(seconds) = timeout else {
        return Ok(());
    };
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(ToolError::new(
            "Invalid timeout: must be a finite number of seconds",
        ));
    }
    if seconds > MAX_TIMEOUT_SECONDS {
        return Err(ToolError::new(format!(
            "Invalid timeout: maximum is {MAX_TIMEOUT_SECONDS} seconds"
        )));
    }
    Ok(())
}

fn is_invalid_output_char(c: char) -> bool {
    matches!(c as u32, 0x00..=0x08 | 0x0b..=0x1f | 0xfff9..=0xfffb)
}

fn character_start(bytes: &[u8], index: usize) -> usize {
    let mut start = index;
    while start < bytes.len() && bytes[start] & 0xc0 == 0x80 {
        start += 1;
    }
    start
}

fn character_end(bytes: &[u8], index: usize) -> usize {
    let mut end = index;
    while end > 0 && bytes.get(end).copied().unwrap_or(0) & 0xc0 == 0x80 {
        end -= 1;
    }
    end
}

fn last_newline_at_or_before(bytes: &[u8], index: usize) -> Option<usize> {
    bytes[..=index].iter().rposition(|&byte| byte == b'\n')
}

fn tail_margin(text: &str) -> String {
    let bytes = text.as_bytes();
    let length = bytes.len();
    let byte_start = if length > DEFAULT_MAX_BYTES {
        character_end(bytes, length - DEFAULT_MAX_BYTES - 1)
    } else {
        0
    };
    let mut line_start = 0;
    let mut newlines = 0;
    let mut cursor = bytes.iter().rposition(|&byte| byte == b'\n');
    while let Some(index) = cursor {
        newlines += 1;
        if newlines > DEFAULT_MAX_LINES {
            line_start = index;
            break;
        }
        if index == 0 {
            break;
        }
        cursor = last_newline_at_or_before(bytes, index - 1);
    }
    let start = byte_start.max(line_start);
    if start == 0 {
        text.to_owned()
    } else {
        String::from_utf8_lossy(&bytes[start..]).into_owned()
    }
}

fn tail_window(text: &str) -> String {
    let bytes = text.as_bytes();
    let length = bytes.len();
    let last = if bytes.last() == Some(&b'\n') {
        length as isize - 2
    } else {
        length as isize - 1
    };
    let mut start = 0;
    let mut lines = 1;
    let mut cursor = if last < 0 {
        None
    } else {
        last_newline_at_or_before(bytes, last as usize)
    };
    while let Some(index) = cursor {
        if lines == DEFAULT_MAX_LINES {
            start = index + 1;
            break;
        }
        lines += 1;
        cursor = if index == 0 {
            None
        } else {
            last_newline_at_or_before(bytes, index - 1)
        };
    }
    if length - start > DEFAULT_MAX_BYTES {
        let from = length - DEFAULT_MAX_BYTES;
        let newline = bytes[from - 1..]
            .iter()
            .position(|&byte| byte == b'\n')
            .map(|offset| offset + from - 1);
        start = match newline {
            Some(index) if index + 1 < length => index + 1,
            _ => character_start(bytes, from),
        };
    }
    if start == 0 {
        text.to_owned()
    } else {
        String::from_utf8_lossy(&bytes[start..]).into_owned()
    }
}

#[derive(Default)]
struct TailOutput {
    stored: String,
}

impl TailOutput {
    fn push(&mut self, text: &str) {
        self.stored.push_str(text);
        self.stored = tail_margin(&self.stored);
    }

    fn snapshot(&self) -> String {
        tail_window(&self.stored)
            .chars()
            .filter(|&c| !is_invalid_output_char(c))
            .collect()
    }
}

fn failure(message: String, text: String, diagnostics: Vec<Diagnostic>) -> ToolError {
    ToolError {
        message,
        text,
        diagnostics,
    }
}

pub fn execute(
    env: &dyn ExecutionEnv,
    args: &BashArgs,
    cancel: &CancelToken,
) -> Result<ToolOutput<()>, ToolError> {
    validate_timeout(args.timeout)?;
    let mut tail = TailOutput::default();
    let outcome = {
        let mut on_output = |text: &str, _stream: OutputStream| -> Result<(), String> {
            tail.push(text);
            Ok(())
        };
        env.exec(
            Command::Line(&args.command),
            ExecOptions {
                cwd: Some(env.cwd().to_owned()),
                timeout: args.timeout,
                on_output: Some(&mut on_output),
                spill: Some(SpillOptions {
                    after_bytes: DEFAULT_MAX_BYTES as u64,
                    after_lines: DEFAULT_MAX_LINES as u64,
                }),
                ..ExecOptions::default()
            },
            cancel,
        )
    };
    let text = tail.snapshot();

    let spill_path = match &outcome {
        Ok(result) => result.spill_path.clone(),
        Err(error) => error.spill_path.clone(),
    };
    let mut diagnostics = Vec::new();
    if let Some(path) = spill_path {
        diagnostics.push(Diagnostic {
            severity: Severity::Info,
            code: Some("full_output"),
            message: format!("Full output: {path}"),
        });
    }

    match outcome {
        Ok(result) if result.exit_code == 0 => Ok(ToolOutput {
            text,
            diagnostics,
            details: (),
        }),
        Ok(result) => Err(failure(
            format!("Command exited with code {}", result.exit_code),
            text,
            diagnostics,
        )),
        Err(error) if error.code == ExecutionErrorCode::Aborted && cancel.is_cancelled() => {
            Err(failure(error.message, text, diagnostics))
        }
        Err(error) if error.code == ExecutionErrorCode::Timeout => Err(failure(
            format!(
                "Command timed out after {} seconds",
                args.timeout.unwrap_or(0.0)
            ),
            text,
            diagnostics,
        )),
        Err(error) if error.code == ExecutionErrorCode::Aborted => {
            Err(failure("Command aborted".to_owned(), text, diagnostics))
        }
        Err(error) => Err(failure(error.message, text, diagnostics)),
    }
}
