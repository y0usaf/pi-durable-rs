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

fn failure(message: String, diagnostics: Vec<Diagnostic>) -> ToolError {
    ToolError {
        message,
        text: String::new(),
        diagnostics,
    }
}

pub fn execute(
    env: &dyn ExecutionEnv,
    args: &BashArgs,
    cancel: &CancelToken,
    output: &mut dyn FnMut(&str) -> Result<(), String>,
) -> Result<ToolOutput<()>, ToolError> {
    validate_timeout(args.timeout)?;
    let mut on_output = |text: &str, _stream: OutputStream| output(text);
    let outcome = env.exec(
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
    );
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
            text: String::new(),
            diagnostics,
            details: (),
            is_error: false,
        }),
        Ok(result) => Err(failure(
            format!("Command exited with code {}", result.exit_code),
            diagnostics,
        )),
        Err(error) if error.code == ExecutionErrorCode::Aborted && cancel.is_cancelled() => {
            Err(failure(error.message, diagnostics))
        }
        Err(error) if error.code == ExecutionErrorCode::Timeout => Err(failure(
            format!(
                "Command timed out after {} seconds",
                args.timeout.unwrap_or(0.0)
            ),
            diagnostics,
        )),
        Err(error) if error.code == ExecutionErrorCode::Aborted => {
            Err(failure("Command aborted".to_owned(), diagnostics))
        }
        Err(error) => Err(failure(error.message, diagnostics)),
    }
}
