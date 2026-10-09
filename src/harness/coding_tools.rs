use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::ai::types::{TextContent, TextType, Tool, UserBlock};
use crate::env::ExecutionEnv;
use crate::harness::output::{OutputChunk, Retain};
use crate::harness::types::{
    Extension, OutputLimitsSpec, PrepareArguments, ToolExecute, ToolExecutionApi,
    ToolExecutionResult, ToolRegistration,
};
use crate::tools::bash::{self, BashArgs};
use crate::tools::edit::{self, EditDetails};
use crate::tools::read::{self, ReadArgs};
use crate::tools::write::{self, WriteArgs};
use crate::tools::{Diagnostic, Severity, ToolError, ToolOutput};
use crate::truncate::{TruncatedBy, TruncationResult};
use crate::types::{ToolDiagnostic, ToolDiagnosticSeverity};

const READ_DESCRIPTION: &str = "Read the contents of a text file. Output is truncated to 2000 lines or 50KB (whichever is hit first). Use offset/limit for large files. When you need the full file, continue with offset until complete.";
const WRITE_DESCRIPTION: &str = "Write content to a file. Creates the file if it doesn't exist, overwrites if it does. Automatically creates parent directories.";
const EDIT_DESCRIPTION: &str = "Edit a single file using exact text replacement. Every edits[].oldText must match a unique, non-overlapping region of the original file. If two changes affect the same block or nearby lines, merge them into one edit instead of emitting overlapping edits. Do not include large unchanged regions just to connect distant changes.";
const BASH_DESCRIPTION: &str = "Execute a bash command in the current working directory. Returns combined stdout and stderr. Output is truncated to last 2000 lines or 50KB (whichever is hit first). If truncated, full output is saved to a temp file. Optionally provide a timeout in seconds.";

pub fn coding_tools() -> Extension {
    Extension {
        name: "coding-tools".to_owned(),
        tools: vec![read_tool(), write_tool(), edit_tool(), bash_tool()],
        sections: Vec::new(),
        wraps: Vec::new(),
    }
}

fn read_tool() -> ToolRegistration {
    let execute: ToolExecute = Arc::new(|args, api, cancel| {
        let outcome = parsed::<ReadArgs>(args)
            .and_then(|args| with_env(api, |env| read::execute(env, &args, cancel)));
        Ok(finish(outcome, |truncation: Option<TruncationResult>| {
            truncation.map(|truncation| json!({"truncation": truncation_json(&truncation)}))
        }))
    });
    registration(
        "read",
        READ_DESCRIPTION,
        read::parameters(),
        None,
        None,
        execute,
    )
}

fn write_tool() -> ToolRegistration {
    let execute: ToolExecute = Arc::new(|args, api, cancel| {
        let outcome = parsed::<WriteArgs>(args)
            .and_then(|args| with_env(api, |env| write::execute(env, &args, cancel)));
        Ok(finish(outcome, |()| None))
    });
    registration(
        "write",
        WRITE_DESCRIPTION,
        write::parameters(),
        None,
        None,
        execute,
    )
}

fn edit_tool() -> ToolRegistration {
    let execute: ToolExecute = Arc::new(|args, api, cancel| {
        let outcome = edit::parse_arguments(args)
            .and_then(|args| with_env(api, |env| edit::execute(env, &args, cancel)));
        Ok(finish(outcome, |details: EditDetails| {
            Some(edit_details_json(&details))
        }))
    });
    let prepare: PrepareArguments = Arc::new(edit::prepare_arguments);
    registration(
        "edit",
        EDIT_DESCRIPTION,
        edit::parameters(),
        None,
        Some(prepare),
        execute,
    )
}

fn bash_tool() -> ToolRegistration {
    let execute: ToolExecute = Arc::new(|args, api, cancel| {
        let outcome = parsed::<BashArgs>(args)
            .and_then(|args| with_env(api, |env| bash::execute(env, &args, cancel)));
        let text = match &outcome {
            Ok(output) => output.text.clone(),
            Err(error) => error.text.clone(),
        };
        if !text.is_empty() {
            api.output(OutputChunk::Text(&text), None)?;
        }
        let mut result = finish(outcome, |()| None);
        result.content = None;
        Ok(result)
    });
    let limits = OutputLimitsSpec {
        max_bytes: None,
        max_lines: None,
        retain: Some(Retain::Tail),
    };
    registration(
        "bash",
        BASH_DESCRIPTION,
        bash::parameters(),
        Some(limits),
        None,
        execute,
    )
}

fn registration(
    name: &str,
    description: &str,
    parameters: Value,
    output_limits: Option<OutputLimitsSpec>,
    prepare_arguments: Option<PrepareArguments>,
    execute: ToolExecute,
) -> ToolRegistration {
    ToolRegistration {
        tool: Tool {
            name: name.to_owned(),
            description: description.to_owned(),
            parameters,
            constrained_sampling: None,
        },
        replay: None,
        execution_mode: None,
        output_limits,
        prepare_arguments,
        execute,
    }
}

fn parsed<A: DeserializeOwned>(args: Value) -> std::result::Result<A, ToolError> {
    serde_json::from_value(args)
        .map_err(|error| ToolError::new(format!("Invalid arguments: {error}")))
}

fn with_env<T>(
    api: &dyn ToolExecutionApi,
    run: impl FnOnce(&dyn ExecutionEnv) -> std::result::Result<T, ToolError>,
) -> std::result::Result<T, ToolError> {
    let env = api
        .env()
        .ok_or_else(|| ToolError::new("No execution environment is configured"))?;
    run(env)
}

fn finish<D>(
    outcome: std::result::Result<ToolOutput<D>, ToolError>,
    details: impl FnOnce(D) -> Option<Value>,
) -> ToolExecutionResult {
    match outcome {
        Ok(output) => ToolExecutionResult {
            content: Some(text_content(&output.text)),
            is_error: Some(output.is_error),
            details: details(output.details),
            diagnostics: Some(diagnostics(&output.diagnostics)),
            ..ToolExecutionResult::default()
        },
        Err(error) => {
            let mapped = if error.diagnostics.is_empty() {
                vec![ToolDiagnostic {
                    severity: ToolDiagnosticSeverity::Error,
                    message: error.message.clone(),
                    code: Some("tool_error".to_owned()),
                }]
            } else {
                diagnostics(&error.diagnostics)
            };
            ToolExecutionResult {
                content: Some(text_content(&error.text)),
                is_error: Some(true),
                diagnostics: Some(mapped),
                ..ToolExecutionResult::default()
            }
        }
    }
}

fn text_content(text: &str) -> Vec<UserBlock> {
    if text.is_empty() {
        return Vec::new();
    }
    vec![UserBlock::Text(TextContent {
        kind: TextType,
        text: text.to_owned(),
        text_signature: None,
    })]
}

fn diagnostics(items: &[Diagnostic]) -> Vec<ToolDiagnostic> {
    items
        .iter()
        .map(|item| ToolDiagnostic {
            severity: match item.severity {
                Severity::Info => ToolDiagnosticSeverity::Info,
                Severity::Warn => ToolDiagnosticSeverity::Warn,
                Severity::Error => ToolDiagnosticSeverity::Error,
            },
            message: item.message.clone(),
            code: item.code.map(str::to_owned),
        })
        .collect()
}

fn truncation_json(truncation: &TruncationResult) -> Value {
    json!({
        "truncated": truncation.truncated,
        "truncatedBy": truncation.truncated_by.map(|by| match by {
            TruncatedBy::Lines => "lines",
            TruncatedBy::Bytes => "bytes",
        }),
        "totalLines": truncation.total_lines,
        "totalBytes": truncation.total_bytes,
        "outputLines": truncation.output_lines,
        "outputBytes": truncation.output_bytes,
        "lastLinePartial": truncation.last_line_partial,
        "firstLineExceedsLimit": truncation.first_line_exceeds_limit,
        "maxLines": truncation.max_lines,
        "maxBytes": truncation.max_bytes,
    })
}

fn edit_details_json(details: &EditDetails) -> Value {
    let mut value = json!({"diff": details.diff, "patch": details.patch});
    if let (Some(line), Some(object)) = (details.first_changed_line, value.as_object_mut()) {
        object.insert("firstChangedLine".to_owned(), json!(line));
    }
    value
}
