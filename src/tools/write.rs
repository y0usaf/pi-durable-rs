use super::file_mutation_queue::with_file_mutation_queue;
use super::path_utils::resolve_tool_path;
use super::{ToolError, ToolOutput, check_aborted};
use crate::cancel::CancelToken;
use crate::env::ExecutionEnv;
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct WriteArgs {
    pub path: String,
    pub content: String,
}

pub fn parameters() -> Value {
    json!({
        "type": "object",
        "required": ["path", "content"],
        "properties": {
            "path": {"type": "string", "description": "Path to the file to write (relative or absolute)"},
            "content": {"type": "string", "description": "Content to write to the file"}
        }
    })
}

pub fn execute(
    env: &dyn ExecutionEnv,
    args: &WriteArgs,
    cancel: &CancelToken,
) -> Result<ToolOutput<()>, ToolError> {
    let absolute = resolve_tool_path(env, &args.path, cancel)?;
    with_file_mutation_queue(env, &absolute, cancel, || {
        check_aborted(cancel)?;
        env.write_file(&absolute, args.content.as_bytes(), cancel)?;
        check_aborted(cancel)?;
        Ok(ToolOutput {
            text: format!("Successfully wrote to {}", args.path),
            diagnostics: Vec::new(),
            details: (),
        })
    })
}
