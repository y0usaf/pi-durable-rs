use super::edit_diff::{
    Edit, apply_edits_to_normalized_content, detect_line_ending, generate_diff_string,
    generate_unified_patch, normalize_to_lf, restore_line_endings, strip_bom,
};
use super::file_mutation_queue::with_file_mutation_queue;
use super::path_utils::resolve_tool_path;
use super::{ToolError, ToolOutput, check_aborted};
use crate::cancel::CancelToken;
use crate::env::{ExecutionEnv, FileError, FileKind};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct EditArgs {
    pub path: String,
    pub edits: Vec<Edit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EditDetails {
    pub diff: String,
    pub patch: String,
    pub first_changed_line: Option<usize>,
}

pub fn parameters() -> Value {
    json!({
        "type": "object",
        "required": ["path", "edits"],
        "properties": {
            "path": {"type": "string", "description": "Path to the file to edit (relative or absolute)"},
            "edits": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["oldText", "newText"],
                    "properties": {
                        "oldText": {
                            "type": "string",
                            "description": "Exact text for one targeted replacement. It must be unique in the original file and must not overlap with any other edits[].oldText in the same call."
                        },
                        "newText": {"type": "string", "description": "Replacement text for this targeted edit."}
                    }
                },
                "description": "One or more targeted replacements. Each edit is matched against the original file, not incrementally. Do not include overlapping or nested edits. If two changes touch the same block or nearby lines, merge them into one edit instead."
            }
        }
    })
}

fn is_single_edit(value: &Value) -> bool {
    value.get("oldText").is_some_and(Value::is_string)
        && value.get("newText").is_some_and(Value::is_string)
}

pub fn prepare_arguments(input: Value) -> Value {
    let Value::Object(mut args) = input else {
        return input;
    };
    match args.get("edits") {
        Some(Value::String(text)) => {
            if let Ok(parsed) = serde_json::from_str::<Value>(text) {
                if parsed.is_array() {
                    args.insert("edits".to_owned(), parsed);
                } else if is_single_edit(&parsed) {
                    args.insert("edits".to_owned(), json!([parsed]));
                }
            }
        }
        Some(single) if is_single_edit(single) => {
            let edit = single.clone();
            args.insert("edits".to_owned(), json!([edit]));
        }
        _ => {}
    }

    if let (Some(Value::String(old_text)), Some(Value::String(new_text))) =
        (args.get("oldText"), args.get("newText"))
    {
        let edit = json!({"oldText": old_text, "newText": new_text});
        let mut edits = match args.get("edits") {
            Some(Value::Array(existing)) => existing.clone(),
            _ => Vec::new(),
        };
        edits.push(edit);
        args.remove("oldText");
        args.remove("newText");
        args.insert("edits".to_owned(), Value::Array(edits));
    }
    Value::Object(args)
}

pub fn parse_arguments(input: Value) -> Result<EditArgs, ToolError> {
    let args: EditArgs = serde_json::from_value(prepare_arguments(input))
        .map_err(|error| ToolError::new(format!("Invalid arguments: {error}")))?;
    if args.edits.is_empty() {
        return Err(ToolError::new(
            "Edit tool input is invalid. edits must contain at least one replacement.",
        ));
    }
    Ok(args)
}

fn access_error(path: &str, error: &FileError) -> ToolError {
    ToolError::new(format!(
        "Could not edit file: {path}. Error code: {}.",
        error.code.as_str()
    ))
}

pub fn execute(
    env: &dyn ExecutionEnv,
    args: &EditArgs,
    cancel: &CancelToken,
) -> Result<ToolOutput<EditDetails>, ToolError> {
    let path = args.path.as_str();
    let absolute = resolve_tool_path(env, path, cancel)?;
    with_file_mutation_queue(env, &absolute, cancel, || {
        check_aborted(cancel)?;
        let info = env
            .file_info(&absolute, cancel)
            .map_err(|error| access_error(path, &error))?;
        if info.kind != FileKind::File && info.kind != FileKind::Symlink {
            return Err(ToolError::new(format!(
                "Could not edit file: {path}. Path is not a file."
            )));
        }

        let content = env
            .read_text_file(&absolute, cancel)
            .map_err(|error| access_error(path, &error))?;
        check_aborted(cancel)?;

        let (bom, text) = strip_bom(&content);
        let original_ending = detect_line_ending(text);
        let normalized = normalize_to_lf(text);
        let applied = apply_edits_to_normalized_content(&normalized, &args.edits, path)?;
        check_aborted(cancel)?;

        let final_content = format!(
            "{bom}{}",
            restore_line_endings(&applied.new_content, original_ending)
        );
        env.write_file(&absolute, final_content.as_bytes(), cancel)
            .map_err(|error| access_error(path, &error))?;
        check_aborted(cancel)?;

        let diff = generate_diff_string(&applied.base_content, &applied.new_content);
        let patch = generate_unified_patch(path, &applied.base_content, &applied.new_content);
        Ok(ToolOutput {
            text: format!(
                "Successfully replaced {} block(s) in {path}.",
                args.edits.len()
            ),
            diagnostics: Vec::new(),
            details: EditDetails {
                diff: diff.diff,
                patch,
                first_changed_line: diff.first_changed_line,
            },
        })
    })
}
