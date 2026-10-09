pub mod bash;
pub mod edit;
mod edit_diff;
mod file_mutation_queue;
pub mod image;
mod jsdiff;
pub mod path_utils;
pub mod read;
pub mod write;

use crate::cancel::CancelToken;
use crate::env::{ExecutionError, FileError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    pub severity: Severity,
    pub code: Option<&'static str>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ToolOutput<D> {
    pub text: String,
    pub diagnostics: Vec<Diagnostic>,
    pub details: D,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolError {
    pub message: String,
    pub text: String,
    pub diagnostics: Vec<Diagnostic>,
}

impl ToolError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            text: String::new(),
            diagnostics: Vec::new(),
        }
    }
}

impl From<FileError> for ToolError {
    fn from(error: FileError) -> Self {
        Self::new(error.message)
    }
}

impl From<ExecutionError> for ToolError {
    fn from(error: ExecutionError) -> Self {
        Self::new(error.message)
    }
}

pub fn check_aborted(cancel: &CancelToken) -> Result<(), ToolError> {
    if cancel.is_cancelled() {
        Err(ToolError::new("Operation aborted"))
    } else {
        Ok(())
    }
}
