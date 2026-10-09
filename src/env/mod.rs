use crate::cancel::CancelToken;
use std::collections::BTreeMap;
use std::fmt;

pub mod decode;
pub mod line_scan;
pub mod node;
mod node_exec;
mod node_watch;
mod path;

pub use decode::{RangeDecoder, starts_with_bom};
pub use node::{NodeExecutionEnv, NodeOptions};
pub use node_watch::WatchOptions;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    File,
    Directory,
    Symlink,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileErrorCode {
    Aborted,
    NotFound,
    PermissionDenied,
    NotDirectory,
    IsDirectory,
    Invalid,
    NotSupported,
    Unknown,
}

impl FileErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Aborted => "aborted",
            Self::NotFound => "not_found",
            Self::PermissionDenied => "permission_denied",
            Self::NotDirectory => "not_directory",
            Self::IsDirectory => "is_directory",
            Self::Invalid => "invalid",
            Self::NotSupported => "not_supported",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileError {
    pub code: FileErrorCode,
    pub message: String,
    pub path: Option<String>,
}

impl FileError {
    pub fn new(code: FileErrorCode, message: impl Into<String>, path: Option<String>) -> Self {
        Self {
            code,
            message: message.into(),
            path,
        }
    }
}

impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for FileError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionErrorCode {
    Aborted,
    Timeout,
    ShellUnavailable,
    SpawnError,
    Unknown,
}

impl ExecutionErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Aborted => "aborted",
            Self::Timeout => "timeout",
            Self::ShellUnavailable => "shell_unavailable",
            Self::SpawnError => "spawn_error",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionError {
    pub code: ExecutionErrorCode,
    pub message: String,
    pub spill_path: Option<String>,
}

impl ExecutionError {
    pub fn new(code: ExecutionErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            spill_path: None,
        }
    }
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ExecutionError {}

#[derive(Debug, Clone, PartialEq)]
pub struct FileInfo {
    pub name: String,
    pub path: String,
    pub kind: FileKind,
    pub size: u64,
    pub mtime_ms: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextLine {
    pub text: String,
    pub terminated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineScan {
    pub newlines: u64,
    pub start: u64,
    pub end: u64,
    pub first_line_end: u64,
    pub last_line_start: u64,
    pub selected_bytes: u64,
    pub first_line_bytes: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DirPage {
    pub entries: Vec<FileInfo>,
    pub done: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchTarget {
    pub path: String,
    pub recursive: bool,
    pub hidden: bool,
    pub names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WatchChange {
    Paths(Vec<String>),
    Error(FileError),
}

pub trait TextLineReader {
    fn read_line(&mut self, cancel: &CancelToken) -> Result<Option<TextLine>, FileError>;
    fn close(&mut self);
}

pub trait BinaryReader {
    fn info(&self, cancel: &CancelToken) -> Result<FileInfo, FileError>;
    fn read(&self, offset: u64, length: u64, cancel: &CancelToken) -> Result<Vec<u8>, FileError>;
    fn scan_lines(
        &self,
        start_line: u64,
        end_line: Option<u64>,
        cancel: &CancelToken,
    ) -> Result<LineScan, FileError>;
    fn close(&mut self);
}

pub trait DirReader {
    fn next(&mut self, max_entries: u64, cancel: &CancelToken) -> Result<DirPage, FileError>;
    fn close(&mut self);
}

pub trait FileWatcher {
    fn close(self: Box<Self>);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpillOptions {
    pub after_bytes: u64,
    pub after_lines: u64,
}

pub type OutputCallback<'a> = &'a mut dyn FnMut(&str, OutputStream);

pub struct ExecOptions<'a> {
    pub cwd: Option<String>,
    pub env: BTreeMap<String, String>,
    pub inherit_env: bool,
    pub timeout: Option<f64>,
    pub on_output: Option<OutputCallback<'a>>,
    pub spill: Option<SpillOptions>,
}

impl Default for ExecOptions<'_> {
    fn default() -> Self {
        Self {
            cwd: None,
            env: BTreeMap::new(),
            inherit_env: true,
            timeout: None,
            on_output: None,
            spill: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Command<'a> {
    Line(&'a str),
    Argv(&'a [&'a str]),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecResult {
    pub exit_code: i32,
    pub spill_path: Option<String>,
}

pub trait ExecutionEnv {
    fn id(&self) -> &str;
    fn cwd(&self) -> &str;
    fn absolute_path(&self, path: &str, cancel: &CancelToken) -> Result<String, FileError>;
    fn join_path(&self, parts: &[&str], cancel: &CancelToken) -> Result<String, FileError>;
    fn read_text_file(&self, path: &str, cancel: &CancelToken) -> Result<String, FileError>;
    fn open_text_line_reader(
        &self,
        path: &str,
        cancel: &CancelToken,
    ) -> Result<Box<dyn TextLineReader>, FileError>;
    fn read_text_lines(
        &self,
        path: &str,
        max_lines: Option<u64>,
        cancel: &CancelToken,
    ) -> Result<Vec<String>, FileError>;
    fn read_binary_file(&self, path: &str, cancel: &CancelToken) -> Result<Vec<u8>, FileError>;
    fn open_binary_reader(
        &self,
        path: &str,
        no_follow: bool,
        cancel: &CancelToken,
    ) -> Result<Box<dyn BinaryReader>, FileError>;
    fn write_file(&self, path: &str, content: &[u8], cancel: &CancelToken)
    -> Result<(), FileError>;
    fn append_file(
        &self,
        path: &str,
        content: &[u8],
        cancel: &CancelToken,
    ) -> Result<(), FileError>;
    fn truncate_file(&self, path: &str, size: u64, cancel: &CancelToken) -> Result<(), FileError>;
    fn flush_file(&self, path: &str, cancel: &CancelToken) -> Result<(), FileError>;
    fn rename_file(
        &self,
        source: &str,
        destination: &str,
        cancel: &CancelToken,
    ) -> Result<(), FileError>;
    fn file_info(&self, path: &str, cancel: &CancelToken) -> Result<FileInfo, FileError>;
    fn list_dir(&self, path: &str, cancel: &CancelToken) -> Result<Vec<FileInfo>, FileError>;
    fn open_dir_reader(
        &self,
        path: &str,
        cancel: &CancelToken,
    ) -> Result<Box<dyn DirReader>, FileError>;
    fn watch(
        &self,
        targets: &[WatchTarget],
        on_change: Box<dyn FnMut(WatchChange) + Send>,
        cancel: &CancelToken,
    ) -> Result<Box<dyn FileWatcher>, FileError>;
    fn canonical_path(&self, path: &str, cancel: &CancelToken) -> Result<String, FileError>;
    fn exists(&self, path: &str, cancel: &CancelToken) -> Result<bool, FileError>;
    fn create_dir(
        &self,
        path: &str,
        recursive: bool,
        cancel: &CancelToken,
    ) -> Result<(), FileError>;
    fn remove(
        &self,
        path: &str,
        recursive: bool,
        force: bool,
        cancel: &CancelToken,
    ) -> Result<(), FileError>;
    fn create_temp_dir(
        &self,
        prefix: Option<&str>,
        cancel: &CancelToken,
    ) -> Result<String, FileError>;
    fn create_temp_file(
        &self,
        prefix: &str,
        suffix: &str,
        cancel: &CancelToken,
    ) -> Result<String, FileError>;
    fn exec(
        &self,
        command: Command<'_>,
        options: ExecOptions<'_>,
        cancel: &CancelToken,
    ) -> Result<ExecResult, ExecutionError>;
    fn cleanup(&self);
}
