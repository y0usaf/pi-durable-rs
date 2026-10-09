use super::decode::StreamDecoder;
use super::line_scan::LineScanner;
use super::node_exec;
use super::node_watch::{self, WatchOptions};
use super::path;
use super::{
    BinaryReader, Command, DirPage, DirReader, ExecOptions, ExecResult, ExecutionEnv,
    ExecutionError, FileError, FileErrorCode, FileInfo, FileKind, FileWatcher, LineScan, TextLine,
    TextLineReader, WatchChange, WatchTarget,
};
use crate::cancel::CancelToken;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, Metadata, OpenOptions};
use std::hash::{BuildHasher, Hasher};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, FileExt, OpenOptionsExt};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
const BINARY_READ_CHUNK: u64 = 1024 * 1024;
const STREAM_CHUNK: usize = 64 * 1024;

#[cfg(target_os = "linux")]
const O_NONBLOCK: i32 = 0o4000;
#[cfg(not(target_os = "linux"))]
const O_NONBLOCK: i32 = 0x0004;
#[cfg(target_os = "linux")]
const O_NOFOLLOW: i32 = 0o400000;
#[cfg(not(target_os = "linux"))]
const O_NOFOLLOW: i32 = 0x0100;
#[cfg(target_os = "linux")]
const ELOOP: i32 = 40;
#[cfg(not(target_os = "linux"))]
const ELOOP: i32 = 62;

pub struct NodeOptions {
    pub cwd: String,
    pub shell_path: Option<String>,
    pub shell_env: BTreeMap<String, String>,
    pub watch: WatchOptions,
}

pub struct NodeExecutionEnv {
    cwd: String,
    pub(super) shell_path: Option<String>,
    pub(super) shell_env: BTreeMap<String, String>,
    watch: WatchOptions,
    active: Mutex<BTreeSet<u32>>,
}

impl NodeExecutionEnv {
    pub fn new(options: NodeOptions) -> Self {
        Self {
            cwd: options.cwd,
            shell_path: options.shell_path,
            shell_env: options.shell_env,
            watch: options.watch,
            active: Mutex::new(BTreeSet::new()),
        }
    }

    pub(super) fn cwd_path(&self) -> &str {
        &self.cwd
    }

    pub(super) fn active(&self) -> MutexGuard<'_, BTreeSet<u32>> {
        self.active.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn resolved(&self, path: &str) -> String {
        path::resolve(&self.cwd, path)
    }
}

pub(super) fn check(cancel: &CancelToken, path: Option<&str>) -> Result<(), FileError> {
    if cancel.is_cancelled() {
        Err(FileError::new(
            FileErrorCode::Aborted,
            "aborted",
            path.map(str::to_owned),
        ))
    } else {
        Ok(())
    }
}

pub(super) fn errno_name(error: &io::Error) -> Option<&'static str> {
    if error.raw_os_error() == Some(ELOOP) {
        return Some("ELOOP");
    }
    match error.kind() {
        io::ErrorKind::NotFound => Some("ENOENT"),
        io::ErrorKind::PermissionDenied => Some("EACCES"),
        io::ErrorKind::AlreadyExists => Some("EEXIST"),
        io::ErrorKind::NotADirectory => Some("ENOTDIR"),
        io::ErrorKind::IsADirectory => Some("EISDIR"),
        io::ErrorKind::DirectoryNotEmpty => Some("ENOTEMPTY"),
        io::ErrorKind::InvalidInput => Some("EINVAL"),
        _ => None,
    }
}

fn file_code(error: &io::Error) -> FileErrorCode {
    match error.kind() {
        io::ErrorKind::NotFound => FileErrorCode::NotFound,
        io::ErrorKind::PermissionDenied => FileErrorCode::PermissionDenied,
        io::ErrorKind::NotADirectory => FileErrorCode::NotDirectory,
        io::ErrorKind::IsADirectory => FileErrorCode::IsDirectory,
        io::ErrorKind::InvalidInput => FileErrorCode::Invalid,
        _ => FileErrorCode::Unknown,
    }
}

pub(super) fn io_error(op: String, path: &str, error: &io::Error) -> FileError {
    let code = file_code(error);
    match errno_name(error) {
        Some(name) => {
            let strerror = match name {
                "EISDIR" => "illegal operation on a directory".to_owned(),
                "EEXIST" => "file already exists".to_owned(),
                _ => {
                    let text = error.to_string();
                    let text = text.split(" (os error").next().unwrap_or_default();
                    let mut chars = text.chars();
                    chars
                        .next()
                        .map(|first| first.to_lowercase().chain(chars).collect())
                        .unwrap_or_default()
                }
            };
            FileError::new(
                code,
                format!("{name}: {strerror}, {op}"),
                Some(path.to_owned()),
            )
        }
        None => FileError::new(code, error.to_string(), Some(path.to_owned())),
    }
}

fn read_failure(path: &str, error: &io::Error) -> FileError {
    if error.kind() == io::ErrorKind::IsADirectory {
        return FileError::new(
            FileErrorCode::IsDirectory,
            "EISDIR: illegal operation on a directory, read",
            Some(path.to_owned()),
        );
    }
    io_error(format!("open '{path}'"), path, error)
}

fn symlink_refused(path: &str) -> FileError {
    FileError::new(
        FileErrorCode::Invalid,
        "Refusing to follow a symbolic link",
        Some(path.to_owned()),
    )
}

fn mtime_ms(meta: &Metadata) -> f64 {
    meta.modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map_or(0.0, |since| since.as_secs_f64() * 1000.0)
}

pub(super) fn info_of(path: &str, meta: &Metadata) -> Option<FileInfo> {
    let kind = if meta.is_file() {
        FileKind::File
    } else if meta.is_dir() {
        FileKind::Directory
    } else if meta.file_type().is_symlink() {
        FileKind::Symlink
    } else {
        return None;
    };
    Some(FileInfo {
        name: path::basename(path).to_owned(),
        path: path.to_owned(),
        kind,
        size: meta.len(),
        mtime_ms: mtime_ms(meta),
    })
}

fn unsupported(path: &str) -> FileError {
    FileError::new(
        FileErrorCode::Invalid,
        "Unsupported file type",
        Some(path.to_owned()),
    )
}

fn random_u64() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos());
    hasher.write_u128(nanos);
    hasher.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
    hasher.write_u32(std::process::id());
    hasher.finish()
}

fn uuid() -> String {
    let hex = format!("{:016x}{:016x}", random_u64(), random_u64());
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn tmpdir() -> String {
    match std::env::var("TMPDIR") {
        Ok(dir) if dir.len() > 1 => dir.trim_end_matches('/').to_owned(),
        _ => "/tmp".to_owned(),
    }
}

struct NodeTextLineReader {
    file: Option<File>,
    path: String,
    decoder: StreamDecoder,
    buffered: String,
    ended: bool,
}

impl TextLineReader for NodeTextLineReader {
    fn read_line(&mut self, cancel: &CancelToken) -> Result<Option<TextLine>, FileError> {
        check(cancel, Some(&self.path))?;
        let Some(file) = self.file.as_mut() else {
            return Err(FileError::new(
                FileErrorCode::Invalid,
                "Text line reader is closed",
                Some(self.path.clone()),
            ));
        };
        let mut chunk = vec![0u8; STREAM_CHUNK];
        loop {
            if let Some(newline) = self.buffered.find('\n') {
                let text = self.buffered[..newline].to_owned();
                self.buffered.drain(..=newline);
                return Ok(Some(TextLine {
                    text,
                    terminated: true,
                }));
            }
            if self.ended {
                if self.buffered.is_empty() {
                    return Ok(None);
                }
                let text = std::mem::take(&mut self.buffered);
                return Ok(Some(TextLine {
                    text,
                    terminated: false,
                }));
            }
            let read = read_retry(file, &mut chunk)
                .map_err(|e| io_error("read".to_owned(), &self.path, &e))?;
            check(cancel, Some(&self.path))?;
            if read == 0 {
                let rest = self.decoder.finish();
                self.buffered.push_str(&rest);
                self.ended = true;
            } else {
                let text = self.decoder.decode(&chunk[..read]);
                self.buffered.push_str(&text);
            }
        }
    }

    fn close(&mut self) {
        self.file = None;
        self.buffered.clear();
    }
}

fn read_retry(file: &mut File, buf: &mut [u8]) -> io::Result<usize> {
    loop {
        match file.read(buf) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            other => return other,
        }
    }
}

struct NodeBinaryReader {
    file: Option<File>,
    path: String,
}

impl NodeBinaryReader {
    fn open(&self) -> Result<&File, FileError> {
        self.file.as_ref().ok_or_else(|| {
            FileError::new(
                FileErrorCode::Invalid,
                "Binary reader is closed",
                Some(self.path.clone()),
            )
        })
    }
}

impl BinaryReader for NodeBinaryReader {
    fn info(&self, cancel: &CancelToken) -> Result<FileInfo, FileError> {
        check(cancel, Some(&self.path))?;
        let meta = self
            .open()?
            .metadata()
            .map_err(|e| io_error("fstat".to_owned(), &self.path, &e))?;
        info_of(&self.path, &meta).ok_or_else(|| unsupported(&self.path))
    }

    fn read(&self, offset: u64, length: u64, cancel: &CancelToken) -> Result<Vec<u8>, FileError> {
        check(cancel, Some(&self.path))?;
        let file = self.open()?;
        if offset > MAX_SAFE_INTEGER || length > MAX_SAFE_INTEGER {
            return Err(FileError::new(
                FileErrorCode::Invalid,
                "Offset and length must be non-negative safe integers",
                Some(self.path.clone()),
            ));
        }
        let mut bytes = Vec::new();
        let mut total = 0u64;
        while total < length {
            let want = (length - total).min(BINARY_READ_CHUNK);
            let mut chunk = vec![0u8; want as usize];
            let read = file
                .read_at(&mut chunk, offset + total)
                .map_err(|e| io_error("read".to_owned(), &self.path, &e))?;
            check(cancel, Some(&self.path))?;
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..read]);
            total += read as u64;
        }
        Ok(bytes)
    }

    fn scan_lines(
        &self,
        start_line: u64,
        end_line: Option<u64>,
        cancel: &CancelToken,
    ) -> Result<LineScan, FileError> {
        check(cancel, Some(&self.path))?;
        let file = self.open()?;
        let invalid = || {
            FileError::new(
                FileErrorCode::Invalid,
                "Invalid line range",
                Some(self.path.clone()),
            )
        };
        if start_line > MAX_SAFE_INTEGER || end_line.is_some_and(|end| end > MAX_SAFE_INTEGER) {
            return Err(invalid());
        }
        let mut scanner = LineScanner::new(start_line, end_line).ok_or_else(invalid)?;
        let mut chunk = vec![0u8; STREAM_CHUNK];
        let mut position = 0u64;
        loop {
            let read = file
                .read_at(&mut chunk, position)
                .map_err(|e| io_error("read".to_owned(), &self.path, &e))?;
            check(cancel, Some(&self.path))?;
            if read == 0 {
                return Ok(scanner.finish());
            }
            scanner.push(&chunk[..read]);
            position += read as u64;
        }
    }

    fn close(&mut self) {
        self.file = None;
    }
}

struct NodeDirReader {
    entries: Option<fs::ReadDir>,
    path: String,
    done: bool,
}

impl DirReader for NodeDirReader {
    fn next(&mut self, max_entries: u64, cancel: &CancelToken) -> Result<DirPage, FileError> {
        check(cancel, Some(&self.path))?;
        let Some(entries) = self.entries.as_mut() else {
            return Err(FileError::new(
                FileErrorCode::Invalid,
                "Directory reader is closed",
                Some(self.path.clone()),
            ));
        };
        if max_entries == 0 {
            return Err(FileError::new(
                FileErrorCode::Invalid,
                "maxEntries must be a positive safe integer",
                Some(self.path.clone()),
            ));
        }
        let mut page = Vec::new();
        while !self.done && (page.len() as u64) < max_entries {
            let Some(entry) = entries.next() else {
                self.done = true;
                break;
            };
            let entry =
                entry.map_err(|e| io_error(format!("scandir '{}'", self.path), &self.path, &e))?;
            check(cancel, Some(&self.path))?;
            let entry_path = path::child(&self.path, &entry.file_name().to_string_lossy());
            match fs::symlink_metadata(&entry_path) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(io_error(
                        format!("lstat '{entry_path}'"),
                        &entry_path,
                        &error,
                    ));
                }
                Ok(meta) => page.extend(info_of(&entry_path, &meta)),
            }
        }
        Ok(DirPage {
            entries: page,
            done: self.done,
        })
    }

    fn close(&mut self) {
        self.entries = None;
        self.done = true;
    }
}

impl ExecutionEnv for NodeExecutionEnv {
    fn id(&self) -> &str {
        "node:local"
    }

    fn cwd(&self) -> &str {
        &self.cwd
    }

    fn absolute_path(&self, path: &str, _cancel: &CancelToken) -> Result<String, FileError> {
        Ok(self.resolved(path))
    }

    fn join_path(&self, parts: &[&str], _cancel: &CancelToken) -> Result<String, FileError> {
        Ok(path::join(parts))
    }

    fn read_text_file(&self, path: &str, cancel: &CancelToken) -> Result<String, FileError> {
        let resolved = self.resolved(path);
        check(cancel, Some(&resolved))?;
        let bytes = fs::read(&resolved).map_err(|e| read_failure(&resolved, &e))?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    fn open_text_line_reader(
        &self,
        path: &str,
        cancel: &CancelToken,
    ) -> Result<Box<dyn TextLineReader>, FileError> {
        let resolved = self.resolved(path);
        check(cancel, Some(&resolved))?;
        let file = File::open(&resolved)
            .map_err(|e| io_error(format!("open '{resolved}'"), &resolved, &e))?;
        check(cancel, Some(&resolved))?;
        Ok(Box::new(NodeTextLineReader {
            file: Some(file),
            path: resolved,
            decoder: StreamDecoder::new(),
            buffered: String::new(),
            ended: false,
        }))
    }

    fn read_text_lines(
        &self,
        path: &str,
        max_lines: Option<u64>,
        cancel: &CancelToken,
    ) -> Result<Vec<String>, FileError> {
        if max_lines == Some(0) {
            return Ok(Vec::new());
        }
        let mut reader = self.open_text_line_reader(path, cancel)?;
        let mut lines = Vec::new();
        let outcome = loop {
            if max_lines.is_some_and(|max| (lines.len() as u64) >= max) {
                break Ok(());
            }
            match reader.read_line(cancel) {
                Err(error) => break Err(error),
                Ok(None) => break Ok(()),
                Ok(Some(line)) => lines.push(line.text),
            }
        };
        reader.close();
        outcome.map(|()| lines)
    }

    fn read_binary_file(&self, path: &str, cancel: &CancelToken) -> Result<Vec<u8>, FileError> {
        let resolved = self.resolved(path);
        check(cancel, Some(&resolved))?;
        fs::read(&resolved).map_err(|e| read_failure(&resolved, &e))
    }

    fn open_binary_reader(
        &self,
        path: &str,
        no_follow: bool,
        cancel: &CancelToken,
    ) -> Result<Box<dyn BinaryReader>, FileError> {
        let resolved = self.resolved(path);
        check(cancel, Some(&resolved))?;
        let flags = O_NONBLOCK | if no_follow { O_NOFOLLOW } else { 0 };
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(flags)
            .open(&resolved)
            .map_err(|e| {
                if no_follow && e.raw_os_error() == Some(ELOOP) {
                    symlink_refused(&resolved)
                } else {
                    io_error(format!("open '{resolved}'"), &resolved, &e)
                }
            })?;
        let meta = file
            .metadata()
            .map_err(|e| io_error("fstat".to_owned(), &resolved, &e))?;
        if meta.is_dir() {
            return Err(FileError::new(
                FileErrorCode::IsDirectory,
                "EISDIR: illegal operation on a directory, read",
                Some(resolved),
            ));
        }
        if !meta.is_file() {
            return Err(FileError::new(
                FileErrorCode::Invalid,
                "Not a regular file",
                Some(resolved),
            ));
        }
        check(cancel, Some(&resolved))?;
        Ok(Box::new(NodeBinaryReader {
            file: Some(file),
            path: resolved,
        }))
    }

    fn write_file(
        &self,
        path: &str,
        content: &[u8],
        cancel: &CancelToken,
    ) -> Result<(), FileError> {
        let resolved = self.resolved(path);
        check(cancel, Some(&resolved))?;
        let parent = path::parent(&resolved);
        fs::create_dir_all(parent)
            .map_err(|e| io_error(format!("mkdir '{parent}'"), parent, &e))?;
        check(cancel, Some(&resolved))?;
        fs::write(&resolved, content)
            .map_err(|e| io_error(format!("open '{resolved}'"), &resolved, &e))
    }

    fn append_file(
        &self,
        path: &str,
        content: &[u8],
        cancel: &CancelToken,
    ) -> Result<(), FileError> {
        let resolved = self.resolved(path);
        check(cancel, Some(&resolved))?;
        let parent = path::parent(&resolved);
        fs::create_dir_all(parent)
            .map_err(|e| io_error(format!("mkdir '{parent}'"), parent, &e))?;
        check(cancel, Some(&resolved))?;
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&resolved)
            .map_err(|e| io_error(format!("open '{resolved}'"), &resolved, &e))?;
        file.write_all(content)
            .map_err(|e| io_error(format!("write '{resolved}'"), &resolved, &e))?;
        check(cancel, Some(&resolved))
    }

    fn truncate_file(&self, path: &str, size: u64, cancel: &CancelToken) -> Result<(), FileError> {
        let resolved = self.resolved(path);
        check(cancel, Some(&resolved))?;
        if size > MAX_SAFE_INTEGER {
            return Err(FileError::new(
                FileErrorCode::Invalid,
                "File size must be a non-negative safe integer",
                Some(resolved),
            ));
        }
        let file = OpenOptions::new()
            .write(true)
            .open(&resolved)
            .map_err(|e| io_error(format!("open '{resolved}'"), &resolved, &e))?;
        file.set_len(size)
            .map_err(|e| io_error(format!("ftruncate '{resolved}'"), &resolved, &e))?;
        check(cancel, Some(&resolved))
    }

    fn flush_file(&self, path: &str, cancel: &CancelToken) -> Result<(), FileError> {
        let resolved = self.resolved(path);
        check(cancel, Some(&resolved))?;
        let file = OpenOptions::new()
            .write(true)
            .open(&resolved)
            .map_err(|e| io_error(format!("open '{resolved}'"), &resolved, &e))?;
        let meta = file
            .metadata()
            .map_err(|e| io_error("fstat".to_owned(), &resolved, &e))?;
        if meta.is_dir() {
            return Err(FileError::new(
                FileErrorCode::IsDirectory,
                "Is a directory",
                Some(resolved),
            ));
        }
        file.sync_all()
            .map_err(|e| io_error(format!("fsync '{resolved}'"), &resolved, &e))?;
        check(cancel, Some(&resolved))
    }

    fn rename_file(
        &self,
        source: &str,
        destination: &str,
        cancel: &CancelToken,
    ) -> Result<(), FileError> {
        let from = self.resolved(source);
        let to = self.resolved(destination);
        check(cancel, Some(&to))?;
        fs::rename(&from, &to)
            .map_err(|e| io_error(format!("rename '{from}' -> '{to}'"), &from, &e))
    }

    fn file_info(&self, path: &str, cancel: &CancelToken) -> Result<FileInfo, FileError> {
        let resolved = self.resolved(path);
        check(cancel, Some(&resolved))?;
        let meta = fs::symlink_metadata(&resolved)
            .map_err(|e| io_error(format!("lstat '{resolved}'"), &resolved, &e))?;
        info_of(&resolved, &meta).ok_or_else(|| unsupported(&resolved))
    }

    fn list_dir(&self, path: &str, cancel: &CancelToken) -> Result<Vec<FileInfo>, FileError> {
        let resolved = self.resolved(path);
        check(cancel, Some(&resolved))?;
        let mut entries = fs::read_dir(&resolved)
            .and_then(|entries| entries.collect::<Result<Vec<_>, _>>())
            .map_err(|e| io_error(format!("scandir '{resolved}'"), &resolved, &e))?;
        entries.sort_by_key(|entry| entry.file_name());
        let mut infos = Vec::new();
        for entry in entries {
            check(cancel, Some(&resolved))?;
            let entry_path = path::child(&resolved, &entry.file_name().to_string_lossy());
            let meta = fs::symlink_metadata(&entry_path)
                .map_err(|e| io_error(format!("lstat '{entry_path}'"), &entry_path, &e))?;
            infos.extend(info_of(&entry_path, &meta));
        }
        Ok(infos)
    }

    fn open_dir_reader(
        &self,
        path: &str,
        cancel: &CancelToken,
    ) -> Result<Box<dyn DirReader>, FileError> {
        let resolved = self.resolved(path);
        check(cancel, Some(&resolved))?;
        let entries = fs::read_dir(&resolved)
            .map_err(|e| io_error(format!("opendir '{resolved}'"), &resolved, &e))?;
        check(cancel, Some(&resolved))?;
        Ok(Box::new(NodeDirReader {
            entries: Some(entries),
            path: resolved,
            done: false,
        }))
    }

    fn watch(
        &self,
        targets: &[WatchTarget],
        on_change: Box<dyn FnMut(WatchChange) + Send>,
        cancel: &CancelToken,
    ) -> Result<Box<dyn FileWatcher>, FileError> {
        check(cancel, None)?;
        let resolved = targets
            .iter()
            .map(|target| WatchTarget {
                path: self.resolved(&target.path),
                ..target.clone()
            })
            .collect::<Vec<_>>();
        let watcher = node_watch::open(&resolved, self.watch, on_change)?;
        if cancel.is_cancelled() {
            watcher.close();
            return Err(FileError::new(FileErrorCode::Aborted, "aborted", None));
        }
        Ok(watcher)
    }

    fn canonical_path(&self, path: &str, cancel: &CancelToken) -> Result<String, FileError> {
        let resolved = self.resolved(path);
        check(cancel, Some(&resolved))?;
        let real = fs::canonicalize(&resolved)
            .map_err(|e| io_error(format!("realpath '{resolved}'"), &resolved, &e))?;
        Ok(real.to_string_lossy().into_owned())
    }

    fn exists(&self, path: &str, cancel: &CancelToken) -> Result<bool, FileError> {
        match self.file_info(path, cancel) {
            Ok(_) => Ok(true),
            Err(error) if error.code == FileErrorCode::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn create_dir(
        &self,
        path: &str,
        recursive: bool,
        cancel: &CancelToken,
    ) -> Result<(), FileError> {
        let resolved = self.resolved(path);
        check(cancel, Some(&resolved))?;
        let made = if recursive {
            fs::create_dir_all(&resolved)
        } else {
            fs::create_dir(&resolved)
        };
        made.map_err(|e| io_error(format!("mkdir '{resolved}'"), &resolved, &e))
    }

    fn remove(
        &self,
        path: &str,
        recursive: bool,
        force: bool,
        cancel: &CancelToken,
    ) -> Result<(), FileError> {
        let resolved = self.resolved(path);
        check(cancel, Some(&resolved))?;
        let meta = match fs::symlink_metadata(&resolved) {
            Ok(meta) => meta,
            Err(error) if force && error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(io_error(format!("lstat '{resolved}'"), &resolved, &error)),
        };
        if meta.is_dir() && !recursive {
            return Err(FileError::new(
                FileErrorCode::Unknown,
                format!("Path is a directory: rm returned EISDIR (is a directory) {resolved}"),
                Some(resolved),
            ));
        }
        let removed = if meta.is_dir() {
            fs::remove_dir_all(&resolved)
        } else {
            fs::remove_file(&resolved)
        };
        removed.map_err(|e| io_error(format!("rm '{resolved}'"), &resolved, &e))
    }

    fn create_temp_dir(
        &self,
        prefix: Option<&str>,
        cancel: &CancelToken,
    ) -> Result<String, FileError> {
        check(cancel, None)?;
        let dir = tmpdir();
        let base = path::join(&[dir.as_str(), prefix.unwrap_or("tmp-")]);
        for _ in 0..100 {
            let candidate = format!("{base}{}", &format!("{:016x}", random_u64())[..6]);
            let made = fs::DirBuilder::new().mode(0o700).create(&candidate);
            match made {
                Ok(()) => return Ok(candidate),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(io_error(format!("mkdtemp '{base}XXXXXX'"), &base, &error));
                }
            }
        }
        Err(FileError::new(
            FileErrorCode::Unknown,
            format!("EEXIST: mkdtemp '{base}XXXXXX'"),
            None,
        ))
    }

    fn create_temp_file(
        &self,
        prefix: &str,
        suffix: &str,
        cancel: &CancelToken,
    ) -> Result<String, FileError> {
        let dir = self.create_temp_dir(Some("tmp-"), cancel)?;
        let file_path = path::child(&dir, &format!("{prefix}{}{suffix}", uuid()));
        File::create(&file_path)
            .map_err(|e| io_error(format!("open '{file_path}'"), &file_path, &e))?;
        Ok(file_path)
    }

    fn exec(
        &self,
        command: Command<'_>,
        options: ExecOptions<'_>,
        cancel: &CancelToken,
    ) -> Result<ExecResult, ExecutionError> {
        node_exec::run(self, command, options, cancel)
    }

    fn cleanup(&self) {
        let pids = std::mem::take(&mut *self.active());
        for pid in pids {
            node_exec::kill_group(pid);
        }
    }
}
