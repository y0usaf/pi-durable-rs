use super::decode::StreamDecoder;
use super::node::{NodeExecutionEnv, errno_name};
use super::path::resolve;
use super::{
    Command, ExecOptions, ExecResult, ExecutionEnv, ExecutionError, ExecutionErrorCode,
    OutputCallback, OutputStream, SpillOptions,
};
use crate::cancel::CancelToken;
use crate::turn::pause;
use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::Path;
use std::process::{Command as Process, ExitStatus, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

const MAX_TIMEOUT_MS: f64 = 2_147_483_647.0;
const EXIT_STDIO_GRACE: Duration = Duration::from_millis(100);
const POLL: Duration = Duration::from_millis(10);
const READ_CHUNK: usize = 64 * 1024;
const OUTPUT_QUEUE: usize = 16;
const SIGKILL: i32 = 9;

unsafe extern "C" {
    fn kill(pid: i32, signal: i32) -> i32;
}

pub(super) fn kill_group(pid: u32) {
    let Ok(pid) = i32::try_from(pid) else {
        return;
    };
    let signalled = unsafe { kill(-pid, SIGKILL) };
    if signalled != 0 {
        unsafe { kill(pid, SIGKILL) };
    }
}

enum Event {
    Data(OutputStream, Vec<u8>),
    Closed,
}

fn aborted() -> ExecutionError {
    ExecutionError::new(ExecutionErrorCode::Aborted, "aborted")
}

fn resolve_timeout(timeout: Option<f64>) -> Result<Option<f64>, ExecutionError> {
    let Some(seconds) = timeout else {
        return Ok(None);
    };
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(ExecutionError::new(
            ExecutionErrorCode::Timeout,
            "Invalid timeout: must be a finite number of seconds",
        ));
    }
    if seconds * 1000.0 > MAX_TIMEOUT_MS {
        return Err(ExecutionError::new(
            ExecutionErrorCode::Timeout,
            format!(
                "Invalid timeout: maximum is {} seconds",
                MAX_TIMEOUT_MS / 1000.0
            ),
        ));
    }
    Ok(Some(seconds))
}

fn shell_on_path() -> Option<String> {
    let output = Process::new("which")
        .arg("bash")
        .stdin(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let first = text.trim().lines().next()?;
    (!first.is_empty() && Path::new(first).exists()).then(|| first.to_owned())
}

fn shell_program(env: &NodeExecutionEnv) -> Result<String, ExecutionError> {
    if let Some(custom) = env.shell_path.as_deref().filter(|path| !path.is_empty()) {
        return if Path::new(custom).exists() {
            Ok(custom.to_owned())
        } else {
            Err(ExecutionError::new(
                ExecutionErrorCode::ShellUnavailable,
                format!("Custom shell path not found: {custom}"),
            ))
        };
    }
    if Path::new("/bin/bash").exists() {
        return Ok("/bin/bash".to_owned());
    }
    Ok(shell_on_path().unwrap_or_else(|| "sh".to_owned()))
}

fn spawn_failure(program: &str, error: &io::Error) -> ExecutionError {
    let message = match errno_name(error) {
        Some(name) => format!("spawn {program} {name}"),
        None => error.to_string(),
    };
    ExecutionError::new(ExecutionErrorCode::SpawnError, message)
}

fn pump<R: Read + Send + 'static>(
    mut source: R,
    stream: OutputStream,
    tx: mpsc::SyncSender<Event>,
) {
    thread::spawn(move || {
        let mut buf = vec![0u8; READ_CHUNK];
        loop {
            match source.read(&mut buf) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    if tx.send(Event::Data(stream, buf[..read].to_vec())).is_err() {
                        return;
                    }
                }
            }
        }
        tx.send(Event::Closed).ok();
    });
}

struct Output<'a> {
    on_output: Option<OutputCallback<'a>>,
    spill: Option<SpillOptions>,
    stdout: StreamDecoder,
    stderr: StreamDecoder,
    seen_bytes: u64,
    seen_newlines: u64,
    prefix: Vec<Vec<u8>>,
    file: Option<std::fs::File>,
    spill_path: Option<String>,
    spill_error: Option<ExecutionError>,
    callback_error: Option<String>,
    failed: bool,
}

impl Output<'_> {
    fn decoder(&mut self, stream: OutputStream) -> &mut StreamDecoder {
        match stream {
            OutputStream::Stdout => &mut self.stdout,
            OutputStream::Stderr => &mut self.stderr,
        }
    }

    fn emit(&mut self, text: &str, stream: OutputStream) {
        if text.is_empty() || self.callback_error.is_some() {
            return;
        }
        if let Some(callback) = self.on_output.as_mut()
            && let Err(message) = callback(text, stream)
        {
            self.callback_error = Some(message);
        }
    }

    fn chunk(
        &mut self,
        stream: OutputStream,
        bytes: &[u8],
        env: &NodeExecutionEnv,
        cancel: &CancelToken,
    ) -> bool {
        let text = self.decoder(stream).decode(bytes);
        self.emit(&text, stream);
        if self.callback_error.is_some() {
            return true;
        }
        let Some(spill) = self.spill else {
            return false;
        };
        if bytes.is_empty() || self.failed {
            return false;
        }
        if self.file.is_some() {
            return self.write(bytes);
        }
        self.seen_bytes += bytes.len() as u64;
        self.seen_newlines += bytes.iter().filter(|&&byte| byte == b'\n').count() as u64;
        let lines = self.seen_newlines + u64::from(bytes.last() != Some(&b'\n'));
        if self.seen_bytes <= spill.after_bytes && lines <= spill.after_lines {
            self.prefix.push(bytes.to_vec());
            return false;
        }
        if !self.start_spill(env, cancel) {
            return true;
        }
        let queued = std::mem::take(&mut self.prefix);
        queued.iter().any(|queued| self.write(queued)) || self.write(bytes)
    }

    fn start_spill(&mut self, env: &NodeExecutionEnv, cancel: &CancelToken) -> bool {
        let opened = env
            .create_temp_file("pi-output-", ".log", cancel)
            .map_err(|error| error.message)
            .and_then(|path| {
                OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .map(|file| (path, file))
                    .map_err(|error| error.to_string())
            });
        match opened {
            Ok((path, file)) => {
                self.spill_path = Some(path);
                self.file = Some(file);
                true
            }
            Err(message) => {
                self.fail_spill(message);
                false
            }
        }
    }

    fn write(&mut self, bytes: &[u8]) -> bool {
        let Some(file) = self.file.as_mut() else {
            return false;
        };
        match file.write_all(bytes) {
            Ok(()) => false,
            Err(error) => {
                self.fail_spill(error.to_string());
                true
            }
        }
    }

    fn fail_spill(&mut self, message: String) {
        if self.failed {
            return;
        }
        self.failed = true;
        self.spill_error = Some(ExecutionError::new(
            ExecutionErrorCode::Unknown,
            format!("Failed to preserve complete shell output: {message}"),
        ));
    }

    fn finish(&mut self) {
        self.file = None;
        let stdout = self.stdout.finish();
        self.emit(&stdout, OutputStream::Stdout);
        let stderr = self.stderr.finish();
        self.emit(&stderr, OutputStream::Stderr);
    }
}

pub(super) fn run(
    env: &NodeExecutionEnv,
    command: Command<'_>,
    options: ExecOptions<'_>,
    cancel: &CancelToken,
) -> Result<ExecResult, ExecutionError> {
    if cancel.is_cancelled() {
        return Err(aborted());
    }
    let timeout = resolve_timeout(options.timeout)?;
    let cwd = options.cwd.as_deref().map_or_else(
        || env.cwd_path().to_owned(),
        |cwd| resolve(env.cwd_path(), cwd),
    );
    let (program, args): (String, Vec<String>) = match command {
        Command::Line(line) => (shell_program(env)?, vec!["-c".to_owned(), line.to_owned()]),
        Command::Argv(argv) => {
            let Some((first, rest)) = argv.split_first() else {
                return Err(ExecutionError::new(
                    ExecutionErrorCode::SpawnError,
                    "Empty argv: no program to run",
                ));
            };
            (
                (*first).to_owned(),
                rest.iter().map(|arg| (*arg).to_owned()).collect(),
            )
        }
    };
    if !Path::new(&cwd).exists() {
        return Err(ExecutionError::new(
            ExecutionErrorCode::SpawnError,
            format!("Working directory does not exist: {cwd}\nCannot execute bash commands."),
        ));
    }

    let mut process = Process::new(&program);
    process
        .args(&args)
        .current_dir(&cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    if options.inherit_env {
        process.envs(env.shell_env.iter());
    } else {
        process.env_clear();
    }
    process.envs(options.env.iter());
    let mut child = process
        .spawn()
        .map_err(|error| spawn_failure(&program, &error))?;
    let pid = child.id();
    env.active().insert(pid);

    let (tx, rx) = mpsc::sync_channel(OUTPUT_QUEUE);
    if let Some(stdout) = child.stdout.take() {
        pump(stdout, OutputStream::Stdout, tx.clone());
    }
    if let Some(stderr) = child.stderr.take() {
        pump(stderr, OutputStream::Stderr, tx.clone());
    }
    drop(tx);

    let started = Instant::now();
    let deadline = timeout.map(|seconds| started + Duration::from_secs_f64(seconds));
    let mut output = Output {
        on_output: options.on_output,
        spill: options.spill,
        stdout: StreamDecoder::new(),
        stderr: StreamDecoder::new(),
        seen_bytes: 0,
        seen_newlines: 0,
        prefix: Vec::new(),
        file: None,
        spill_path: None,
        spill_error: None,
        callback_error: None,
        failed: false,
    };
    let mut timed_out = false;
    let mut killed = false;
    let mut exit: Option<(ExitStatus, Instant)> = None;
    let mut open_streams = 2;
    let mut waited: Result<(), ExecutionError> = Ok(());
    loop {
        if !timed_out && deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            timed_out = true;
        }
        if (timed_out || cancel.is_cancelled()) && !killed {
            killed = true;
            kill_group(pid);
        }
        if exit.is_none() {
            match child.try_wait() {
                Ok(Some(status)) => exit = Some((status, Instant::now())),
                Ok(None) => {}
                Err(error) => {
                    waited = Err(spawn_failure(&program, &error));
                    break;
                }
            }
        }
        if let Some((_, exited_at)) = exit
            && (open_streams == 0 || exited_at.elapsed() >= EXIT_STDIO_GRACE)
        {
            break;
        }
        match pause(|| rx.recv_timeout(POLL)) {
            Ok(Event::Data(stream, bytes)) => {
                if exit.is_some() {
                    exit = exit.map(|(status, _)| (status, Instant::now()));
                }
                if output.chunk(stream, &bytes, env, cancel) {
                    killed = true;
                    kill_group(pid);
                }
            }
            Ok(Event::Closed) => open_streams -= 1,
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                open_streams = 0;
                pause(|| thread::sleep(POLL));
            }
        }
    }
    env.active().remove(&pid);
    output.finish();
    waited?;
    if let Some(message) = output.callback_error.take() {
        return Err(ExecutionError::new(
            ExecutionErrorCode::CallbackError,
            message,
        ));
    }
    let spill_path = output.spill_path.clone();
    let interrupted = if timed_out {
        Some(ExecutionError::new(
            ExecutionErrorCode::Timeout,
            format!("timeout:{}", options.timeout.unwrap_or_default()),
        ))
    } else if cancel.is_cancelled() {
        Some(aborted())
    } else {
        None
    };
    if let Some(mut error) = interrupted {
        error.spill_path = spill_path;
        return Err(error);
    }
    if let Some(error) = output.spill_error.take() {
        return Err(error);
    }
    let (status, _) = exit.ok_or_else(aborted)?;
    let exit_code = status
        .code()
        .unwrap_or_else(|| status.signal().map_or(1, |signal| 128 + signal));
    Ok(ExecResult {
        exit_code,
        spill_path,
    })
}
