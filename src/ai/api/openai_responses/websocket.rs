use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use ring::digest::{self, SHA1_FOR_LEGACY_USE_ONLY};
use ring::rand::{SecureRandom, SystemRandom};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use serde_json::{Value, json};

use super::events;
use super::request::{self, Compat, Turn};
use crate::ai::event_stream::AssistantMessageEventStream;
use crate::ai::http::HttpError;
use crate::ai::types::{
    AssistantMessage, Message, Model, StopReason, TranscriptContext, Transport,
};
use crate::ai::utils::uuid::uuidv7;
use crate::cancel::{CancelToken, Subscription};

const BETA: &str = "responses_websockets=2026-02-06";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const CACHE_TTL: Duration = Duration::from_secs(5 * 60);
const MAX_AGE: Duration = Duration::from_secs(55 * 60);
const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_FRAME: u64 = 64 * 1024 * 1024;
const MAX_HEADERS: usize = 128;
const STATUSES: [&str; 6] = [
    "completed",
    "incomplete",
    "failed",
    "cancelled",
    "queued",
    "in_progress",
];

pub(super) struct Attempt<'a> {
    pub model: &'a Model,
    pub compat: &'a Compat,
    pub turn: &'a Turn,
    pub url: &'a str,
    pub headers: &'a [(String, String)],
    pub body: &'a Value,
    pub session: Option<&'a str>,
    pub account: &'a str,
    pub transport: Option<Transport>,
    pub timeout: Option<Duration>,
    pub signal: Option<&'a CancelToken>,
}

#[derive(Default)]
struct State {
    sessions: HashMap<(String, String), Entry>,
    fallback: HashSet<String>,
    next_id: u64,
}

struct Entry {
    id: u64,
    socket: Option<Socket>,
    created: Instant,
    idle_since: Instant,
    continuation: Option<Continuation>,
}

impl Entry {
    fn is_fresh(&self) -> bool {
        self.created.elapsed() < MAX_AGE && self.idle_since.elapsed() < CACHE_TTL
    }
}

#[derive(Clone)]
struct Continuation {
    request: Value,
    response_id: String,
    items: Vec<Value>,
}

fn state() -> MutexGuard<'static, State> {
    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    STATE
        .get_or_init(|| Mutex::new(State::default()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

pub(super) fn run(
    att: &Attempt,
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
) -> Option<Result<(), String>> {
    if att
        .session
        .is_some_and(|session| state().fallback.contains(session))
    {
        return None;
    }
    let cached = matches!(
        att.transport,
        None | Some(Transport::Auto | Transport::WebsocketCached)
    );
    let request_id = match att.session {
        Some(session) => session.chars().take(64).collect(),
        None => match uuidv7(None) {
            Ok(id) => id,
            Err(error) => return Some(Err(error)),
        },
    };
    let handshake = handshake_headers(att.headers, &request_id);
    let mut connection_retry = true;
    let mut continuation_retry = true;
    loop {
        let mut lease = match Lease::acquire(att, &handshake) {
            Ok(lease) => lease,
            Err(_) if cancelled(att) => return Some(Err(HttpError::Cancelled.to_string())),
            Err(_) => return fall_back(att.session),
        };
        let link = lease.socket.link(att.signal);
        let first = match open(&mut lease, att, cached) {
            Ok(first) => first,
            Err(_) if cancelled(att) => return Some(Err(HttpError::Cancelled.to_string())),
            Err(_) => {
                let reused = lease.reused;
                lease.release(false);
                if reused {
                    continue;
                }
                return fall_back(att.session);
            }
        };
        match error_code(&first).as_str() {
            "previous_response_not_found" if continuation_retry => {
                continuation_retry = false;
                lease.release(false);
                continue;
            }
            "websocket_connection_limit_reached" => {
                lease.release(false);
                if connection_retry {
                    connection_retry = false;
                    continue;
                }
                return fall_back(att.session);
            }
            _ => {}
        }
        return Some(stream(att, lease, link, first, cached, out, output));
    }
}

fn stream(
    att: &Attempt,
    mut lease: Lease,
    link: Option<Subscription>,
    first: String,
    cached: bool,
    out: &AssistantMessageEventStream,
    output: &mut AssistantMessage,
) -> Result<(), String> {
    let mut frames = Frames {
        socket: &mut lease.socket,
        first: Some(first),
        pending: Vec::new(),
        offset: 0,
        terminal: false,
        failed: false,
    };
    let result = events::process(
        BufReader::new(&mut frames),
        att.model,
        att.signal,
        out,
        output,
    );
    let (terminal, failed) = (frames.terminal, frames.failed);
    if failed && !cancelled(att) {
        state().fallback.extend(att.session.map(str::to_owned));
    }
    let stopped = matches!(output.stop_reason, StopReason::Error | StopReason::Aborted);
    let keep = terminal && result.is_ok() && !stopped;
    if let (true, true, Some(response_id), Ok(items)) = (
        keep,
        cached && lease.key.is_some(),
        output.response_id.clone(),
        response_items(att, output),
    ) {
        lease.continuation = Some(Continuation {
            request: att.body.clone(),
            response_id,
            items,
        });
    }
    drop(link);
    lease.release(keep);
    result
}

fn response_items(att: &Attempt, output: &AssistantMessage) -> Result<Vec<Value>, String> {
    let context = TranscriptContext {
        messages: vec![Message::Assistant(Arc::new(output.clone()))],
    };
    let body = request::build(att.model, &context, att.compat, att.turn)?;
    Ok(body["input"].as_array().cloned().unwrap_or_default())
}

fn open(lease: &mut Lease, att: &Attempt, cached: bool) -> io::Result<String> {
    let frame = request_frame(att, cached, &mut lease.continuation);
    let text = serde_json::to_string(&frame).map_err(io::Error::other)?;
    lease.socket.send_text(&text)?;
    loop {
        let text = lease.socket.read_text()?;
        if event_field(&text, "type") != "codex.rate_limits" {
            return Ok(text);
        }
    }
}

fn request_frame(att: &Attempt, cached: bool, continuation: &mut Option<Continuation>) -> Value {
    let mut frame = if cached {
        cached_body(att.body, continuation)
    } else {
        att.body.clone()
    };
    frame["type"] = json!("response.create");
    frame
}

fn cached_body(body: &Value, continuation: &mut Option<Continuation>) -> Value {
    let Some(known) = continuation.as_ref() else {
        return body.clone();
    };
    if without_input(body) != without_input(&known.request) {
        *continuation = None;
        return body.clone();
    }
    let baseline: Vec<Value> = input_items(&known.request)
        .iter()
        .chain(known.items.iter())
        .cloned()
        .collect();
    let current = input_items(body);
    if current.len() < baseline.len() || current[..baseline.len()] != baseline[..] {
        *continuation = None;
        return body.clone();
    }
    let mut request = body.clone();
    request["previous_response_id"] = json!(known.response_id);
    request["input"] = Value::Array(current[baseline.len()..].to_vec());
    request
}

fn without_input(body: &Value) -> Value {
    let mut clone = body.clone();
    if let Some(object) = clone.as_object_mut() {
        object.remove("input");
        object.remove("previous_response_id");
    }
    clone
}

fn input_items(body: &Value) -> &[Value] {
    body["input"].as_array().map_or(&[], Vec::as_slice)
}

fn event_field(text: &str, field: &str) -> String {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|event| event[field].as_str().map(str::to_owned))
        .unwrap_or_default()
}

fn error_code(text: &str) -> String {
    let Ok(event) = serde_json::from_str::<Value>(text) else {
        return String::new();
    };
    [
        &event["code"],
        &event["error"]["code"],
        &event["response"]["error"]["code"],
    ]
    .into_iter()
    .find_map(|code| code.as_str().filter(|code| !code.is_empty()))
    .unwrap_or_default()
    .to_owned()
}

fn map_frame(text: &str) -> Result<Option<(String, bool)>, String> {
    let text = text.trim();
    if text.is_empty() || text == "[DONE]" {
        return Ok(None);
    }
    let mut event: Value = serde_json::from_str(text)
        .map_err(|error| format!("Invalid Codex WebSocket JSON: {error}"))?;
    match event["type"].as_str().unwrap_or_default() {
        "" => Ok(None),
        "response.done" | "response.completed" | "response.incomplete" => {
            if let Some(response) = event.get_mut("response").and_then(Value::as_object_mut) {
                let known = response
                    .get("status")
                    .and_then(Value::as_str)
                    .is_some_and(|status| STATUSES.contains(&status));
                if !known {
                    response.remove("status");
                }
            }
            event["type"] = json!("response.completed");
            Ok(Some((event.to_string(), true)))
        }
        _ => Ok(Some((text.to_owned(), false))),
    }
}

struct Frames<'a> {
    socket: &'a mut Socket,
    first: Option<String>,
    pending: Vec<u8>,
    offset: usize,
    terminal: bool,
    failed: bool,
}

impl Read for Frames<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.offset == self.pending.len() {
            if self.terminal {
                return Ok(0);
            }
            let text = match self.first.take() {
                Some(text) => text,
                None => match self.socket.read_text() {
                    Ok(text) => text,
                    Err(error) => {
                        self.failed = true;
                        return Err(error);
                    }
                },
            };
            if let Some((data, terminal)) = map_frame(&text).map_err(io::Error::other)? {
                self.pending = format!("data: {data}\n\n").into_bytes();
                self.offset = 0;
                self.terminal = terminal;
            }
        }
        let count = buf.len().min(self.pending.len() - self.offset);
        buf[..count].copy_from_slice(&self.pending[self.offset..self.offset + count]);
        self.offset += count;
        Ok(count)
    }
}

struct Lease {
    socket: Socket,
    key: Option<(String, String)>,
    id: u64,
    reused: bool,
    continuation: Option<Continuation>,
}

impl Lease {
    fn acquire(att: &Attempt, handshake: &[(String, String)]) -> Result<Self, String> {
        let key = att
            .session
            .map(|session| (session.to_owned(), att.account.to_owned()));
        let mut cacheable = key.is_some();
        let mut stale = None;
        if let Some(key) = &key {
            let mut state = state();
            let status = state
                .sessions
                .get(key)
                .map(|entry| (entry.socket.is_some(), entry.is_fresh()));
            match status {
                Some((false, _)) => cacheable = false,
                Some((true, true)) => {
                    if let Some(entry) = state.sessions.get_mut(key)
                        && let Some(socket) = entry.socket.take()
                    {
                        return Ok(Self {
                            socket,
                            key: Some(key.clone()),
                            id: entry.id,
                            reused: true,
                            continuation: entry.continuation.clone(),
                        });
                    }
                }
                Some((true, false)) => stale = state.sessions.remove(key),
                None => {}
            }
        }
        drop(stale);
        let socket = Socket::connect(att.url, handshake, CONNECT_TIMEOUT, att.timeout)?;
        let key = if cacheable { key } else { None };
        let id = match &key {
            Some(key) => {
                let mut state = state();
                state.next_id += 1;
                let id = state.next_id;
                let now = Instant::now();
                state.sessions.insert(
                    key.clone(),
                    Entry {
                        id,
                        socket: None,
                        created: now,
                        idle_since: now,
                        continuation: None,
                    },
                );
                id
            }
            None => 0,
        };
        Ok(Self {
            socket,
            key,
            id,
            reused: false,
            continuation: None,
        })
    }

    fn release(self, keep: bool) {
        let Self {
            socket,
            key,
            id,
            continuation,
            ..
        } = self;
        let Some(key) = key else {
            return;
        };
        let mut state = state();
        let owned = state.sessions.get(&key).is_some_and(|entry| entry.id == id);
        if !owned {
            return;
        }
        if keep {
            if let Some(entry) = state.sessions.get_mut(&key) {
                entry.socket = Some(socket);
                entry.idle_since = Instant::now();
                entry.continuation = continuation;
            }
        } else {
            let removed = state.sessions.remove(&key);
            drop(state);
            drop(removed);
            drop(socket);
        }
    }
}

fn fall_back(session: Option<&str>) -> Option<Result<(), String>> {
    if let Some(session) = session {
        state().fallback.insert(session.to_owned());
    }
    None
}

fn cancelled(att: &Attempt) -> bool {
    att.signal.is_some_and(CancelToken::is_cancelled)
}

fn handshake_headers(base: &[(String, String)], request_id: &str) -> Vec<(String, String)> {
    let mut headers: Vec<(String, String)> = base
        .iter()
        .filter(|(name, _)| {
            !["openai-beta", "session-id", "x-client-request-id"]
                .iter()
                .any(|known| name.eq_ignore_ascii_case(known))
        })
        .cloned()
        .collect();
    headers.push(("OpenAI-Beta".to_owned(), BETA.to_owned()));
    headers.push(("session-id".to_owned(), request_id.to_owned()));
    headers.push(("x-client-request-id".to_owned(), request_id.to_owned()));
    headers
}

trait Duplex: Read + Write + Send {}

impl<T: Read + Write + Send> Duplex for T {}

pub(super) struct Socket {
    reader: BufReader<Box<dyn Duplex>>,
    closer: TcpStream,
    idle: Option<Duration>,
}

impl Socket {
    fn connect(
        url: &str,
        headers: &[(String, String)],
        timeout: Duration,
        idle: Option<Duration>,
    ) -> Result<Self, String> {
        let target = Target::parse(url)?;
        let mut last = format!("{}: no addresses", target.host);
        let addrs = (target.host.as_str(), target.port)
            .to_socket_addrs()
            .map_err(|error| format!("{}: {error}", target.host))?;
        let tcp = addrs
            .into_iter()
            .find_map(|addr| match TcpStream::connect_timeout(&addr, timeout) {
                Ok(stream) => Some(stream),
                Err(error) => {
                    last = format!("{addr}: {error}");
                    None
                }
            })
            .ok_or(last)?;
        tcp.set_nodelay(true).ok();
        tcp.set_read_timeout(Some(timeout)).ok();
        tcp.set_write_timeout(Some(timeout)).ok();
        let closer = tcp.try_clone().map_err(|error| error.to_string())?;
        let duplex: Box<dyn Duplex> = if target.tls {
            let name = ServerName::try_from(target.host.clone())
                .map_err(|error| format!("{}: {error}", target.host))?;
            let connection =
                ClientConnection::new(tls_config()?, name).map_err(|error| error.to_string())?;
            Box::new(StreamOwned::new(connection, tcp))
        } else {
            Box::new(tcp)
        };
        let mut reader = BufReader::new(duplex);
        let key = base64(&random::<16>()?);
        let mut request = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n",
            target.path, target.authority
        );
        for (name, value) in headers {
            if name.contains(['\r', '\n', ':']) || value.contains(['\r', '\n']) {
                return Err(format!("invalid WebSocket header {name:?}"));
            }
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        request.push_str("\r\n");
        reader
            .get_mut()
            .write_all(request.as_bytes())
            .and_then(|()| reader.get_mut().flush())
            .map_err(|error| error.to_string())?;
        let (status, response_headers) = read_head(&mut reader)?;
        if status != 101 {
            return Err(format!("WebSocket handshake failed: HTTP {status}"));
        }
        let expected = base64(
            digest::digest(&SHA1_FOR_LEGACY_USE_ONLY, format!("{key}{GUID}").as_bytes()).as_ref(),
        );
        let accepted = response_headers
            .iter()
            .any(|(name, value)| name == "sec-websocket-accept" && *value == expected);
        if !accepted {
            return Err("WebSocket handshake: bad Sec-WebSocket-Accept".to_owned());
        }
        closer.set_read_timeout(idle).ok();
        Ok(Self {
            reader,
            closer,
            idle,
        })
    }

    fn link(&self, signal: Option<&CancelToken>) -> Option<Subscription> {
        let closer = self.closer.try_clone().ok()?;
        signal.map(|token| {
            token.subscribe(move || {
                let _ = closer.shutdown(Shutdown::Both);
            })
        })
    }

    fn send_text(&mut self, text: &str) -> io::Result<()> {
        self.send_frame(0x1, text.as_bytes())
    }

    fn send_frame(&mut self, opcode: u8, payload: &[u8]) -> io::Result<()> {
        let mask = random::<4>().map_err(io::Error::other)?;
        let mut frame = vec![0x80 | opcode];
        let len = payload.len();
        if len < 126 {
            frame.push(0x80 | len as u8);
        } else if let Ok(len) = u16::try_from(len) {
            frame.push(0x80 | 126);
            frame.extend(len.to_be_bytes());
        } else {
            frame.push(0x80 | 127);
            frame.extend((len as u64).to_be_bytes());
        }
        frame.extend(mask);
        frame.extend(
            payload
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ mask[index % 4]),
        );
        let writer = self.reader.get_mut();
        writer.write_all(&frame)?;
        writer.flush()
    }

    fn read_text(&mut self) -> io::Result<String> {
        let mut message = Vec::new();
        loop {
            let (fin, opcode, payload) = self.read_frame()?;
            match opcode {
                0x0..=0x2 => {
                    message.extend(payload);
                    if fin {
                        return String::from_utf8(message)
                            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error));
                    }
                }
                0x8 => {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "WebSocket closed by server",
                    ));
                }
                0x9 => self.send_frame(0xA, &payload)?,
                _ => {}
            }
        }
    }

    fn read_frame(&mut self) -> io::Result<(bool, u8, Vec<u8>)> {
        let mut head = [0u8; 2];
        self.fill(&mut head)?;
        let fin = head[0] & 0x80 != 0;
        let opcode = head[0] & 0x0f;
        let len = match head[1] & 0x7f {
            126 => {
                let mut bytes = [0u8; 2];
                self.fill(&mut bytes)?;
                u64::from(u16::from_be_bytes(bytes))
            }
            127 => {
                let mut bytes = [0u8; 8];
                self.fill(&mut bytes)?;
                u64::from_be_bytes(bytes)
            }
            short => u64::from(short),
        };
        if len > MAX_FRAME {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WebSocket frame too large",
            ));
        }
        let mut payload = vec![0u8; len as usize];
        self.fill(&mut payload)?;
        Ok((fin, opcode, payload))
    }

    fn fill(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.reader
            .read_exact(buf)
            .map_err(|error| match (error.kind(), self.idle) {
                (io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock, Some(idle)) => {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("WebSocket idle timeout after {}ms", idle.as_millis()),
                    )
                }
                _ => error,
            })
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        let _ = self.send_frame(0x8, &1000u16.to_be_bytes());
        let _ = self.closer.shutdown(Shutdown::Both);
    }
}

struct Target {
    tls: bool,
    host: String,
    port: u16,
    authority: String,
    path: String,
}

impl Target {
    fn parse(url: &str) -> Result<Self, String> {
        let (tls, rest) =
            if let Some(rest) = url.strip_prefix("wss://").or(url.strip_prefix("https://")) {
                (true, rest)
            } else if let Some(rest) = url.strip_prefix("ws://").or(url.strip_prefix("http://")) {
                (false, rest)
            } else {
                return Err(format!("unsupported WebSocket URL scheme: {url}"));
            };
        let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let (authority, tail) = rest.split_at(end);
        let path = if tail.is_empty() {
            "/".to_owned()
        } else if tail.starts_with('/') {
            tail.to_owned()
        } else {
            format!("/{tail}")
        };
        let (host, port) = if let Some(inner) = authority.strip_prefix('[') {
            let (host, after) = inner
                .split_once(']')
                .ok_or_else(|| format!("unterminated IPv6 host: {url}"))?;
            (host, after.strip_prefix(':'))
        } else {
            match authority.rsplit_once(':') {
                Some((host, port)) => (host, Some(port)),
                None => (authority, None),
            }
        };
        if host.is_empty() {
            return Err(format!("WebSocket URL has no host: {url}"));
        }
        let port = match port {
            Some(port) => port
                .parse::<u16>()
                .map_err(|_| format!("invalid WebSocket port: {url}"))?,
            None if tls => 443,
            None => 80,
        };
        Ok(Self {
            tls,
            host: host.to_owned(),
            port,
            authority: authority.to_owned(),
            path,
        })
    }
}

fn read_head(reader: &mut impl BufRead) -> Result<(u16, Vec<(String, String)>), String> {
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|error| error.to_string())?;
    let status = line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| format!("WebSocket handshake: bad status line {:?}", line.trim_end()))?;
    let mut headers = Vec::new();
    loop {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .map_err(|error| error.to_string())?;
        let trimmed = line.trim_end();
        if read == 0 || trimmed.is_empty() || headers.len() >= MAX_HEADERS {
            return Ok((status, headers));
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_owned()));
        }
    }
}

fn random<const N: usize>() -> Result<[u8; N], String> {
    let mut bytes = [0u8; N];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| "system random source failed".to_owned())?;
    Ok(bytes)
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk.iter().enumerate().fold(0u32, |acc, (index, byte)| {
            acc | u32::from(*byte) << (16 - 8 * index)
        });
        for index in 0..4 {
            if index <= chunk.len() {
                out.push(char::from(
                    ALPHABET[((n >> (18 - 6 * index)) & 63) as usize],
                ));
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn tls_config() -> Result<Arc<ClientConfig>, String> {
    static CONFIG: OnceLock<Result<Arc<ClientConfig>, String>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let mut roots = RootCertStore::empty();
            match std::env::var_os("SSL_CERT_FILE") {
                Some(path) => {
                    for cert in CertificateDer::pem_file_iter(&path)
                        .map_err(|error| format!("SSL_CERT_FILE {path:?}: {error}"))?
                    {
                        let cert =
                            cert.map_err(|error| format!("SSL_CERT_FILE {path:?}: {error}"))?;
                        roots
                            .add(cert)
                            .map_err(|error| format!("SSL_CERT_FILE {path:?}: {error}"))?;
                    }
                }
                None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
            }
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let config = ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|error| error.to_string())?
                .with_root_certificates(roots)
                .with_no_client_auth();
            Ok(Arc::new(config))
        })
        .clone()
}
