use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};

use crate::cancel::{CancelToken, Subscription};

const MAX_HEAD_BYTES: usize = 64 * 1024;
const MAX_TEXT_BYTES: u64 = 32 * 1024 * 1024;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(600);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpError {
    Cancelled,
    Timeout(String),
    Connect(String),
    Io(String),
    Protocol(String),
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HttpError::Cancelled => f.write_str("Request was aborted"),
            HttpError::Timeout(message) => write!(f, "Request timed out: {message}"),
            HttpError::Connect(message) => write!(f, "Connection error: {message}"),
            HttpError::Io(message) => write!(f, "Connection error: {message}"),
            HttpError::Protocol(message) => write!(f, "Invalid HTTP response: {message}"),
        }
    }
}

impl std::error::Error for HttpError {}

impl HttpError {
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            HttpError::Timeout(_) | HttpError::Connect(_) | HttpError::Io(_)
        )
    }
}

pub fn io_error(error: &io::Error, cancel: Option<&CancelToken>) -> HttpError {
    if cancel.is_some_and(CancelToken::is_cancelled) {
        return HttpError::Cancelled;
    }
    match error.kind() {
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => {
            HttpError::Timeout(error.to_string())
        }
        _ => HttpError::Io(error.to_string()),
    }
}

#[derive(Debug, Clone)]
pub struct Request {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub timeout: Option<Duration>,
}

impl Request {
    pub fn new(method: &str, url: impl Into<String>) -> Self {
        Self {
            method: method.to_owned(),
            url: url.into(),
            headers: Vec::new(),
            body: Vec::new(),
            timeout: None,
        }
    }

    pub fn header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.push((name.to_owned(), value.into()));
        self
    }

    pub fn json(mut self, body: &serde_json::Value) -> Self {
        self.body = serde_json::to_vec(body).expect("a JSON value serializes");
        self.headers
            .push(("content-type".to_owned(), "application/json".to_owned()));
        self
    }

    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }
}

struct Url {
    tls: bool,
    host: String,
    port: u16,
    target: String,
}

fn parse_url(url: &str) -> Result<Url, HttpError> {
    let bad = |why: &str| HttpError::Protocol(format!("{why}: {url}"));
    let (tls, rest) = if let Some(rest) = url.strip_prefix("https://") {
        (true, rest)
    } else if let Some(rest) = url.strip_prefix("http://") {
        (false, rest)
    } else {
        return Err(bad("URL must start with http:// or https://"));
    };
    let rest = rest.split('#').next().unwrap_or(rest);
    let end = rest.find(['/', '?']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(end);
    if authority.contains('@') {
        return Err(bad("URL credentials are not supported"));
    }
    let (host, port) = if let Some(inner) = authority.strip_prefix('[') {
        let close = inner
            .find(']')
            .ok_or_else(|| bad("unterminated IPv6 host"))?;
        let port = inner[close + 1..].strip_prefix(':');
        (inner[..close].to_owned(), port)
    } else {
        match authority.rsplit_once(':') {
            Some((host, port)) => (host.to_owned(), Some(port)),
            None => (authority.to_owned(), None),
        }
    };
    if host.is_empty() {
        return Err(bad("URL has no host"));
    }
    let port = match port {
        Some(port) => port.parse::<u16>().map_err(|_| bad("invalid port"))?,
        None if tls => 443,
        None => 80,
    };
    let target = if tail.is_empty() {
        "/".to_owned()
    } else if tail.starts_with('?') {
        format!("/{tail}")
    } else {
        tail.to_owned()
    };
    Ok(Url {
        tls,
        host,
        port,
        target,
    })
}

fn tls_config() -> Result<Arc<ClientConfig>, HttpError> {
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
        .map_err(HttpError::Connect)
}

enum Conn {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Conn::Plain(stream) => stream.read(buf),
            Conn::Tls(stream) => stream.read(buf),
        }
    }
}

impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Conn::Plain(stream) => stream.write(buf),
            Conn::Tls(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Conn::Plain(stream) => stream.flush(),
            Conn::Tls(stream) => stream.flush(),
        }
    }
}

enum Framing {
    Chunked { remaining: u64, done: bool },
    Length(u64),
    Close,
}

pub struct Body {
    inner: BufReader<Conn>,
    framing: Framing,
    cancel: Option<CancelToken>,
    _subscription: Option<Subscription>,
}

fn read_line(reader: &mut impl BufRead, limit: usize) -> io::Result<String> {
    let mut line = Vec::new();
    let read = reader.take(limit as u64).read_until(b'\n', &mut line)?;
    if read == 0 {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    if line.last() != Some(&b'\n') {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "line too long"));
    }
    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line.pop();
    }
    String::from_utf8(line)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF-8 line"))
}

impl Body {
    fn read_inner(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.framing {
            Framing::Close => self.inner.read(buf),
            Framing::Length(remaining) => {
                if *remaining == 0 || buf.is_empty() {
                    return Ok(0);
                }
                let want = buf.len().min(*remaining as usize);
                let read = self.inner.read(&mut buf[..want])?;
                if read == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                *remaining -= read as u64;
                Ok(read)
            }
            Framing::Chunked { remaining, done } => {
                if *done || buf.is_empty() {
                    return Ok(0);
                }
                if *remaining == 0 {
                    let line = read_line(&mut self.inner, 4096)?;
                    let size = line.split(';').next().unwrap_or("").trim();
                    let size = u64::from_str_radix(size, 16).map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("bad chunk size {size:?}"),
                        )
                    })?;
                    if size == 0 {
                        while !read_line(&mut self.inner, 8192)?.is_empty() {}
                        *done = true;
                        return Ok(0);
                    }
                    *remaining = size;
                }
                let want = buf.len().min(*remaining as usize);
                let read = self.inner.read(&mut buf[..want])?;
                if read == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                *remaining -= read as u64;
                if *remaining == 0 && !read_line(&mut self.inner, 16)?.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "missing chunk terminator",
                    ));
                }
                Ok(read)
            }
        }
    }
}

impl Read for Body {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let result = self.read_inner(buf);
        if self.cancel.as_ref().is_some_and(CancelToken::is_cancelled) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "Request was aborted",
            ));
        }
        result
    }
}

pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    body: Body,
}

impl Response {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }

    pub fn into_body(self) -> Body {
        self.body
    }

    pub fn into_reader(self) -> BufReader<Body> {
        BufReader::new(self.body)
    }

    pub fn text(self) -> Result<String, HttpError> {
        let cancel = self.body.cancel.clone();
        let mut bytes = Vec::new();
        let mut reader = self.body.take(MAX_TEXT_BYTES);
        reader
            .read_to_end(&mut bytes)
            .map_err(|error| io_error(&error, cancel.as_ref()))?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    pub fn retry_after(&self) -> Option<Duration> {
        if let Some(ms) = self
            .header("retry-after-ms")
            .and_then(|v| v.trim().parse::<f64>().ok())
            && ms >= 0.0
        {
            return Some(Duration::from_millis(ms as u64));
        }
        let seconds = self.header("retry-after")?.trim().parse::<f64>().ok()?;
        (seconds >= 0.0).then(|| Duration::from_millis((seconds * 1000.0) as u64))
    }
}

fn connect(url: &Url, cancel: Option<&CancelToken>) -> Result<TcpStream, HttpError> {
    let addrs = (url.host.as_str(), url.port)
        .to_socket_addrs()
        .map_err(|error| HttpError::Connect(format!("{}: {error}", url.host)))?;
    let mut last = None;
    for addr in addrs {
        if cancel.is_some_and(CancelToken::is_cancelled) {
            return Err(HttpError::Cancelled);
        }
        match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
            Ok(stream) => return Ok(stream),
            Err(error) => last = Some(format!("{addr}: {error}")),
        }
    }
    Err(HttpError::Connect(
        last.unwrap_or_else(|| format!("{}: no addresses", url.host)),
    ))
}

pub fn send(request: &Request, cancel: Option<&CancelToken>) -> Result<Response, HttpError> {
    if cancel.is_some_and(CancelToken::is_cancelled) {
        return Err(HttpError::Cancelled);
    }
    let url = parse_url(&request.url)?;
    for (name, value) in &request.headers {
        if name.contains(['\r', '\n', ':']) || value.contains(['\r', '\n']) {
            return Err(HttpError::Protocol(format!("invalid header {name:?}")));
        }
    }
    let timeout = request.timeout.unwrap_or(DEFAULT_TIMEOUT);
    let tcp = connect(&url, cancel)?;
    tcp.set_nodelay(true).ok();
    tcp.set_read_timeout(Some(timeout)).ok();
    tcp.set_write_timeout(Some(timeout)).ok();
    let closer = tcp
        .try_clone()
        .map_err(|error| HttpError::Io(error.to_string()))?;
    let subscription = cancel.map(|token| {
        token.subscribe(move || {
            let _ = closer.shutdown(Shutdown::Both);
        })
    });
    let fail = |error: io::Error| io_error(&error, cancel);
    let mut conn = if url.tls {
        let name = ServerName::try_from(url.host.clone())
            .map_err(|error| HttpError::Connect(format!("{}: {error}", url.host)))?;
        let connection = ClientConnection::new(tls_config()?, name)
            .map_err(|error| HttpError::Connect(error.to_string()))?;
        Conn::Tls(Box::new(StreamOwned::new(connection, tcp)))
    } else {
        Conn::Plain(tcp)
    };

    let mut head = format!("{} {} HTTP/1.1\r\nHost: {}", request.method, url.target, {
        let default = if url.tls { 443 } else { 80 };
        let host = if url.host.contains(':') {
            format!("[{}]", url.host)
        } else {
            url.host.clone()
        };
        if url.port == default {
            host
        } else {
            format!("{host}:{}", url.port)
        }
    });
    let has = |name: &str| {
        request
            .headers
            .iter()
            .any(|(key, _)| key.eq_ignore_ascii_case(name))
    };
    for (name, value) in &request.headers {
        head.push_str(&format!("\r\n{name}: {value}"));
    }
    if !has("accept-encoding") {
        head.push_str("\r\naccept-encoding: identity");
    }
    if !has("user-agent") {
        head.push_str(concat!(
            "\r\nuser-agent: pi-durable/",
            env!("CARGO_PKG_VERSION")
        ));
    }
    head.push_str("\r\nconnection: close");
    if !request.body.is_empty() || request.method != "GET" {
        head.push_str(&format!("\r\ncontent-length: {}", request.body.len()));
    }
    head.push_str("\r\n\r\n");
    conn.write_all(head.as_bytes()).map_err(fail)?;
    conn.write_all(&request.body).map_err(fail)?;
    conn.flush().map_err(fail)?;

    let mut reader = BufReader::with_capacity(16 * 1024, conn);
    let (status, headers) = loop {
        let line = read_line(&mut reader, MAX_HEAD_BYTES).map_err(fail)?;
        let mut parts = line.splitn(3, ' ');
        let version = parts.next().unwrap_or("");
        let status = parts
            .next()
            .and_then(|code| code.parse::<u16>().ok())
            .filter(|_| version.starts_with("HTTP/1."))
            .ok_or_else(|| HttpError::Protocol(format!("bad status line {line:?}")))?;
        let mut headers = Vec::new();
        let mut total = line.len();
        loop {
            let line = read_line(&mut reader, MAX_HEAD_BYTES).map_err(fail)?;
            if line.is_empty() {
                break;
            }
            total += line.len();
            if total > MAX_HEAD_BYTES {
                return Err(HttpError::Protocol("response head too large".to_owned()));
            }
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| HttpError::Protocol(format!("bad header line {line:?}")))?;
            headers.push((name.trim().to_owned(), value.trim().to_owned()));
        }
        if status >= 200 {
            break (status, headers);
        }
    };

    let find = |name: &str| {
        headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    };
    if let Some(encoding) = find("content-encoding")
        && !encoding.eq_ignore_ascii_case("identity")
    {
        return Err(HttpError::Protocol(format!(
            "unsupported content-encoding {encoding:?}"
        )));
    }
    let framing =
        if find("transfer-encoding").is_some_and(|v| v.to_ascii_lowercase().contains("chunked")) {
            Framing::Chunked {
                remaining: 0,
                done: false,
            }
        } else if let Some(length) = find("content-length") {
            Framing::Length(
                length
                    .parse::<u64>()
                    .map_err(|_| HttpError::Protocol(format!("bad content-length {length:?}")))?,
            )
        } else if status == 204 || status == 304 || request.method == "HEAD" {
            Framing::Length(0)
        } else {
            Framing::Close
        };
    Ok(Response {
        status,
        headers,
        body: Body {
            inner: reader,
            framing,
            cancel: cancel.cloned(),
            _subscription: subscription,
        },
    })
}

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub max_delay: Option<Duration>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 2,
            max_delay: Some(Duration::from_secs(60)),
        }
    }
}

fn jitter() -> f64 {
    let bits = RandomState::new().build_hasher().finish();
    (bits >> 11) as f64 / (1u64 << 53) as f64
}

pub fn status_retryable(status: u16) -> bool {
    matches!(status, 408 | 409 | 429) || status >= 500
}

pub fn send_retrying(
    request: &Request,
    cancel: Option<&CancelToken>,
    policy: RetryPolicy,
) -> Result<Response, HttpError> {
    let mut attempt = 0u32;
    loop {
        let delay = match send(request, cancel) {
            Ok(response) => {
                if !status_retryable(response.status) || attempt >= policy.max_retries {
                    return Ok(response);
                }
                let requested = response.retry_after();
                if let (Some(requested), Some(max)) = (requested, policy.max_delay)
                    && !max.is_zero()
                    && requested > max
                {
                    return Ok(response);
                }
                requested
                    .filter(|d| *d <= Duration::from_secs(60))
                    .unwrap_or_else(|| backoff(attempt))
            }
            Err(error) => {
                if !error.retryable() || attempt >= policy.max_retries {
                    return Err(error);
                }
                backoff(attempt)
            }
        };
        attempt += 1;
        let cancelled = match cancel {
            Some(token) => token.wait_timeout(delay),
            None => {
                std::thread::sleep(delay);
                false
            }
        };
        if cancelled {
            return Err(HttpError::Cancelled);
        }
    }
}

fn backoff(attempt: u32) -> Duration {
    let base = (0.5 * 2f64.powi(attempt as i32)).min(8.0);
    Duration::from_secs_f64(base * (1.0 - 0.25 * jitter()))
}
