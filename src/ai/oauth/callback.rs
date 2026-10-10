use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use crate::ai::oauth::query_param;
use crate::cancel::CancelToken;

const POLL: Duration = Duration::from_millis(50);
const READ_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_HEAD_BYTES: usize = 8 * 1024;
const CANCELLED: &str = "Login cancelled";

/// A bound loopback listener waiting for the browser's redirect.
pub struct Listener {
    listener: TcpListener,
    port: u16,
}

enum Step {
    Wait,
    Code(String),
    Failed(String),
}

impl Listener {
    /// Binds `host` on `preferred`, or on any free port if that one is taken.
    pub fn bind(host: &str, preferred: u16) -> io::Result<Self> {
        let listener =
            TcpListener::bind((host, preferred)).or_else(|_| TcpListener::bind((host, 0)))?;
        listener.set_nonblocking(true)?;
        let port = listener.local_addr()?.port();
        Ok(Self { listener, port })
    }

    /// The port actually bound.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Serves until a request on `path` carries `state` and a `code`, and returns the code.
    /// A request with the wrong state or no code gets an error page and the wait goes on; a provider
    /// `error` ends the wait with its description. Cancelling ends the wait within about one poll.
    /// The listener is closed when this returns.
    pub fn wait_code(
        self,
        provider: &str,
        path: &str,
        state: &str,
        cancel: &CancelToken,
    ) -> Result<String, String> {
        loop {
            if cancel.is_cancelled() {
                return Err(CANCELLED.to_owned());
            }
            match self.listener.accept() {
                Ok((stream, _)) => match serve(stream, provider, path, state) {
                    Ok(Step::Code(code)) => return Ok(code),
                    Ok(Step::Failed(error)) => return Err(error),
                    Ok(Step::Wait) | Err(_) => {}
                },
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if cancel.wait_timeout(POLL) {
                        return Err(CANCELLED.to_owned());
                    }
                }
                Err(error) => return Err(format!("OAuth callback server failed: {error}")),
            }
        }
    }
}

fn serve(mut stream: TcpStream, provider: &str, path: &str, state: &str) -> io::Result<Step> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    let head = read_head(&mut stream)?;
    let line = head.lines().next().unwrap_or("");
    let mut parts = line.split(' ');
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("");
    let (route, query) = target.split_once('?').unwrap_or((target, ""));
    if method != "GET" || route != path {
        respond(
            &mut stream,
            "404 Not Found",
            &page("Authentication failed", "Callback route not found.", None),
        )?;
        return Ok(Step::Wait);
    }
    if query_param(query, "state").as_deref() != Some(state) {
        respond(
            &mut stream,
            "400 Bad Request",
            &page("Authentication failed", "State mismatch.", None),
        )?;
        return Ok(Step::Wait);
    }
    if let Some(error) = query_param(query, "error") {
        let description = query_param(query, "error_description").unwrap_or(error);
        let message = format!("{provider} authorization failed.");
        respond(
            &mut stream,
            "400 Bad Request",
            &page("Authentication failed", &message, Some(&description)),
        )?;
        return Ok(Step::Failed(format!(
            "{provider} authorization failed: {description}"
        )));
    }
    let Some(code) = query_param(query, "code").filter(|code| !code.is_empty()) else {
        respond(
            &mut stream,
            "400 Bad Request",
            &page("Authentication failed", "Missing authorization code.", None),
        )?;
        return Ok(Step::Wait);
    };
    let message = format!("Signed in to {provider}. You may now close this page.");
    respond(
        &mut stream,
        "200 OK",
        &page("Authentication successful", &message, None),
    )?;
    Ok(Step::Code(code))
}

fn read_head(stream: &mut TcpStream) -> io::Result<String> {
    let mut head = Vec::new();
    let mut chunk = [0u8; 1024];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") && head.len() < MAX_HEAD_BYTES {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        head.extend_from_slice(&chunk[..read]);
    }
    Ok(String::from_utf8_lossy(&head).into_owned())
}

fn respond(stream: &mut TcpStream, status: &str, body: &str) -> io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\ncontent-type: text/html; charset=utf-8\r\ncache-control: no-store\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn page(heading: &str, message: &str, details: Option<&str>) -> String {
    let details = details
        .map(|details| format!("<pre>{}</pre>", escape(details)))
        .unwrap_or_default();
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>{h}</title></head>\
<body style=\"background:#09090b;color:#fafafa;font-family:system-ui,sans-serif;text-align:center;padding:24px\">\
<h1>{h}</h1><p>{m}</p>{details}</body></html>",
        h = escape(heading),
        m = escape(message),
    )
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
