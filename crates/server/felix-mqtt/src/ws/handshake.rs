//! The HTTP upgrade that turns a connection into a WebSocket (RFC 6455, 4.2).
//!
//! The request is read into a buffer capped at `max_header_bytes` and checked
//! before anything is allocated for it or answered, so a client that sends a
//! stream of header bytes, or something that is not HTTP at all, is dropped at
//! the cap.

use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::BytesMut;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The subprotocol name MQTT registered for WebSockets.
const SUBPROTOCOL: &str = "mqtt";
/// Mixed into the accept hash by RFC 6455.
const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
/// Most header lines one request may carry.
const MAX_HEADERS: usize = 64;
/// Bytes asked of the socket at a time.
const READ_CHUNK: usize = 1024;
/// Time allowed to write a refusal.
const REFUSAL_TIMEOUT: Duration = Duration::from_secs(2);

/// Which `Origin` a browser may connect from.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum AllowedOrigins {
    /// A request that carries an `Origin` is refused. Browsers always send
    /// one; most other clients do not.
    #[default]
    None,
    /// Any origin.
    Any,
    /// Exactly these, compared case-insensitively (`https://app.example.com`).
    Only(Vec<String>),
}

impl AllowedOrigins {
    fn allows(&self, origin: &str) -> bool {
        match self {
            Self::None => false,
            Self::Any => true,
            Self::Only(list) => list.iter().any(|ok| ok.eq_ignore_ascii_case(origin)),
        }
    }
}

/// What the upgrade is held to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    pub allowed_origins: AllowedOrigins,
    /// Largest request, request line and headers together.
    pub max_header_bytes: usize,
    /// Time from accept to a finished upgrade.
    pub upgrade_timeout: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            allowed_origins: AllowedOrigins::None,
            max_header_bytes: 8 * 1024,
            upgrade_timeout: Duration::from_secs(10),
        }
    }
}

/// An upgrade the broker will not serve: the HTTP status it answers with and a
/// short fixed reason, which is also the metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refusal {
    pub status: u16,
    pub reason: &'static str,
}

impl Refusal {
    const fn new(status: u16, reason: &'static str) -> Self {
        Self { status, reason }
    }

    fn response(&self) -> String {
        let (text, extra) = match self.status {
            403 => ("Forbidden", ""),
            408 => ("Request Timeout", ""),
            426 => (
                "Upgrade Required",
                "Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n",
            ),
            431 => ("Request Header Fields Too Large", ""),
            _ => ("Bad Request", ""),
        };
        format!(
            "HTTP/1.1 {} {text}\r\n{extra}Connection: close\r\nContent-Length: 0\r\n\r\n",
            self.status
        )
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "websocket upgrade refused ({}): {}",
            self.status, self.reason
        )
    }
}

impl std::error::Error for Refusal {}

/// Read the upgrade request, answer it, and return what the client sent after
/// it. On a refusal the refusal has been sent (best effort) and the stream
/// should be dropped.
pub(super) async fn accept<S>(stream: &mut S, settings: &Settings) -> Result<BytesMut, Refusal>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let outcome =
        match tokio::time::timeout(settings.upgrade_timeout, read_head(stream, settings)).await {
            Ok(Ok((head, rest))) => parse(&head, settings).map(|key| (key, rest)),
            Ok(Err(refusal)) => Err(refusal),
            Err(_) => Err(Refusal::new(408, "ws_upgrade_timeout")),
        };
    match outcome {
        Ok((key, rest)) => {
            let reply = format!(
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
                 Sec-WebSocket-Accept: {}\r\nSec-WebSocket-Protocol: {SUBPROTOCOL}\r\n\r\n",
                accept_key(&key)
            );
            let sent = tokio::time::timeout(REFUSAL_TIMEOUT, stream.write_all(reply.as_bytes()));
            match sent.await {
                Ok(Ok(())) => Ok(rest),
                _ => Err(Refusal::new(400, "ws_upgrade_write")),
            }
        }
        Err(refusal) => {
            // A client that is gone or not reading gets nothing more.
            let _ = tokio::time::timeout(REFUSAL_TIMEOUT, async {
                let _ = stream.write_all(refusal.response().as_bytes()).await;
                let _ = stream.shutdown().await;
            })
            .await;
            Err(refusal)
        }
    }
}

/// Read up to and including the blank line that ends the request head, and
/// the bytes that came after it.
async fn read_head<S>(stream: &mut S, settings: &Settings) -> Result<(Vec<u8>, BytesMut), Refusal>
where
    S: AsyncRead + Unpin,
{
    let mut buf = Vec::with_capacity(READ_CHUNK.min(settings.max_header_bytes));
    let mut chunk = [0u8; READ_CHUNK];
    let mut scanned = 0;
    loop {
        // A request line starts with its method. Anything else, MQTT sent to
        // this port by mistake included, is refused without waiting for more.
        if buf.len() >= 4 && !buf.starts_with(b"GET ") {
            return Err(Refusal::new(400, "ws_not_http"));
        }
        if let Some(end) = find_head_end(&buf, scanned) {
            let rest = BytesMut::from(&buf[end..]);
            buf.truncate(end);
            return Ok((buf, rest));
        }
        if buf.len() >= settings.max_header_bytes {
            return Err(Refusal::new(431, "ws_header_too_large"));
        }
        scanned = buf.len().saturating_sub(3);
        let room = (settings.max_header_bytes - buf.len()).min(READ_CHUNK);
        match stream.read(&mut chunk[..room]).await {
            Ok(0) | Err(_) => return Err(Refusal::new(400, "ws_closed_in_upgrade")),
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
}

/// Where the request head ends (just past the blank line), searching from
/// `from`.
fn find_head_end(buf: &[u8], from: usize) -> Option<usize> {
    buf.get(from..)?
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|at| from + at + 4)
}

/// Check a request head and return the client's `Sec-WebSocket-Key`.
pub fn parse(head: &[u8], settings: &Settings) -> Result<String, Refusal> {
    const BAD: fn(&'static str) -> Refusal = |reason| Refusal::new(400, reason);
    // Header values are ASCII here. Allowing anything else would only widen
    // what has to be reasoned about.
    if head
        .iter()
        .any(|&b| !(b == b'\r' || b == b'\n' || b == b'\t' || (0x20..0x7F).contains(&b)))
    {
        return Err(BAD("ws_bad_request"));
    }
    let text = std::str::from_utf8(head).map_err(|_| BAD("ws_bad_request"))?;
    // A bare CR or LF would be a line break to some parsers and not to this
    // one, so a header could be hidden from it.
    if text.split("\r\n").any(|line| line.contains(['\r', '\n'])) {
        return Err(BAD("ws_bad_request"));
    }
    let mut lines = text.split("\r\n");

    let request = lines.next().unwrap_or_default();
    let mut parts = request.split(' ');
    let (Some("GET"), Some(target), Some("HTTP/1.1"), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(BAD("ws_bad_request"));
    };
    if !target.starts_with('/') {
        return Err(BAD("ws_bad_request"));
    }

    let mut upgrade = false;
    let mut connection = false;
    let mut subprotocol = false;
    let mut version = None;
    let mut key = None;
    let mut origin = None;
    let mut count = 0;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        count += 1;
        if count > MAX_HEADERS {
            return Err(Refusal::new(431, "ws_header_too_large"));
        }
        // A folded header continues the previous line, which we do not allow.
        let Some((name, value)) = line.split_once(':') else {
            return Err(BAD("ws_bad_request"));
        };
        if name.is_empty() || name.contains([' ', '\t']) {
            return Err(BAD("ws_bad_request"));
        }
        let value = value.trim_matches([' ', '\t']);
        let once = |slot: &mut Option<String>| {
            if slot.replace(value.to_string()).is_some() {
                return Err(BAD("ws_bad_request"));
            }
            Ok(())
        };
        match name.to_ascii_lowercase().as_str() {
            "upgrade" => upgrade |= has_token(value, "websocket"),
            "connection" => connection |= has_token(value, "upgrade"),
            "sec-websocket-protocol" => subprotocol |= has_token(value, SUBPROTOCOL),
            "sec-websocket-version" => once(&mut version)?,
            "sec-websocket-key" => once(&mut key)?,
            "origin" => once(&mut origin)?,
            // The upgrade is a bodyless GET. A body would be read as frames.
            "content-length" if value != "0" => return Err(BAD("ws_bad_request")),
            "transfer-encoding" => return Err(BAD("ws_bad_request")),
            _ => {}
        }
    }

    if !upgrade || !connection {
        return Err(Refusal::new(426, "ws_not_upgrade"));
    }
    if version.as_deref() != Some("13") {
        return Err(Refusal::new(426, "ws_version"));
    }
    if let Some(origin) = origin
        && !settings.allowed_origins.allows(&origin)
    {
        return Err(Refusal::new(403, "ws_origin"));
    }
    if !subprotocol {
        return Err(BAD("ws_subprotocol"));
    }
    let key = key.ok_or(BAD("ws_bad_key"))?;
    match STANDARD.decode(&key) {
        Ok(raw) if raw.len() == 16 => Ok(key),
        _ => Err(BAD("ws_bad_key")),
    }
}

/// Whether the comma-separated list `value` has `token`, ignoring case.
fn has_token(value: &str, token: &str) -> bool {
    value
        .split(',')
        .any(|item| item.trim_matches([' ', '\t']).eq_ignore_ascii_case(token))
}

/// The `Sec-WebSocket-Accept` value for a client key.
fn accept_key(key: &str) -> String {
    let mut hash = Sha1::new();
    hash.update(key.as_bytes());
    hash.update(GUID.as_bytes());
    STANDARD.encode(hash.finalize())
}

#[cfg(test)]
pub(super) fn accept_key_for_test(key: &str) -> String {
    accept_key(key)
}
