//! The connection loop: read, decode, answer, write.

use std::convert::Infallible;
use std::time::Duration;

use bytes::BytesMut;
use felix_mqtt_wire::{DecodeError, Disconnect, Limits, Packet, ReasonCode, Version};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{Instant, sleep_until, timeout};
use tokio_util::sync::CancellationToken;

use super::Shared;
use super::connect::Session;
use super::publish::{MAX_QUOTA_HOLD, Publishing};
use crate::metrics;

/// A write that has not finished by now means the peer has stopped reading.
/// The connection is closed rather than held while it fills our buffers.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// Time allowed to flush a final packet before the socket is shut.
const GOODBYE_TIMEOUT: Duration = Duration::from_secs(2);
/// A client is closed when it is silent for this many keepalive intervals.
const KEEPALIVE_GRACE: f32 = 1.5;

/// Why a connection ends.
#[derive(Debug)]
pub(super) enum End {
    /// The peer closed it, or I/O failed, or it said `DISCONNECT`.
    Gone,
    /// Count it and close. What the client should hear is already queued.
    Close(&'static str),
    /// Tell a 5.0 client why, count it if a reason is given, and close.
    Disconnect(ReasonCode, Option<&'static str>),
}

pub(super) struct Connection<S> {
    pub(super) io: S,
    pub(super) inbound: BytesMut,
    pub(super) outbound: BytesMut,
    /// `None` until `CONNECT`.
    pub(super) version: Option<Version>,
}

pub(super) async fn serve<S>(
    shared: &Shared,
    stream: S,
    peer_certs: Vec<Vec<u8>>,
    shutdown: CancellationToken,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let _open = metrics::OpenConnection::new();
    let mut conn = Connection {
        io: stream,
        inbound: BytesMut::new(),
        outbound: BytesMut::new(),
        version: None,
    };
    let end = match conn.accept(shared, &peer_certs, &shutdown).await {
        Ok(session) => match conn.run(shared, session, &shutdown).await {
            Err(end) => end,
            Ok(never) => match never {},
        },
        Err(end) => end,
    };
    conn.finish(end).await;
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> Connection<S> {
    /// Answer packets until the connection ends.
    async fn run(
        &mut self,
        shared: &Shared,
        session: Session,
        shutdown: &CancellationToken,
    ) -> Result<Infallible, End> {
        let limits = shared.settings.limits();
        let version = session.version;
        let max_inflight = usize::from(shared.settings.max_inflight);
        let keepalive = session.keep_alive.mul_f32(KEEPALIVE_GRACE);

        let revoked = session.principal.revoked();
        tokio::pin!(revoked);
        let expiry = session
            .principal
            .expires_at()
            .map(|at| Instant::now() + until_unix(at));
        let expired = async {
            match expiry {
                Some(at) => sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(expired);
        let silent = sleep_until(Instant::now() + keepalive);
        tokio::pin!(silent);
        let mut publishing = Publishing::new(shared, &session);
        // Reading stops until then, to hold the tenant to its publish quota.
        let mut muted_until: Option<Instant> = None;

        loop {
            // One read can hold many packets, so the limit is checked per
            // packet as well as before reading.
            while publishing.pending() <= max_inflight
                && let Some(packet) = self.next_packet(Some(version), &limits)?
            {
                self.handle(shared, &session, &mut publishing, packet)?;
            }
            if let Some(hold) = publishing.start(&shared.writes)
                && !hold.is_zero()
            {
                muted_until = Some(Instant::now() + hold.min(MAX_QUOTA_HOLD));
            }
            self.release(&mut publishing)?;
            self.flush().await?;

            let now = Instant::now();
            let muted = muted_until.filter(|until| *until > now);
            // Stopped reading, by quota or because the broker is behind, TCP
            // pushes back on the client. It cannot ping meanwhile, so the
            // keepalive clock waits for reading to resume.
            // One more than the limit is read, so a 5.0 client that sends a
            // publish past `Receive Maximum` is seen doing it.
            let reading = muted.is_none() && publishing.pending() <= max_inflight;
            if !reading {
                silent.as_mut().reset(now + keepalive);
            }
            let unmute = async {
                match muted {
                    Some(until) => sleep_until(until).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    return Err(End::Disconnect(ReasonCode::ServerShuttingDown, None));
                }
                _ = &mut revoked => {
                    return Err(End::Disconnect(ReasonCode::NotAuthorized, Some("revoked")));
                }
                _ = &mut expired => {
                    return Err(End::Disconnect(ReasonCode::NotAuthorized, Some("token_expired")));
                }
                _ = &mut silent, if reading => {
                    return Err(End::Disconnect(ReasonCode::KeepAliveTimeout, Some("keepalive_timeout")));
                }
                result = publishing.completed(), if publishing.is_writing() => {
                    self.settle(&mut publishing, version, result)?;
                }
                _ = unmute => {}
                read = self.io.read_buf(&mut self.inbound), if reading => {
                    match read {
                        Ok(0) | Err(_) => return Err(End::Gone),
                        Ok(_) => silent.as_mut().reset(Instant::now() + keepalive),
                    }
                }
            }
        }
    }

    /// The next whole packet in the buffer, if there is one.
    pub(super) fn next_packet(
        &mut self,
        version: Option<Version>,
        limits: &Limits,
    ) -> Result<Option<Packet>, End> {
        Packet::decode(&mut self.inbound, version, limits).map_err(|err| decode_end(&err, version))
    }

    pub(super) fn queue(&mut self, packet: &Packet) -> Result<(), End> {
        // A connection that has not named a version has no 5.0 to refuse in.
        let version = self.version.unwrap_or(Version::V311);
        packet.encode(version, &mut self.outbound).map_err(|err| {
            tracing::warn!(error = %err, "mqtt reply could not be encoded");
            End::Close("internal_error")
        })
    }

    pub(super) async fn flush(&mut self) -> Result<(), End> {
        if self.outbound.is_empty() {
            return Ok(());
        }
        match timeout(WRITE_TIMEOUT, self.io.write_all_buf(&mut self.outbound)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(End::Gone),
            Err(_) => Err(End::Close("write_timeout")),
        }
    }

    /// Say goodbye, as far as the protocol allows, and close.
    async fn finish(&mut self, end: End) {
        let reason = match end {
            End::Gone => return,
            End::Close(reason) => {
                metrics::refused(reason);
                None
            }
            End::Disconnect(code, reason) => {
                if let Some(reason) = reason {
                    metrics::refused(reason);
                }
                Some(code)
            }
        };
        if let (Some(code), Some(Version::V5)) = (reason, self.version) {
            let _ = self.queue(&Packet::Disconnect(Disconnect::new(code)));
        }
        let _ = timeout(GOODBYE_TIMEOUT, async {
            if !self.outbound.is_empty() {
                let _ = self.io.write_all_buf(&mut self.outbound).await;
            }
            let _ = self.io.shutdown().await;
        })
        .await;
    }
}

/// How the end of a connection reads for a packet that could not be decoded.
fn decode_end(err: &DecodeError, version: Option<Version>) -> End {
    tracing::debug!(error = %err, "mqtt packet refused");
    let reason = match err {
        // Before `CONNECT` only a small packet is allowed, which is a
        // different failure from a large one after it.
        DecodeError::PacketTooLarge { .. } if version.is_none() => "unauthenticated_frame_size",
        DecodeError::PacketTooLarge { .. } => "packet_too_large",
        DecodeError::InvalidTopicName(_) => "invalid_topic_name",
        _ => match err.reason_code() {
            ReasonCode::MalformedPacket => "malformed_packet",
            ReasonCode::ProtocolError => "protocol_error",
            _ => "invalid_packet",
        },
    };
    End::Disconnect(err.reason_code(), Some(reason))
}

/// Time from now until `unix_secs`, zero if it has passed.
pub(super) fn until_unix(unix_secs: i64) -> Duration {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs() as i64);
    Duration::from_secs((unix_secs - now).max(0) as u64)
}
