//! The request loop: frames in, frames out, one at a time.

use anyhow::{Context, Result, bail};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::Shared;
use crate::api::{self, Answer, Session};

/// The largest request accepted. Well above what a producer sends by default
/// (librdkafka caps a request at `message.max.bytes`, 1 MB); the cap is what
/// keeps a hostile length prefix from allocating gigabytes.
const MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
/// The largest request accepted before the connection authenticates. All it
/// may send then is `ApiVersions` and the SASL exchange, a few hundred bytes.
pub(crate) const MAX_UNAUTHENTICATED_REQUEST_BYTES: usize = 64 * 1024;
/// A request body's buffer grows as its bytes arrive, from at most this much,
/// so a length prefix alone never commits memory the peer has not sent.
const INITIAL_BODY_CAPACITY: usize = 64 * 1024;

pub(super) async fn serve<S>(
    shared: &Shared,
    mut stream: S,
    shutdown: &CancellationToken,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut session = Session::new(shared);
    let auth_deadline = Instant::now() + shared.settings.auth_timeout;
    loop {
        let authenticated = session.authenticated();
        let mut size = [0u8; 4];
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = tokio::time::sleep_until(auth_deadline), if !authenticated => {
                return Err(auth_timed_out(shared));
            }
            read = stream.read_exact(&mut size) => {
                if read.is_err() {
                    // The client went away between requests: a normal close.
                    return Ok(());
                }
            }
        }
        let limit = if authenticated {
            MAX_REQUEST_BYTES
        } else {
            MAX_UNAUTHENTICATED_REQUEST_BYTES
        };
        let size = i32::from_be_bytes(size);
        let size = usize::try_from(size)
            .ok()
            .filter(|size| (8..=limit).contains(size))
            .with_context(|| {
                crate::metrics::refused(if authenticated {
                    "frame_size"
                } else {
                    "unauthenticated_frame_size"
                });
                format!("request size {size} is out of range (at most {limit} bytes)")
            })?;
        let mut frame = Vec::with_capacity(size.min(INITIAL_BODY_CAPACITY));
        let mut body = (&mut stream).take(size as u64);
        let read_body = body.read_to_end(&mut frame);
        let read = if authenticated {
            read_body.await
        } else {
            match tokio::time::timeout_at(auth_deadline, read_body).await {
                Ok(read) => read,
                Err(_) => return Err(auth_timed_out(shared)),
            }
        };
        read.context("read request body")?;
        if frame.len() != size {
            bail!("connection closed mid-request");
        }

        match api::handle(shared, &mut session, Bytes::from(frame), shutdown).await? {
            Answer::Respond {
                correlation_id,
                header_version,
                body,
            } => {
                let mut out = BytesMut::with_capacity(body.len() + 9);
                let header_len = if header_version >= 1 { 5 } else { 4 };
                out.put_i32((header_len + body.len()) as i32);
                out.put_i32(correlation_id);
                if header_version >= 1 {
                    // No tagged fields.
                    out.put_u8(0);
                }
                out.put_slice(&body);
                stream.write_all(&out).await.context("write response")?;
            }
            Answer::Silent => {}
            Answer::Close(reason) => bail!("closing the connection: {reason}"),
        }
        if session.closing() {
            return Ok(());
        }
        let throttle = session.take_throttle();
        if !throttle.is_zero() {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                _ = tokio::time::sleep(throttle) => {}
            }
        }
    }
}

fn auth_timed_out(shared: &Shared) -> anyhow::Error {
    crate::metrics::refused("auth_timeout");
    anyhow::anyhow!(
        "the connection did not authenticate within {:?}",
        shared.settings.auth_timeout
    )
}
