//! The request loop: frames in, frames out, in order.

use anyhow::{Context, Result, bail};
use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesOrdered;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use super::Shared;
use crate::api::{self, Answer, Handled, Session};

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

/// The most answers a connection waits on at once. Past it the connection
/// stops reading, so a client that never reads its answers holds a bounded
/// amount of memory. Above the five requests an idempotent Kafka producer
/// keeps in flight.
pub(crate) const MAX_PIPELINED: usize = 8;

/// What the reader hands the writer, in request order.
type Queued<'a> = BoxFuture<'a, Result<Reply>>;

enum Reply {
    Answer(Answer),
    /// Every answer queued before this one is written.
    Drained(oneshot::Sender<()>),
}

pub(super) async fn serve<S>(
    shared: &Shared,
    stream: S,
    peer_certs: Vec<Vec<u8>>,
    shutdown: &CancellationToken,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    // A produce's claim is ordered, its commit wait is not: the reader claims
    // each request's place and queues its answer, and the writer waits on the
    // queued answers together and writes them in request order.
    let (reader, writer) = tokio::io::split(stream);
    let (queue, queued) = mpsc::channel(1);
    let read = read_requests(shared, reader, peer_certs, shutdown, queue);
    let write = write_answers(writer, queued);
    tokio::pin!(read, write);
    tokio::select! {
        read = &mut read => {
            // The answers already queued are still owed, but a connection the
            // reader gave up on (an ended credential, a bad frame) must not
            // stay open behind a client that stops reading.
            let written = match &read {
                Ok(()) => write.await,
                Err(_) => tokio::time::timeout(CLOSING_WRITE_LIMIT, write)
                    .await
                    .unwrap_or(Ok(())),
            };
            read.and(written)
        }
        // The writer only ends first when it fails.
        written = &mut write => written,
    }
}

async fn read_requests<'a, R>(
    shared: &'a Shared,
    mut stream: R,
    peer_certs: Vec<Vec<u8>>,
    shutdown: &CancellationToken,
    queue: mpsc::Sender<Queued<'a>>,
) -> Result<()>
where
    R: AsyncRead + Unpin + Send,
{
    let mut session = Session::new(shared, peer_certs);
    let auth_deadline = Instant::now() + shared.settings.auth_timeout;
    // Whether a produce was queued since the writer last drained.
    let mut pipelined = false;
    // Watched while idle and while a request is handled, so neither an idle
    // connection nor a long-poll fetch outlives the credential.
    let mut ended: BoxFuture<'static, ()> = Box::pin(std::future::pending());
    loop {
        if let Some(next) = session.take_end() {
            ended = next;
        }
        let authenticated = session.authenticated();
        let mut size = [0u8; 4];
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            () = &mut ended => return credential_ended(&queue).await,
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

        let parsed = api::parse(Bytes::from(frame))?;
        // Anything but a produce sees every earlier produce answered first, as
        // it would on a connection that answers one request at a time: a
        // fetch reads what was produced before it.
        if !parsed.pipelines() && std::mem::take(&mut pipelined) {
            drain(&queue).await?;
        }
        let handled = tokio::select! {
            biased;
            () = &mut ended => return credential_ended(&queue).await,
            handled = api::handle(shared, &mut session, parsed, shutdown) => handled?,
        };
        match handled {
            Handled::Later(answer) => {
                pipelined = true;
                send(
                    &queue,
                    Box::pin(async move { answer.await.map(Reply::Answer) }),
                )
                .await?;
            }
            Handled::Now(Answer::Close(reason)) => {
                drain(&queue).await?;
                bail!("closing the connection: {reason}");
            }
            Handled::Now(answer) => {
                send(
                    &queue,
                    Box::pin(std::future::ready(Ok(Reply::Answer(answer)))),
                )
                .await?;
            }
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

/// How long a connection being closed waits for its queued answers to be
/// written. A client that stops reading must not keep it open.
const CLOSING_WRITE_LIMIT: Duration = Duration::from_secs(5);

/// Close a connection whose credential was revoked or reached its end time,
/// once the answers already queued are written, or after
/// [`CLOSING_WRITE_LIMIT`].
async fn credential_ended(queue: &mpsc::Sender<Queued<'_>>) -> Result<()> {
    tracing::info!("closing a kafka connection: its credential ended");
    let _ = tokio::time::timeout(CLOSING_WRITE_LIMIT, drain(queue)).await;
    bail!("closing the connection: credential ended");
}

/// Queue an answer, waiting while the pipeline is full: the connection stops
/// reading until an earlier answer is written.
async fn send<'a>(queue: &mpsc::Sender<Queued<'a>>, answer: Queued<'a>) -> Result<()> {
    queue
        .send(answer)
        .await
        .map_err(|_| anyhow::anyhow!("the connection's writer stopped"))
}

/// Wait until every answer queued so far is written.
async fn drain<'a>(queue: &mpsc::Sender<Queued<'a>>) -> Result<()> {
    let (drained, written) = oneshot::channel();
    send(
        queue,
        Box::pin(std::future::ready(Ok(Reply::Drained(drained)))),
    )
    .await?;
    written
        .await
        .map_err(|_| anyhow::anyhow!("the connection's writer stopped"))
}

/// Write each queued answer in the order it was queued, waiting on up to
/// [`MAX_PIPELINED`] of them at once, until the reader is done and every
/// answer is written.
async fn write_answers<'a, W>(mut stream: W, mut queued: mpsc::Receiver<Queued<'a>>) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    let mut waiting = FuturesOrdered::new();
    let mut reading = true;
    loop {
        tokio::select! {
            biased;
            Some(reply) = waiting.next(), if !waiting.is_empty() => match reply? {
                Reply::Answer(Answer::Respond {
                    correlation_id,
                    header_version,
                    body,
                }) => {
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
                Reply::Answer(Answer::Silent | Answer::Close(_)) => {}
                Reply::Drained(drained) => {
                    let _ = drained.send(());
                }
            },
            next = queued.recv(), if reading && waiting.len() < MAX_PIPELINED => match next {
                Some(answer) => waiting.push_back(answer),
                None => reading = false,
            },
            else => return Ok(()),
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
