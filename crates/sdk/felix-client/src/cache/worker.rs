//! The cache worker: one task that owns one bi-directional stream and runs
//! one request/response round trip at a time on it.
//!
//! Strictly sequential exchanges keep each response next to its request, so
//! matching them is just a request-id check. Callers reach a worker through a
//! bounded queue, which is what pushes back when it falls behind.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_wire::Message;
use quinn::{RecvStream, SendStream};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use super::{CacheConditionResult, VersionedValue};
use crate::frame_io::{read_frame_cache_timed_into_with_limit, write_frame_parts};
#[cfg(feature = "telemetry")]
use crate::timings;

pub(crate) struct CacheWorker {
    pub(crate) tx: mpsc::Sender<CacheRequest>,
    pub(crate) conn_index: usize,
}

pub(crate) enum CacheRequest {
    Put {
        request_id: u64,
        message: Message,
        response: oneshot::Sender<Result<()>>,
    },
    /// A request answered with `CacheValue`: a read, or a delete reporting what
    /// it removed. One variant because the exchange is identical — only the
    /// message sent differs, and the worker does not need to know which.
    Get {
        request_id: u64,
        message: Message,
        response: oneshot::Sender<Result<Option<Bytes>>>,
    },
    /// A request answered with `CounterValue`: an add reporting the sum it
    /// produced, or a read of the current one. Same exchange, different
    /// answer shape.
    Counter {
        request_id: u64,
        message: Message,
        response: oneshot::Sender<Result<Option<i64>>>,
    },
    /// A get that wants the value's version too.
    Versioned {
        request_id: u64,
        message: Message,
        response: oneshot::Sender<Result<Option<VersionedValue>>>,
    },
    /// A conditional put or delete, answered with `CacheConditionResult`.
    Conditional {
        request_id: u64,
        message: Message,
        response: oneshot::Sender<Result<CacheConditionResult>>,
    },
}

pub(crate) async fn run_cache_worker_with_limit(
    conn_index: usize,
    mut send: SendStream,
    mut recv: RecvStream,
    mut rx: mpsc::Receiver<CacheRequest>,
    cache_conn_counts: Arc<Vec<AtomicUsize>>,
    max_frame_bytes: usize,
) {
    // Single writer for a cache stream; handles sequential request/response pairs.
    let sample = crate::telemetry::t_should_sample();
    #[cfg(not(feature = "telemetry"))]
    let _ = sample;
    #[cfg(feature = "telemetry")]
    if sample {
        let open_ns = 0;
        timings::record_cache_open_stream_ns(open_ns);
    }
    let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
    debug!(conn_index, "cache worker started");
    while let Some(request) = rx.recv().await {
        debug!(conn_index, "cache worker received request");
        let result = handle_cache_request(
            &mut send,
            &mut recv,
            request,
            &mut frame_scratch,
            max_frame_bytes,
        )
        .await;

        // Decrement "in-flight ops" gauge for this connection, saturating at 0.
        let counter = &cache_conn_counts[conn_index];
        let mut current = counter.load(Ordering::Relaxed);
        while current > 0 {
            match counter.compare_exchange(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    t_gauge!("felix_client_cache_conn_ops", "conn" => conn_index.to_string())
                        .set((current - 1) as f64);
                    break;
                }
                Err(next) => current = next,
            }
        }

        if let Err(err) = result {
            debug!(conn_index, error = %err, "cache worker request failed");
            break;
        }
    }
    let _ = send.finish();
    debug!(conn_index, "cache worker exited");
}

async fn handle_cache_request(
    send: &mut SendStream,
    recv: &mut RecvStream,
    request: CacheRequest,
    frame_scratch: &mut BytesMut,
    max_frame_bytes: usize,
) -> Result<()> {
    let sample = crate::telemetry::t_should_sample();
    match request {
        CacheRequest::Put {
            request_id,
            message,
            response,
        } => {
            let result = cache_round_trip(
                send,
                recv,
                message,
                sample,
                request_id,
                frame_scratch,
                max_frame_bytes,
            )
            .await;
            match result {
                Ok(_) => {
                    let _ = response.send(Ok(()));
                    Ok(())
                }
                Err(err) => {
                    let stream = stream_outcome(&err);
                    let _ = response.send(Err(err));
                    stream
                }
            }
        }
        CacheRequest::Get {
            request_id,
            message,
            response,
        } => {
            let result = cache_round_trip(
                send,
                recv,
                message,
                sample,
                request_id,
                frame_scratch,
                max_frame_bytes,
            )
            .await;
            match result {
                Ok((value, _)) => {
                    let _ = response.send(Ok(value));
                    Ok(())
                }
                Err(err) => {
                    let stream = stream_outcome(&err);
                    let _ = response.send(Err(err));
                    stream
                }
            }
        }
        CacheRequest::Versioned {
            request_id,
            message,
            response,
        } => {
            let result = cache_round_trip(
                send,
                recv,
                message,
                sample,
                request_id,
                frame_scratch,
                max_frame_bytes,
            )
            .await;
            let stream = result.as_ref().err().map_or(Ok(()), stream_outcome);
            let _ = response.send(result.and_then(|(value, version)| match (value, version) {
                (Some(value), Some(version)) => Ok(Some(VersionedValue { value, version })),
                (None, _) => Ok(None),
                // A broker reaching an owner that predates versions answers
                // the read without one.
                (Some(_), None) => Err(anyhow::anyhow!(
                    "the broker answered without a version: the key's owner does not keep them"
                )),
            }));
            stream
        }
        CacheRequest::Conditional {
            request_id,
            message,
            response,
        } => {
            match conditional_round_trip(
                send,
                recv,
                message,
                sample,
                request_id,
                frame_scratch,
                max_frame_bytes,
            )
            .await
            {
                Ok(answer) => {
                    let _ = response.send(Ok(answer));
                    Ok(())
                }
                Err(err) => {
                    let stream = stream_outcome(&err);
                    let _ = response.send(Err(err));
                    stream
                }
            }
        }
        CacheRequest::Counter {
            request_id,
            message,
            response,
        } => {
            let result = counter_round_trip(
                send,
                recv,
                message,
                sample,
                request_id,
                frame_scratch,
                max_frame_bytes,
            )
            .await;
            match result {
                Ok(value) => {
                    let _ = response.send(Ok(value));
                    Ok(())
                }
                Err(err) => {
                    let stream = stream_outcome(&err);
                    let _ = response.send(Err(err));
                    stream
                }
            }
        }
    }
}

/// Whether the stream can take the next request after `err`.
///
/// A refusal the broker sent back is a whole answer, so the stream is still in
/// step and the worker keeps going. Anything else may have left half a frame
/// on it, so the worker stops.
fn stream_outcome(err: &anyhow::Error) -> Result<()> {
    if err.downcast_ref::<crate::error::BrokerError>().is_some() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("cache stream failed"))
    }
}

async fn cache_round_trip(
    send: &mut SendStream,
    recv: &mut RecvStream,
    message: Message,
    sample: bool,
    request_id: u64,
    frame_scratch: &mut BytesMut,
    max_frame_bytes: usize,
) -> Result<(Option<Bytes>, Option<u64>)> {
    // Encode -> write -> read -> decode in one stream round trip.
    let encode_start = crate::telemetry::t_now_if(sample);
    #[cfg(not(feature = "telemetry"))]
    let _ = encode_start;
    let frame = message.encode().context("encode message")?;
    #[cfg(feature = "telemetry")]
    if let Some(start) = encode_start {
        let encode_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_encode_ns(encode_ns);
    }
    let write_start = crate::telemetry::t_now_if(sample);
    #[cfg(not(feature = "telemetry"))]
    let _ = write_start;
    write_frame_parts(send, &frame).await?;
    #[cfg(feature = "telemetry")]
    if let Some(start) = write_start {
        let write_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_write_ns(write_ns);
    }
    let frame =
        match read_frame_cache_timed_into_with_limit(recv, sample, frame_scratch, max_frame_bytes)
            .await?
        {
            Some(frame) => frame,
            None => return Err(anyhow::anyhow!("cache response closed")),
        };
    let decode_start = crate::telemetry::t_now_if(sample);
    #[cfg(not(feature = "telemetry"))]
    let _ = decode_start;
    let response = Message::decode(frame).context("decode message")?;
    #[cfg(feature = "telemetry")]
    if let Some(start) = decode_start {
        let decode_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_decode_ns(decode_ns);
    }
    match response {
        Message::CacheOk {
            request_id: resp_id,
        } => {
            if resp_id != request_id {
                return Err(anyhow::anyhow!("cache put request id mismatch"));
            }
            Ok((None, None))
        }
        Message::Ok => Err(anyhow::anyhow!(
            "cache response missing request id (protocol violation)"
        )),
        Message::CacheValue {
            value,
            request_id: resp_id,
            version,
            ..
        } => {
            if let Some(resp_id) = resp_id
                && resp_id != request_id
            {
                return Err(anyhow::anyhow!("cache get request id mismatch"));
            }
            Ok((value, version))
        }
        Message::Error {
            message,
            code,
            retry,
            detail,
        } => Err(crate::error::refused(
            "cache error",
            message,
            code,
            retry,
            detail,
        )),
        other => Err(anyhow::anyhow!("cache response unexpected: {other:?}")),
    }
}

/// The counter exchange: identical plumbing, a `CounterValue` answer.
async fn counter_round_trip(
    send: &mut SendStream,
    recv: &mut RecvStream,
    message: Message,
    sample: bool,
    request_id: u64,
    frame_scratch: &mut BytesMut,
    max_frame_bytes: usize,
) -> Result<Option<i64>> {
    let frame = message.encode().context("encode message")?;
    write_frame_parts(send, &frame).await?;
    let frame =
        match read_frame_cache_timed_into_with_limit(recv, sample, frame_scratch, max_frame_bytes)
            .await?
        {
            Some(frame) => frame,
            None => return Err(anyhow::anyhow!("counter response closed")),
        };
    match Message::decode(frame).context("decode message")? {
        Message::CounterValue {
            value,
            request_id: resp_id,
        } => {
            if resp_id != request_id {
                return Err(anyhow::anyhow!("counter request id mismatch"));
            }
            Ok(value)
        }
        Message::Error {
            message,
            code,
            retry,
            detail,
        } => Err(crate::error::refused(
            "counter error",
            message,
            code,
            retry,
            detail,
        )),
        other => Err(anyhow::anyhow!("counter response unexpected: {other:?}")),
    }
}

/// The conditional-write exchange: a `CacheConditionResult` answer.
async fn conditional_round_trip(
    send: &mut SendStream,
    recv: &mut RecvStream,
    message: Message,
    sample: bool,
    request_id: u64,
    frame_scratch: &mut BytesMut,
    max_frame_bytes: usize,
) -> Result<CacheConditionResult> {
    let frame = message.encode().context("encode message")?;
    write_frame_parts(send, &frame).await?;
    let frame =
        match read_frame_cache_timed_into_with_limit(recv, sample, frame_scratch, max_frame_bytes)
            .await?
        {
            Some(frame) => frame,
            None => return Err(anyhow::anyhow!("cache response closed")),
        };
    match Message::decode(frame).context("decode message")? {
        Message::CacheConditionResult {
            applied,
            version,
            request_id: resp_id,
        } => {
            if resp_id != request_id {
                return Err(anyhow::anyhow!("cache condition request id mismatch"));
            }
            Ok(CacheConditionResult { applied, version })
        }
        Message::Error {
            message,
            code,
            retry,
            detail,
        } => Err(crate::error::refused(
            "cache error",
            message,
            code,
            retry,
            detail,
        )),
        other => Err(anyhow::anyhow!("cache response unexpected: {other:?}")),
    }
}
