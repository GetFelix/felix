//! QUIC response writer loop.
//!
//! This module owns the *single-writer* side of a broker control stream.
//! The broker reads control frames (publish, subscribe, cache put/get, etc.) on the receive side,
//! and enqueues responses/acks onto an `mpsc` channel. A dedicated task runs this writer loop and
//! is the *only* code that ever writes to the `quinn::SendStream`.
//!
//! Why a single writer?
//! - `quinn::SendStream` does not support concurrent writes safely without external coordination.
//! - Serializing writes in one task eliminates interleaving/corruption and avoids mutex contention.
//! - Backpressure is applied via queue depth + throttle signals rather than blocking many tasks.
//!
//! Responsibilities:
//! - Drain `Outgoing` items (either JSON `Message` or binary `Frame` for cache fast-path replies),
//!   taking everything already queued and writing it in one call. It never waits for more.
//! - Maintain per-stream and global ack queue depth gauges.
//! - Toggle `ack_throttle` when depth crosses high/low watermarks.
//! - On any write/encode failure, initiate cooperative shutdown by signaling `cancel`.
//!
//! This file is intentionally small and hot-path oriented: comments explain invariants and
//! cancellation/backpressure behavior, while logic stays straightforward.

// Writer loop owns the SendStream and serializes all outbound responses.
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use felix_wire::{Frame, FrameHeader};
use quinn::SendStream;
use tokio::sync::{mpsc, watch};

use super::hooks::{
    check_frame_write_hook, encode_cache_message_with_hook, encode_message_with_hook,
    should_reset_throttle,
};
use crate::observability::timings;
use crate::serving::quic::GLOBAL_ACK_DEPTH;
use crate::serving::quic::client_error::ErrorCodeSupport;
use crate::serving::quic::handlers::publish::{AckOrder, Outgoing, decrement_depth};
use crate::serving::quic::telemetry::{t_counter, t_histogram, t_now_if, t_should_sample};

// Longest error text carried in a binary publish ack. Well under the u16 wire
// limit, and long enough for the broker's own messages ("forbidden",
// "tenant mismatch", "stream full", decode errors) to survive intact.
const MAX_ACK_MESSAGE_BYTES: usize = 512;

// Most answers taken off the queue for one write. Only what is already queued
// is taken, so this bounds a write's size, never adds a wait.
const WRITE_BATCH_MAX: usize = 64;

// Drains outgoing responses, updates depth counters, and handles shutdown on error.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_writer_loop(
    mut send: SendStream,
    mut out_ack_rx: mpsc::Receiver<Outgoing>,
    error_codes: Arc<ErrorCodeSupport>,
    ack_order: Arc<AckOrder>,
    stall_limit: Duration,
    out_ack_depth_worker: Arc<AtomicUsize>,
    ack_throttle_tx_writer: watch::Sender<bool>,
    cancel_tx_writer: watch::Sender<bool>,
    mut cancel_rx_writer: watch::Receiver<bool>,
) {
    let mut ready = Vec::with_capacity(WRITE_BATCH_MAX);
    let mut chunks = Vec::with_capacity(2 * WRITE_BATCH_MAX);
    loop {
        // Only a pipelining stream holds answers back, and only then can one
        // lost answer strand the rest.
        let stalled_at = ack_order
            .blocked_since()
            .map(|since| tokio::time::Instant::from_std(since + stall_limit));
        tokio::select! {
            changed = cancel_rx_writer.changed() => {
                if changed.is_err() || *cancel_rx_writer.borrow() {
                    break;
                }
            }
            _ = sleep_until_or_never(stalled_at) => {
                tracing::warn!(
                    ?stall_limit,
                    "closing control stream: a pipelined publish was never answered"
                );
                t_counter!("felix_broker_publish_order_stalls_total").increment(1);
                let _ = ack_throttle_tx_writer.send(false);
                let _ = cancel_tx_writer.send(true);
                break;
            }
            outgoing = out_ack_rx.recv() => {
                let Some(outgoing) = outgoing else { break };
                ack_order.release(outgoing, &mut ready);
                // Everything already queued goes out in the same write.
                let mut dequeued = 1;
                while dequeued < WRITE_BATCH_MAX {
                    let Ok(outgoing) = out_ack_rx.try_recv() else { break };
                    ack_order.release(outgoing, &mut ready);
                    dequeued += 1;
                }
                let failed = write_ready(&mut send, &mut ready, &mut chunks, &error_codes)
                    .await
                    .is_err();
                // One decrement per dequeue, however many answers it released.
                let mut depth_update: Option<(usize, usize)> = None;
                for _ in 0..dequeued {
                    if let Some((prev, cur)) = decrement_depth(
                        &out_ack_depth_worker,
                        &GLOBAL_ACK_DEPTH,
                        "felix_broker_out_ack_depth",
                    ) {
                        depth_update = Some((depth_update.map_or(prev, |(first, _)| first), cur));
                    }
                }
                if failed {
                    let _ = ack_throttle_tx_writer.send(false);
                    let _ = cancel_tx_writer.send(true);
                    break;
                }
                if should_reset_throttle(depth_update) {
                    let _ = ack_throttle_tx_writer.send(false);
                }
            }
        }
    }
    let _ = send.finish();
}

async fn sleep_until_or_never(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Encode every ready response and write them in one call. An error means the
/// stream is unusable.
async fn write_ready(
    send: &mut SendStream,
    ready: &mut Vec<Outgoing>,
    chunks: &mut Vec<Bytes>,
    error_codes: &ErrorCodeSupport,
) -> Result<(), ()> {
    if ready.is_empty() {
        return Ok(());
    }
    chunks.clear();
    let sample = t_should_sample();
    let mut encoded = Ok(());
    for outgoing in ready.drain(..) {
        if encoded.is_ok() {
            encoded = encode_outgoing(outgoing, error_codes, chunks, sample);
        }
    }
    encoded?;
    let write_start = t_now_if(sample);
    if let Err(err) = send.write_all_chunks(chunks.as_mut_slice()).await {
        tracing::info!(error = %err, "quic response stream closed");
        return Err(());
    }
    if let Some(start) = write_start {
        let write_ns = start.elapsed().as_nanos() as u64;
        timings::record_quic_write_ns(write_ns);
        t_histogram!("felix_broker_quic_write_ns").record(write_ns as f64);
    }
    Ok(())
}

/// Append one response's bytes to `chunks`.
fn encode_outgoing(
    outgoing: Outgoing,
    error_codes: &ErrorCodeSupport,
    chunks: &mut Vec<Bytes>,
    sample: bool,
) -> Result<(), ()> {
    #[cfg(not(feature = "telemetry"))]
    let _ = sample;
    match outgoing {
        Outgoing::Message(message) => {
            let message = error_codes.shape(message);
            #[cfg(feature = "telemetry")]
            let is_publish_ack = matches!(
                message,
                felix_wire::Message::PublishOk { .. } | felix_wire::Message::PublishError { .. }
            );
            let frame = match encode_message_with_hook(message) {
                Ok(frame) => frame,
                Err(err) => {
                    tracing::info!(error = %err, "encode response failed");
                    return Err(());
                }
            };
            push_frame(chunks, frame);
            #[cfg(feature = "telemetry")]
            {
                let counters = super::super::telemetry::frame_counters();
                counters.ack_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                if is_publish_ack {
                    counters.ack_items_out_ok.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Outgoing::PublishAck {
            request_id,
            error,
            code,
            detail,
            forwarded_to,
            offset,
        } => {
            // The ack's message field is u16-length on the wire. Bound it
            // here so an unusually long internal error can never make the
            // ack unencodable — dropping an ack strands a client that is
            // synchronously waiting for it.
            let error = error.as_deref().map(truncate_ack_message);
            let code = code
                .as_ref()
                .filter(|_| error_codes.binary_ack())
                .map(|(code, retry)| (code, *retry));
            let detail = detail.as_ref().filter(|_| error_codes.binary_ack_detail());
            let offset = offset.filter(|_| error_codes.ack_offset());
            let bytes = match felix_wire::binary::encode_publish_ack_bytes_at(
                request_id,
                error,
                code,
                detail,
                offset,
                forwarded_to.as_ref(),
            ) {
                Ok(bytes) => bytes,
                Err(err) => {
                    tracing::info!(error = %err, "encode publish ack failed");
                    return Err(());
                }
            };
            chunks.push(bytes);
            #[cfg(feature = "telemetry")]
            {
                let counters = super::super::telemetry::frame_counters();
                counters.ack_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                counters.ack_items_out_ok.fetch_add(1, Ordering::Relaxed);
            }
        }
        Outgoing::CacheMessage(message) => {
            let message = error_codes.shape(message);
            let encode_start = t_now_if(sample);
            let frame = match encode_cache_message_with_hook(message) {
                Ok(frame) => frame,
                Err(err) => {
                    tracing::info!(error = %err, "encode cache response failed");
                    return Err(());
                }
            };
            if let Some(start) = encode_start {
                timings::record_cache_encode_ns(start.elapsed().as_nanos() as u64);
            }
            if let Err(err) = check_frame_write_hook() {
                tracing::info!(error = %err, "quic response stream closed");
                return Err(());
            }
            push_frame(chunks, frame);
            #[cfg(feature = "telemetry")]
            {
                let counters = super::super::telemetry::frame_counters();
                counters.ack_frames_out_ok.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    Ok(())
}

fn push_frame(chunks: &mut Vec<Bytes>, frame: Frame) {
    let mut header = [0u8; FrameHeader::LEN];
    frame.header.encode_into(&mut header);
    #[cfg(feature = "telemetry")]
    {
        let counters = super::super::telemetry::frame_counters();
        counters.frames_out_ok.fetch_add(1, Ordering::Relaxed);
        counters.bytes_out.fetch_add(
            (FrameHeader::LEN + frame.payload.len()) as u64,
            Ordering::Relaxed,
        );
    }
    chunks.push(Bytes::copy_from_slice(&header));
    chunks.push(frame.payload);
}

// Truncate on a char boundary so the result stays valid UTF-8.
fn truncate_ack_message(message: &str) -> &str {
    if message.len() <= MAX_ACK_MESSAGE_BYTES {
        return message;
    }
    let mut end = MAX_ACK_MESSAGE_BYTES;
    while end > 0 && !message.is_char_boundary(end) {
        end -= 1;
    }
    &message[..end]
}
