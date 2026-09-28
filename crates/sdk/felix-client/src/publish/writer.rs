//! The writer task that owns one publish stream.
//!
//! Quinn's `SendStream` is effectively single-writer: concurrent writes from
//! several tasks serialize on a lock inside Quinn. So each publish stream has
//! exactly one task writing to it, fed by a bounded queue, and pending acks
//! are resolved by that same task.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_wire::{AckMode, FrameHeader, Message};
use quinn::{RecvStream, SendStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use super::AckOutcome;
use super::ack::read_ack;
use crate::frame_io::write_frame_parts;
#[cfg(feature = "telemetry")]
use crate::telemetry::frame_counters;
#[cfg(feature = "telemetry")]
use crate::timings;

pub(crate) struct PublishWorker {
    pub(crate) tx: mpsc::Sender<PublishRequest>,
    pub(crate) handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
    pub(crate) request_counter: AtomicU64,
    /// Frame-flag bits the broker advertised for this stream during auth.
    pub(crate) server_flags: u16,
    /// The publish window the broker granted this stream's connection, with
    /// answers in request order. `0` when it did not.
    pub(crate) publish_window: u32,
}

pub(crate) enum PublishRequest {
    Message {
        message: Message,
        ack: AckMode,
        request_id: Option<u64>,
        _permit: OwnedSemaphorePermit,
        response: oneshot::Sender<AckOutcome>,
    },
    BinaryBytes {
        bytes: Bytes,
        item_count: usize,
        sample: bool,
        /// `AckMode::None` for fire-and-forget frames. Anything else means the
        /// frame was encoded with `FLAG_BINARY_PUBLISH_ACKED` and the writer must
        /// block for the broker's ack before reporting the result.
        ack: AckMode,
        request_id: Option<u64>,
        _permit: OwnedSemaphorePermit,
        response: oneshot::Sender<AckOutcome>,
    },
    Finish {
        response: oneshot::Sender<AckOutcome>,
    },
}

/// A publish that has been written to the stream but whose broker ack has not
/// arrived yet. The admission permit rides along so the in-flight byte budget
/// stays reserved until the broker answers, not merely until the frame is
/// written.
struct PendingAck {
    request_id: u64,
    response: oneshot::Sender<AckOutcome>,
    _permit: OwnedSemaphorePermit,
    /// This publish's slot in the connection's window, freed with the answer.
    _window: Option<OwnedSemaphorePermit>,
    // Read only by the telemetry counters in the ack reader.
    #[cfg_attr(not(feature = "telemetry"), allow(dead_code))]
    batch_count: u64,
    #[cfg_attr(not(feature = "telemetry"), allow(dead_code))]
    item_count: u64,
}

pub(crate) async fn run_publisher_writer_with_limit(
    mut send: SendStream,
    mut recv: RecvStream,
    mut rx: mpsc::Receiver<PublishRequest>,
    _chunk_bytes: usize,
    max_frame_bytes: usize,
    window: Option<Arc<Semaphore>>,
) -> Result<()> {
    // Single writer: serialize publish requests over one bi-directional
    // stream. Acked publishes pipeline — written back to back, their acks
    // resolved once the write queue drains — so concurrent
    // publishers on a stream are not capped at one request per round trip.
    //
    // Pending acks are held here rather than handed to a reader task: a
    // publisher that awaits each publish before issuing the next has nothing
    // to pipeline, and a channel hop plus a task wakeup on that path is pure
    // latency (~2-3% of a loopback RTT). With the queue in-task, the serial
    // case resolves its ack inline exactly as it did before pipelining, and
    // only a caller with work actually queued behind it pays for batching.
    //
    // Depth is bounded by the publisher's in-flight byte budget
    // (`publish_inflight_bytes`), since each entry holds its admission permit
    // until the broker answers, and by the connection's publish window when
    // the broker granted one.
    let mut ack_scratch = BytesMut::with_capacity(64 * 1024);
    let mut json_scratch = BytesMut::with_capacity(64 * 1024);
    let mut pending: VecDeque<PendingAck> = VecDeque::new();

    // On a broken stream: fail everything already written and everything
    // still queued, with the same message.
    macro_rules! fail_worker {
        ($message:expr) => {{
            let message: String = $message;
            for stale in pending.drain(..) {
                #[cfg(feature = "telemetry")]
                {
                    let counters = frame_counters();
                    counters.pub_frames_out_err.fetch_add(1, Ordering::Relaxed);
                    counters
                        .pub_batches_out_err
                        .fetch_add(stale.batch_count, Ordering::Relaxed);
                    counters
                        .pub_items_out_err
                        .fetch_add(stale.item_count, Ordering::Relaxed);
                }
                let _ = stale.response.send(Err(anyhow::anyhow!(message.clone())));
            }
            drain_publish_queue(&mut rx, &message).await;
            return Err(anyhow::anyhow!(message));
        }};
    }
    // Resolve every outstanding ack. The broker answers each publish as it
    // completes, not in the order they were written, so an answer goes to the
    // request it names. One naming no outstanding request means the stream
    // cannot be trusted, and neither can anything waiting on it.
    macro_rules! resolve_one {
        () => {{
            {
                match read_ack(&mut recv, &mut ack_scratch, max_frame_bytes).await {
                    Ok((answered, answer)) => {
                        let Some(entry) = pending
                            .iter()
                            .position(|entry| entry.request_id == answered)
                            .and_then(|index| pending.remove(index))
                        else {
                            fail_worker!(format!(
                                "publish failed: ack for request {answered}, which is not outstanding"
                            ));
                        };
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            let (frames, batches, items) = if answer.is_ok() {
                                (
                                    &counters.pub_frames_out_ok,
                                    &counters.pub_batches_out_ok,
                                    &counters.pub_items_out_ok,
                                )
                            } else {
                                (
                                    &counters.pub_frames_out_err,
                                    &counters.pub_batches_out_err,
                                    &counters.pub_items_out_err,
                                )
                            };
                            frames.fetch_add(1, Ordering::Relaxed);
                            batches.fetch_add(entry.batch_count, Ordering::Relaxed);
                            items.fetch_add(entry.item_count, Ordering::Relaxed);
                        }
                        // A refusal answers this request only; the broker keeps
                        // serving the stream, so the others still get their own
                        // answers.
                        let _ = entry.response.send(answer);
                    }
                    Err(err) => fail_worker!(err.to_string()),
                }
            }
        }};
    }
    macro_rules! resolve_pending {
        () => {{
            while !pending.is_empty() {
                resolve_one!();
            }
        }};
    }
    macro_rules! submit_pending {
        ($pending:expr) => {{
            pending.push_back($pending);
        }};
    }
    let mut finish_response: Option<oneshot::Sender<AckOutcome>> = None;
    loop {
        let request = if pending.is_empty() {
            match rx.recv().await {
                Some(request) => request,
                None => break,
            }
        } else {
            // Acks outstanding: keep writing while work is immediately
            // available (that is what pipelining buys), otherwise settle up
            // before parking.
            match rx.try_recv() {
                Ok(request) => request,
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    resolve_pending!();
                    continue;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    resolve_pending!();
                    break;
                }
            }
        };
        // A slot in the connection's window before an acked publish goes out.
        // While none is free, settle this stream's own answers: they may be
        // what holds the slots, and waiting on them unread would deadlock.
        let needs_ack = match &request {
            PublishRequest::Message { ack, .. } | PublishRequest::BinaryBytes { ack, .. } => {
                *ack != AckMode::None
            }
            PublishRequest::Finish { .. } => false,
        };
        let mut window_permit = match window.as_ref().filter(|_| needs_ack) {
            None => None,
            Some(window) => loop {
                match Arc::clone(window).try_acquire_owned() {
                    Ok(permit) => break Some(permit),
                    Err(_) if !pending.is_empty() => resolve_one!(),
                    Err(_) => break Arc::clone(window).acquire_owned().await.ok(),
                }
            },
        };
        match request {
            PublishRequest::Message {
                message,
                ack,
                request_id,
                _permit,
                response,
            } => match message {
                Message::PublishBatch {
                    tenant_id,
                    namespace,
                    stream,
                    payloads,
                    key,
                    request_id: msg_request_id,
                    ack: msg_ack,
                } => {
                    #[cfg(feature = "telemetry")]
                    let sample = crate::telemetry::t_should_sample();
                    #[cfg(not(feature = "telemetry"))]
                    let sample = false;
                    #[cfg(not(feature = "telemetry"))]
                    let _ = sample;
                    let json_len = felix_wire::text::publish_batch_json_len(
                        &tenant_id,
                        &namespace,
                        &stream,
                        &payloads,
                        key.as_deref(),
                        msg_request_id,
                        msg_ack,
                    )?;
                    json_scratch.clear();
                    json_scratch.reserve(FrameHeader::LEN + json_len);
                    json_scratch.resize(FrameHeader::LEN, 0);
                    #[cfg(feature = "telemetry")]
                    let encode_start = crate::telemetry::t_now_if(sample);
                    let stats = felix_wire::text::write_publish_batch_json(
                        &mut json_scratch,
                        &tenant_id,
                        &namespace,
                        &stream,
                        &payloads,
                        key.as_deref(),
                        msg_request_id,
                        msg_ack,
                    )?;
                    #[cfg(not(feature = "telemetry"))]
                    let _ = stats;
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = encode_start {
                        let encode_ns = start.elapsed().as_nanos() as u64;
                        timings::record_encode_ns(encode_ns);
                        timings::record_text_encode_ns(encode_ns);
                        t_histogram!("felix_client_encode_ns").record(encode_ns as f64);
                    }
                    #[cfg(feature = "telemetry")]
                    if stats.reallocs > 0 {
                        let counters = frame_counters();
                        counters
                            .text_encode_reallocs
                            .fetch_add(stats.reallocs, Ordering::Relaxed);
                    }
                    #[cfg(feature = "telemetry")]
                    let build_start = crate::telemetry::t_now_if(sample);
                    let payload_len = json_scratch.len() - FrameHeader::LEN;
                    let header = FrameHeader::new(0, payload_len as u32);
                    let mut header_bytes = [0u8; FrameHeader::LEN];
                    header.encode_into(&mut header_bytes);
                    json_scratch[..FrameHeader::LEN].copy_from_slice(&header_bytes);
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = build_start {
                        let build_ns = start.elapsed().as_nanos() as u64;
                        timings::record_text_batch_build_ns(build_ns);
                        t_histogram!("client_text_batch_build_ns").record(build_ns as f64);
                    }
                    let bytes = json_scratch.split().freeze();
                    #[cfg(feature = "telemetry")]
                    let write_start = crate::telemetry::t_now_if(sample);
                    #[cfg(feature = "telemetry")]
                    let await_start = crate::telemetry::t_now_if(sample);
                    let write_result = send
                        .write_all(&bytes)
                        .await
                        .context("write publish batch frame");
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = await_start {
                        let await_ns = start.elapsed().as_nanos() as u64;
                        timings::record_send_await_ns(await_ns);
                        t_histogram!("client_send_await_ns").record(await_ns as f64);
                    }
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = write_start {
                        let write_ns = start.elapsed().as_nanos() as u64;
                        timings::record_write_ns(write_ns);
                        t_histogram!("felix_client_write_ns").record(write_ns as f64);
                    }
                    let item_count = payloads.len() as u64;
                    #[cfg(not(feature = "telemetry"))]
                    let _ = item_count;
                    match write_result {
                        Ok(()) => {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                counters
                                    .bytes_out
                                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                            }
                            if ack == AckMode::None {
                                #[cfg(feature = "telemetry")]
                                {
                                    let counters = frame_counters();
                                    counters.pub_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                    counters.pub_batches_out_ok.fetch_add(1, Ordering::Relaxed);
                                    counters
                                        .pub_items_out_ok
                                        .fetch_add(item_count, Ordering::Relaxed);
                                }
                                let _ = response.send(Ok(None));
                            } else if let Some(request_id) = request_id {
                                submit_pending!(PendingAck {
                                    request_id,
                                    response,
                                    _permit,
                                    _window: window_permit.take(),
                                    batch_count: 1,
                                    item_count,
                                });
                            } else {
                                let message = "missing request_id for acked publish".to_string();
                                let _ = response.send(Err(anyhow::anyhow!(message.clone())));
                                fail_worker!(message);
                            }
                        }
                        Err(err) => {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.pub_frames_out_err.fetch_add(1, Ordering::Relaxed);
                                counters.pub_batches_out_err.fetch_add(1, Ordering::Relaxed);
                                counters
                                    .pub_items_out_err
                                    .fetch_add(item_count, Ordering::Relaxed);
                            }
                            let message = err.to_string();
                            let _ = response.send(Err(err));
                            fail_worker!(message);
                        }
                    }
                }
                other => {
                    #[cfg(feature = "telemetry")]
                    let sample = crate::telemetry::t_should_sample();
                    #[cfg(feature = "telemetry")]
                    let encode_start = crate::telemetry::t_now_if(sample);
                    let frame = match other.encode().context("encode message") {
                        Ok(frame) => frame,
                        Err(err) => {
                            let _ = response.send(Err(err));
                            continue;
                        }
                    };
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = encode_start {
                        let encode_ns = start.elapsed().as_nanos() as u64;
                        timings::record_encode_ns(encode_ns);
                        t_histogram!("felix_client_encode_ns").record(encode_ns as f64);
                    }
                    #[cfg(feature = "telemetry")]
                    let write_start = crate::telemetry::t_now_if(sample);
                    let write_result = write_frame_parts(&mut send, &frame).await;
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = write_start {
                        let write_ns = start.elapsed().as_nanos() as u64;
                        timings::record_write_ns(write_ns);
                        t_histogram!("felix_client_write_ns").record(write_ns as f64);
                    }
                    let (batch_count, item_count) = match &other {
                        Message::Publish { .. } => (1u64, 1u64),
                        Message::PublishBatch { payloads, .. } => (1u64, payloads.len() as u64),
                        _ => (0u64, 0u64),
                    };
                    #[cfg(not(feature = "telemetry"))]
                    let _ = (batch_count, item_count);
                    match write_result {
                        Ok(()) => {
                            if ack == AckMode::None {
                                #[cfg(feature = "telemetry")]
                                {
                                    let counters = frame_counters();
                                    counters.pub_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                    if batch_count > 0 {
                                        counters
                                            .pub_batches_out_ok
                                            .fetch_add(batch_count, Ordering::Relaxed);
                                        counters
                                            .pub_items_out_ok
                                            .fetch_add(item_count, Ordering::Relaxed);
                                    }
                                }
                                let _ = response.send(Ok(None));
                            } else if let Some(request_id) = request_id {
                                submit_pending!(PendingAck {
                                    request_id,
                                    response,
                                    _permit,
                                    _window: window_permit.take(),
                                    batch_count,
                                    item_count,
                                });
                            } else {
                                let message = "missing request_id for acked publish".to_string();
                                let _ = response.send(Err(anyhow::anyhow!(message.clone())));
                                fail_worker!(message);
                            }
                        }
                        Err(err) => {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.pub_frames_out_err.fetch_add(1, Ordering::Relaxed);
                                if batch_count > 0 {
                                    counters
                                        .pub_batches_out_err
                                        .fetch_add(batch_count, Ordering::Relaxed);
                                    counters
                                        .pub_items_out_err
                                        .fetch_add(item_count, Ordering::Relaxed);
                                }
                            }
                            let message = err.to_string();
                            let _ = response.send(Err(err));
                            fail_worker!(message);
                        }
                    }
                }
            },
            PublishRequest::BinaryBytes {
                bytes,
                item_count,
                sample,
                ack,
                request_id,
                _permit,
                response,
            } => {
                #[cfg(not(feature = "telemetry"))]
                let _ = (item_count, sample);
                #[cfg(feature = "telemetry")]
                let write_start = crate::telemetry::t_now_if(sample);
                #[cfg(feature = "telemetry")]
                let chunk_start = crate::telemetry::t_now_if(sample);
                let write_result = send
                    .write_all(&bytes)
                    .await
                    .context("write binary batch frame");
                #[cfg(feature = "telemetry")]
                if let Some(start) = chunk_start {
                    let await_ns = start.elapsed().as_nanos() as u64;
                    timings::record_send_await_ns(await_ns);
                    t_histogram!("client_send_await_ns").record(await_ns as f64);
                }
                #[cfg(feature = "telemetry")]
                if let Some(start) = write_start {
                    let write_ns = start.elapsed().as_nanos() as u64;
                    timings::record_write_ns(write_ns);
                    t_histogram!("felix_client_write_ns").record(write_ns as f64);
                }
                match write_result {
                    Ok(()) => {
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            counters.frames_out_ok.fetch_add(1, Ordering::Relaxed);
                            counters
                                .bytes_out
                                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        }
                        if ack == AckMode::None {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.pub_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                counters.pub_batches_out_ok.fetch_add(1, Ordering::Relaxed);
                                counters
                                    .pub_items_out_ok
                                    .fetch_add(item_count as u64, Ordering::Relaxed);
                            }
                            let _ = response.send(Ok(None));
                        } else if let Some(request_id) = request_id {
                            submit_pending!(PendingAck {
                                request_id,
                                response,
                                _permit,
                                _window: window_permit.take(),
                                batch_count: 1,
                                item_count: item_count as u64,
                            });
                        } else {
                            let message = "missing request_id for acked publish".to_string();
                            let _ = response.send(Err(anyhow::anyhow!(message.clone())));
                            fail_worker!(message);
                        }
                    }
                    Err(err) => {
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            counters.pub_frames_out_err.fetch_add(1, Ordering::Relaxed);
                            counters.pub_batches_out_err.fetch_add(1, Ordering::Relaxed);
                            counters
                                .pub_items_out_err
                                .fetch_add(item_count as u64, Ordering::Relaxed);
                        }
                        let message = err.to_string();
                        let _ = response.send(Err(err));
                        fail_worker!(message);
                    }
                }
            }
            PublishRequest::Finish { response } => {
                // Settle what is already written before closing, so those
                // callers get their acks rather than a stream teardown.
                resolve_pending!();
                finish_response = Some(response);
                break;
            }
        }
    }
    // Graceful shutdown: close our send side, then drain the peer's side to
    // EOF — the same close protocol `finish_publisher_stream` implements.
    let finish_result = send.finish().map_err(anyhow::Error::from);
    let result = {
        let drain_result = async {
            let mut buf = [0u8; 8192];
            loop {
                match recv.read(&mut buf).await {
                    Ok(Some(_)) => continue,
                    Ok(None) => break,
                    Err(err) => return Err(err.into()),
                }
            }
            Ok(())
        }
        .await;
        finish_result.and(drain_result)
    };
    if let Some(response) = finish_response {
        let _ = response.send(match &result {
            Ok(()) => Ok(None),
            Err(err) => Err(anyhow::anyhow!(err.to_string())),
        });
    }
    result
}

#[cfg(test)]
pub(crate) async fn run_publisher_writer(
    send: SendStream,
    recv: RecvStream,
    rx: mpsc::Receiver<PublishRequest>,
    _chunk_bytes: usize,
) -> Result<()> {
    run_publisher_writer_with_limit(
        send,
        recv,
        rx,
        _chunk_bytes,
        crate::config::DEFAULT_MAX_FRAME_BYTES,
        None,
    )
    .await
}

pub(crate) async fn drain_publish_queue(rx: &mut mpsc::Receiver<PublishRequest>, message: &str) {
    while let Some(request) = rx.recv().await {
        match request {
            PublishRequest::Message { response, .. }
            | PublishRequest::BinaryBytes { response, .. }
            | PublishRequest::Finish { response } => {
                let _ = response.send(Err(anyhow::anyhow!(message.to_string())));
            }
        }
    }
}

#[cfg(test)]
pub(crate) async fn finish_publisher_stream(
    send: &mut SendStream,
    recv: &mut RecvStream,
) -> Result<()> {
    send.finish()?;
    let mut buf = [0u8; 8192];
    loop {
        match recv.read(&mut buf).await {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}
