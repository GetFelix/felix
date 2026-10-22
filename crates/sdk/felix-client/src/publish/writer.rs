//! The writer task that owns one publish stream.
//!
//! Quinn's `SendStream` is effectively single-writer: concurrent writes from
//! several tasks serialize on a lock inside Quinn. So each publish stream has
//! exactly one task writing to it, fed by a bounded queue, and pending acks
//! are resolved by that same task.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_wire::{AckMode, FrameHeader, Message};
use quinn::{RecvStream, SendStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use super::ack::read_ack;
use super::{AckOutcome, Acked};
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
    /// The publish window the broker granted, with answers in request order.
    /// `0` when it did not.
    pub(crate) publish_window: u32,
    /// The broker on this stream advertised `FEATURE_PUBLISH_WIDTH`. Asked
    /// per stream: one that did not would take `publish_width` as a frame it
    /// cannot decode and close the stream.
    pub(crate) takes_width: bool,
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
        // Read only when telemetry times the write.
        #[cfg_attr(not(feature = "telemetry"), allow(dead_code))]
        sample: bool,
        /// A framed `publish_width` naming the width a keyed batch was routed
        /// by. Written ahead of the batch unless it is the one this stream
        /// last carried.
        declaration: Option<Bytes>,
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
    /// This publish's slot in the publish window, freed with the answer.
    _window: Option<OwnedSemaphorePermit>,
    // Read only by the telemetry counters in the ack reader.
    #[cfg_attr(not(feature = "telemetry"), allow(dead_code))]
    batch_count: u64,
    #[cfg_attr(not(feature = "telemetry"), allow(dead_code))]
    item_count: u64,
}

/// The ack read in progress. It is kept across loop turns so an ack that is
/// half read when a new request arrives is not lost.
type AckFuture = Pin<Box<dyn Future<Output = AckReadDone> + Send>>;

/// The read half and its scratch buffer, handed back with the ack they read.
type AckReadDone = (RecvStream, BytesMut, Result<(u64, AckOutcome)>);

/// Frames the writer has taken off its queue but not yet written. They go out
/// as one write, so one wakeup of the connection carries the whole burst.
struct Staged {
    ack: AckMode,
    request_id: Option<u64>,
    response: oneshot::Sender<AckOutcome>,
    permit: OwnedSemaphorePermit,
    window: Option<OwnedSemaphorePermit>,
    batch_count: u64,
    item_count: u64,
    /// Bytes this publish adds to the stream, for the output counters.
    #[cfg_attr(not(feature = "telemetry"), allow(dead_code))]
    frame_bytes: u64,
}

/// How many writes a publish stream has made and how many publishes they
/// carried. Their ratio says how well queued publishes coalesce.
#[derive(Debug, Default)]
pub(crate) struct WriteStats {
    pub(crate) writes: AtomicU64,
    pub(crate) frames: AtomicU64,
}

/// A burst stops growing at this many bytes, about one connection send window
/// of small frames, so a deep queue still reaches the wire in steady pieces.
const BURST_BYTES: usize = 64 * 1024;
/// ... or at this many publishes, so tiny frames cannot hold a burst open.
const BURST_FRAMES: usize = 256;

pub(crate) async fn run_publisher_writer_with_limit(
    send: SendStream,
    recv: RecvStream,
    rx: mpsc::Receiver<PublishRequest>,
    chunk_bytes: usize,
    max_frame_bytes: usize,
    window: Option<Arc<Semaphore>>,
) -> Result<()> {
    run_publisher_writer_counted(
        send,
        recv,
        rx,
        chunk_bytes,
        max_frame_bytes,
        window,
        Arc::default(),
    )
    .await
}

pub(crate) async fn run_publisher_writer_counted(
    mut send: SendStream,
    recv: RecvStream,
    mut rx: mpsc::Receiver<PublishRequest>,
    _chunk_bytes: usize,
    max_frame_bytes: usize,
    window: Option<Arc<Semaphore>>,
    stats: Arc<WriteStats>,
) -> Result<()> {
    // Single writer: serialize publish requests over one bi-directional
    // stream. Everything already queued goes out as one write, and acks are
    // read while the queue is still being filled, so a stream never stalls
    // waiting for answers before it writes the next burst. A lone publish on
    // an idle writer is written at once: nothing waits for company.
    //
    // Pending acks are held here rather than handed to a reader task: a
    // publisher that awaits each publish before issuing the next has nothing
    // to pipeline, and a channel hop plus a task wakeup on that path is pure
    // latency (~2-3% of a loopback RTT). The ack read is a future this task
    // keeps across loop turns, so a half-read ack survives a new request
    // arriving first.
    //
    // Depth is bounded by the publisher's in-flight byte budget
    // (`publish_inflight_bytes`), since each entry holds its admission permit
    // until the broker answers, and by the publish window when the broker
    // granted one.
    let mut json_scratch = BytesMut::with_capacity(64 * 1024);
    let mut pending: VecDeque<PendingAck> = VecDeque::new();
    let mut staged: Vec<Staged> = Vec::new();
    let mut chunks: Vec<Bytes> = Vec::new();
    let mut staged_bytes = 0usize;
    #[cfg(feature = "telemetry")]
    let mut sampled = false;
    // The ack future owns the read half while it runs, so it can outlive any
    // one loop turn without borrowing from this frame.
    let mut idle_recv = Some((recv, BytesMut::with_capacity(64 * 1024)));
    let mut ack_read: Option<AckFuture> = None;
    // The `publish_width` the broker holds for this stream. One width's
    // declaration is one allocation, so the pointer says whether it is sent.
    let mut declared: Option<Bytes> = None;

    let mut finish_response: Option<oneshot::Sender<AckOutcome>> = None;
    // The request taken off the queue that is not yet staged: it may be
    // waiting on a window slot when the stream breaks.
    let mut held: Option<PublishRequest> = None;
    let mut header_scratch = BytesMut::with_capacity(1024);

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
            // Whatever was taken off the queue but never written gets the
            // error too, not a dropped channel.
            for entry in staged.drain(..) {
                let _ = entry.response.send(Err(anyhow::anyhow!(message.clone())));
            }
            if let Some(request) = held.take() {
                match request {
                    PublishRequest::Message { response, .. }
                    | PublishRequest::BinaryBytes { response, .. }
                    | PublishRequest::Finish { response } => {
                        let _ = response.send(Err(anyhow::anyhow!(message.clone())));
                    }
                }
            }
            if let Some(response) = finish_response.take() {
                let _ = response.send(Err(anyhow::anyhow!(message.clone())));
            }
            drain_publish_queue(&mut rx, &message).await;
            return Err(anyhow::anyhow!(message));
        }};
    }
    // Hand one ack to the request it names. The broker answers each publish
    // as it completes, not necessarily in the order they were written, so an
    // answer goes to the request it names. One naming no outstanding request
    // means the stream cannot be trusted, and neither can anything waiting
    // on it.
    macro_rules! apply_ack {
        ($read:expr) => {{
            match $read {
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
        }};
    }
    macro_rules! start_ack_read {
        () => {{
            if ack_read.is_none() {
                let (mut half, mut scratch) = idle_recv.take().expect("recv half is idle");
                ack_read = Some(Box::pin(async move {
                    let answer = read_ack(&mut half, &mut scratch, max_frame_bytes).await;
                    (half, scratch, answer)
                }));
            }
            ack_read.as_mut().expect("ack read started")
        }};
    }
    macro_rules! finish_ack_read {
        ($done:expr) => {{
            let (half, scratch, answer) = $done;
            idle_recv = Some((half, scratch));
            ack_read = None;
            answer
        }};
    }
    macro_rules! resolve_one {
        () => {{
            let done = start_ack_read!().await;
            apply_ack!(finish_ack_read!(done));
        }};
    }
    macro_rules! resolve_pending {
        () => {{
            while !pending.is_empty() {
                resolve_one!();
            }
        }};
    }
    // Write everything staged as one write, then move each publish on to
    // waiting for its ack (or answer it, when it asked for none).
    macro_rules! flush {
        () => {{
            if !staged.is_empty() {
                #[cfg(feature = "telemetry")]
                let write_start = crate::telemetry::t_now_if(std::mem::take(&mut sampled));
                let write_result = send
                    .write_all_chunks(&mut chunks)
                    .await
                    .context("write publish frames");
                #[cfg(feature = "telemetry")]
                if let Some(start) = write_start {
                    let write_ns = start.elapsed().as_nanos() as u64;
                    timings::record_write_ns(write_ns);
                    timings::record_send_await_ns(write_ns);
                    t_histogram!("felix_client_write_ns").record(write_ns as f64);
                    t_histogram!("client_send_await_ns").record(write_ns as f64);
                }
                #[cfg(feature = "telemetry")]
                t_histogram!("felix_client_frames_per_write").record(staged.len() as f64);
                chunks.clear();
                staged_bytes = 0;
                stats
                    .writes
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                stats
                    .frames
                    .fetch_add(staged.len() as u64, std::sync::atomic::Ordering::Relaxed);
                match write_result {
                    Ok(()) => {
                        let mut missing_id = false;
                        for entry in staged.drain(..) {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                counters
                                    .bytes_out
                                    .fetch_add(entry.frame_bytes, Ordering::Relaxed);
                            }
                            if entry.ack == AckMode::None {
                                #[cfg(feature = "telemetry")]
                                {
                                    let counters = frame_counters();
                                    counters.pub_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                    if entry.batch_count > 0 {
                                        counters
                                            .pub_batches_out_ok
                                            .fetch_add(entry.batch_count, Ordering::Relaxed);
                                        counters
                                            .pub_items_out_ok
                                            .fetch_add(entry.item_count, Ordering::Relaxed);
                                    }
                                }
                                let _ = entry.response.send(Ok(Acked::default()));
                            } else if let Some(request_id) = entry.request_id {
                                pending.push_back(PendingAck {
                                    request_id,
                                    response: entry.response,
                                    _permit: entry.permit,
                                    _window: entry.window,
                                    batch_count: entry.batch_count,
                                    item_count: entry.item_count,
                                });
                            } else {
                                missing_id = true;
                                let _ = entry.response.send(Err(anyhow::anyhow!(
                                    "missing request_id for acked publish"
                                )));
                            }
                        }
                        if missing_id {
                            fail_worker!("missing request_id for acked publish".to_string());
                        }
                    }
                    Err(err) => {
                        let message = err.to_string();
                        let mut first = Some(err);
                        for entry in staged.drain(..) {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.pub_frames_out_err.fetch_add(1, Ordering::Relaxed);
                                if entry.batch_count > 0 {
                                    counters
                                        .pub_batches_out_err
                                        .fetch_add(entry.batch_count, Ordering::Relaxed);
                                    counters
                                        .pub_items_out_err
                                        .fetch_add(entry.item_count, Ordering::Relaxed);
                                }
                            }
                            let err = first
                                .take()
                                .unwrap_or_else(|| anyhow::anyhow!(message.clone()));
                            let _ = entry.response.send(Err(err));
                        }
                        fail_worker!(message);
                    }
                }
            }
        }};
    }
    loop {
        let first = if pending.is_empty() {
            match rx.recv().await {
                Some(request) => request,
                None => break,
            }
        } else {
            // Acks outstanding: read them as they come while still taking
            // new work, so the writer never waits out an ack barrier.
            tokio::select! {
                biased;
                done = start_ack_read!() => {
                    apply_ack!(finish_ack_read!(done));
                    continue;
                }
                request = rx.recv() => match request {
                    Some(request) => request,
                    None => {
                        resolve_pending!();
                        break;
                    }
                },
            }
        };
        let mut next = Some(first);
        let mut queue_closed = false;
        while let Some(request) = next.take() {
            // A slot in the publish window before an acked publish goes out.
            // While none is free, write what is staged and settle this
            // stream's own answers: they may be what holds the slots, and
            // waiting on them unread would deadlock.
            let needs_ack = match &request {
                PublishRequest::Message { ack, .. } | PublishRequest::BinaryBytes { ack, .. } => {
                    *ack != AckMode::None
                }
                PublishRequest::Finish { .. } => false,
            };
            held = Some(request);
            let window_permit = match window.as_ref().filter(|_| needs_ack) {
                None => None,
                Some(window) => loop {
                    match Arc::clone(window).try_acquire_owned() {
                        Ok(permit) => break Some(permit),
                        Err(_) if !staged.is_empty() => flush!(),
                        Err(_) if !pending.is_empty() => resolve_one!(),
                        Err(_) => break Arc::clone(window).acquire_owned().await.ok(),
                    }
                },
            };
            let request = held.take().expect("request is held");
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
                        // Encoding a whole burst here delays the first write; the
                        // JSON path is the compatibility path, not the fast one.
                        let json_len = match felix_wire::text::publish_batch_json_len(
                            &tenant_id,
                            &namespace,
                            &stream,
                            &payloads,
                            key.as_deref(),
                            msg_request_id,
                            msg_ack,
                        ) {
                            Ok(len) => len,
                            Err(err) => {
                                let message = err.to_string();
                                let _ = response.send(Err(anyhow::anyhow!(message.clone())));
                                fail_worker!(message);
                            }
                        };
                        json_scratch.clear();
                        json_scratch.reserve(FrameHeader::LEN + json_len);
                        json_scratch.resize(FrameHeader::LEN, 0);
                        #[cfg(feature = "telemetry")]
                        let encode_start = crate::telemetry::t_now_if(sample);
                        let stats = match felix_wire::text::write_publish_batch_json(
                            &mut json_scratch,
                            &tenant_id,
                            &namespace,
                            &stream,
                            &payloads,
                            key.as_deref(),
                            msg_request_id,
                            msg_ack,
                        ) {
                            Ok(stats) => stats,
                            Err(err) => {
                                let message = err.to_string();
                                let _ = response.send(Err(anyhow::anyhow!(message.clone())));
                                fail_worker!(message);
                            }
                        };
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
                        staged_bytes += bytes.len();
                        staged.push(Staged {
                            ack,
                            request_id,
                            response,
                            permit: _permit,
                            window: window_permit,
                            batch_count: 1,
                            item_count: payloads.len() as u64,
                            frame_bytes: bytes.len() as u64,
                        });
                        chunks.push(bytes);
                    }
                    other => match other.encode().context("encode message") {
                        Ok(frame) => {
                            let (batch_count, item_count) = match &other {
                                Message::Publish { .. } => (1u64, 1u64),
                                Message::PublishBatch { payloads, .. } => {
                                    (1u64, payloads.len() as u64)
                                }
                                _ => (0u64, 0u64),
                            };
                            let mut header_bytes = [0u8; FrameHeader::LEN];
                            frame.header.encode_into(&mut header_bytes);
                            let frame_bytes = FrameHeader::LEN + frame.payload.len();
                            staged_bytes += frame_bytes;
                            staged.push(Staged {
                                ack,
                                request_id,
                                response,
                                permit: _permit,
                                window: window_permit,
                                batch_count,
                                item_count,
                                frame_bytes: frame_bytes as u64,
                            });
                            // One shared allocation for every header in the burst.
                            header_scratch.extend_from_slice(&header_bytes);
                            chunks.push(header_scratch.split().freeze());
                            chunks.push(frame.payload);
                        }
                        Err(err) => {
                            let _ = response.send(Err(err));
                        }
                    },
                },
                PublishRequest::BinaryBytes {
                    bytes,
                    item_count,
                    sample,
                    declaration,
                    ack,
                    request_id,
                    _permit,
                    response,
                } => {
                    if let Some(declaration) = declaration.filter(|declaration| {
                        declared
                            .as_ref()
                            .is_none_or(|last| last.as_ptr() != declaration.as_ptr())
                    }) {
                        staged_bytes += declaration.len();
                        declared = Some(declaration.clone());
                        chunks.push(declaration);
                    }
                    #[cfg(feature = "telemetry")]
                    {
                        sampled |= sample;
                    }
                    #[cfg(not(feature = "telemetry"))]
                    let _ = sample;
                    staged_bytes += bytes.len();
                    staged.push(Staged {
                        ack,
                        request_id,
                        response,
                        permit: _permit,
                        window: window_permit,
                        batch_count: 1,
                        item_count: item_count as u64,
                        frame_bytes: bytes.len() as u64,
                    });
                    chunks.push(bytes);
                }
                PublishRequest::Finish { response } => {
                    finish_response = Some(response);
                    break;
                }
            }
            if staged_bytes >= BURST_BYTES || staged.len() >= BURST_FRAMES {
                break;
            }
            match rx.try_recv() {
                Ok(request) => next = Some(request),
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => queue_closed = true,
            }
        }
        flush!();
        if finish_response.is_some() || queue_closed {
            // Settle what is already written before closing, so those
            // callers get their acks rather than a stream teardown.
            resolve_pending!();
            break;
        }
    }
    drop(ack_read);
    let (mut recv, _) = idle_recv.expect("recv half is idle");
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
            Ok(()) => Ok(Acked::default()),
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
