//! The two tasks behind a subscription: an I/O task that reads frames off
//! the event stream, and a dispatch task that decodes them into events.
//!
//! The I/O task runs colocated with the connection's transport drivers,
//! since it is woken per slice of arriving data; dispatch stays on the
//! application's runtime.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use anyhow::Context;
use bytes::{Bytes, BytesMut};
use felix_wire::{Frame, Message};
use quinn::RecvStream;
use tokio::sync::mpsc;

use super::queue::{Enqueued, enqueue_with_policy};
use super::{QueuedEvent, ShardMoved, Subscription};
use crate::config::ClientSubQueuePolicy;
use crate::connection::StreamLease;
use crate::frame_io::read_frame_into_with_limit;
#[cfg(feature = "telemetry")]
use crate::telemetry::frame_counters;
use crate::telemetry::log_decode_error;
#[cfg(feature = "telemetry")]
use crate::timings;
use crate::{SubscriptionLagged, SubscriptionLost};

pub(crate) struct SubscriptionPipelineConfig {
    pub(crate) recv: RecvStream,
    /// Connection whose transport drivers deliver this stream's data; the read
    /// task is colocated with them via `spawn_pump`.
    pub(crate) connection: felix_transport::QuicConnection,
    pub(crate) queue_capacity: usize,
    pub(crate) queue_policy: ClientSubQueuePolicy,
    pub(crate) subscription_id: u64,
    pub(crate) tenant_id: Arc<str>,
    pub(crate) namespace: Arc<str>,
    pub(crate) stream: Arc<str>,
    pub(crate) event_conn_index: usize,
    /// Counts the subscription against its connection until it is dropped.
    pub(crate) lease: StreamLease,
    pub(crate) event_conn_counts: Arc<Vec<AtomicUsize>>,
    pub(crate) max_frame_bytes: usize,
    /// The tail when the broker registered this subscription. Records below it
    /// are history the application asked for, and are never dropped.
    pub(crate) live_offset: Option<u64>,
    #[cfg(feature = "telemetry")]
    pub(crate) bench_embed_ts: bool,
}

impl Subscription {
    pub(crate) fn spawn_pipeline(config: SubscriptionPipelineConfig) -> Self {
        let capacity = config.queue_capacity.max(1);
        let (frame_tx, frame_rx) = mpsc::channel(capacity);
        let (event_tx, event_rx) = mpsc::channel(capacity);
        let shard_moved = Arc::new(OnceLock::new());
        let end = Arc::new(End::default());

        // The io task is woken per slice of arriving stream data, so it runs
        // colocated with the connection's drivers; dispatch has no
        // transport-facing wakeups and stays on the app runtime.
        config.connection.spawn_pump(run_subscription_io_task(
            config.recv,
            frame_tx,
            config.queue_policy,
            capacity,
            config.max_frame_bytes,
            config.live_offset,
            Arc::clone(&end),
        ));
        tokio::spawn(run_subscription_dispatch_task(
            frame_rx,
            event_tx,
            config.queue_policy,
            capacity,
            config.subscription_id,
            Arc::clone(&shard_moved),
            config.live_offset,
            end,
        ));

        Self {
            event_rx,
            #[cfg(feature = "telemetry")]
            last_poll: None,
            tenant_id: config.tenant_id,
            namespace: config.namespace,
            stream: config.stream,
            event_conn_index: config.event_conn_index,
            _lease: config.lease,
            event_conn_counts: config.event_conn_counts,
            #[cfg(feature = "telemetry")]
            bench_embed_ts: config.bench_embed_ts,
            start_offset: None,
            live_offset: None,
            queue_capacity: None,
            shard_moved,
        }
    }
}

/// Why a subscription's pipeline stopped early, for the dispatch task to
/// report once the events before it are queued.
struct End {
    /// The first offset either queue dropped. Both tasks stop at their first
    /// drop, so the lower of the two is where to resume.
    first_dropped: AtomicU64,
    /// The read failed.
    broken: Mutex<Option<anyhow::Error>>,
}

impl Default for End {
    fn default() -> Self {
        Self {
            first_dropped: AtomicU64::new(u64::MAX),
            broken: Mutex::new(None),
        }
    }
}

impl End {
    fn dropped(&self, offset: u64) {
        self.first_dropped.fetch_min(offset, Ordering::AcqRel);
    }

    fn broke(&self, err: anyhow::Error) {
        *self.broken.lock().unwrap_or_else(|e| e.into_inner()) = Some(err);
    }

    /// A drop is reported before a broken stream: the reader stopped at the
    /// drop, so the break is no news.
    fn take_error(&self) -> Option<anyhow::Error> {
        match self.first_dropped.load(Ordering::Acquire) {
            u64::MAX => self.broken.lock().unwrap_or_else(|e| e.into_inner()).take(),
            resume_from => Some(anyhow::Error::new(SubscriptionLagged { resume_from })),
        }
    }
}

struct QueuedFrame {
    frame: Frame,
    enqueued_at: Instant,
}

async fn run_subscription_io_task(
    mut recv: RecvStream,
    frame_tx: mpsc::Sender<QueuedFrame>,
    queue_policy: ClientSubQueuePolicy,
    queue_capacity: usize,
    max_frame_bytes: usize,
    live_offset: Option<u64>,
    end: Arc<End>,
) {
    let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
    #[cfg(feature = "telemetry")]
    let mut last_poll = Instant::now();
    loop {
        #[cfg(feature = "telemetry")]
        {
            let now = Instant::now();
            let poll_gap_ns = now.duration_since(last_poll).as_nanos() as u64;
            last_poll = now;
            timings::record_sub_poll_gap_ns(poll_gap_ns);
            t_histogram!("client_sub_poll_gap_ns").record(poll_gap_ns as f64);
        }

        let first =
            match read_frame_into_with_limit(&mut recv, &mut frame_scratch, true, max_frame_bytes)
                .await
            {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(err) => {
                    tracing::debug!(error = %err, "subscription io task stopped");
                    // Set before `frame_tx` drops, so dispatch sees it once the
                    // queued frames are drained. Reporting this as a clean end
                    // would let a consumer loop exit quietly on a dead broker.
                    let transport = err.chain().any(|cause| {
                        cause.is::<quinn::ReadError>() || cause.is::<quinn::ReadExactError>()
                    });
                    let err = if transport {
                        anyhow::Error::new(SubscriptionLost {
                            reason: format!("{err:#}"),
                        })
                    } else {
                        err.context("read subscription stream")
                    };
                    end.broke(err);
                    break;
                }
            };
        let control = first.header.flags == 0;
        let base_offset = felix_wire::binary::peek_event_batch_base_offset(&first);
        let history = is_history(base_offset, live_offset);
        let queued = QueuedFrame {
            frame: first,
            enqueued_at: Instant::now(),
        };
        // Events come in binary batches; a JSON frame can be `shard_moved`,
        // and dropping that would lose where to resume, so it waits for room.
        // So does replayed history: waiting here stops reading the stream,
        // which slows the broker's disk reads to the application's pace.
        let outcome = if control || history {
            match frame_tx.send(queued).await {
                Ok(()) => Enqueued::Queued,
                Err(_) => Enqueued::Closed,
            }
        } else {
            enqueue_frame(&frame_tx, queued, queue_policy, queue_capacity).await
        };
        match (outcome, base_offset) {
            (Enqueued::Queued, _) | (Enqueued::Dropped, None) => {}
            // A durable stream ends at the drop, so the subscriber learns of
            // it without waiting for a later event.
            (Enqueued::Dropped, Some(offset)) => {
                end.dropped(offset);
                break;
            }
            (Enqueued::Closed, _) => break,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_subscription_dispatch_task(
    mut frame_rx: mpsc::Receiver<QueuedFrame>,
    event_tx: mpsc::Sender<QueuedEvent>,
    queue_policy: ClientSubQueuePolicy,
    queue_capacity: usize,
    subscription_id: u64,
    shard_moved: Arc<OnceLock<ShardMoved>>,
    live_offset: Option<u64>,
    end: Arc<End>,
) {
    'frames: while let Some(queued_frame) = frame_rx.recv().await {
        let queue_wait_ns = queued_frame.enqueued_at.elapsed().as_nanos() as u64;
        #[cfg(feature = "telemetry")]
        {
            timings::record_sub_queue_wait_ns(queue_wait_ns);
            t_histogram!("client_sub_queue_wait_ns").record(queue_wait_ns as f64);
            timings::record_sub_time_in_queue_ns(queue_wait_ns);
            t_histogram!("client_sub_time_in_queue_ns").record(queue_wait_ns as f64);
        }
        #[cfg(not(feature = "telemetry"))]
        let _ = queue_wait_ns;
        metrics::counter!("felix_client_sub_queue_dequeued_total").increment(1);
        metrics::gauge!("felix_client_sub_queue_len")
            .set((queue_capacity.saturating_sub(frame_rx.capacity())) as f64);

        #[cfg(feature = "telemetry")]
        let sample = crate::telemetry::t_should_sample();
        #[cfg(feature = "telemetry")]
        let decode_start = crate::telemetry::t_now_if(sample);

        let (payloads, base_offset, skipped_before, publisher, timestamps) =
            if queued_frame.frame.header.flags & felix_wire::FLAG_BINARY_EVENT_BATCH_SHARED != 0 {
                match felix_wire::binary::decode_shared_event_batch(&queued_frame.frame)
                    .context("decode shared binary event batch")
                {
                    Ok(batch) => {
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            counters.sub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
                            counters.sub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
                            counters
                                .sub_items_in_ok
                                .fetch_add(batch.payloads.len() as u64, Ordering::Relaxed);
                        }
                        (
                            batch.payloads,
                            batch.base_offset,
                            batch.skipped_before,
                            publisher_of(batch.publisher),
                            batch.timestamps,
                        )
                    }
                    Err(err) => {
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                        }
                        log_decode_error("shared_binary_event_batch", &err, &queued_frame.frame);
                        let _ = enqueue_event(
                            &event_tx,
                            QueuedEvent::Error(err.context("decode shared binary event batch")),
                            queue_policy,
                            queue_capacity,
                        )
                        .await;
                        return;
                    }
                }
            } else if queued_frame.frame.header.flags & felix_wire::FLAG_BINARY_EVENT_BATCH != 0 {
                match felix_wire::binary::decode_event_batch(&queued_frame.frame)
                    .context("decode binary event batch")
                {
                    Ok(batch) => {
                        if batch.subscription_id != subscription_id {
                            tracing::debug!(
                                expected = subscription_id,
                                got = batch.subscription_id,
                                "subscription id mismatch in dispatch"
                            );
                            let _ = enqueue_event(
                                &event_tx,
                                QueuedEvent::Error(anyhow::anyhow!(
                                    "subscription id mismatch: expected {} got {}",
                                    subscription_id,
                                    batch.subscription_id
                                )),
                                queue_policy,
                                queue_capacity,
                            )
                            .await;
                            return;
                        }
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            counters.sub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
                            counters.sub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
                            counters
                                .sub_items_in_ok
                                .fetch_add(batch.payloads.len() as u64, Ordering::Relaxed);
                        }
                        (
                            batch.payloads,
                            batch.base_offset,
                            batch.skipped_before,
                            publisher_of(batch.publisher),
                            batch.timestamps,
                        )
                    }
                    Err(err) => {
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                        }
                        log_decode_error("binary_event_batch", &err, &queued_frame.frame);
                        let _ = enqueue_event(
                            &event_tx,
                            QueuedEvent::Error(err.context("decode binary event batch")),
                            queue_policy,
                            queue_capacity,
                        )
                        .await;
                        return;
                    }
                }
            } else {
                let message =
                    match Message::decode(queued_frame.frame.clone()).context("decode message") {
                        Ok(message) => message,
                        Err(err) => {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                            }
                            log_decode_error("event_message", &err, &queued_frame.frame);
                            let _ = enqueue_event(
                                &event_tx,
                                QueuedEvent::Error(err.context("decode message")),
                                queue_policy,
                                queue_capacity,
                            )
                            .await;
                            return;
                        }
                    };
                #[cfg(feature = "telemetry")]
                {
                    let counters = frame_counters();
                    counters.sub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
                }
                match message {
                    Message::Event {
                        payload, offset, ..
                    } => {
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            counters.sub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
                            counters.sub_items_in_ok.fetch_add(1, Ordering::Relaxed);
                        }
                        (vec![Bytes::from(payload)], offset, 0, None, None)
                    }
                    Message::EventBatch {
                        payloads,
                        base_offset,
                        ..
                    } => {
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            counters.sub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
                            counters
                                .sub_items_in_ok
                                .fetch_add(payloads.len() as u64, Ordering::Relaxed);
                        }
                        (
                            payloads.into_iter().map(Bytes::from).collect(),
                            base_offset,
                            0,
                            None,
                            None,
                        )
                    }
                    Message::ShardMoved {
                        resume_from,
                        node_id,
                        addr,
                        generation,
                        ..
                    } => {
                        // Returning drops the event sender, so the reader sees
                        // the events queued before this and then `None`, by
                        // which time the slot is set.
                        let _ = shard_moved.set(ShardMoved {
                            resume_from,
                            node_id,
                            addr,
                            generation,
                        });
                        return;
                    }
                    Message::SubscriptionLagged { resume_from, .. } => {
                        end.dropped(resume_from);
                        break 'frames;
                    }
                    _ => {
                        let _ = enqueue_event(
                            &event_tx,
                            QueuedEvent::Error(anyhow::anyhow!(
                                "unexpected message on subscription stream"
                            )),
                            queue_policy,
                            queue_capacity,
                        )
                        .await;
                        return;
                    }
                }
            };

        #[cfg(feature = "telemetry")]
        if let Some(start) = decode_start {
            let decode_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_decode_ns(decode_ns);
            t_histogram!("sub_decode_ns").record(decode_ns as f64);
        }

        #[cfg(feature = "telemetry")]
        let dispatch_start = crate::telemetry::t_now_if(sample);
        for (index, payload) in payloads.into_iter().enumerate() {
            // Offsets in a batch are contiguous by construction, so each event's
            // offset is the batch base plus its position.
            let offset = base_offset.map(|base| base + index as u64);
            let policy = if is_history(offset, live_offset) {
                ClientSubQueuePolicy::Block
            } else {
                queue_policy
            };
            let outcome = enqueue_event(
                &event_tx,
                // The skip describes the offsets before the batch, so only its
                // first event carries it.
                QueuedEvent::Payload {
                    payload,
                    offset,
                    skipped_before: if index == 0 { skipped_before } else { 0 },
                    publisher: publisher.clone(),
                    timestamp_micros: timestamps
                        .as_ref()
                        .and_then(|times| times.get(index).copied()),
                },
                policy,
                queue_capacity,
            )
            .await;
            match (outcome, offset) {
                (Enqueued::Queued, _) | (Enqueued::Dropped, None) => {}
                (Enqueued::Dropped, Some(offset)) => {
                    end.dropped(offset);
                    break 'frames;
                }
                (Enqueued::Closed, _) => return,
            }
        }
        #[cfg(feature = "telemetry")]
        if let Some(start) = dispatch_start {
            let dispatch_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_dispatch_ns(dispatch_ns);
            t_histogram!("sub_dispatch_ns").record(dispatch_ns as f64);
        }
    }
    if let Some(err) = end.take_error() {
        // Blocking: this is the last thing the reader will see, and dropping it
        // would turn the failure back into a clean end.
        let _ = enqueue_event(
            &event_tx,
            QueuedEvent::Error(err),
            ClientSubQueuePolicy::Block,
            queue_capacity,
        )
        .await;
    }
}

/// Whether a record (or the first of a batch) is history rather than live.
///
/// The overflow policy exists so a publisher never waits on a slow reader.
/// History has no publisher waiting on it: the broker reads it off disk for
/// this subscription alone, so dropping it only loses what was asked for.
fn is_history(offset: Option<u64>, live_offset: Option<u64>) -> bool {
    matches!((offset, live_offset), (Some(offset), Some(live)) if offset < live)
}

async fn enqueue_frame(
    tx: &mpsc::Sender<QueuedFrame>,
    item: QueuedFrame,
    policy: ClientSubQueuePolicy,
    queue_capacity: usize,
) -> Enqueued {
    enqueue_with_policy(
        tx,
        item,
        policy,
        queue_capacity,
        "felix_client_sub_queue_enqueued_total",
        "felix_client_sub_queue_dropped_total",
        "felix_client_sub_queue_drop_old_emulated_total",
    )
    .await
}

async fn enqueue_event(
    tx: &mpsc::Sender<QueuedEvent>,
    item: QueuedEvent,
    policy: ClientSubQueuePolicy,
    queue_capacity: usize,
) -> Enqueued {
    enqueue_with_policy(
        tx,
        item,
        policy,
        queue_capacity,
        "felix_client_sub_dispatch_enqueued_total",
        "felix_client_sub_dispatch_dropped_total",
        "felix_client_sub_dispatch_drop_old_emulated_total",
    )
    .await
}

/// A batch's publisher, shared by each of its events. Principals are UTF-8;
/// anything else is shown lossily rather than dropped.
fn publisher_of(publisher: Option<Bytes>) -> Option<Arc<str>> {
    publisher.map(|publisher| Arc::from(String::from_utf8_lossy(&publisher)))
}
