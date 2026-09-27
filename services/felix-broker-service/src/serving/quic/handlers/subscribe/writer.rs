//! Frame-writing primitives and the two writer task loops (per-lane and per-connection).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Weak;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::{StreamExt, stream::FuturesUnordered};
#[cfg(test)]
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::observability::timings;
use crate::serving::quic::handlers::subscribe::conn_counts::{
    connection_subscriber_register, connection_subscriber_unregister,
};
use crate::serving::quic::handlers::subscribe::lane::{
    ConnectionCommand, LaneCommand, LaneDelivery, LaneRuntimeConfig, LaneSubscriber,
    WriterLaneManager,
};
use crate::serving::quic::telemetry::{t_counter, t_histogram, t_now_if, t_should_sample};

#[cfg(test)]
pub(super) async fn write_parts_to<W>(
    writer: &mut W,
    parts: &felix_wire::binary::EncodedEventBatchParts,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    for segment in parts.segments() {
        writer
            .write_all(segment.as_ref())
            .await
            .context("write subscription frame segment")?;
    }
    Ok(())
}

#[cfg(test)]
pub(super) async fn write_parts(
    send: &mut quinn::SendStream,
    parts: felix_wire::binary::EncodedEventBatchParts,
) -> Result<()> {
    let mut segments = parts.into_segments();
    send.write_all_chunks(segments.as_mut_slice())
        .await
        .context("write subscription frame chunks")?;
    Ok(())
}

#[cfg(test)]
pub(super) async fn write_parts_many(
    send: &mut quinn::SendStream,
    frames: Vec<felix_wire::binary::EncodedEventBatchParts>,
) -> Result<usize> {
    let mut total = 0usize;
    let mut segments = Vec::new();
    for frame in frames {
        total = total.saturating_add(frame.frame_len());
        segments.extend(frame.into_segments());
    }
    send.write_all_chunks(segments.as_mut_slice())
        .await
        .context("write subscription coalesced frame chunks")?;
    Ok(total)
}

pub(super) async fn write_frame(send: &mut quinn::SendStream, frame: Bytes) -> Result<()> {
    send.write_all(frame.as_ref())
        .await
        .context("write subscription frame")?;
    Ok(())
}

pub(super) async fn write_frames_many(
    send: &mut quinn::SendStream,
    mut frames: Vec<Bytes>,
) -> Result<usize> {
    let total = frames.iter().map(Bytes::len).sum();
    send.write_all_chunks(frames.as_mut_slice())
        .await
        .context("write subscription coalesced frames")?;
    Ok(total)
}

pub(super) async fn run_writer_lane(
    lane_id: usize,
    mut rx: mpsc::Receiver<LaneCommand>,
    lane_cfg: LaneRuntimeConfig,
    manager: Weak<WriterLaneManager>,
) {
    let lane_label = lane_id.to_string();
    while let Some(first_cmd) = rx.recv().await {
        let Some(manager) = manager.upgrade() else {
            break;
        };
        let mut pending = Vec::with_capacity(lane_cfg.flush_max_items.max(1));
        pending.push(first_cmd);

        while pending.len() < lane_cfg.flush_max_items {
            match rx.try_recv() {
                Ok(cmd) => pending.push(cmd),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        metrics::gauge!("felix_sub_lane_queue_len", "lane" => lane_label.clone())
            .set(rx.len() as f64);

        for cmd in pending {
            match cmd {
                LaneCommand::Register {
                    subscriber_id,
                    connection_id,
                    connection,
                    event_send,
                    guard,
                } => {
                    let Some(connection_id) = connection_id else {
                        continue;
                    };
                    manager
                        .subscriber_connections
                        .insert(subscriber_id, connection_id);
                    connection_subscriber_register(Some(connection_id));
                    let _ = manager
                        .enqueue_connection(
                            connection_id,
                            ConnectionCommand::Register {
                                subscriber_id,
                                connection,
                                connection_id: Some(connection_id),
                                event_send,
                                guard,
                            },
                        )
                        .await;
                }
                LaneCommand::Unregister {
                    subscriber_id,
                    connection_id,
                    last,
                } => {
                    let carried = connection_id;
                    // Prefer the id carried on the command; fall back to the map
                    // for any caller that still has an entry. Either way the
                    // per-connection writer must be told and the connection
                    // subscriber count must be decremented exactly once.
                    let connection_id = connection_id.or_else(|| {
                        manager
                            .subscriber_connections
                            .remove(&subscriber_id)
                            .map(|(_, id)| id)
                    });
                    if let Some(connection_id) = connection_id {
                        manager.subscriber_connections.remove(&subscriber_id);
                        let _ = manager
                            .enqueue_connection(
                                connection_id,
                                ConnectionCommand::Unregister {
                                    subscriber_id,
                                    last,
                                },
                            )
                            .await;
                        connection_subscriber_unregister(Some(connection_id));
                    }
                    manager.unregister_subscriber(subscriber_id, carried);
                }
                LaneCommand::Delivery {
                    subscriber_id,
                    frame,
                    item_count,
                    first_enqueued_at,
                    enqueue_at,
                } => {
                    let Some(connection_id) = manager
                        .subscriber_connections
                        .get(&subscriber_id)
                        .map(|entry| *entry.value())
                    else {
                        continue;
                    };
                    let _ = manager
                        .enqueue_connection(
                            connection_id,
                            ConnectionCommand::Delivery {
                                subscriber_id,
                                frame,
                                item_count,
                                first_enqueued_at,
                                enqueue_at,
                            },
                        )
                        .await;
                }
            }
        }
    }
    metrics::gauge!("felix_sub_lane_queue_len", "lane" => lane_label).set(0.0);
}

/// How the per-connection writer treats a subscriber that stops accepting
/// writes.
#[derive(Debug, Clone, Copy)]
pub(super) struct ConnectionWriterConfig {
    pub(super) max_bytes_per_write: usize,
    /// Frames one subscriber may have waiting behind its current write.
    pub(super) max_queued_per_subscriber: usize,
    /// `SubQueuePolicy::Block`: a subscriber at its limit stops the writer
    /// taking more commands, which is how that policy pushes back on the
    /// publisher. Otherwise the frame over the limit is dropped.
    pub(super) block: bool,
}

/// Write every subscription of one connection.
///
/// Commands are taken while writes are in flight, and each subscriber has
/// its own bounded queue. A subscription whose stream is flow-controlled only
/// fills its own queue; the others keep being written. Taking commands only
/// between rounds of writes let one stalled stream stop the whole connection,
/// and the lanes then dropped frames for every subscription on it.
pub(super) async fn run_connection_writer(
    connection_id: u64,
    mut rx: mpsc::Receiver<ConnectionCommand>,
    config: ConnectionWriterConfig,
) {
    let ConnectionWriterConfig {
        max_bytes_per_write,
        max_queued_per_subscriber,
        block,
    } = config;
    let max_queued_per_subscriber = max_queued_per_subscriber.max(1);
    // Only read by the telemetry build's per-connection series.
    #[cfg_attr(not(feature = "telemetry"), allow(unused_variables))]
    let conn_label = connection_id.to_string();
    let mut debug_window_start = Instant::now();
    let mut debug_writes = 0u64;
    let mut debug_bytes = 0u64;
    let mut debug_dequeues = 0u64;
    let mut subscribers: HashMap<u64, LaneSubscriber> = HashMap::new();
    let mut deliveries: HashMap<u64, VecDeque<LaneDelivery>> = HashMap::new();
    // Unregistered, but still writing what was queued before the unregister.
    let mut leaving: HashSet<u64> = HashSet::new();
    let mut in_flight: HashSet<u64> = HashSet::new();
    let mut writes = FuturesUnordered::new();
    let mut open = true;
    loop {
        // Start the next write for every subscriber that is idle and has
        // something queued, coalescing what fits in one write.
        let ready: Vec<u64> = deliveries
            .iter()
            .filter_map(|(subscriber_id, queue)| {
                (!queue.is_empty() && !in_flight.contains(subscriber_id)).then_some(*subscriber_id)
            })
            .collect();
        for subscriber_id in ready {
            let Some(mut subscriber) = subscribers.remove(&subscriber_id) else {
                // Never registered here, or already gone: nothing to write to.
                deliveries.remove(&subscriber_id);
                continue;
            };
            let Some(queue) = deliveries.get_mut(&subscriber_id) else {
                continue;
            };
            let Some(first) = queue.pop_front() else {
                continue;
            };
            debug_dequeues = debug_dequeues.saturating_add(1);

            let first_subscriber_id = first.subscriber_id;
            let mut frames = vec![first.frame];
            let mut item_count = first.item_count;
            let mut coalesced_bytes = frames[0].len();
            while let Some(next) = queue.front() {
                let next_len = next.frame.len();
                if coalesced_bytes.saturating_add(next_len) > max_bytes_per_write {
                    break;
                }
                if let Some(next_frame) = queue.pop_front() {
                    item_count = item_count.saturating_add(next_frame.item_count);
                    coalesced_bytes = coalesced_bytes.saturating_add(next_frame.frame.len());
                    frames.push(next_frame.frame);
                }
            }

            let sample = t_should_sample();
            let queue_wait_ns = first.enqueue_at.elapsed().as_nanos() as u64;
            let first_dequeue_ns = first.first_enqueued_at.elapsed().as_nanos() as u64;
            t_histogram!("broker_sub_lane_queue_wait_ns", "connection_id" => conn_label.clone())
                .record(queue_wait_ns as f64);
            t_histogram!(
                "broker_sub_lane_dequeue_to_write_start_ns",
                "connection_id" => conn_label.clone()
            )
            .record(queue_wait_ns as f64);
            t_histogram!(
                "broker_sub_time_to_first_dequeue_ns",
                "connection_id" => conn_label.clone()
            )
            .record(first_dequeue_ns as f64);

            let write_start = t_now_if(sample);
            t_counter!("broker_sub_conn_write_calls_total", "connection_id" => conn_label.clone())
                .increment(1);
            t_histogram!("broker_sub_conn_writes_per_flush", "connection_id" => conn_label.clone())
                .record(frames.len() as f64);
            in_flight.insert(subscriber_id);
            writes.push(async move {
                let write_result = if frames.len() == 1 {
                    write_frame(
                        &mut subscriber.event_send,
                        frames.pop().expect("single frame"),
                    )
                    .await
                    .map(|_| coalesced_bytes)
                } else {
                    write_frames_many(&mut subscriber.event_send, frames).await
                };
                let write_ns = write_start.map(|start| start.elapsed().as_nanos() as u64);
                (
                    subscriber_id,
                    first_subscriber_id,
                    subscriber,
                    item_count,
                    write_result,
                    write_ns,
                )
            });
        }

        // Under `Block`, a subscriber at its limit holds everyone: that policy
        // exists to push back rather than drop.
        let accepting = open
            && !(block
                && deliveries
                    .values()
                    .any(|queue| queue.len() >= max_queued_per_subscriber));

        tokio::select! {
            command = rx.recv(), if accepting => {
                let Some(first_cmd) = command else {
                    open = false;
                    continue;
                };
                let mut pending = Vec::with_capacity(64);
                pending.push(first_cmd);
                while pending.len() < 64 {
                    match rx.try_recv() {
                        Ok(cmd) => pending.push(cmd),
                        Err(_) => break,
                    }
                }
                metrics::histogram!("felix_sub_conn_queue_depth").record(rx.len() as f64);
                for cmd in pending {
                    match cmd {
                        ConnectionCommand::Register {
                            subscriber_id,
                            connection,
                            connection_id,
                            event_send,
                            guard,
                        } => {
                            subscribers.insert(
                                subscriber_id,
                                LaneSubscriber {
                                    event_send,
                                    _connection: connection,
                                    _connection_id: connection_id,
                                    _unsubscribe_guard: guard,
                                },
                            );
                        }
                        ConnectionCommand::Unregister {
                            subscriber_id,
                            last,
                        } => {
                            if let Some(frame) = last {
                                // Never dropped for being over the limit: it may
                                // be the frame that says where to resume.
                                let now = Instant::now();
                                deliveries
                                    .entry(subscriber_id)
                                    .or_default()
                                    .push_back(LaneDelivery {
                                        subscriber_id,
                                        frame,
                                        item_count: 0,
                                        first_enqueued_at: now,
                                        enqueue_at: now,
                                    });
                            }
                            // What was queued ahead of this is the subscription's
                            // last, and is written before the stream is dropped.
                            let draining = in_flight.contains(&subscriber_id)
                                || deliveries
                                    .get(&subscriber_id)
                                    .is_some_and(|queue| !queue.is_empty());
                            if draining {
                                leaving.insert(subscriber_id);
                            } else {
                                subscribers.remove(&subscriber_id);
                                deliveries.remove(&subscriber_id);
                            }
                        }
                        ConnectionCommand::Delivery {
                            subscriber_id,
                            frame,
                            item_count,
                            first_enqueued_at,
                            enqueue_at,
                        } => {
                            if leaving.contains(&subscriber_id) {
                                continue;
                            }
                            let queue = deliveries.entry(subscriber_id).or_default();
                            // `Block` never drops: a batch taken just before the
                            // bound was hit may overshoot it, and the writer then
                            // stops taking more until this subscriber drains.
                            if !block && queue.len() >= max_queued_per_subscriber {
                                // This subscriber is not keeping up; the others
                                // on the connection are not made to wait for it.
                                metrics::counter!("felix_sub_queue_dropped_total")
                                    .increment(item_count as u64);
                                continue;
                            }
                            queue.push_back(LaneDelivery {
                                subscriber_id,
                                frame,
                                item_count,
                                first_enqueued_at,
                                enqueue_at,
                            });
                        }
                    }
                }
            }
            Some((
                subscriber_id,
                first_subscriber_id,
                subscriber,
                _item_count,
                write_result,
                write_ns,
            )) = writes.next(), if !writes.is_empty() => {
                in_flight.remove(&subscriber_id);
                match write_result {
                    Ok(bytes_written) => {
                        let done = leaving.contains(&subscriber_id)
                            && deliveries
                                .get(&subscriber_id)
                                .is_none_or(|queue| queue.is_empty());
                        if done {
                            leaving.remove(&subscriber_id);
                            deliveries.remove(&subscriber_id);
                            drop(subscriber);
                        } else {
                            subscribers.insert(subscriber_id, subscriber);
                        }
                        debug_writes = debug_writes.saturating_add(1);
                        debug_bytes = debug_bytes.saturating_add(bytes_written as u64);
                        t_histogram!(
                            "broker_sub_conn_avg_bytes_per_write",
                            "connection_id" => conn_label.clone()
                        )
                        .record(bytes_written as f64);
                        t_counter!(
                            "broker_sub_conn_bytes_written_total",
                            "connection_id" => conn_label.clone()
                        )
                        .increment(bytes_written as u64);
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = crate::serving::quic::telemetry::frame_counters();
                            counters
                                .frames_out_ok
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            counters
                                .bytes_out
                                .fetch_add(bytes_written as u64, std::sync::atomic::Ordering::Relaxed);
                            counters
                                .sub_frames_out_ok
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            counters
                                .sub_batches_out_ok
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            counters
                                .sub_items_out_ok
                                .fetch_add(_item_count as u64, std::sync::atomic::Ordering::Relaxed);
                        }
                        if let Some(write_ns) = write_ns {
                            timings::record_sub_write_ns(write_ns);
                            timings::record_sub_write_await_ns(write_ns);
                            timings::record_quic_write_ns(write_ns);
                            t_histogram!("broker_sub_write_blocked_ns").record(write_ns as f64);
                            t_histogram!(
                                "broker_sub_conn_write_ns",
                                "connection_id" => conn_label.clone()
                            )
                            .record(write_ns as f64);
                            t_histogram!(
                                "broker_sub_conn_write_await_ns",
                                "connection_id" => conn_label.clone()
                            )
                            .record(write_ns as f64);
                        }
                    }
                    Err(err) => {
                        // The stream is gone, and so is everything queued for it.
                        leaving.remove(&subscriber_id);
                        deliveries.remove(&subscriber_id);
                        t_counter!(
                            "broker_sub_conn_write_errors_total",
                            "connection_id" => conn_label.clone()
                        )
                        .increment(1);
                        metrics::counter!("felix_subscriber_disconnect_total").increment(1);
                        tracing::info!(
                            connection_id,
                            subscriber_id = first_subscriber_id,
                            error = %err,
                            "connection writer subscriber stream closed"
                        );
                    }
                }
            }
            // Closed, and every write has finished.
            else => break,
        }

        if debug_window_start.elapsed() >= Duration::from_secs(1) {
            let avg_bytes_per_write = if debug_writes == 0 {
                0.0
            } else {
                debug_bytes as f64 / debug_writes as f64
            };
            tracing::debug!(
                connection_id,
                queue_depth = rx.len(),
                dequeues_per_sec = debug_dequeues,
                writes_per_sec = debug_writes,
                avg_bytes_per_write,
                "subscriber connection throughput window"
            );
            debug_window_start = Instant::now();
            debug_writes = 0;
            debug_bytes = 0;
            debug_dequeues = 0;
        }
    }
}
