//! Feeder task: drains the broker subscription queue into a writer lane.

use std::sync::{Arc, Weak};
use std::time::Instant;

use bytes::Bytes;
use felix_broker::{DeliveryEnvelope, SubscriptionReceiver};

use crate::observability::tenants::TenantDelivery;
use crate::observability::timings;
use crate::serving::quic::handlers::publish::SubscriptionLimiter;
use crate::serving::quic::handlers::subscribe::config::EventWriterConfig;
use crate::serving::quic::handlers::subscribe::lane::{LaneCommand, WriterLaneManager};
use crate::serving::quic::telemetry::{t_histogram, t_now_if, t_should_sample};

/// A batch this feeder assembled itself, in the frame shape the subscriber
/// negotiated.
fn encode_batch(
    config: &EventWriterConfig,
    batch: &[Bytes],
    base_offset: Option<u64>,
    skipped_before: u64,
) -> felix_wire::Result<Bytes> {
    match (config.offsets_enabled, base_offset) {
        (true, Some(base)) if config.skip_enabled => {
            felix_wire::binary::encode_shared_event_batch_bytes_with_skip(
                batch,
                base,
                skipped_before,
            )
        }
        (true, Some(base)) => {
            felix_wire::binary::encode_shared_event_batch_bytes_with_offset(batch, base)
        }
        _ => felix_wire::binary::encode_shared_event_batch_bytes(batch),
    }
}

pub(super) async fn run_lane_feeder(
    mut event_rx: SubscriptionReceiver,
    manager: Weak<WriterLaneManager>,
    lane_idx: usize,
    connection_id: Option<u64>,
    config: EventWriterConfig,
    subscriptions: Arc<SubscriptionLimiter>,
    delivery: TenantDelivery,
) {
    let max_events = config.max_events.max(1);
    let max_bytes = config.max_bytes.max(1);
    let _lane_flush_hints = (
        config.flush_max_items,
        config.flush_max_delay,
        config.max_bytes_per_write,
    );
    let mut pending: Option<DeliveryEnvelope> = None;
    let mut busy = false;

    loop {
        let envelope = match pending.take() {
            Some(envelope) => envelope,
            None => match event_rx.recv().await {
                Some(envelope) => envelope,
                None => break,
            },
        };
        let first_enqueued_at = envelope.enqueued_at();
        let Some(manager) = manager.upgrade() else {
            break;
        };

        if envelope.len() > 1 || config.single_event_mode {
            let payloads = envelope.payloads();
            let payload_bytes: usize = payloads.iter().map(Bytes::len).sum();
            if payloads.len() <= max_events && payload_bytes <= max_bytes {
                let prefix_start = t_now_if(t_should_sample());
                let frame = match if config.skip_enabled {
                    envelope.shared_event_frame_with_skip()
                } else if config.offsets_enabled {
                    envelope.shared_event_frame_with_offsets()
                } else {
                    envelope.shared_event_frame()
                } {
                    Ok(frame) => frame,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            subscriber_id = config.subscription_id,
                            "encode shared lane delivery failed"
                        );
                        continue;
                    }
                };
                if let Some(start) = prefix_start {
                    let prefix_ns = start.elapsed().as_nanos() as u64;
                    timings::record_sub_prefix_ns(prefix_ns);
                    t_histogram!("felix_broker_sub_prefix_build_ns").record(prefix_ns as f64);
                }
                enqueue_lane_frame(
                    &manager,
                    lane_idx,
                    config.subscription_id,
                    frame,
                    payloads.len(),
                    first_enqueued_at,
                )
                .await;
                delivery.record(payloads.len(), payload_bytes);
                continue;
            }

            let mut start = 0usize;
            while start < payloads.len() {
                let mut end = start;
                let mut bytes = 0usize;
                while end < payloads.len()
                    && end - start < max_events
                    && (end == start || bytes.saturating_add(payloads[end].len()) <= max_bytes)
                {
                    bytes = bytes.saturating_add(payloads[end].len());
                    end += 1;
                }
                let batch = &payloads[start..end];
                // A split batch keeps its own base offset: the envelope's
                // offsets are contiguous, so this sub-batch begins `start`
                // records into the run. Only the first follows the skip.
                let skipped = if start == 0 {
                    envelope.skipped_before()
                } else {
                    0
                };
                let encoded = encode_batch(
                    &config,
                    batch,
                    envelope.base_offset().map(|base| base + start as u64),
                    skipped,
                );
                match encoded {
                    Ok(frame) => {
                        enqueue_lane_frame(
                            &manager,
                            lane_idx,
                            config.subscription_id,
                            frame,
                            batch.len(),
                            first_enqueued_at,
                        )
                        .await;
                        delivery.record(batch.len(), bytes);
                    }
                    Err(err) => tracing::warn!(
                        error = %err,
                        subscriber_id = config.subscription_id,
                        "encode split lane delivery failed"
                    ),
                }
                start = end;
            }
            continue;
        }

        let first = envelope.payloads()[0].clone();

        let mut batch = Vec::with_capacity(max_events);
        let mut batch_bytes = first.len();
        batch.push(first);
        // Coalescing merges *separate* publishes into one frame, and their
        // offsets are only contiguous if nothing landed between them. One
        // `base_offset` describes the frame only while that holds, so a break in
        // the run ends the batch exactly as a byte or count limit would.
        let batch_base = envelope.base_offset();
        let batch_skipped = envelope.skipped_before();
        let mut expected_next = batch_base.map(|base| base + envelope.len() as u64);
        // Wait for more only while the previous batch found events already
        // queued behind its first, i.e. arrivals outpace this feeder. Otherwise a
        // lone event would sit out `flush_delay` for a batch that never fills.
        // One deadline for the whole batch: a per-recv timeout would let a
        // steady stream hold the first event until the count or byte cap.
        // A zero delay must not arm a timer at all: tokio rounds every deadline
        // up to its next 1 ms tick, so even `now + 0` can wait most of a ms.
        let deadline = (busy && !config.flush_delay.is_zero())
            .then(|| tokio::time::Instant::now() + config.flush_delay);
        let mut found_queued = false;

        while !config.single_event_mode && batch.len() < max_events && batch_bytes < max_bytes {
            let next = match event_rx.try_recv() {
                Ok(envelope) => {
                    found_queued = true;
                    Some(envelope)
                }
                Err(_) => match deadline {
                    Some(deadline) => tokio::time::timeout_at(deadline, event_rx.recv())
                        .await
                        .ok()
                        .flatten(),
                    None => None,
                },
            };
            let Some(envelope) = next else {
                break;
            };
            if envelope.len() != 1 {
                pending = Some(envelope);
                break;
            }
            if config.offsets_enabled && envelope.base_offset() != expected_next {
                // Numbering this into the current frame would misreport it.
                pending = Some(envelope);
                break;
            }
            let payload = envelope.payloads()[0].clone();
            if batch_bytes.saturating_add(payload.len()) > max_bytes {
                pending = Some(envelope);
                break;
            }
            batch_bytes += payload.len();
            batch.push(payload);
            expected_next = expected_next.map(|next| next + 1);
        }
        busy = found_queued;

        let sample = t_should_sample();
        let enqueue_start = t_now_if(sample);
        let prefix_start = t_now_if(sample);
        let encoded = encode_batch(&config, &batch, batch_base, batch_skipped);
        let frame = match encoded {
            Ok(frame) => frame,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    subscriber_id = config.subscription_id,
                    "encode lane delivery failed"
                );
                continue;
            }
        };
        if let Some(start) = prefix_start {
            let prefix_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_prefix_ns(prefix_ns);
            t_histogram!("felix_broker_sub_prefix_build_ns").record(prefix_ns as f64);
        }

        enqueue_lane_frame(
            &manager,
            lane_idx,
            config.subscription_id,
            frame,
            batch.len(),
            first_enqueued_at,
        )
        .await;
        delivery.record(batch.len(), batch_bytes);
        if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            t_histogram!("broker_sub_lane_enqueue_ns", "lane" => lane_idx.to_string())
                .record(enqueue_ns as f64);
        }
    }
    if let Some(manager) = manager.upgrade() {
        let last = match event_rx.moved() {
            Some(moved) if config.shard_moved_enabled => {
                match shard_moved_frame(config.subscription_id, moved) {
                    Ok(frame) => Some(frame),
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            subscriber_id = config.subscription_id,
                            "encode shard_moved failed"
                        );
                        None
                    }
                }
            }
            _ => None,
        };
        // The lane forgets the subscriber when it handles this, after the
        // deliveries queued ahead of it. Forgetting it here instead would
        // race those deliveries, which the lane routes by that mapping.
        let unregistered = manager
            .enqueue(
                lane_idx,
                LaneCommand::Unregister {
                    subscriber_id: config.subscription_id,
                    connection_id,
                    last,
                },
            )
            .await
            .is_ok();
        if !unregistered {
            manager.unregister_subscriber(config.subscription_id, connection_id);
        }
    }
    subscriptions.release();
}

pub(super) async fn enqueue_lane_frame(
    manager: &WriterLaneManager,
    lane_idx: usize,
    subscriber_id: u64,
    frame: Bytes,
    item_count: usize,
    first_enqueued_at: Instant,
) {
    let cmd = LaneCommand::Delivery {
        subscriber_id,
        frame,
        item_count,
        first_enqueued_at,
        enqueue_at: Instant::now(),
    };
    if manager.enqueue(lane_idx, cmd).await.is_err() {
        metrics::counter!("felix_subscriber_lane_dropped_total").increment(1);
    }
}

/// The last frame of a subscription whose shard moved away.
pub(super) fn shard_moved_frame(
    subscription_id: u64,
    moved: &felix_broker::ShardMoved,
) -> felix_wire::Result<Bytes> {
    let message = felix_wire::Message::ShardMoved {
        subscription_id,
        resume_from: moved.resume_from,
        node_id: moved.to.node_id.clone(),
        addr: moved.to.addr.clone(),
        generation: moved.to.generation,
    };
    Ok(message.encode()?.encode())
}
