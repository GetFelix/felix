//! QUIC subscribe handling and event-stream writer.
//!
//! A subscribe arrives on the bi-directional control stream; events go back on a
//! fresh uni-directional stream. The `EventStreamHello` written first is what
//! binds `subscription_id -> stream`, which is why event batches need no
//! per-subscriber identifier and can be encoded once and shared across every
//! subscriber.
//!
//! Fanout uses `try_send` against bounded per-subscriber queues, so a full queue
//! drops that batch (counted by `felix_subscribe_dropped_total`) rather than
//! stalling publish. Backpressure stays local to the slow subscriber.
//!
//! # Subscribing to a shard this broker does not own
//!
//! Answered with a redirect naming the owner, never served locally.
//! `docs/subscribe-routing.md` records that decision and the measurements
//! behind it. Serving it locally is the one answer that must not happen: the
//! subscription succeeds, delivers nothing, and is indistinguishable from a
//! stream that simply has no traffic.
//!
//! Delivery is sharded into independent writer lanes to cut write-path
//! contention at high fanout. Lane assignment is deterministic so per-subscriber
//! ordering holds. Batches coalesce until whichever comes first: `max_events`,
//! `max_bytes`, or `flush_delay`.
//!
//! `WriterLaneManager` and `handle_subscribe_message` are the only names this
//! module exposes to the rest of the transport.

mod config;
mod conn_counts;
#[cfg(test)]
mod event_writer;
mod feeder;
mod lane;
mod replay;
mod writer;

pub(crate) use lane::{LaneCommand, WriterLaneManager};

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use felix_broker::Broker;
use felix_wire::{Message, StartPosition};
use tokio::sync::mpsc;
use tracing::Instrument;

use super::publish::{Outgoing, SubscriptionLimiter, send_outgoing_critical};
use crate::observability::tenants::TenantDelivery;
use crate::serving::quic::SUBSCRIPTION_ID;
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::codec::write_message;
use crate::serving::quic::telemetry::t_counter;
use config::EventWriterConfig;
use feeder::run_lane_feeder;
use replay::{CountingSink, EventFormat, write_replay};

/// Application code on an event stream reset because this broker stopped
/// serving the shard before the subscription went live.
const NOT_SERVING_HERE: quinn::VarInt = quinn::VarInt::from_u32(1);

/// Handle a subscribe request received on the bi-directional control stream.
///
/// This function is invoked from the control stream read loop when a `Message::Subscribe`
/// is decoded.
///
/// Responsibilities:
/// - Allocate/derive a `subscription_id`.
/// - Ask the broker core for a subscription receiver.
/// - Acknowledge the subscribe on the control stream (`Message::Subscribed` or `Message::Error`).
/// - Open a uni stream for event delivery, send `EventStreamHello`, and spawn the event writer.
///
/// Return value semantics:
/// - `Ok(true)` means: “handled; keep the control stream alive / continue.”
///   (This is consistent with the caller’s pattern where subscribe is a control-plane operation.)
/// - `Err(_)` bubbles up unexpected failures (e.g., encoding errors).
///
/// Error handling strategy:
/// - If we can’t subscribe or can’t open the event stream, we **reply with `Message::Error`**
///   on the control stream (via the outbound ack queue) and return `Ok(true)` so the control
///   loop can continue / cleanly terminate based on higher-level policy.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_subscribe_message(
    broker: Arc<Broker>,
    connection: felix_transport::QuicConnection,
    config: crate::config::BrokerConfig,
    subscriptions: &Arc<SubscriptionLimiter>,
    lane_manager: &Arc<WriterLaneManager>,
    ingress: Option<&crate::shards::routing::IngressRouter>,
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<std::sync::atomic::AtomicUsize>,
    ack_throttle_tx: &tokio::sync::watch::Sender<bool>,
    ack_timeout_state: &Arc<tokio::sync::Mutex<super::publish::AckTimeoutState>>,
    cancel_tx: &tokio::sync::watch::Sender<bool>,
    tenant_id: String,
    namespace: String,
    stream: String,
    subscription_id: Option<u64>,
    start: Option<StartPosition>,
    shard: Option<u32>,
    peer_flags: u16,
    peer_features: u32,
) -> Result<bool> {
    // Which shard of the stream this subscription reads.
    //
    // A subscription reads one shard. A stream's shards can have different
    // owners and a subscription is bound to one connection, so a whole
    // multi-shard stream is one subscription per shard (#297). Absent means 0,
    // which is every record of a single-shard stream.
    let shard = shard.unwrap_or(0);
    // Offsets ride the event batch only for a client that negotiated the bit.
    // One that did not gets exactly the frames it got before this existed.
    let offsets_enabled = felix_wire::supports(peer_flags, felix_wire::FLAG_EVENT_BATCH_OFFSETS);
    // A skip count describes offsets, so it is sent only with them.
    let skip_enabled =
        offsets_enabled && felix_wire::supports(peer_flags, felix_wire::FLAG_EVENT_BATCH_SKIPPED);
    // Subscribe is a control-plane request: acknowledgements/metadata stay on this bi stream.
    // Actual event delivery happens on a fresh uni stream (broker -> client).
    let span = tracing::trace_span!(
        "subscribe",
        tenant_id = %tenant_id,
        namespace = %namespace,
        stream = %stream
    );
    async move {
        // Client may provide an explicit subscription_id (useful for idempotency/testing).
        // If absent, we allocate one from a global atomic counter.
        let subscription_id = subscription_id
            .unwrap_or_else(|| SUBSCRIPTION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed));

        // Enforce the per-connection subscription cap before asking the broker core for a
        // subscriber queue — no point allocating one just to reject it immediately after.
        if !subscriptions.try_reserve(config.max_subscriptions_per_conn) {
            t_counter!("felix_subscribe_requests_total", "result" => "error").increment(1);
            t_counter!("felix_broker_subscribe_conn_limit_rejected_total").increment(1);
            super::publish::handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(
                        ClientError::limit_exceeded("max subscriptions per connection exceeded")
                            .into_message(),
                    ),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
            return Ok(true);
        }

        // Ask broker core for a managed subscriber queue.
        //
        // With a start position the broker registers the live subscription
        // first and hands back the history needed to reach it -- see
        // `Broker::subscribe_from`. A plain subscribe from a client that reads
        // offsets is `latest`, so it learns where live delivery began. Without
        // offsets it is the tail-only path every client used before resume
        // existed, byte for byte.
        // On failure, respond on the control stream (through the ack queue) and keep
        // the stream alive.
        let start = start.or(offsets_enabled.then_some(StartPosition::Latest));
        let mut replay = None;
        let mut join = None;
        let mut subscription = match start {
            None => match broker
                .subscribe(&tenant_id, &namespace, &stream, shard)
                .await
            {
                Ok(subscription) => subscription,
                Err(err) => {
                    return subscribe_failed(
                        subscribe_error_message(err),
                        subscriptions,
                        out_ack_tx,
                        out_ack_depth,
                        ack_throttle_tx,
                        ack_timeout_state,
                        cancel_tx,
                    )
                    .await;
                }
            },
            Some(start) => match broker
                .subscribe_from(&tenant_id, &namespace, &stream, shard, start)
                .await
            {
                Ok(resumed) => {
                    replay = Some((resumed.history, resumed.backlog, resumed.backlog_start));
                    join = resumed.join;
                    resumed.subscription
                }
                Err(err) => {
                    return subscribe_failed(
                        subscribe_error_message(err),
                        subscriptions,
                        out_ack_tx,
                        out_ack_depth,
                        ack_throttle_tx,
                        ack_timeout_state,
                        cancel_tx,
                    )
                    .await;
                }
            },
        };

        // A shard's fence closes before its readers are ended, and the routes
        // that admitted this request catch up only after. A subscription that
        // registered after the ending would never be ended and would wait on
        // a shard nobody writes here, so it is refused and the client retries
        // where the shard is now. Checked after registering: a fence still
        // open here means the ending is yet to come and will include it.
        let key = crate::shards::ShardKey {
            tenant_id: tenant_id.clone(),
            namespace: namespace.clone(),
            stream: stream.clone(),
            shard,
            kind: crate::shards::ShardKind::Stream,
        };
        if ingress.is_some_and(|ingress| ingress.fence().is_closed(&key)) {
            drop(subscription);
            let reason = crate::shards::routing::Reason::Moving;
            return subscribe_failed(
                ClientError::unavailable(
                    &reason,
                    format!("stream {stream} stopped being served here"),
                )
                .into_message(),
                subscriptions,
                out_ack_tx,
                out_ack_depth,
                ack_throttle_tx,
                ack_timeout_state,
                cancel_tx,
            )
            .await;
        }

        // Open a uni stream for event delivery. If this fails, respond with error on
        // control stream.
        if matches!(
            config.sub_stream_mode,
            crate::config::SubStreamMode::HashedPool
        ) {
            t_counter!(
                "broker_sub_stream_mode_fallback_total",
                "mode" => "hashed_pool"
            )
            .increment(1);
            tracing::debug!(
                requested_streams_per_conn = config.sub_streams_per_conn,
                "hashed_pool stream mode not enabled yet; using per_subscriber stream mode"
            );
        }
        let mut event_send = match connection.open_uni().await {
            Ok(send) => send,
            Err(err) => {
                subscriptions.release();
                t_counter!("felix_subscribe_requests_total", "result" => "error").increment(1);
                super::publish::handle_ack_enqueue_result(
                    send_outgoing_critical(
                        out_ack_tx,
                        out_ack_depth,
                        "felix_broker_out_ack_depth",
                        ack_throttle_tx,
                        Outgoing::Message(
                            ClientError::internal(err.to_string())
                                .with_retry(felix_wire::RetryClass::Retry)
                                .into_message(),
                        ),
                    )
                    .await,
                    ack_timeout_state,
                    ack_throttle_tx,
                    cancel_tx,
                )
                .await?;
                return Ok(true);
            }
        };

        // First write a hello on the uni stream so the client can bind:
        //   subscription_id -> this uni stream
        // before any events arrive. If this fails, we treat it as the subscriber being gone.
        if let Err(err) = write_message(
            &mut event_send,
            Message::EventStreamHello { subscription_id },
        )
        .await
        {
            subscriptions.release();
            tracing::info!(error = %err, "subscription event stream closed");
            return Ok(true);
        }

        // Acknowledge *before* streaming any history.
        //
        // The client waits for `Subscribed` before it registers the event stream
        // with its router and starts reading it. Writing replay first therefore
        // deadlocks as soon as the history exceeds the QUIC per-stream receive
        // window (64 MiB by default): the broker blocks for flow-control credit
        // that the client will not grant until it reads, and the client will not
        // read until it sees the acknowledgement the broker cannot send. Small
        // replays fit in the window and hide it, which is what makes it a
        // production bug rather than a test failure.
        t_counter!("felix_subscribe_requests_total", "result" => "ok").increment(1);
        super::publish::handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                // Only to a client that gets offsets on its events; without them
                // there is nothing to compare these against.
                Outgoing::Message(Message::Subscribed {
                    subscription_id,
                    start_offset: join
                        .filter(|_| offsets_enabled)
                        .map(|join| join.start_offset),
                    live_offset: join
                        .filter(|_| offsets_enabled)
                        .map(|join| join.live_offset),
                }),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;

        // Replay goes out here, on the raw uni stream, before the lane manager is
        // registered for live delivery. That ordering is the delivery half of the
        // seam: the broker already registered the live subscription, so events are
        // queueing in `subscription` while these bytes are written, and they cannot
        // overtake replay because nothing drains that queue until registration
        // below. History, then backlog, then live -- contiguous.
        let delivery = TenantDelivery::for_tenant(&tenant_id);
        if let Some((history, backlog, backlog_start)) = replay
            && let Err(err) = write_replay(
                &mut CountingSink {
                    inner: &mut event_send,
                    delivery: &delivery,
                },
                &broker,
                &tenant_id,
                &namespace,
                &stream,
                shard,
                subscription_id,
                history,
                backlog,
                backlog_start,
                &mut subscription,
                config.event_batch_max_events.max(1),
                config.event_batch_max_bytes.max(1),
                EventFormat {
                    offsets: offsets_enabled,
                    skips: skip_enabled,
                },
            )
            .await
        {
            subscriptions.release();
            tracing::info!(error = %err, "subscription replay failed");
            // Stopped serving the shard mid-replay (lease lapsed, shard moved).
            // Finishing the stream would read as a clean end; a reset reads
            // as a lost subscription, which the client resumes from its last
            // offset wherever the shard is served now.
            if err.chain().any(|cause| {
                matches!(
                    cause.downcast_ref::<felix_broker::BrokerError>(),
                    Some(felix_broker::BrokerError::NotReadable { .. })
                )
            }) {
                let _ = event_send.reset(NOT_SERVING_HERE);
            }
            return Ok(true);
        }

        // Batching configuration.
        // Note: `batch_size` is the “fanout batch size” (publisher -> broker internal),
        // but for subscriber delivery we compute and enforce independent limits.
        let max_events = config
            .event_batch_max_events
            .min(config.fanout_batch_size.max(1));
        let max_bytes = config.event_batch_max_bytes.max(1);
        let flush_delay = Duration::from_micros(config.event_batch_max_delay_us);

        let writer_config = EventWriterConfig {
            subscription_id,
            max_events,
            max_bytes,
            flush_delay,
            single_event_mode: config.fanout_batch_size <= 1,
            offsets_enabled,
            skip_enabled,
            shard_moved_enabled: felix_wire::supports_feature(
                peer_features,
                felix_wire::FEATURE_SHARD_MOVED,
            ),
            flush_max_items: config.subscriber_flush_max_items.max(1),
            flush_max_delay: Duration::from_micros(config.subscriber_flush_max_delay_us.max(1)),
            max_bytes_per_write: config.subscriber_max_bytes_per_write.max(1),
        };
        let connection_id = connection.info().id.0;
        let manager = Arc::clone(lane_manager);
        let lane_idx = manager.select_lane(subscription_id, Some(connection_id));
        let (event_rx, unsubscribe_guard) = subscription.into_parts();
        if manager
            .enqueue(
                lane_idx,
                LaneCommand::Register {
                    subscriber_id: subscription_id,
                    connection,
                    connection_id: Some(connection_id),
                    event_send,
                    guard: unsubscribe_guard,
                },
            )
            .await
            .is_err()
        {
            manager.unregister_subscriber(subscription_id, Some(connection_id));
            subscriptions.release();
            tracing::warn!(
                lane = lane_idx,
                subscription_id,
                "subscriber lane closed during register"
            );
            t_counter!("felix_subscribe_requests_total", "result" => "error").increment(1);
            super::publish::handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(
                        ClientError::overloaded("subscriber lane closed during register")
                            .into_message(),
                    ),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
            return Ok(true);
        }

        let feeder_subscriptions = Arc::clone(subscriptions);
        let feeder_manager = Arc::downgrade(&manager);
        let feeder = async move {
            run_lane_feeder(
                event_rx,
                feeder_manager,
                lane_idx,
                Some(connection_id),
                writer_config,
                feeder_subscriptions,
                delivery,
            )
            .await;
        };
        // With core shards enabled, run this subscription's feeder on the shard
        // that owns its stream: the fanout enqueue (publish worker) and this
        // dequeue then happen on the same core, so the subscriber queue becomes a
        // core-local handoff instead of a cross-core wakeup. Mapping must match
        // `publish_worker_index` (handle id % shard count).
        let shard_runtime = match crate::serving::core_shards::global_shards(&config) {
            Some(shards) => match broker
                .resolve_stream_handle(&tenant_id, &namespace, &stream, shard)
                .await
            {
                Ok(handle) => Some(shards.handle_for(handle.id()).clone()),
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "stream handle unavailable; feeder on main runtime"
                    );
                    None
                }
            },
            None => None,
        };
        match shard_runtime {
            Some(runtime) => {
                runtime.spawn(feeder);
            }
            None => {
                tokio::spawn(feeder);
            }
        }
        Ok(true)
    }
    .instrument(span)
    .await
}

/// Turn a broker error into the most specific protocol message available.
///
/// A cursor rejection is machine-readable so the client can choose a remedy;
/// everything else stays a generic `Error`.
fn subscribe_error_message(err: felix_broker::BrokerError) -> Message {
    match err {
        felix_broker::BrokerError::CursorTooOld { oldest, requested } => {
            Message::SubscribeCursorError {
                reason: felix_wire::CursorErrorReason::TooOld,
                requested,
                available: oldest,
            }
        }
        felix_broker::BrokerError::CursorInFuture { requested, tail } => {
            Message::SubscribeCursorError {
                reason: felix_wire::CursorErrorReason::InFuture,
                requested,
                available: tail,
            }
        }
        other => {
            let message = other.to_string();
            ClientError::from_broker(&other, message).into_message()
        }
    }
}

/// Report a failed subscribe on the control stream and keep the stream alive.
///
/// Extracted because the tail-only and resume paths fail identically, and
/// duplicating the ack-queue plumbing between them is how the two drift apart.
#[allow(clippy::too_many_arguments)]
async fn subscribe_failed(
    message: Message,
    subscriptions: &Arc<SubscriptionLimiter>,
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<std::sync::atomic::AtomicUsize>,
    ack_throttle_tx: &tokio::sync::watch::Sender<bool>,
    ack_timeout_state: &Arc<tokio::sync::Mutex<super::publish::AckTimeoutState>>,
    cancel_tx: &tokio::sync::watch::Sender<bool>,
) -> Result<bool> {
    subscriptions.release();
    t_counter!("felix_subscribe_requests_total", "result" => "error").increment(1);
    super::publish::handle_ack_enqueue_result(
        send_outgoing_critical(
            out_ack_tx,
            out_ack_depth,
            "felix_broker_out_ack_depth",
            ack_throttle_tx,
            Outgoing::Message(message),
        )
        .await,
        ack_timeout_state,
        ack_throttle_tx,
        cancel_tx,
    )
    .await?;
    Ok(true)
}

#[cfg(test)]
mod tests;
