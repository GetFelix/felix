//! Control stream (bi-directional QUIC stream)
//!
//! This module implements the *read side* of the broker's bidirectional control stream.
//! The control stream is the request/response path used by clients for:
//!   - Publish / PublishBatch (optionally requesting an ack)
//!   - Subscribe (establishing a subscription and spawning a uni-directional event stream)
//!   - CachePut / CacheGet (request/response cache API)
//!
//! Key design points:
//!   1) Single-writer response path (implemented elsewhere): the read loop never writes to the
//!      SendStream directly; it enqueues `Outgoing` responses into an outbound channel drained by a
//!      dedicated writer task.
//!
//!   2) Fast-path binary batching: when a frame is marked with FLAG_BINARY_PUBLISH_BATCH we bypass
//!      JSON decoding and dispatch to a specialized handler. This keeps the hot path cheap.
//!
//!   3) Cooperative cancellation: `cancel_rx_read` (watch) allows the writer or other tasks to
//!      request the control loop stop (e.g., writer detects the peer closed, or backpressure logic
//!      decides to tear down).
//!
//!   4) Backpressure / throttling coordination: `ack_throttle_rx/tx` is a watch channel used to
//!      communicate whether the outbound response queue is in a throttled state (watermarks are
//!      enforced in the writer/enqueue helpers). The control loop passes the current throttled state
//!      into publish handlers so they can adjust behavior.
//!
//!   5) Ack-on-commit mode: when `config.ack_on_commit` is enabled, publish handlers may defer the
//!      ack until the publish commits. `commit_acks` lets whoever settles the publish send
//!      that ack itself, and runs its timeout.
//!
//! Return value convention:
//!   Ok(true)  => graceful close / stream should be considered "done" (no error)
//!   Ok(false) => protocol error or peer sent Error/unexpected message
//!   Err(_)    => hard failure (decode/IO/etc.)

mod authz;
mod cache;
mod commit;
mod counter;
mod discovery;
mod group;
mod inspect;
mod publish;
mod record_time;
mod responder;
mod session;
mod stream_read;
mod subscribe;
mod unsupported;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;

use crate::serving::quic::codec::FrameScratch;
use anyhow::{Context, Result};
use felix_broker::Broker;
use felix_wire::Message;
use tokio::sync::{Semaphore, mpsc, watch};

use super::frame_source::FrameSource;
use crate::config::BrokerConfig;
use crate::observability::timings;
use crate::serving::auth::{AuthContext, BrokerAuth};
use crate::serving::quic::client_error::{ClientError, ErrorCodeSupport};
use crate::serving::quic::handlers::publish::{
    AckOrder, AckTimeoutState, CommitAcks, Outgoing, PublishContext, StreamHandleCache,
    handle_ack_enqueue_result, handle_acked_binary_publish_batch_control,
    handle_binary_publish_batch_control, send_outgoing_critical,
};
use crate::serving::quic::telemetry::{t_histogram, t_now_if, t_should_sample};
use responder::{Responder, send_control_error};

/// Main control loop: read frames, decode messages, and dispatch to handlers.
///
/// The loop is intentionally structured as:
///   read frame -> (optional fast-path) -> decode -> dispatch.
///
/// Important parameters:
///   - `source`: abstract frame source (RecvStream in prod, test doubles in unit tests).
///   - `stream_cache` / `stream_cache_key`: per-connection cache of stream scope lookups used by
///     publish handlers to avoid repeatedly touching shared metadata for hot streams.
///   - `out_ack_tx` / `out_ack_depth`: outbound response queue + depth gauge used for backpressure.
///   - `ack_throttle_rx/tx`: shared throttling state; this loop reads current state, handlers/writer
///     update it.
///   - `ack_timeout_state`: shared state used to detect/report ack enqueue timeouts.
///   - `commit_acks`: answers for publishes acknowledged on commit.
///   - `frame_scratch`: bytes read past the current frame, kept for the next read.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_control_loop<S: FrameSource + ?Sized>(
    source: &mut S,
    broker: Arc<Broker>,
    connection: felix_transport::QuicConnection,
    config: BrokerConfig,
    auth: Arc<BrokerAuth>,
    publish_ctx: PublishContext,
    stream_cache: StreamHandleCache,
    stream_cache_key: String,
    out_ack_tx: mpsc::Sender<Outgoing>,
    out_ack_depth: Arc<AtomicUsize>,
    ack_throttle_rx: watch::Receiver<bool>,
    ack_throttle_tx: watch::Sender<bool>,
    ack_timeout_state: Arc<parking_lot::Mutex<AckTimeoutState>>,
    cancel_tx: watch::Sender<bool>,
    mut cancel_rx_read: watch::Receiver<bool>,
    commit_acks: CommitAcks,
    frame_scratch: &mut FrameScratch,
    error_codes: Arc<ErrorCodeSupport>,
    ack_order: Arc<AckOrder>,
) -> Result<bool> {
    // If we observe EOF from the peer (source returns None), we treat it as a graceful close.
    // Otherwise, we will cancel downstream tasks and tear down the connection cooperatively.
    let mut graceful_close = false;
    let mut session = Session {
        auth_ctx: None,
        peer_flags: felix_wire::ORIGINAL_V1_FLAGS,
        peer_features: 0,
        commit_ack: false,
        error_codes,
        ack_order,
        publish_window: None,
        stream_cache,
        stream_cache_key,
    };
    let authz_ctx = Responder {
        out_ack_tx: &out_ack_tx,
        out_ack_depth: &out_ack_depth,
        ack_throttle_tx: &ack_throttle_tx,
        ack_timeout_state: &ack_timeout_state,
        cancel_tx: &cancel_tx,
    };
    // Held until this stream authenticates; see `preauth`.
    let mut preauth_permit = Some(publish_ctx.preauth.admit_stream().await);
    loop {
        if *cancel_rx_read.borrow() {
            break;
        }
        // Snapshot current throttling state (set by writer/enqueue helpers when outbound queue
        // crosses watermarks). Handlers may use this to shed work or alter ack behavior.
        let throttled = *ack_throttle_rx.borrow();
        let sample = t_should_sample();
        let read_start = t_now_if(sample);
        // We need to be responsive to cancellation even while blocked on network reads.
        // `watch::Receiver::changed()` wakes when the cancel flag flips.
        let frame = tokio::select! {
            changed = cancel_rx_read.changed() => {
                if changed.is_err() || *cancel_rx_read.borrow() {
                    break;
                }
                continue;
            }
            frame = source.next_frame(
                publish_ctx
                    .preauth
                    .frame_cap(session.auth_ctx.is_some(), config.max_frame_bytes),
                frame_scratch,
            ) => {
                match frame? {
                    Some(frame) => frame,
                    // EOF: the peer cleanly finished the control stream.
                    None => {
                        graceful_close = true;
                        break;
                    }
                }
            }
        };
        let read_ns = read_start.map(|start| start.elapsed().as_nanos() as u64);
        // Flag bits select the payload layout, so an unrecognised bit means we do
        // not know how to parse the body. Reject rather than mask it off and
        // misparse — see `felix_wire::KNOWN_FLAGS`.
        //
        // This is a per-frame error, not a stream-fatal one: the frame reader has
        // already consumed exactly `header.length` bytes, so the stream is sitting
        // on the next frame boundary and stays parseable. Answering and continuing
        // means the peer actually receives the diagnostic — tearing the stream down
        // here would race the writer task and usually deliver EOF instead.
        if felix_wire::has_unknown_flags(frame.header.flags) {
            send_control_error(
                &out_ack_tx,
                &out_ack_depth,
                &ack_throttle_tx,
                &ack_timeout_state,
                &cancel_tx,
                ClientError::invalid("unsupported frame flags"),
            )
            .await?;
            continue;
        }
        // Fast-path: binary publish batch frames avoid JSON decode/allocations.
        if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_BATCH != 0 {
            let acked = frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_ACKED != 0;
            if session.auth_ctx.is_none() {
                send_control_error(
                    &out_ack_tx,
                    &out_ack_depth,
                    &ack_throttle_tx,
                    &ack_timeout_state,
                    &cancel_tx,
                    ClientError::unauthenticated("auth required"),
                )
                .await?;
                return Ok(false);
            }
            if acked
                && session.ack_order.is_enabled()
                && let Ok((request_id, ack)) = felix_wire::binary::peek_acked_publish_prefix(&frame)
                && ack != felix_wire::AckMode::None
                && !admit_pipelined(
                    &session.ack_order,
                    session.publish_window.as_ref(),
                    &mut cancel_rx_read,
                    request_id,
                )
                .await
            {
                break;
            }
            if acked {
                handle_acked_binary_publish_batch_control(
                    &broker,
                    &mut session.stream_cache,
                    &mut session.stream_cache_key,
                    &publish_ctx,
                    &frame,
                    session.auth_ctx.as_ref(),
                    throttled,
                    session.commit_ack,
                    sample,
                    &out_ack_tx,
                    &out_ack_depth,
                    &ack_throttle_tx,
                    &ack_timeout_state,
                    &cancel_tx,
                    &commit_acks,
                    session.peer_flags,
                    session.peer_features,
                )
                .await?;
            } else {
                handle_binary_publish_batch_control(
                    &broker,
                    &mut session.stream_cache,
                    &mut session.stream_cache_key,
                    &publish_ctx,
                    &frame,
                    session.auth_ctx.as_ref(),
                    sample,
                    &cancel_tx,
                )
                .await?;
            }
            continue;
        }
        // Slow-path: decode JSON control message. Decode errors are considered fatal protocol
        // violations and terminate the stream.
        let decode_start = t_now_if(sample);
        let message = match Message::decode(frame.clone()).context("decode message") {
            Ok(message) => message,
            Err(err) => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = crate::serving::quic::telemetry::frame_counters();
                    counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_frames_in_err.fetch_add(1, Ordering::Relaxed);
                    counters.pub_batches_in_err.fetch_add(1, Ordering::Relaxed);
                }
                crate::serving::quic::telemetry::log_decode_error("control_message", &err, &frame);
                return Err(err);
            }
        };
        let decode_ns = decode_start.map(|start| start.elapsed().as_nanos() as u64);
        if let Some(decode_ns) = decode_ns {
            timings::record_decode_ns(decode_ns);
            t_histogram!("felix_broker_decode_ns").record(decode_ns as f64);
        }
        if session.ack_order.is_enabled()
            && let Some(request_id) = pipelined_request(&message)
            && !admit_pipelined(
                &session.ack_order,
                session.publish_window.as_ref(),
                &mut cancel_rx_read,
                request_id,
            )
            .await
        {
            break;
        }
        let cx = Ctx {
            broker: &broker,
            connection: &connection,
            config: &config,
            auth: &auth,
            publish_ctx: &publish_ctx,
            authz_ctx: &authz_ctx,
            out_ack_tx: &out_ack_tx,
            out_ack_depth: &out_ack_depth,
            ack_throttle_tx: &ack_throttle_tx,
            ack_timeout_state: &ack_timeout_state,
            cancel_tx: &cancel_tx,
            commit_acks: &commit_acks,
            throttled,
            sample,
            read_ns,
            decode_ns,
        };
        // Dispatch by message type. Most handlers are responsible for enqueuing responses into
        // `out_ack_tx` rather than writing directly to the network.
        let step = match message {
            // Newer than this broker, or an extension it does not have.
            message @ (Message::Unknown | Message::Extension { .. }) => {
                unsupported::unsupported(&cx, &session, &frame, &message).await?
            }
            Message::Auth {
                tenant_id,
                token,
                client_flags,
                client_features,
                // Nothing reads the extended word until a feature lives there.
                client_features_hi: _,
            } => {
                session::authenticate(
                    &cx,
                    &mut session,
                    tenant_id,
                    token,
                    client_flags,
                    client_features,
                )
                .await?
            }
            Message::Publish {
                tenant_id,
                namespace,
                stream,
                payload,
                key,
                request_id,
                ack,
            } => {
                publish::publish(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    stream,
                    payload,
                    key,
                    request_id,
                    ack,
                )
                .await?
            }
            Message::PublishBatch {
                tenant_id,
                namespace,
                stream,
                payloads,
                key,
                request_id,
                ack,
            } => {
                publish::publish_batch(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    stream,
                    payloads,
                    key,
                    request_id,
                    ack,
                )
                .await?
            }
            Message::PublishIdempotent {
                tenant_id,
                namespace,
                stream,
                payloads,
                key,
                request_id,
                producer_id,
                sequence,
            } => {
                publish::publish_idempotent(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    stream,
                    payloads,
                    key,
                    request_id,
                    producer_id,
                    sequence,
                )
                .await?
            }
            Message::ProducerInit { request_id } => {
                publish::producer_init(&cx, &mut session, request_id).await?
            }
            Message::Topology => discovery::topology(&cx, &mut session).await?,
            Message::StreamShards {
                tenant_id,
                namespace,
                stream,
                request_id,
            } => {
                discovery::stream_shards(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    stream,
                    request_id,
                )
                .await?
            }
            Message::CacheShards {
                tenant_id,
                namespace,
                cache,
                request_id,
            } => {
                discovery::cache_shards(&cx, &mut session, tenant_id, namespace, cache, request_id)
                    .await?
            }
            Message::ShardOwners {
                tenant_id,
                namespace,
                name,
                kind,
                request_id,
            } => {
                discovery::shard_owners(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    name,
                    kind,
                    request_id,
                )
                .await?
            }
            Message::ShardInspect {
                tenant_id,
                namespace,
                name,
                kind,
                shard,
                request_id,
            } => {
                let key = inspect::target(tenant_id, namespace, name, kind, shard);
                inspect::shard_inspect(&cx, &mut session, key, request_id).await?
            }
            Message::SubscriptionsList {
                filter,
                limit,
                cursor,
                request_id,
            } => {
                inspect::subscriptions_list(&cx, &mut session, filter, limit, cursor, request_id)
                    .await?
            }
            Message::Subscribe {
                tenant_id,
                namespace,
                stream,
                subscription_id,
                start,
                shard,
                queue_capacity,
            } => {
                subscribe::subscribe(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    stream,
                    subscription_id,
                    start,
                    shard,
                    queue_capacity,
                )
                .await?
            }
            // Broker -> client only; a client sending one is a protocol error.
            Message::SubscribeCursorError { .. } => {
                subscribe::subscribe_cursor_error(&cx, &mut session).await?
            }
            Message::CachePut {
                tenant_id,
                namespace,
                cache,
                key,
                value,
                request_id,
                ttl_ms,
            } => {
                cache::cache_put(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    cache,
                    key,
                    value,
                    request_id,
                    ttl_ms,
                )
                .await?
            }
            Message::CacheGet {
                tenant_id,
                namespace,
                cache,
                key,
                request_id,
            } => {
                cache::cache_get(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    cache,
                    key,
                    request_id,
                )
                .await?
            }
            Message::CacheWatch {
                tenant_id,
                namespace,
                cache,
                key,
                prefix,
                shard,
                from_offset,
                retained,
                subscription_id,
            } => {
                cache::cache_watch(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    cache,
                    key,
                    prefix,
                    shard,
                    from_offset,
                    retained,
                    subscription_id,
                )
                .await?
            }
            Message::CounterAdd {
                tenant_id,
                namespace,
                cache,
                key,
                delta,
                request_id,
            } => {
                counter::counter_add(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    cache,
                    key,
                    delta,
                    request_id,
                )
                .await?
            }
            Message::CounterGet {
                tenant_id,
                namespace,
                cache,
                key,
                request_id,
            } => {
                counter::counter_get(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    cache,
                    key,
                    request_id,
                )
                .await?
            }
            Message::GroupPoll {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                max_records,
                wait_ms,
                request_id,
                consumer,
                reclaim,
                visibility_ms,
            } => {
                group::group_poll(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    stream,
                    shard,
                    group,
                    max_records,
                    wait_ms,
                    request_id,
                    consumer,
                    reclaim,
                    visibility_ms,
                )
                .await?
            }
            Message::GroupAck {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                offset,
                request_id,
            } => {
                group::group_settle(
                    &cx,
                    &mut session,
                    group::GroupTarget {
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                        group,
                    },
                    offset,
                    crate::serving::group_ops::Settle::Ack,
                    request_id,
                )
                .await?
            }
            Message::GroupNack {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                offset,
                request_id,
                delay_ms,
                attempts,
            } => {
                group::group_settle(
                    &cx,
                    &mut session,
                    group::GroupTarget {
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                        group,
                    },
                    offset,
                    crate::serving::group_ops::Settle::Nack {
                        delay: std::time::Duration::from_millis(delay_ms),
                        // Zero is a client that did not name the delivery.
                        attempts: (attempts != 0).then_some(attempts),
                    },
                    request_id,
                )
                .await?
            }
            Message::GroupDeadLetter {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                offset,
                request_id,
            } => {
                group::group_settle(
                    &cx,
                    &mut session,
                    group::GroupTarget {
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                        group,
                    },
                    offset,
                    crate::serving::group_ops::Settle::DeadLetter,
                    request_id,
                )
                .await?
            }
            Message::GroupExtend {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                offset,
                attempts,
                extend_ms,
                request_id,
            } => {
                group::group_extend(
                    &cx,
                    &mut session,
                    group::GroupTarget {
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                        group,
                    },
                    offset,
                    attempts,
                    extend_ms,
                    request_id,
                )
                .await?
            }
            Message::GroupDeadLetters {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                request_id,
            } => {
                group::group_dead_letters(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    stream,
                    shard,
                    group,
                    request_id,
                )
                .await?
            }
            Message::GroupDiscard {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                offset,
                request_id,
            } => {
                group::group_discard(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    stream,
                    shard,
                    group,
                    offset,
                    request_id,
                )
                .await?
            }
            Message::GroupRedrive {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                offset,
                request_id,
            } => {
                group::group_redrive(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    stream,
                    shard,
                    group,
                    offset,
                    request_id,
                )
                .await?
            }
            Message::GroupSeek {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                start,
                if_new,
                request_id,
            } => {
                group::group_admin(
                    &cx,
                    &mut session,
                    group::GroupTarget {
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                        group,
                    },
                    group::GroupAdmin::Seek { start, if_new },
                    request_id,
                )
                .await?
            }
            Message::GroupDescribe {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                request_id,
            } => {
                group::group_admin(
                    &cx,
                    &mut session,
                    group::GroupTarget {
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                        group,
                    },
                    group::GroupAdmin::Describe,
                    request_id,
                )
                .await?
            }
            Message::GroupDelete {
                tenant_id,
                namespace,
                stream,
                shard,
                group,
                request_id,
            } => {
                group::group_admin(
                    &cx,
                    &mut session,
                    group::GroupTarget {
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                        group,
                    },
                    group::GroupAdmin::Delete,
                    request_id,
                )
                .await?
            }
            Message::CachePutIf {
                tenant_id,
                namespace,
                cache,
                key,
                value,
                ttl_ms,
                condition,
                request_id,
            } => {
                cache::cache_conditional(
                    &cx,
                    &mut session,
                    cache::ConditionalRequest {
                        tenant_id,
                        namespace,
                        cache,
                        key,
                        request_id,
                        request: crate::serving::forward::CacheRequest::PutIf {
                            value,
                            // Zero is "no expiry" on the forward, as for a put.
                            ttl_ms: ttl_ms.unwrap_or(0),
                            condition: match condition {
                                felix_wire::CacheCondition::Absent => {
                                    felix_storage::CacheCondition::Absent
                                }
                                felix_wire::CacheCondition::Version(version) => {
                                    felix_storage::CacheCondition::Version(version)
                                }
                            },
                        },
                    },
                )
                .await?
            }
            Message::CacheDeleteIf {
                tenant_id,
                namespace,
                cache,
                key,
                version,
                request_id,
            } => {
                cache::cache_conditional(
                    &cx,
                    &mut session,
                    cache::ConditionalRequest {
                        tenant_id,
                        namespace,
                        cache,
                        key,
                        request_id,
                        request: crate::serving::forward::CacheRequest::DeleteIf { version },
                    },
                )
                .await?
            }
            Message::CacheDelete {
                tenant_id,
                namespace,
                cache,
                key,
                request_id,
            } => {
                cache::cache_delete(
                    &cx,
                    &mut session,
                    tenant_id,
                    namespace,
                    cache,
                    key,
                    request_id,
                )
                .await?
            }
            Message::Commit {
                tenant_id,
                namespace,
                stream,
                entity_key,
                event,
                changes,
                request_id,
                expected_offset,
            } => {
                commit::commit(
                    &cx,
                    &mut session,
                    commit::Target {
                        tenant_id,
                        namespace,
                        stream,
                        entity_key: Some(entity_key),
                    },
                    event,
                    changes,
                    expected_offset,
                    request_id,
                )
                .await?
            }
            Message::PublishIf {
                tenant_id,
                namespace,
                stream,
                payloads,
                key,
                expected_offset,
                request_id,
            } => {
                commit::publish_if(
                    &cx,
                    &mut session,
                    commit::Target {
                        tenant_id,
                        namespace,
                        stream,
                        entity_key: key,
                    },
                    payloads,
                    expected_offset,
                    request_id,
                )
                .await?
            }
            Message::StateGet {
                tenant_id,
                namespace,
                stream,
                entity_key,
                key,
                request_id,
            } => {
                commit::state_get(
                    &cx,
                    &mut session,
                    commit::Target {
                        tenant_id,
                        namespace,
                        stream,
                        entity_key: Some(entity_key),
                    },
                    key,
                    request_id,
                )
                .await?
            }
            Message::OffsetForTime {
                tenant_id,
                namespace,
                stream,
                shard,
                at_micros,
                request_id,
            } => {
                record_time::offset_for_time(
                    &cx,
                    &mut session,
                    record_time::ShardTarget {
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                    },
                    at_micros,
                    request_id,
                )
                .await?
            }
            Message::StreamRead {
                tenant_id,
                namespace,
                stream,
                shard,
                from,
                end,
                max_records,
                max_bytes,
                request_id,
            } => {
                stream_read::stream_read(
                    &cx,
                    &mut session,
                    record_time::ShardTarget {
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                    },
                    stream_read::ReadBounds {
                        from,
                        end,
                        max_records,
                        max_bytes,
                    },
                    request_id,
                )
                .await?
            }
            Message::GroupRecords { .. }
            | Message::StreamRecords { .. }
            | Message::OffsetValue { .. }
            | Message::CommitOk { .. }
            | Message::StateValue { .. }
            | Message::Unsupported { .. }
            | Message::GroupDeadLetterList { .. }
            | Message::GroupPosition { .. }
            | Message::GroupInfo { .. }
            | Message::GroupDeleted { .. }
            | Message::GroupExtended { .. }
            | Message::CacheValue { .. }
            | Message::CacheOk { .. }
            | Message::CacheConditionResult { .. }
            | Message::CounterValue { .. }
            | Message::CacheWatchStarted { .. }
            | Message::CacheEvent { .. }
            | Message::CacheWatchLagged { .. }
            | Message::Event { .. }
            | Message::EventBatch { .. }
            | Message::Subscribed { .. }
            | Message::EventStreamHello { .. }
            | Message::ShardMoved { .. }
            | Message::SubscriptionLagged { .. }
            | Message::PublishOk { .. }
            | Message::PublishError { .. }
            | Message::PublishRefused { .. }
            | Message::ProducerInitOk { .. }
            | Message::AuthOk { .. }
            | Message::TopologyView { .. }
            | Message::StreamShardsView { .. }
            | Message::CacheShardsView { .. }
            | Message::ShardOwnersView { .. }
            | Message::ShardInspectInfo { .. }
            | Message::SubscriptionsListInfo { .. }
            | Message::NotLeader { .. }
            | Message::Ok => {
                // Protocol hygiene: these message types should never arrive on the control stream
                // from the client. Treat as a protocol violation and close.
                handle_ack_enqueue_result(
                    send_outgoing_critical(
                        &out_ack_tx,
                        &out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &ack_throttle_tx,
                        Outgoing::Message(
                            ClientError::invalid("unexpected message type").into_message(),
                        ),
                    )
                    .await,
                    &ack_timeout_state,
                    &ack_throttle_tx,
                    &cancel_tx,
                )
                .await?;
                Step::Close(false)
            }
            Message::Error { .. } => Step::Close(false),
        };
        if let Step::Close(graceful) = step {
            return Ok(graceful);
        }
        if preauth_permit.is_some() && session.auth_ctx.is_some() {
            preauth_permit = None;
            publish_ctx.preauth.mark_authenticated();
        }
    }
    // `graceful_close` only tracks EOF from the peer. Any other early-exit path returns false
    // (protocol error) or Err (hard failure).
    Ok(graceful_close)
}

/// What an arm tells the loop to do next.
enum Step {
    /// Read the next frame.
    Next,
    /// End the stream; `true` is a graceful close, as `run_control_loop` returns.
    Close(bool),
}

/// The per-stream state the arms change.
struct Session {
    auth_ctx: Option<AuthContext>,
    /// Frame-flag bits the client understands. Narrowed to the pre-negotiation
    /// set until an `Auth` says otherwise.
    peer_flags: u16,
    /// Optional messages this client understands. Nothing until an `Auth` says
    /// otherwise.
    peer_features: u32,
    /// Acknowledged publishes are answered after the write: the broker-wide
    /// `ack_on_commit`, or `FEATURE_ACK_ON_COMMIT` from this client.
    commit_ack: bool,
    /// Shared with the writer, which shapes every error to what `Auth` offered.
    error_codes: Arc<ErrorCodeSupport>,
    /// Shared with the writer, which holds answers back into request order
    /// once `Auth` asks for pipelining.
    ack_order: Arc<AckOrder>,
    /// This stream's publish window, once `Auth` grants one.
    publish_window: Option<Arc<Semaphore>>,
    stream_cache: StreamHandleCache,
    stream_cache_key: String,
}

/// The request a publish is answered under, if it is answered at all.
fn pipelined_request(message: &Message) -> Option<u64> {
    match message {
        Message::Publish {
            request_id: Some(request_id),
            ack,
            ..
        }
        | Message::PublishBatch {
            request_id: Some(request_id),
            ack,
            ..
        } if *ack != Some(felix_wire::AckMode::None) => Some(*request_id),
        Message::PublishIdempotent { request_id, .. } => Some(*request_id),
        _ => None,
    }
}

/// Take a slot in the stream's publish window and register the publish
/// for an in-order answer. False when the stream was cancelled while it
/// waited.
///
/// Waiting here stops this stream's reads, which is the backpressure: the
/// client's frames queue in QUIC flow control, not in the tenant's share of
/// the publish queue.
async fn admit_pipelined(
    order: &AckOrder,
    window: Option<&Arc<Semaphore>>,
    cancel_rx: &mut watch::Receiver<bool>,
    request_id: u64,
) -> bool {
    let permit = match window {
        None => None,
        Some(window) => match Arc::clone(window).try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => {
                crate::serving::quic::telemetry::t_counter!(
                    "felix_broker_publish_window_full_total"
                )
                .increment(1);
                tokio::select! {
                    permit = Arc::clone(window).acquire_owned() => permit.ok(),
                    _ = cancelled(cancel_rx) => return false,
                }
            }
        },
    };
    order.register(request_id, permit);
    true
}

async fn cancelled(cancel_rx: &mut watch::Receiver<bool>) {
    while !*cancel_rx.borrow_and_update() {
        if cancel_rx.changed().await.is_err() {
            return;
        }
    }
}

/// What every arm reads: the connection's shared handles and this frame's
/// sampling state.
#[derive(Clone, Copy)]
struct Ctx<'a> {
    broker: &'a Arc<Broker>,
    connection: &'a felix_transport::QuicConnection,
    config: &'a BrokerConfig,
    auth: &'a Arc<BrokerAuth>,
    publish_ctx: &'a PublishContext,
    authz_ctx: &'a Responder<'a>,
    out_ack_tx: &'a mpsc::Sender<Outgoing>,
    out_ack_depth: &'a Arc<AtomicUsize>,
    ack_throttle_tx: &'a watch::Sender<bool>,
    ack_timeout_state: &'a Arc<parking_lot::Mutex<AckTimeoutState>>,
    cancel_tx: &'a watch::Sender<bool>,
    commit_acks: &'a CommitAcks,
    throttled: bool,
    sample: bool,
    read_ns: Option<u64>,
    decode_ns: Option<u64>,
}
