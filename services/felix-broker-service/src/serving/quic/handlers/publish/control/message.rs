//! A single JSON publish on the control stream.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use felix_broker::Broker;
use felix_wire::Message;
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot, watch};
use tracing::Instrument;

use super::batch::{overloaded_after_enqueue, refusal_for_client};
use crate::observability::timings;
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::errors::AckEnqueueError;
use crate::serving::quic::handlers::publish::ack::{
    AckEncoding, AckTimeoutState, AckWaiterMessage, EnqueuePolicy, Outgoing,
    handle_ack_enqueue_result, send_outgoing_best_effort, send_outgoing_critical,
};
use crate::serving::quic::handlers::publish::ingress::{PublishTarget, enqueue_tenant_publish};
use crate::serving::quic::handlers::publish::route::{
    internal_ack, needs_quorum, publish_target, resolve_route, resolve_shard,
};
use crate::serving::quic::handlers::publish::{
    PublishContext, PublishJob, StreamHandleCache, record_json_publish,
};
use crate::serving::quic::telemetry::{
    count_publish, count_publish_accepted, t_consume_instant, t_counter, t_histogram, t_now_if,
};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_publish_message(
    broker: &Broker,
    publish_ctx: &PublishContext,
    stream_cache: &mut StreamHandleCache,
    stream_cache_key: &mut String,
    throttled: bool,
    ack_on_commit: bool,
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<AtomicUsize>,
    ack_throttle_tx: &watch::Sender<bool>,
    ack_timeout_state: &Arc<Mutex<AckTimeoutState>>,
    cancel_tx: &watch::Sender<bool>,
    ack_waiters: &Arc<Semaphore>,
    ack_waiter_tx: &mpsc::Sender<AckWaiterMessage>,
    ack_wait_timeout: Duration,
    tenant_id: String,
    namespace: String,
    stream: String,
    payload: Vec<u8>,
    key: Option<bytes::Bytes>,
    request_id: Option<u64>,
    ack: Option<felix_wire::AckMode>,
    sample: bool,
    // The publisher's token, carried on a forward for the owner to verify.
    credential: String,
) -> Result<()> {
    record_json_publish("publish");
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::serving::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_items_in_ok.fetch_add(1, Ordering::Relaxed);
    }
    if throttled {
        // Overload shed path:
        // - We intentionally skip broker work.
        // - We attempt to return a PublishError / Error if an ack was requested.
        // - If the outbound queue is full, we currently may drop this error ack.
        //   This can strand clients waiting for an ack. Consider switching these
        //   sends to critical enqueue or closing the stream when full.
        let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerMessage);
        if ack_mode != felix_wire::AckMode::None {
            if let Some(request_id) = request_id {
                let result = send_outgoing_best_effort(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(
                        ClientError::overloaded("server overloaded").into_publish_error(request_id),
                    ),
                )
                .await;
                if !matches!(result, Err(AckEnqueueError::Full)) {
                    handle_ack_enqueue_result(
                        result,
                        ack_timeout_state,
                        ack_throttle_tx,
                        cancel_tx,
                    )
                    .await?;
                }
            } else {
                let result = send_outgoing_best_effort(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(ClientError::overloaded("server overloaded").into_message()),
                )
                .await;
                if !matches!(result, Err(AckEnqueueError::Full)) {
                    handle_ack_enqueue_result(
                        result,
                        ack_timeout_state,
                        ack_throttle_tx,
                        cancel_tx,
                    )
                    .await?;
                }
            }
        }
        count_publish("dropped");
        return Ok(());
    }
    // Publish protocol (control stream):
    // - Client sends Publish { payload, request_id?, ack }.
    // - Broker enqueues payload (fanout path) and responds:
    //   - AckMode::None -> no response.
    //   - AckMode::PerMessage -> PublishOk/PublishError with request_id (acks may be out of order).
    // - request_id is required for any acked publish.
    let start = crate::serving::quic::telemetry::t_instant_now();
    t_consume_instant(start);
    let payload_len = payload.len();
    let enqueue_start = t_now_if(sample);
    // Ack mode determines if we wait for broker commit or reply immediately.
    let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerMessage);
    // Protocol invariant: any acked publish must include request_id, because acks
    // may be out-of-order and request_id is the only correlator.
    if ack_mode != felix_wire::AckMode::None && request_id.is_none() {
        handle_ack_enqueue_result(
            send_outgoing_critical(
                out_ack_tx,
                out_ack_depth,
                "felix_broker_out_ack_depth",
                ack_throttle_tx,
                Outgoing::Message(
                    ClientError::invalid("missing request_id for acked publish").into_message(),
                ),
            )
            .await,
            ack_timeout_state,
            ack_throttle_tx,
            cancel_tx,
        )
        .await?;
        return Ok(());
    }
    // Resolved once and then carried: the route and the batch must name the
    // same shard, or the owner appends to a different log than the one this
    // broker dispatched on.
    let shard = resolve_shard(publish_ctx, &tenant_id, &namespace, &stream, key.as_deref());
    let target = publish_target(
        resolve_route(
            broker,
            publish_ctx.authority(),
            stream_cache,
            stream_cache_key,
            &tenant_id,
            &namespace,
            &stream,
            shard,
        )
        .await,
        publish_ctx,
        &tenant_id,
        &namespace,
        &stream,
        shard,
        internal_ack(ack),
        &credential,
    );
    let refusal = target.as_ref().err().cloned();
    let target = target.ok();

    // A forwarded publish is acknowledged only once the owner has answered,
    // whatever `ack_on_commit` says. That setting is a local policy — "accepted
    // by this broker is good enough" — and for a forward this broker has
    // accepted nothing: the data is not on its disk, and the owner may still
    // refuse it.
    let forwarding = matches!(target, Some(PublishTarget::Forward { .. }));
    // A `Quorum` publish is acknowledged only once a majority holds it,
    // whatever `ack_on_commit` says -- the same reasoning as forwarding, one
    // step further. `ack_on_commit` is a local policy meaning "accepted by this
    // broker is good enough", and `Quorum` is precisely the stream that said it
    // is not.
    //
    // Without this the quorum wait still runs, in a worker holding a response
    // channel nobody created, and its answer -- including its refusals -- goes
    // nowhere. The client is told the record is on a majority the moment it is
    // queued. `ack_on_commit` is off by default, so that was every direct
    // `Quorum` publish.
    let quorum = needs_quorum(&target);
    // Last because it reads the clock: an ack on enqueue promises a write the
    // lease may not live to allow.
    let commit_ack = ack_on_commit
        || forwarding
        || quorum
        || (ack_mode != felix_wire::AckMode::None && publish_ctx.must_wait_for_write());
    let (response_tx, response_rx) = if ack_mode != felix_wire::AckMode::None && commit_ack {
        let (response_tx, response_rx) = oneshot::channel();
        (Some(response_tx), Some(response_rx))
    } else {
        (None, None)
    };
    let Some(target) = target else {
        count_publish("error");
        if ack_mode != felix_wire::AckMode::None {
            let request_id = request_id.expect("request id checked");
            handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(
                        refusal_for_client(refusal, &tenant_id, &namespace, &stream)
                            .into_publish_error(request_id),
                    ),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
        }
        return Ok(());
    };
    let enqueue_result = enqueue_tenant_publish(
        publish_ctx,
        &tenant_id,
        PublishJob {
            target,
            payloads: vec![Bytes::from(payload)],
            response: response_tx,
            acked_on_enqueue: ack_mode != felix_wire::AckMode::None && !commit_ack,
            admission_permit: None,
            fenced: None,
        },
        if ack_mode == felix_wire::AckMode::None {
            publish_ctx.overflow_policy()
        } else if commit_ack {
            EnqueuePolicy::Wait
        } else {
            EnqueuePolicy::Fail
        },
        Some(cancel_tx.subscribe()),
    )
    .await;
    if let Some(start) = enqueue_start {
        let enqueue_ns = start.elapsed().as_nanos() as u64;
        timings::record_fanout_ns(enqueue_ns);
        t_histogram!("felix_broker_ingress_enqueue_ns").record(enqueue_ns as f64);
    }
    let span = tracing::trace_span!(
        "publish",
        tenant_id = %tenant_id,
        namespace = %namespace,
        stream = %stream
    );
    async move {
        match enqueue_result {
            Ok(true) => {
                if ack_mode == felix_wire::AckMode::None {
                    count_publish_accepted("accepted", payload_len as u64);
                }
            }
            Ok(false) => {
                count_publish("dropped");
                if ack_mode != felix_wire::AckMode::None {
                    let request_id = request_id.expect("request id checked");
                    handle_ack_enqueue_result(
                        send_outgoing_critical(
                            out_ack_tx,
                            out_ack_depth,
                            "felix_broker_out_ack_depth",
                            ack_throttle_tx,
                            Outgoing::Message(
                                ClientError::overloaded("ingress overloaded")
                                    .into_publish_error(request_id),
                            ),
                        )
                        .await,
                        ack_timeout_state,
                        ack_throttle_tx,
                        cancel_tx,
                    )
                    .await?;
                }
                return Ok(());
            }
            Err(err) => {
                count_publish("error");
                if ack_mode != felix_wire::AckMode::None {
                    let request_id = request_id.expect("request id checked");
                    handle_ack_enqueue_result(
                        send_outgoing_critical(
                            out_ack_tx,
                            out_ack_depth,
                            "felix_broker_out_ack_depth",
                            ack_throttle_tx,
                            // Nothing was enqueued.
                            Outgoing::Message(
                                ClientError::not_enqueued(&err).into_publish_error(request_id),
                            ),
                        )
                        .await,
                        ack_timeout_state,
                        ack_throttle_tx,
                        cancel_tx,
                    )
                    .await?;
                }
                return Ok(());
            }
        }
        if ack_mode == felix_wire::AckMode::None {
            return Ok(());
        }
        // A forward has no enqueue-ack mode: this broker enqueued the batch to send
        // it somewhere else, which is not a fact worth acknowledging. The commit-ack
        // path below waits for the owner's answer instead.
        //
        // Nor does a `Quorum` publish. "Accepted into the ingress queue" is not an
        // answer to "is this on a majority", and answering it anyway is how a
        // `Quorum` stream came to behave exactly like a `Leader` one whenever
        // `ack_on_commit` was off -- which is the default.
        if !commit_ack {
            // Enqueue-ack mode:
            // Ack means "accepted into the ingress queue", not "committed". This keeps
            // latency low but can report success even if a later broker error occurs.
            // Fire-and-forget ack after enqueue when configured.
            let request_id = request_id.expect("request id checked");
            handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(Message::PublishOk { request_id }),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
            count_publish_accepted("ok", payload_len as u64);
            #[cfg(feature = "telemetry")]
            {
                t_histogram!("felix_publish_latency_ms", "mode" => "enqueue")
                    .record(start.elapsed().as_secs_f64() * 1000.0);
            }
            return Ok(());
        }
        let request_id = request_id.expect("request id checked");
        let response_rx = response_rx.expect("response rx available");
        let payload_len_for_metrics = payload_len as u64;
        // Commit-ack mode:
        // We bound the number of in-flight commit acks. If exhausted, we fail fast.
        // Correctness note: failing after enqueue means the publish may still commit;
        // the client will see an error/overload even though the publish succeeded.
        // If that is unacceptable, we must enforce admission *before* enqueue.
        let permit = match Arc::clone(ack_waiters).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                let _ = send_outgoing_best_effort(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(overloaded_after_enqueue().into_publish_error(request_id)),
                )
                .await;
                t_counter!("felix_broker_ack_waiters_exhausted_total").increment(1);
                return Ok(());
            }
        };
        let msg = AckWaiterMessage::Publish {
            request_id,
            // There is no binary encoding for single publishes; the binary fast path
            // is batch-only, so this waiter always replies in JSON.
            encoding: AckEncoding::Json,
            payload_len: payload_len_for_metrics,
            start,
            response_rx,
            permit,
        };
        match ack_waiter_tx.try_send(msg) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(msg)) => {
                drop(match msg {
                    AckWaiterMessage::Publish { permit, .. }
                    | AckWaiterMessage::PublishBatch { permit, .. } => permit,
                });
                t_counter!("felix_broker_ack_waiter_queue_full_total").increment(1);
                let _ = send_outgoing_best_effort(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(overloaded_after_enqueue().into_publish_error(request_id)),
                )
                .await;
                return Ok(());
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(msg)) => {
                drop(match msg {
                    AckWaiterMessage::Publish { permit, .. }
                    | AckWaiterMessage::PublishBatch { permit, .. } => permit,
                });
                t_counter!("felix_broker_ack_waiter_queue_full_total").increment(1);
                let _ = send_outgoing_best_effort(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    Outgoing::Message(overloaded_after_enqueue().into_publish_error(request_id)),
                )
                .await;
                return Ok(());
            }
        }
        let _ = ack_wait_timeout;
        Ok(())
    }
    .instrument(span)
    .await
}
