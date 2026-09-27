//! A JSON publish batch on the control stream.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "telemetry")]
use std::sync::atomic::Ordering;

use anyhow::Result;
use bytes::Bytes;
use felix_broker::Broker;
use felix_wire::Message;
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot, watch};
use tracing::Instrument;

use crate::observability::timings;
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::errors::AckEnqueueError;
use crate::serving::quic::handlers::publish::ack::{
    AckEncoding, AckTimeoutState, AckWaiterMessage, EnqueuePolicy, Outgoing,
    handle_ack_enqueue_result, send_outgoing_best_effort, send_outgoing_critical,
};
use crate::serving::quic::handlers::publish::ingress::{PublishTarget, enqueue_tenant_publish};
use crate::serving::quic::handlers::publish::route::{
    PublishRoute, internal_ack, local_shard_key, needs_quorum, publish_target, resolve_route,
    resolve_shard,
};
use crate::serving::quic::handlers::publish::{
    PublishContext, PublishJob, StreamHandleCache, record_json_publish,
};
use crate::serving::quic::telemetry::{
    count_publish, count_publish_accepted, t_counter, t_histogram, t_now_if,
};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_publish_batch_message(
    // Frame-flag bits the client advertised, so the ack may name a forwarding
    // owner only when the client can parse one. Zero from the JSON path, which
    // has no ack frame to put it in.
    peer_flags: u16,
    broker: &Broker,
    publish_ctx: &PublishContext,
    stream_cache: &mut StreamHandleCache,
    stream_cache_key: &mut String,
    throttled: bool,
    ack_on_commit: bool,
    encoding: AckEncoding,
    out_ack_tx: &mpsc::Sender<Outgoing>,
    out_ack_depth: &Arc<AtomicUsize>,
    ack_throttle_tx: &watch::Sender<bool>,
    ack_timeout_state: &Arc<Mutex<AckTimeoutState>>,
    cancel_tx: &watch::Sender<bool>,
    ack_waiters: &Arc<Semaphore>,
    ack_waiter_tx: &mpsc::Sender<AckWaiterMessage>,
    tenant_id: String,
    namespace: String,
    stream: String,
    payloads: Vec<Vec<u8>>,
    key: Option<bytes::Bytes>,
    request_id: Option<u64>,
    ack: Option<felix_wire::AckMode>,
    sample: bool,
    // The publisher's token, carried on a forward for the owner to verify.
    credential: String,
    // `(producer_id, sequence)` for a `publish_idempotent`; the batch is then
    // appended once however many times it arrives, acknowledged only once
    // committed, and never forwarded.
    producer: Option<(u64, u64)>,
) -> Result<()> {
    record_json_publish("publish_batch");
    #[cfg(feature = "telemetry")]
    {
        let counters = crate::serving::quic::telemetry::frame_counters();
        counters.pub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        counters.pub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
        counters
            .pub_items_in_ok
            .fetch_add(payloads.len() as u64, Ordering::Relaxed);
    }
    if throttled {
        // Overload shed path:
        // - We intentionally skip broker work.
        // - We attempt to return a PublishError / Error if an ack was requested.
        // - If the outbound queue is full, we currently may drop this error ack.
        //   This can strand clients waiting for an ack. Consider switching these
        //   sends to critical enqueue or closing the stream when full.
        let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerBatch);
        if ack_mode != felix_wire::AckMode::None {
            if let Some(request_id) = request_id {
                let result = send_outgoing_best_effort(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    encoding.error(request_id, ClientError::overloaded("server overloaded")),
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
    // PublishBatch protocol (control stream):
    // - Client sends PublishBatch { payloads, request_id?, ack }.
    // - Broker enqueues all payloads as one unit and responds:
    //   - AckMode::None -> no response.
    //   - AckMode::PerBatch -> PublishOk/PublishError with request_id (acks may be out of order).
    // - request_id is required for any acked publish.
    let span = tracing::trace_span!(
        "publish_batch",
        tenant_id = %tenant_id,
        namespace = %namespace,
        stream = %stream,
        count = payloads.len()
    );
    async move {
        let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerBatch);
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
                        ClientError::invalid("missing request_id for acked publish batch")
                            .into_message(),
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
        let route = resolve_route(
            broker,
            publish_ctx.authority(),
            stream_cache,
            stream_cache_key,
            &tenant_id,
            &namespace,
            &stream,
            shard,
        )
        .await;
        let target = match producer {
            None => publish_target(
                route,
                publish_ctx,
                &tenant_id,
                &namespace,
                &stream,
                shard,
                internal_ack(ack),
                &credential,
            ),
            Some((producer_id, sequence)) => match route {
                PublishRoute::Local {
                    handle,
                    generation,
                    fenced,
                } => Ok(PublishTarget::Idempotent {
                    handle,
                    shard: local_shard_key(publish_ctx, &tenant_id, &namespace, &stream, shard),
                    generation,
                    fenced,
                    producer_id,
                    sequence,
                }),
                // Only the leader holds the sequences a re-send is checked
                // against, so an idempotent batch is not forwarded: forwarded, a
                // duplicate could land on the owner from two ingress brokers with
                // nothing to tell the second from the first. The producer is told
                // where to go instead.
                PublishRoute::Forward(owner) => {
                    count_publish("not_owner");
                    let request_id = request_id.expect("request id checked");
                    let addr = publish_ctx
                        .client_endpoints
                        .as_ref()
                        .and_then(|endpoints| endpoints.redirect_addr(&owner.node_id));
                    handle_ack_enqueue_result(
                    send_outgoing_critical(
                        out_ack_tx,
                        out_ack_depth,
                        "felix_broker_out_ack_depth",
                        ack_throttle_tx,
                        Outgoing::Message(Message::PublishRefused {
                            request_id,
                            message: format!(
                                "shard {shard} of {tenant_id}/{namespace}/{stream} is led by {}; \
                                 an idempotent publish must go to the leader",
                                owner.node_id
                            ),
                            reason: felix_wire::PublishRefusalReason::NotLeader {
                                node_id: owner.node_id,
                                addr,
                            },
                        }),
                    )
                    .await,
                    ack_timeout_state,
                    ack_throttle_tx,
                    cancel_tx,
                )
                .await?;
                    return Ok(());
                }
                PublishRoute::Refused(refusal) => Err(refusal),
            },
        };
        let refusal = target.as_ref().err().cloned();
        let target = target.ok();
        // An idempotent publish is acknowledged only once committed, like a
        // forward or a `Quorum` publish: "accepted into the queue" says nothing
        // about whether the sequence was taken, which is what the producer needs
        // to know before it sends the next.
        let idempotent = producer.is_some();
        // See the single-publish path: a forward is acknowledged only once the owner
        // has answered, whatever `ack_on_commit` says, and a `Quorum` publish only
        // once a majority holds it.
        let forwarding = matches!(target, Some(PublishTarget::Forward { .. }));
        // Who to tell the client about, when this batch is being forwarded.
        //
        // Resolved here because this is where the routing decision is: by the time
        // the ack is written the target has been consumed. The client address comes
        // from the endpoint registry rather than `ForwardTarget::advertise_addr`,
        // which is the *peer* listener -- telling a client to publish to the
        // internal port would send it somewhere that does not speak to clients.
        //
        // Only for a client that advertised the bit. Setting a flag it did not
        // offer makes it reject the frame, and the frame acknowledges a publish
        // that already succeeded.
        let hint_owner = matches!(encoding, AckEncoding::Binary)
            && felix_wire::supports(peer_flags, felix_wire::FLAG_BINARY_PUBLISH_ACK_OWNER);
        let forwarded_to = match (&target, hint_owner) {
            (Some(PublishTarget::Forward { target, .. }), true) => {
                let addr = publish_ctx
                    .client_endpoints
                    .as_ref()
                    .and_then(|endpoints| endpoints.redirect_addr(&target.node_id));
                Some(felix_wire::binary::PublishOwner {
                    node_id: target.node_id.clone(),
                    addr,
                    generation: target.generation,
                })
            }
            _ => None,
        };
        let quorum = needs_quorum(&target);
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
                        encoding.error(
                            request_id,
                            refusal_for_client(refusal, &tenant_id, &namespace, &stream),
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
        let payload_bytes = payloads
            .iter()
            .map(|payload| payload.len())
            .collect::<Vec<_>>();
        let payloads = payloads.into_iter().map(Bytes::from).collect::<Vec<_>>();
        // Same reasoning as the JSON path above: `Quorum` outranks the local
        // ack-on-commit policy, because it is the stream saying this broker alone
        // cannot answer for the record.
        //
        // The lease check goes last because it reads the clock: see
        // `PublishContext::must_wait_for_write`.
        let commit_ack = ack_on_commit
            || forwarding
            || quorum
            || idempotent
            || (ack_mode != felix_wire::AckMode::None && publish_ctx.must_wait_for_write());
        let (response_tx, response_rx) = if ack_mode != felix_wire::AckMode::None && commit_ack {
            let (response_tx, response_rx) = oneshot::channel();
            (Some(response_tx), Some(response_rx))
        } else {
            (None, None)
        };
        let fanout_start = t_now_if(sample);
        let enqueue_result = enqueue_tenant_publish(
            publish_ctx,
            &tenant_id,
            PublishJob {
                target,
                payloads,
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
        if let Some(start) = fanout_start {
            let fanout_ns = start.elapsed().as_nanos() as u64;
            timings::record_fanout_ns(fanout_ns);
            t_histogram!("felix_broker_ingress_enqueue_ns").record(fanout_ns as f64);
        }
        match enqueue_result {
            Ok(true) => {
                if ack_mode == felix_wire::AckMode::None {
                    count_publish_accepted("accepted", payload_bytes.iter().sum::<usize>() as u64);
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
                            encoding
                                .error(request_id, ClientError::overloaded("ingress overloaded")),
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
                            encoding.error(request_id, ClientError::not_enqueued(&err)),
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
            // Batch ack can be sent once enqueued if commit acks are disabled.
            let request_id = request_id.expect("request id checked");
            handle_ack_enqueue_result(
                send_outgoing_critical(
                    out_ack_tx,
                    out_ack_depth,
                    "felix_broker_out_ack_depth",
                    ack_throttle_tx,
                    encoding.ok(request_id),
                )
                .await,
                ack_timeout_state,
                ack_throttle_tx,
                cancel_tx,
            )
            .await?;
            count_publish_accepted("ok", payload_bytes.iter().sum::<usize>() as u64);
            return Ok(());
        }
        let request_id = request_id.expect("request id checked");
        let response_rx = response_rx.expect("response rx available");
        let payload_bytes_for_metrics = payload_bytes;
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
                    encoding.error(request_id, overloaded_after_enqueue()),
                )
                .await;
                t_counter!("felix_broker_ack_waiters_exhausted_total").increment(1);
                return Ok(());
            }
        };
        let msg = AckWaiterMessage::PublishBatch {
            forwarded_to,
            request_id,
            encoding,
            payload_bytes: payload_bytes_for_metrics,
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
                    encoding.error(request_id, overloaded_after_enqueue()),
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
                    encoding.error(request_id, overloaded_after_enqueue()),
                )
                .await;
                return Ok(());
            }
        }
        Ok(())
    }
    .instrument(span)
    .await
}

/// Why a publish that never got a target was refused, as the client sees it.
///
/// Only a stream that does not resolve is "stream not found", in the words
/// clients without codes have always matched on. Anything else -- a shard that
/// is not servable, a lapsed lease, an owner this broker cannot forward to --
/// keeps its own code, reason and message: calling it not found would make a
/// retryable refusal read as a permanent one.
pub(super) fn refusal_for_client(
    refusal: Option<ClientError>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
) -> ClientError {
    match refusal {
        Some(refusal) if *refusal.code() != felix_wire::ErrorCode::NotFound => refusal.prefixed(
            &format!("publish to {tenant_id}/{namespace}/{stream} refused"),
        ),
        refusal => refusal
            .unwrap_or_else(|| ClientError::not_found(""))
            .reworded(format!(
                "stream not found: tenant={tenant_id} namespace={namespace} stream={stream}"
            )),
    }
}

/// The batch is already with the worker, so it may still land: overloaded, but
/// with the outcome unknown rather than "not applied".
pub(super) fn overloaded_after_enqueue() -> ClientError {
    ClientError::overloaded("server overloaded").with_retry(felix_wire::RetryClass::OutcomeUnknown)
}
