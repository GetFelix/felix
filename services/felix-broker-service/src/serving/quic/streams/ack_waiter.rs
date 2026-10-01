//! Ack waiter loop decouples "publish commit completion" from the control-stream read loop.
//!
//! Why this exists:
//! - The control stream is responsible for ingesting client requests (Publish / PublishBatch) and must
//!   stay responsive. If we were to await broker commit completion inline while reading, the control
//!   stream would stall under load and amplify head-of-line blocking.
//! - Instead, request handling enqueues a waiter message containing a oneshot receiver that completes
//!   when the publish worker finishes its broker call.
//! - This task waits on those receivers and emits ACK/ERR responses back to the client.
//!
//! Key properties:
//! - Responses may be emitted out-of-order (completion order), which is acceptable because request_id
//!   provides correlation.
//! - A semaphore (permit) bounds the number of in-flight waiters; every path MUST drop the permit to
//!   prevent deadlock/leak (see drop(permit) on all branches).
//! - Cancellation (cancel_rx) is checked both in the outer select loop and inside each waiter future
//!   to ensure we can abort promptly even while waiting on a timeout.

use crate::serving::quic::client_error::ClientError;
use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, stream::FuturesUnordered};
use tokio::sync::{Mutex, mpsc, watch};

use crate::serving::quic::handlers::publish::{
    AckTimeoutState, AckWaiterMessage, AckWaiterResult, Outgoing, handle_ack_enqueue_result,
    send_outgoing_critical,
};
#[cfg(feature = "telemetry")]
use crate::serving::quic::telemetry::t_histogram;
use crate::serving::quic::telemetry::{
    count_publish, count_publish_accepted, t_consume_instant, t_counter,
};

// Waits on publish completion futures and emits ack responses in completion order.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_ack_waiter_loop(
    mut ack_waiter_rx: mpsc::Receiver<AckWaiterMessage>,
    out_ack_tx_waiter: mpsc::Sender<Outgoing>,
    out_ack_depth_waiter: Arc<std::sync::atomic::AtomicUsize>,
    ack_throttle_tx_waiter: watch::Sender<bool>,
    ack_timeout_state_waiter: Arc<Mutex<AckTimeoutState>>,
    cancel_tx_waiter: watch::Sender<bool>,
    mut cancel_rx_waiter: watch::Receiver<bool>,
    ack_wait_timeout: Duration,
) {
    // Ack waiter task (only meaningful when ack_on_commit=true):
    //
    // High-level flow:
    // 1) The control stream read loop receives a Publish/PublishBatch request.
    // 2) The broker publish worker is spawned/queued and returns a oneshot receiver representing
    //    "commit complete" (or error) for that specific request.
    // 3) The control stream enqueues an AckWaiterMessage containing that receiver and a semaphore
    //    permit that limits concurrent in-flight waiters.
    // 4) This task:
    //    - stores each waiter as a future in `FuturesUnordered` (so completions are polled fairly),
    //    - awaits completions (or timeouts),
    //    - and pushes an Outgoing::Message onto the outbound ACK channel.
    //
    // Performance/behavior:
    // - FuturesUnordered yields results as soon as any waiter completes (completion-order ACKs).
    // - The outbound ack channel is subject to backpressure; we use `send_outgoing_critical` and
    //   `handle_ack_enqueue_result` to update metrics/state and possibly trigger throttling/cancel.
    // - Timeouts are applied per-request to prevent indefinitely waiting on a stuck publish worker.
    //
    // Correctness note: every waiter must drop its semaphore permit on *all* paths.
    let mut pending = FuturesUnordered::new();
    // `pending` holds per-request futures that resolve when the publish worker responds or when
    // the per-request timeout fires. FuturesUnordered lets us drive many waiters concurrently
    // without imposing ordering.
    let mut closed = false;
    // We mark `closed` when the sender side of `ack_waiter_rx` is dropped. We still need to drain
    // any already-enqueued waiters in `pending` before exiting, otherwise we could drop permits
    // and/or lose in-flight responses.
    loop {
        tokio::select! {
            changed = cancel_rx_waiter.changed() => {
                if changed.is_err() || *cancel_rx_waiter.borrow() {
                    break;
                }
            }
            message = ack_waiter_rx.recv() => {
                match message {
                    // New waiter request: wrap it as a future and push into `pending`.
                    Some(message) => {
                        // Clone the cancellation watch receiver so each waiter can be cancelled independently
                        // while still sharing the same underlying watch channel.
                        let mut cancel_rx = cancel_rx_waiter.clone();
                        // The future resolves to Option<AckWaiterResult>:
                        // - None => cancelled (we intentionally drop the response)
                        // - Some(..) => completed or timed out
                        let fut = async move {
                            match message {
                                AckWaiterMessage::Publish {
                                    request_id,
                                    encoding,
                                    payload_len,
                                    start,
                                    response_rx,
                                    permit,
                                } => {
                                    // Wait for either:
                                    // - cancellation (return None), or
                                    // - publish worker response with a bounded timeout.
                                    let response = tokio::select! {
                                        _ = cancel_rx.changed() => return None,
                                        response = tokio::time::timeout(ack_wait_timeout, response_rx) => response,
                                    };
                                    // Always release the semaphore permit once the waiter is no longer in-flight.
                                    drop(permit);
                                    match response {
                                        Ok(response) => Some(AckWaiterResult::Publish {
                                            request_id,
                                            encoding,
                                            payload_len,
                                            start,
                                            response,
                                        }),
                                        Err(_) => Some(AckWaiterResult::PublishTimeout {
                                            request_id,
                                            encoding,
                                            start,
                                        }),
                                    }
                                }
                                AckWaiterMessage::PublishBatch {
                                    request_id,
                                    encoding,
                                    payload_bytes,
                                    response_rx,
                                    permit,
                                    forwarded_to,
                                } => {
                                    // Wait for either:
                                    // - cancellation (return None), or
                                    // - publish worker response with a bounded timeout.
                                    let response = tokio::select! {
                                        _ = cancel_rx.changed() => return None,
                                        response = tokio::time::timeout(ack_wait_timeout, response_rx) => response,
                                    };
                                    // Always release the semaphore permit once the waiter is no longer in-flight.
                                    drop(permit);
                                    match response {
                                        Ok(response) => Some(AckWaiterResult::PublishBatch {
                                            request_id,
                                            encoding,
                                            payload_bytes,
                                            response,
                                            forwarded_to,
                                        }),
                                        Err(_) => Some(AckWaiterResult::PublishBatchTimeout {
                                            request_id,
                                            encoding,
                                        }),
                                    }
                                }
                            }
                        };
                        pending.push(fut);
                    }
                    None => {
                        // No more waiter messages will arrive. We'll exit once `pending` is drained.
                        closed = true;
                    }
                }
            }
            result = pending.next(), if !pending.is_empty() => {
                // Drive the next completed waiter (if any). `flatten()` discards cancelled waiters (None).
                if let Some(result) = result.flatten() {
                    match result {
                        AckWaiterResult::Publish {
                            request_id,
                            encoding,
                            payload_len,
                            start,
                            response,
                        } => {
                            // `start` measures end-to-end time from request ingest to commit completion.
                            t_consume_instant(start);
                            match response {
                                Ok(Ok(offset)) => {
                                    // Send a success ACK back to the client. This may apply backpressure/throttling
                                    // depending on outbound queue depth; failures here are treated as transport-level issues.
                                    count_publish_accepted("ok", payload_len);
                                    #[cfg(feature = "telemetry")]
                                    {
                                        t_histogram!(
                                            "felix_publish_latency_ms",
                                            "mode" => "commit"
                                        )
                                        .record(start.elapsed().as_secs_f64() * 1000.0);
                                    }
                                    if let Err(err) = handle_ack_enqueue_result(
                                        send_outgoing_critical(
                                            &out_ack_tx_waiter,
                                            &out_ack_depth_waiter,
                                            "felix_broker_out_ack_depth",
                                            &ack_throttle_tx_waiter,
                                            encoding.ok(request_id, offset),
                                        )
                                        .await,
                                        &ack_timeout_state_waiter,
                                        &ack_throttle_tx_waiter,
                                        &cancel_tx_waiter,
                                    )
                                    .await
                                    {
                                        tracing::info!(error = %err, "ack enqueue failed");
                                    }
                                }
                                Ok(Err(err)) => {
                                    // Publish worker completed but the broker reported a logical error: surface it as PublishError.
                                    count_publish("error");
                                    if let Err(err) = handle_ack_enqueue_result(
                                        send_outgoing_critical(
                                            &out_ack_tx_waiter,
                                            &out_ack_depth_waiter,
                                            "felix_broker_out_ack_depth",
                                            &ack_throttle_tx_waiter,
                                            encoding.refuse(request_id, &err),
                                        )
                                        .await,
                                        &ack_timeout_state_waiter,
                                        &ack_throttle_tx_waiter,
                                        &cancel_tx_waiter,
                                    )
                                    .await
                                    {
                                        tracing::info!(error = %err, "ack enqueue failed");
                                    }
                                }
                                Err(_) => {
                                    // The publish worker dropped the response channel: treat as an internal error.
                                    count_publish("error");
                                    if let Err(err) = handle_ack_enqueue_result(
                                        send_outgoing_critical(
                                            &out_ack_tx_waiter,
                                            &out_ack_depth_waiter,
                                            "felix_broker_out_ack_depth",
                                            &ack_throttle_tx_waiter,
                                            encoding.error(request_id, ClientError::internal("publish worker dropped response")),
                                        )
                                        .await,
                                        &ack_timeout_state_waiter,
                                        &ack_throttle_tx_waiter,
                                        &cancel_tx_waiter,
                                    )
                                    .await
                                    {
                                        tracing::info!(error = %err, "ack enqueue failed");
                                    }
                                }
                            }
                        }
                        AckWaiterResult::PublishTimeout {
                            request_id,
                            encoding,
                            start,
                        } => {
                            // Worker did not respond within `ack_wait_timeout`: emit an error so the client can retry/fail fast.
                            t_consume_instant(start);
                            count_publish("error");
                            t_counter!("felix_broker_ack_waiter_timeout_total")
                                .increment(1);
                            #[cfg(feature = "telemetry")]
                            {
                                t_histogram!(
                                    "felix_publish_latency_ms",
                                    "mode" => "commit"
                                )
                                .record(start.elapsed().as_secs_f64() * 1000.0);
                            }
                            if let Err(err) = handle_ack_enqueue_result(
                                send_outgoing_critical(
                                    &out_ack_tx_waiter,
                                    &out_ack_depth_waiter,
                                    "felix_broker_out_ack_depth",
                                    &ack_throttle_tx_waiter,
                                    encoding.error(request_id, ClientError::new(felix_wire::ErrorCode::Unacknowledged, "publish commit timeout")),
                                )
                                .await,
                                &ack_timeout_state_waiter,
                                &ack_throttle_tx_waiter,
                                &cancel_tx_waiter,
                            )
                            .await
                            {
                                tracing::info!(error = %err, "ack enqueue failed");
                            }
                        }
                        AckWaiterResult::PublishBatch {
                            request_id,
                            encoding,
                            payload_bytes,
                            forwarded_to,
                            response,
                        } => {
                            // Batch variant: similar to Publish, but records bytes per message in the batch.
                            match response {
                                Ok(Ok(offset)) => {
                                    count_publish_accepted(
                                        "ok",
                                        payload_bytes.iter().sum::<usize>() as u64,
                                    );
                                    if let Err(err) = handle_ack_enqueue_result(
                                        send_outgoing_critical(
                                            &out_ack_tx_waiter,
                                            &out_ack_depth_waiter,
                                            "felix_broker_out_ack_depth",
                                            &ack_throttle_tx_waiter,
                                            encoding.ok_forwarded(request_id, offset, forwarded_to),
                                        )
                                        .await,
                                        &ack_timeout_state_waiter,
                                        &ack_throttle_tx_waiter,
                                        &cancel_tx_waiter,
                                    )
                                    .await
                                    {
                                        tracing::info!(error = %err, "ack enqueue failed");
                                    }
                                }
                                Ok(Err(err)) => {
                                    count_publish("error");
                                    if let Err(err) = handle_ack_enqueue_result(
                                        send_outgoing_critical(
                                            &out_ack_tx_waiter,
                                            &out_ack_depth_waiter,
                                            "felix_broker_out_ack_depth",
                                            &ack_throttle_tx_waiter,
                                            encoding.refuse(request_id, &err),
                                        )
                                        .await,
                                        &ack_timeout_state_waiter,
                                        &ack_throttle_tx_waiter,
                                        &cancel_tx_waiter,
                                    )
                                    .await
                                    {
                                        tracing::info!(error = %err, "ack enqueue failed");
                                    }
                                }
                                Err(_) => {
                                    count_publish("error");
                                    if let Err(err) = handle_ack_enqueue_result(
                                        send_outgoing_critical(
                                            &out_ack_tx_waiter,
                                            &out_ack_depth_waiter,
                                            "felix_broker_out_ack_depth",
                                            &ack_throttle_tx_waiter,
                                            encoding.error(request_id, ClientError::internal("publish batch worker dropped response")),
                                        )
                                        .await,
                                        &ack_timeout_state_waiter,
                                        &ack_throttle_tx_waiter,
                                        &cancel_tx_waiter,
                                    )
                                    .await
                                    {
                                        tracing::info!(error = %err, "ack enqueue failed");
                                    }
                                }
                            }
                        }
                        AckWaiterResult::PublishBatchTimeout {
                            request_id,
                            encoding,
                        } => {
                            count_publish("error");
                            t_counter!("felix_broker_ack_waiter_timeout_total")
                                .increment(1);
                            if let Err(err) = handle_ack_enqueue_result(
                                send_outgoing_critical(
                                    &out_ack_tx_waiter,
                                    &out_ack_depth_waiter,
                                    "felix_broker_out_ack_depth",
                                    &ack_throttle_tx_waiter,
                                    encoding.error(request_id, ClientError::new(felix_wire::ErrorCode::Unacknowledged, "publish commit timeout")),
                                )
                                .await,
                                &ack_timeout_state_waiter,
                                &ack_throttle_tx_waiter,
                                &cancel_tx_waiter,
                            )
                            .await
                            {
                                tracing::info!(error = %err, "ack enqueue failed");
                            }
                        }
                    }
                }
            }
        }
        // Exit condition: channel is closed AND no in-flight waiters remain.
        if closed && pending.is_empty() {
            break;
        }
    }
}
