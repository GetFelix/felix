//! Ack protocol: outgoing envelopes, waiter messages, the timeout window, and the
//! helpers that push acks onto the writer with the right backpressure policy.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Result, anyhow};
use felix_wire::Message;
use tokio::sync::{Mutex, mpsc, oneshot, watch};

use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::errors::AckEnqueueError;
use crate::serving::quic::telemetry::{t_counter, t_gauge};
use crate::serving::quic::{
    ACK_ENQUEUE_TIMEOUT, ACK_HI_WATER, ACK_TIMEOUT_THRESHOLD, ACK_TIMEOUT_WINDOW, GLOBAL_ACK_DEPTH,
};

/// Items pushed to the outbound writer loop (ack/response path).
///
/// The writer loop is the single owner of the QUIC `SendStream` for the control stream.
/// All responses/acks are funneled into that task to avoid concurrent writes.
///
/// Variants:
/// - `Message`: normal control responses (PublishOk/Error, SubscribeOk, etc.).
/// - `CacheMessage`: cache fast-path replies that may be encoded differently or routed
///   separately from general control responses (depending on the writer implementation).
/// - `PublishAck`: binary acknowledgement for a publish that arrived on the binary
///   acked path. Kept as a distinct variant rather than reusing `Message` because
///   the reply encoding has to match the request encoding — a client that sent a
///   `FLAG_BINARY_PUBLISH_ACKED` frame is reading a binary ack frame, not JSON.
#[derive(Debug)]
pub(crate) enum Outgoing {
    Message(Message),
    CacheMessage(Message),
    PublishAck {
        request_id: u64,
        /// `None` acknowledges success; `Some` reports failure.
        error: Option<String>,
        /// The failure's code. The writer drops it for a client that did not
        /// advertise `FLAG_BINARY_PUBLISH_ACK_CODE`.
        code: Option<(felix_wire::ErrorCode, felix_wire::RetryClass)>,
        /// The failure's detail, such as why a shard is unavailable. The
        /// writer drops it for a client that did not advertise
        /// `FLAG_BINARY_PUBLISH_ACK_DETAIL`.
        detail: Option<felix_wire::ErrorDetail>,
        /// Set when the batch was forwarded, naming the shard's owner so the
        /// client can send the next one straight there.
        ///
        /// Only ever populated for a client that advertised
        /// `FLAG_BINARY_PUBLISH_ACK_OWNER`: the bit changes the ack's payload
        /// layout, and a client that cannot parse it rejects the whole frame --
        /// an acknowledgement for a publish that succeeded.
        forwarded_to: Option<felix_wire::binary::PublishOwner>,
    },
}

/// Which encoding a pending ack must be answered in.
///
/// The reply has to match the request. A client that sent a JSON `PublishBatch`
/// is blocked reading a JSON `PublishOk`; a client that sent a binary
/// `FLAG_BINARY_PUBLISH_ACKED` frame is blocked reading a binary ack frame.
/// Answering in the wrong one strands the client on a frame it cannot parse,
/// which is why this is carried all the way to the emit site rather than decided
/// there — the commit-ack path emits from the ack-waiter task, long after the
/// request frame itself is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AckEncoding {
    Json,
    Binary,
    /// A `publish_idempotent`: JSON, with a refusal the producer can act on
    /// answered as `publish_refused` rather than as prose.
    Idempotent,
}

impl AckEncoding {
    /// Build a success ack in this encoding.
    pub(crate) fn ok(self, request_id: u64) -> Outgoing {
        self.ok_forwarded(request_id, None)
    }

    /// A success ack that names where the batch was forwarded, when it was.
    ///
    /// Only the binary encoding carries it. A JSON `PublishOk` has nowhere to
    /// put it without changing a message every client parses, and the JSON path
    /// is compatibility traffic that is not worth optimising -- a client on it
    /// is already paying more than forwarding costs.
    pub(crate) fn ok_forwarded(
        self,
        request_id: u64,
        forwarded_to: Option<felix_wire::binary::PublishOwner>,
    ) -> Outgoing {
        match self {
            AckEncoding::Json | AckEncoding::Idempotent => {
                Outgoing::Message(Message::PublishOk { request_id })
            }
            AckEncoding::Binary => Outgoing::PublishAck {
                request_id,
                error: None,
                code: None,
                detail: None,
                forwarded_to,
            },
        }
    }

    /// Build a failure ack in this encoding.
    pub(crate) fn error(self, request_id: u64, error: ClientError) -> Outgoing {
        match self {
            AckEncoding::Json | AckEncoding::Idempotent => {
                Outgoing::Message(error.into_publish_error(request_id))
            }
            AckEncoding::Binary => Outgoing::PublishAck {
                request_id,
                code: Some((error.code().clone(), error.retry())),
                detail: error.detail().cloned(),
                error: Some(error.message().to_string()),
                // A failed publish has no owner worth caching: the batch did
                // not land anywhere, so where it would have gone is not a
                // route the client should adopt.
                forwarded_to: None,
            },
        }
    }

    /// Build the ack for a publish the worker refused.
    ///
    /// The same as [`Self::error`] except for an idempotent publish, where a
    /// refusal the producer must act on carries its reason: a gap means stop,
    /// an unknown producer means start again, and a string would make every
    /// client parse prose to tell them apart.
    pub(crate) fn refuse(self, request_id: u64, err: &anyhow::Error) -> Outgoing {
        if self == AckEncoding::Idempotent
            && let Some(reason) = refusal_reason(err)
        {
            return Outgoing::Message(Message::PublishRefused {
                request_id,
                reason,
                message: err.to_string(),
            });
        }
        self.error(request_id, ClientError::from_anyhow(err))
    }
}

/// Admission policy when the ingress publish queue is full.
///
/// - `Drop`: shed load silently (best for fire-and-forget / non-acked traffic).
/// - `Fail`: reject immediately with an error (best for acked traffic when you prefer fast failure).
/// - `Wait`: bounded backpressure, capped by a single `publish_ctx.wait_timeout`
///   deadline spanning *both* admission and the queue send. Only for publishes
///   that carry an ack, because a timeout here surfaces to the client as a
///   `PublishError` it can retry.
/// - `Backpressure`: unbounded but cancellable. Never sheds and never times out;
///   it ends only when capacity frees up or the connection is torn down.
///
/// The split between the last two is the point. `Wait` and `Backpressure` used to
/// be one policy, and applying a wall-clock timeout to an *unacked* publish turned
/// overload back into silent loss — the exact outcome the wait existed to prevent,
/// with no way to tell the client. A timer is the wrong bound for backpressure:
/// the right one is liveness, so `Backpressure` waits on capacity and gives up only
/// when the connection does.
pub(crate) enum EnqueuePolicy {
    Drop,
    Fail,
    Wait,
    Backpressure,
}

/// Result reported by the ack-waiter task for commit-ack publishes.
///
/// The waiter task is responsible for awaiting the worker completion signal (oneshot),
/// applying timeouts, and producing a normalized result for the response writer.
pub(crate) enum AckWaiterResult {
    Publish {
        request_id: u64,
        encoding: AckEncoding,
        payload_len: u64,
        start: crate::serving::quic::telemetry::TelemetryInstant,
        response: Result<Result<()>, oneshot::error::RecvError>,
    },
    PublishTimeout {
        request_id: u64,
        encoding: AckEncoding,
        start: crate::serving::quic::telemetry::TelemetryInstant,
    },
    PublishBatch {
        request_id: u64,
        encoding: AckEncoding,
        payload_bytes: Vec<usize>,
        response: Result<Result<()>, oneshot::error::RecvError>,
        /// Carried from the enqueue so a successful ack can name the owner a
        /// forwarded batch went to. Resolved there rather than here because
        /// that is where the routing decision was made.
        forwarded_to: Option<felix_wire::binary::PublishOwner>,
    },
    PublishBatchTimeout {
        request_id: u64,
        encoding: AckEncoding,
    },
}

/// Message sent to the ack-waiter task to track one in-flight commit-ack request.
///
/// Carries the oneshot receiver and a semaphore permit (`ack_waiters`) which bounds the number of
/// in-flight commit acks. Releasing the permit signals “this commit-ack slot is free again”.
pub(crate) enum AckWaiterMessage {
    Publish {
        request_id: u64,
        encoding: AckEncoding,
        payload_len: u64,
        start: crate::serving::quic::telemetry::TelemetryInstant,
        response_rx: oneshot::Receiver<Result<()>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    },
    PublishBatch {
        request_id: u64,
        encoding: AckEncoding,
        payload_bytes: Vec<usize>,
        response_rx: oneshot::Receiver<Result<()>>,
        permit: tokio::sync::OwnedSemaphorePermit,
        /// The shard's owner, when this batch was forwarded to one and the
        /// client advertised the flag bit that carries it.
        forwarded_to: Option<felix_wire::binary::PublishOwner>,
    },
}

/// Tracks consecutive outbound-ack enqueue timeouts in a sliding time window.
///
/// This is a defensive mechanism: if we cannot enqueue responses for too long, the control stream
/// is likely unhealthy (client not reading, writer wedged, or extreme overload). In that case we
/// throttle and eventually cancel the stream cooperatively.
pub(crate) struct AckTimeoutState {
    window_start: Instant,
    count: u32,
}

impl AckTimeoutState {
    /// Create a new timeout state window starting at `now`.
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            window_start: now,
            count: 0,
        }
    }

    /// Reset the window and streak counters (typically after a successful enqueue).
    pub(crate) fn reset(&mut self, now: Instant) {
        self.window_start = now;
        self.count = 0;
    }

    /// Record one timeout and return the current streak count within the active window.
    ///
    /// If the window has elapsed, we start a new window and reset the streak to 1.
    pub(crate) fn register_timeout(&mut self, now: Instant) -> u32 {
        if now.duration_since(self.window_start) > ACK_TIMEOUT_WINDOW {
            self.window_start = now;
            self.count = 1;
        } else {
            self.count = self.count.saturating_add(1);
        }
        self.count
    }
}

pub(crate) async fn send_outgoing_critical(
    tx: &mpsc::Sender<Outgoing>,
    depth: &Arc<AtomicUsize>,
    gauge: &'static str,
    throttle_tx: &watch::Sender<bool>,
    message: Outgoing,
) -> std::result::Result<(), AckEnqueueError> {
    #[cfg(not(feature = "telemetry"))]
    let _ = gauge;
    // "Critical" means: if we cannot enqueue within a short bound, we prefer to cancel/close the
    // control stream rather than allow an acked request to wait forever.
    // This is used for acks/responses where the client is very likely waiting.
    // Critical enqueue with timeout; cancel the stream if it cannot be queued.
    let send_result = tokio::time::timeout(ACK_ENQUEUE_TIMEOUT, tx.send(message)).await;
    match send_result {
        Ok(Ok(())) => {
            let prev = depth.fetch_add(1, Ordering::Relaxed);
            let cur = prev + 1;
            let global = GLOBAL_ACK_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
            t_gauge!(gauge).set(global as f64);
            if prev < ACK_HI_WATER && cur >= ACK_HI_WATER {
                let _ = throttle_tx.send(true);
            }
            Ok(())
        }
        Ok(Err(_)) => Err(AckEnqueueError::Closed),
        Err(_) => {
            t_counter!("felix_broker_out_ack_timeout_total").increment(1);
            Err(AckEnqueueError::Timeout)
        }
    }
}

pub(crate) async fn send_outgoing_best_effort(
    tx: &mpsc::Sender<Outgoing>,
    depth: &Arc<AtomicUsize>,
    gauge: &'static str,
    throttle_tx: &watch::Sender<bool>,
    message: Outgoing,
) -> std::result::Result<(), AckEnqueueError> {
    #[cfg(not(feature = "telemetry"))]
    let _ = gauge;
    // Best-effort is used when the server is already overloaded and we are shedding load.
    // NOTE: Dropping an ack/error for a request that asked for an ack can strand the client until
    // it times out. If this becomes problematic, switch these paths to `send_outgoing_critical`
    // or close the stream when enqueue fails.
    // Best-effort enqueue; fail fast if the ack queue is full to avoid deadlocks.
    match tx.try_send(message) {
        Ok(()) => {
            let prev = depth.fetch_add(1, Ordering::Relaxed);
            let cur = prev + 1;
            let global = GLOBAL_ACK_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
            t_gauge!(gauge).set(global as f64);
            if prev < ACK_HI_WATER && cur >= ACK_HI_WATER {
                let _ = throttle_tx.send(true);
            }
            Ok(())
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            t_counter!("felix_broker_out_ack_full_total").increment(1);
            Err(AckEnqueueError::Full)
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(AckEnqueueError::Closed),
    }
}

pub(crate) async fn handle_ack_enqueue_result(
    result: std::result::Result<(), AckEnqueueError>,
    state: &Arc<Mutex<AckTimeoutState>>,
    throttle_tx: &watch::Sender<bool>,
    cancel_tx: &watch::Sender<bool>,
) -> Result<()> {
    match result {
        Ok(()) => {
            let mut guard = state.lock().await;
            guard.reset(Instant::now());
            Ok(())
        }
        Err(AckEnqueueError::Timeout) => {
            let _ = throttle_tx.send(true);
            let now = Instant::now();
            let mut guard = state.lock().await;
            let count = guard.register_timeout(now);
            if count >= ACK_TIMEOUT_THRESHOLD {
                crate::serving::quic::errors::record_ack_enqueue_failure_metrics(
                    "ack_queue_timeout",
                );
                let _ = cancel_tx.send(true);
                return Err(anyhow!(
                    "closing control stream: ack enqueue timeout streak"
                ));
            }
            Ok(())
        }
        Err(AckEnqueueError::Full) => Err(anyhow!("ack queue full")),
        Err(AckEnqueueError::Closed) => {
            crate::serving::quic::errors::record_ack_enqueue_failure_metrics("ack_queue_closed");
            let _ = throttle_tx.send(false);
            let _ = cancel_tx.send(true);
            Err(anyhow!("closing control stream: ack_queue_closed"))
        }
    }
}

/// The typed reason behind a worker's refusal of an idempotent publish, when
/// it has one.
fn refusal_reason(err: &anyhow::Error) -> Option<felix_wire::PublishRefusalReason> {
    use felix_wire::PublishRefusalReason as Reason;
    match err.downcast_ref::<felix_broker::BrokerError>()? {
        felix_broker::BrokerError::SequenceGap { expected } => Some(Reason::SequenceGap {
            expected: *expected,
        }),
        felix_broker::BrokerError::UnknownProducer { .. } => Some(Reason::UnknownProducer),
        felix_broker::BrokerError::SequenceExpired { .. } => Some(Reason::SequenceExpired),
        felix_broker::BrokerError::SequenceReused { .. } => Some(Reason::SequenceReused),
        _ => None,
    }
}
