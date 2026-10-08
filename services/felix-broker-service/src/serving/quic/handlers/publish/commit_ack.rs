//! Acks sent by whoever settles the publish.
//!
//! A publish acknowledged on commit is answered from the place its result is
//! known: the executor for an in-memory write, the commit task for a durable
//! one. The answer goes straight onto the stream's writer queue, so nothing
//! sits between durability and the write but that queue.
//!
//! The ack timeout is one deadline queue per control stream, swept by one
//! task on a coarse tick, instead of a timer per publish. Deadlines are armed
//! in the order publishes are queued and all have the same length, so the
//! queue is already in deadline order.
//!
//! Each answer is sent at most once. The publish's settle, its timeout, and the
//! control loop's own refusals race for it through `Slot::state`.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::time::Instant;

use super::PublishResult;
use super::ack::{
    AckEncoding, AckTimeoutState, Outgoing, handle_ack_enqueue_result, note_ack_enqueue_result,
    note_enqueued, send_outgoing_critical,
};
use crate::serving::quic::ACK_WAITERS_MAX;
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::errors::AckEnqueueError;
#[cfg(feature = "telemetry")]
use crate::serving::quic::telemetry::t_histogram;
use crate::serving::quic::telemetry::{
    TelemetryInstant, count_publish, count_publish_accepted, t_consume_instant, t_counter,
};

/// Created, not yet queued.
const PENDING: u8 = 0;
/// Queued; the timeout runs.
const ARMED: u8 = 1;
const ANSWERED: u8 = 2;
/// The publish was dropped before it was queued, so nobody answered.
const ORPHANED: u8 = 3;

const SWEEP_TICK_MIN: Duration = Duration::from_millis(1);
const SWEEP_TICK_MAX: Duration = Duration::from_millis(100);

/// The answers one control stream owes for publishes acknowledged on commit.
#[derive(Clone)]
pub(crate) struct CommitAcks {
    inner: Arc<Inner>,
}

struct Inner {
    out_ack_tx: mpsc::Sender<Outgoing>,
    out_ack_depth: Arc<AtomicUsize>,
    throttle_tx: watch::Sender<bool>,
    timeout_state: Arc<Mutex<AckTimeoutState>>,
    cancel_tx: watch::Sender<bool>,
    /// Bounds the answers owed at once.
    waiters: Arc<Semaphore>,
    timeout: Duration,
    deadlines: Mutex<VecDeque<Deadline>>,
    sweeper_idle: AtomicBool,
    wake: Notify,
    closed: AtomicBool,
}

struct Deadline {
    at: Instant,
    slot: Arc<Slot>,
}

struct Slot {
    state: AtomicU8,
    permit: Mutex<Option<OwnedSemaphorePermit>>,
    request_id: u64,
    encoding: AckEncoding,
    payload_bytes: u64,
    forwarded_to: Option<felix_wire::binary::PublishOwner>,
    start: TelemetryInstant,
    /// A single publish records its commit latency; a batch does not.
    single: bool,
}

/// What one acknowledged publish is answered with.
pub(crate) struct AckRequest {
    pub(crate) request_id: u64,
    pub(crate) encoding: AckEncoding,
    pub(crate) payload_bytes: u64,
    pub(crate) forwarded_to: Option<felix_wire::binary::PublishOwner>,
    pub(crate) start: TelemetryInstant,
    pub(crate) single: bool,
}

/// Travels with the publish job and answers the client when the job settles.
/// Dropped unsettled after its publish was queued, it answers with an error.
pub(crate) struct CommitReply {
    inner: Arc<Inner>,
    slot: Option<Arc<Slot>>,
}

/// Stays with the control loop, which says whether the publish was queued.
pub(crate) struct PendingAck {
    inner: Arc<Inner>,
    slot: Arc<Slot>,
}

/// What [`PendingAck::arm`] did.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Armed {
    /// The timeout runs until the publish settles.
    Waiting,
    /// Already answered; nothing more to do.
    Answered,
    /// Too many answers are owed. The caller must refuse the publish.
    Exhausted,
}

impl CommitAcks {
    pub(crate) fn new(
        out_ack_tx: mpsc::Sender<Outgoing>,
        out_ack_depth: Arc<AtomicUsize>,
        throttle_tx: watch::Sender<bool>,
        timeout_state: Arc<Mutex<AckTimeoutState>>,
        cancel_tx: watch::Sender<bool>,
        waiters: Arc<Semaphore>,
        timeout: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                out_ack_tx,
                out_ack_depth,
                throttle_tx,
                timeout_state,
                cancel_tx,
                waiters,
                timeout,
                deadlines: Mutex::new(VecDeque::new()),
                sweeper_idle: AtomicBool::new(false),
                wake: Notify::new(),
                closed: AtomicBool::new(false),
            }),
        }
    }

    /// A stream's commit acks with their own throttle, timeout window and
    /// cancel signal, for tests that only read the answers.
    #[cfg(test)]
    pub(crate) fn for_test(out_ack_tx: &mpsc::Sender<Outgoing>, waiters: Arc<Semaphore>) -> Self {
        Self::new(
            out_ack_tx.clone(),
            Arc::new(AtomicUsize::new(0)),
            watch::channel(false).0,
            Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now()))),
            watch::channel(false).0,
            waiters,
            Duration::from_secs(5),
        )
    }

    /// Commit acks whose answers go nowhere, for tests of what happens
    /// before the answer.
    #[cfg(test)]
    pub(crate) fn detached(waiters: Arc<Semaphore>) -> Self {
        Self::for_test(&mpsc::channel(1).0, waiters)
    }

    /// Create the answer for one publish, before it is queued, so that a
    /// publish settled before the control loop hears back is still answered.
    pub(crate) fn expect(&self, request: AckRequest) -> (CommitReply, PendingAck) {
        let slot = Arc::new(Slot {
            state: AtomicU8::new(PENDING),
            permit: Mutex::new(None),
            request_id: request.request_id,
            encoding: request.encoding,
            payload_bytes: request.payload_bytes,
            forwarded_to: request.forwarded_to,
            start: request.start,
            single: request.single,
        });
        (
            CommitReply {
                inner: Arc::clone(&self.inner),
                slot: Some(Arc::clone(&slot)),
            },
            PendingAck {
                inner: Arc::clone(&self.inner),
                slot,
            },
        )
    }

    /// No more publishes will be armed. The sweep ends once every armed
    /// publish is answered.
    pub(crate) fn close(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
        self.inner.wake.notify_one();
    }

    /// Answer every armed publish whose timeout passes before it settles.
    ///
    /// Sleeps on the tick only while something is armed, so an idle stream
    /// has no timer at all.
    pub(crate) async fn run_deadlines(self, mut cancel_rx: watch::Receiver<bool>) {
        let inner = self.inner;
        let tick = (inner.timeout / 8).clamp(SWEEP_TICK_MIN, SWEEP_TICK_MAX);
        loop {
            if !inner.sweep(Instant::now()) {
                if inner.closed.load(Ordering::SeqCst) {
                    return;
                }
                inner.sweeper_idle.store(true, Ordering::SeqCst);
                // Re-checked after going idle: an arm in between saw the
                // sweeper busy and did not wake it.
                if inner.deadlines.lock().is_empty() && !inner.closed.load(Ordering::SeqCst) {
                    tokio::select! {
                        _ = inner.wake.notified() => {}
                        _ = cancelled(&mut cancel_rx) => return,
                    }
                }
                inner.sweeper_idle.store(false, Ordering::SeqCst);
                continue;
            }
            tokio::select! {
                _ = tokio::time::sleep(tick) => {}
                _ = cancelled(&mut cancel_rx) => return,
            }
        }
    }
}

impl Inner {
    /// Drop answered deadlines, time out expired ones, and say whether any
    /// remain.
    fn sweep(self: &Arc<Self>, now: Instant) -> bool {
        let mut expired = Vec::new();
        let left = {
            let mut deadlines = self.deadlines.lock();
            while let Some(front) = deadlines.front() {
                if front.slot.is_answered() {
                    deadlines.pop_front();
                } else if front.at <= now {
                    expired.extend(deadlines.pop_front().map(|deadline| deadline.slot));
                } else {
                    break;
                }
            }
            // A publish that outlives the ones behind it holds them in the
            // queue. Only `ACK_WAITERS_MAX` can be unanswered, so compacting
            // keeps it near that.
            if deadlines.len() > 2 * ACK_WAITERS_MAX {
                deadlines.retain(|deadline| !deadline.slot.is_answered());
            }
            !deadlines.is_empty()
        };
        for slot in expired {
            if slot.take(|state| state == ARMED) {
                count_publish("error");
                t_counter!("felix_broker_ack_waiter_timeout_total").increment(1);
                t_consume_instant(slot.start);
                self.answer(
                    &slot,
                    slot.encoding.error(
                        slot.request_id,
                        ClientError::new(
                            felix_wire::ErrorCode::Unacknowledged,
                            "publish commit timeout",
                        ),
                    ),
                );
            }
        }
        left
    }

    fn arm(&self, slot: Arc<Slot>) {
        let at = Instant::now() + self.timeout;
        {
            let mut deadlines = self.deadlines.lock();
            if deadlines.len() > 4 * ACK_WAITERS_MAX {
                deadlines.retain(|deadline| !deadline.slot.is_answered());
            }
            deadlines.push_back(Deadline { at, slot });
        }
        if self.sweeper_idle.swap(false, Ordering::SeqCst) {
            self.wake.notify_one();
        }
    }

    fn answer(self: &Arc<Self>, slot: &Slot, outgoing: Outgoing) {
        drop(slot.permit.lock().take());
        self.deliver(outgoing);
    }

    /// Put an answer on the writer's queue without waiting when there is room.
    fn deliver(self: &Arc<Self>, outgoing: Outgoing) {
        match self.out_ack_tx.try_send(outgoing) {
            Ok(()) => {
                note_enqueued(
                    &self.out_ack_depth,
                    "felix_broker_out_ack_depth",
                    &self.throttle_tx,
                );
                let _ = note_ack_enqueue_result(
                    Ok(()),
                    &self.timeout_state,
                    &self.throttle_tx,
                    &self.cancel_tx,
                );
            }
            // A full queue is the slow client's problem, not the settling
            // task's: it may be an executor other streams are waiting on.
            Err(mpsc::error::TrySendError::Full(outgoing)) => {
                let Ok(runtime) = tokio::runtime::Handle::try_current() else {
                    return;
                };
                let inner = Arc::clone(self);
                runtime.spawn(async move {
                    let result = send_outgoing_critical(
                        &inner.out_ack_tx,
                        &inner.out_ack_depth,
                        "felix_broker_out_ack_depth",
                        &inner.throttle_tx,
                        outgoing,
                    )
                    .await;
                    if let Err(err) = handle_ack_enqueue_result(
                        result,
                        &inner.timeout_state,
                        &inner.throttle_tx,
                        &inner.cancel_tx,
                    )
                    .await
                    {
                        tracing::info!(error = %err, "ack enqueue failed");
                    }
                });
            }
            // The stream is gone, or going.
            Err(mpsc::error::TrySendError::Closed(_)) => {
                if !*self.cancel_tx.borrow()
                    && let Err(err) = note_ack_enqueue_result(
                        Err(AckEnqueueError::Closed),
                        &self.timeout_state,
                        &self.throttle_tx,
                        &self.cancel_tx,
                    )
                {
                    tracing::info!(error = %err, "ack enqueue failed");
                }
            }
        }
    }
}

impl Slot {
    fn is_answered(&self) -> bool {
        self.state.load(Ordering::Acquire) == ANSWERED
    }

    /// Take the right to answer, if the current state allows it.
    fn take(&self, from: impl Fn(u8) -> bool) -> bool {
        self.state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                from(state).then_some(ANSWERED)
            })
            .is_ok()
    }

    fn dropped(&self) -> Outgoing {
        count_publish("error");
        self.encoding.error(
            self.request_id,
            ClientError::internal("publish worker dropped response"),
        )
    }
}

impl CommitReply {
    /// Answer the client with the publish's result, unless it was already
    /// answered (timed out, or refused by the control loop).
    pub(crate) fn send(mut self, result: PublishResult) {
        let Some(slot) = self.slot.take() else {
            return;
        };
        if !slot.take(|state| state == PENDING || state == ARMED) {
            return;
        }
        t_consume_instant(slot.start);
        let outgoing = match result {
            Ok(offset) => {
                count_publish_accepted("ok", slot.payload_bytes);
                #[cfg(feature = "telemetry")]
                if slot.single {
                    t_histogram!("felix_publish_latency_ms", "mode" => "commit")
                        .record(slot.start.elapsed().as_secs_f64() * 1000.0);
                }
                slot.encoding
                    .ok_forwarded(slot.request_id, offset, slot.forwarded_to.clone())
            }
            Err(err) => {
                count_publish("error");
                slot.encoding.refuse(slot.request_id, &err)
            }
        };
        #[cfg(not(feature = "telemetry"))]
        let _ = slot.single;
        self.inner.answer(&slot, outgoing);
    }
}

impl Drop for CommitReply {
    fn drop(&mut self) {
        let Some(slot) = self.slot.take() else {
            return;
        };
        let prev =
            slot.state
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| match state {
                    PENDING => Some(ORPHANED),
                    ARMED => Some(ANSWERED),
                    _ => None,
                });
        if prev == Ok(ARMED) {
            self.inner.answer(&slot, slot.dropped());
        }
    }
}

impl PendingAck {
    /// The publish was queued: start its timeout.
    pub(crate) fn arm(self) -> Armed {
        let Ok(permit) = Arc::clone(&self.inner.waiters).try_acquire_owned() else {
            t_counter!("felix_broker_ack_waiters_exhausted_total").increment(1);
            return if self
                .slot
                .take(|state| state == PENDING || state == ORPHANED)
            {
                Armed::Exhausted
            } else {
                Armed::Answered
            };
        };
        // Stored before arming, so whoever answers releases it.
        *self.slot.permit.lock() = Some(permit);
        match self
            .slot
            .state
            .compare_exchange(PENDING, ARMED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                self.inner.arm(self.slot);
                Armed::Waiting
            }
            // Settled before it could be armed.
            Err(ANSWERED) => {
                drop(self.slot.permit.lock().take());
                Armed::Answered
            }
            // Queued, then dropped before it ran.
            Err(_) => {
                if self.slot.take(|state| state == ORPHANED) {
                    self.inner.answer(&self.slot, self.slot.dropped());
                } else {
                    drop(self.slot.permit.lock().take());
                }
                Armed::Answered
            }
        }
    }

    /// The publish was not queued. True when the caller is the one to
    /// answer it; false when it was already answered.
    pub(crate) fn refuse(self) -> bool {
        self.slot
            .take(|state| state == PENDING || state == ORPHANED)
    }
}

async fn cancelled(cancel_rx: &mut watch::Receiver<bool>) {
    while !*cancel_rx.borrow_and_update() {
        if cancel_rx.changed().await.is_err() {
            // Every sender is gone; nobody can cancel, so wait forever.
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(test)]
mod tests;
