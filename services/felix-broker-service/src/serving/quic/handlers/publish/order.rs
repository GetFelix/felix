//! In-order answers for a stream whose client pipelines its publishes.
//!
//! Publishes finish in whatever order their shards commit them, and the ack
//! waiter answers in completion order. A client that offered
//! `FEATURE_PUBLISH_PIPELINE` is promised request order instead, so the control
//! loop registers each acknowledged publish as it reads it, and the stream's
//! writer holds an answer back until every publish read before it has been
//! answered. Holding back is the only change: nothing is answered that would
//! not have been, and nothing is answered differently.
//!
//! Each registration can carry a permit from the connection's publish window.
//! It is released when the answer is written, which is what bounds how many
//! acknowledged publishes one connection has unanswered.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use felix_wire::Message;
use tokio::sync::OwnedSemaphorePermit;

use super::ack::Outgoing;

#[derive(Default)]
pub(crate) struct AckOrder {
    enabled: AtomicBool,
    slots: Mutex<VecDeque<Slot>>,
}

struct Slot {
    request_id: u64,
    answer: Option<Outgoing>,
    registered: Instant,
    _window: Option<OwnedSemaphorePermit>,
}

impl AckOrder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Answer this stream's publishes in request order from now on.
    pub(crate) fn enable(&self) {
        self.enabled.store(true, Ordering::Release);
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    /// Record that the publish `request_id` was read and will be answered.
    ///
    /// Must be called before the publish is handed on, so no answer can reach
    /// the writer ahead of its registration.
    pub(crate) fn register(&self, request_id: u64, window: Option<OwnedSemaphorePermit>) {
        self.lock().push_back(Slot {
            request_id,
            answer: None,
            registered: Instant::now(),
            _window: window,
        });
    }

    /// Pass `outgoing` through the order and append to `ready` everything that
    /// may now be written, in the order it must be written.
    ///
    /// Anything that is not a publish answer, or answers a publish that was
    /// never registered, goes straight through.
    pub(crate) fn release(&self, outgoing: Outgoing, ready: &mut Vec<Outgoing>) {
        if !self.is_enabled() {
            ready.push(outgoing);
            return;
        }
        let Some(request_id) = answered_request(&outgoing) else {
            ready.push(outgoing);
            return;
        };
        let mut slots = self.lock();
        let Some(slot) = slots
            .iter_mut()
            .find(|slot| slot.request_id == request_id && slot.answer.is_none())
        else {
            ready.push(outgoing);
            return;
        };
        slot.answer = Some(outgoing);
        while slots.front().is_some_and(|slot| slot.answer.is_some()) {
            if let Some(answer) = slots.pop_front().and_then(|slot| slot.answer) {
                ready.push(answer);
            }
        }
    }

    /// When the oldest unanswered publish was read, if a later answer is
    /// waiting on it.
    ///
    /// Every registered publish is answered within the ack timeout, so a head
    /// that outlives it lost its answer somewhere; the writer closes the
    /// stream rather than hold everything behind it forever.
    pub(crate) fn blocked_since(&self) -> Option<Instant> {
        let slots = self.lock();
        let head = slots.front()?;
        slots
            .iter()
            .skip(1)
            .any(|slot| slot.answer.is_some())
            .then_some(head.registered)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Slot>> {
        // A panic while holding this lock leaves plain data behind, never a
        // half-applied invariant, so the poison is not worth propagating.
        self.slots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The request a publish answer is for, if `outgoing` is one.
fn answered_request(outgoing: &Outgoing) -> Option<u64> {
    match outgoing {
        Outgoing::PublishAck { request_id, .. } => Some(*request_id),
        Outgoing::Message(
            Message::PublishOk { request_id }
            | Message::PublishError { request_id, .. }
            | Message::PublishRefused { request_id, .. },
        ) => Some(*request_id),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
