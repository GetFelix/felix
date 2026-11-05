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
//! That includes an in-memory publish's ack, which can wait behind an earlier
//! durable publish's commit ack on the same stream.
//!
//! A client that also offered `FEATURE_PUBLISH_ANSWER_ON_COMPLETION` gets
//! request order only among its idempotent publishes, which need it: a
//! producer must see batch n fail before a sequence gap for batch n+1. Every
//! other answer is written as soon as it is ready, so a publish to a stalled
//! shard does not hold back publishes to other shards on the same stream.
//! Those still take a window slot and keep it until they are answered.
//!
//! Each registration can carry a permit from the stream's publish window.
//! It is released when the answer is written, which is what bounds how many
//! acknowledged publishes one stream has unanswered.

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
    /// Only idempotent publishes wait for earlier answers.
    on_completion: AtomicBool,
    slots: Mutex<VecDeque<Slot>>,
}

struct Slot {
    request_id: u64,
    /// Answered in request order among the other ordered slots.
    ordered: bool,
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

    /// From now on keep request order only among idempotent publishes, and
    /// answer the rest as they complete.
    pub(crate) fn answer_on_completion(&self) {
        self.on_completion.store(true, Ordering::Release);
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    /// Record that the publish `request_id` was read and will be answered.
    /// `idempotent` publishes keep request order among themselves either way.
    ///
    /// Must be called before the publish is handed on, so no answer can reach
    /// the writer ahead of its registration.
    pub(crate) fn register(
        &self,
        request_id: u64,
        idempotent: bool,
        window: Option<OwnedSemaphorePermit>,
    ) {
        let ordered = idempotent || !self.on_completion.load(Ordering::Acquire);
        self.lock().push_back(Slot {
            request_id,
            ordered,
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
        let Some(index) = slots
            .iter()
            .position(|slot| slot.request_id == request_id && slot.answer.is_none())
        else {
            ready.push(outgoing);
            return;
        };
        if !slots[index].ordered {
            // Dropping the slot frees its window permit.
            slots.remove(index);
            ready.push(outgoing);
            return;
        }
        slots[index].answer = Some(outgoing);
        // Unordered slots still waiting do not hold anything back.
        while let Some(head) = slots.iter().position(|slot| slot.ordered)
            && slots[head].answer.is_some()
        {
            if let Some(answer) = slots.remove(head).and_then(|slot| slot.answer) {
                ready.push(answer);
            }
        }
    }

    /// When the oldest unanswered ordered publish was read, if a later answer
    /// is held behind it.
    ///
    /// Every registered publish is answered within the ack timeout, so a head
    /// that outlives it lost its answer somewhere; the writer closes the
    /// stream rather than hold everything behind it forever.
    pub(crate) fn blocked_since(&self) -> Option<Instant> {
        let slots = self.lock();
        let mut ordered = slots.iter().filter(|slot| slot.ordered);
        let head = ordered.next()?;
        ordered
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
            Message::PublishOk { request_id, .. }
            | Message::PublishError { request_id, .. }
            | Message::PublishRefused { request_id, .. },
        ) => Some(*request_id),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
