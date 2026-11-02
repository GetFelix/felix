//! A shard's assignment as the control plane confirms it.
//!
//! A copy with no promise on disk answers a leader only as the control plane
//! backs it (see `ReplicaHandler::refuse_unbacked`). The routing view is
//! seeded from whichever control-plane member answered, which can lag, so
//! such a copy also asks for a read the control plane's leader confirmed
//! (#1332).
//!
//! The read never runs on the peer lane ([`ConfirmedReads`]): the peer
//! server answers a lane's requests one at a time, so a control-plane round
//! trip there would hold up every other shard queued behind it (#1791).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use felix_router::ShardKey;

/// What a confirmed read found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Confirmed {
    /// The control plane names `leader` at `generation`.
    Assigned { generation: u64, leader: String },
    /// The control plane has no assignment for the shard.
    Unassigned,
    /// The control plane does not serve confirmed reads (an older build, or
    /// a Raft leader that predates them). The routing view answers alone, as
    /// it did before.
    NotServed,
    /// No confirmed answer arrived. Nothing is known, so nothing is answered.
    Unavailable,
}

/// Reads a shard's assignment no older than any write the control plane
/// acknowledged before the call.
#[async_trait::async_trait]
pub trait ConfirmedRoutes: Send + Sync {
    async fn confirmed(&self, key: &ShardKey) -> Confirmed;
}

/// How long an answer is used under elections. A learner checks every batch
/// it takes; short, since only a fresh read sees the control plane move past
/// a generation.
pub(crate) const FRESH_UNDER_ELECTIONS: Duration = Duration::from_millis(500);

/// How long an answer is used without elections. Any read taken since this
/// process started is at least as new as every promise a wipe could have
/// cost the copy, so using an older one refuses no less than `Forget` under
/// `PromiseFromRoutes` in `docs/formal/FelixShard.tla`. The bound is for a
/// control plane that starts serving the read.
pub(crate) const FRESH: Duration = Duration::from_secs(10);

/// How long after the last request for a shard its answer is kept fresh.
/// Longer than a leader's longest wait between fence attempts
/// (`FENCE_RETRY_MAX`), so a retry finds a fresh answer however short the
/// freshness bound.
const WANTED_FOR: Duration = Duration::from_secs(5);

/// How soon a read that found nothing is tried again while still wanted.
const UNANSWERED_RETRY: Duration = Duration::from_millis(200);

/// Past this many shards, ones nobody asked about for [`FRESH`] are dropped.
const PRUNE_ABOVE: usize = 1024;

/// Confirmed reads, made in the background so that no peer request waits on
/// the control plane. One task per shard incarnation reads while requests
/// for it keep coming, refreshing the answer before it goes stale.
#[derive(Debug, Default)]
pub(crate) struct ConfirmedReads {
    slots: parking_lot::Mutex<HashMap<(ShardKey, u64), Slot>>,
}

#[derive(Debug)]
struct Slot {
    /// The last answer that named a leader or said the read is not served,
    /// with when it arrived.
    answer: Option<(Confirmed, Instant)>,
    /// Whether a task is reading for this slot.
    reading: bool,
    /// The freshness the last request needed.
    fresh: Duration,
    /// The last request for it.
    wanted: Instant,
}

impl ConfirmedReads {
    /// The answer for `key` at `incarnation` if one arrived within `fresh`,
    /// starting a read if none is under way. Never waits on the control
    /// plane: `None` means ask again later.
    pub(crate) fn answer(
        self: &Arc<Self>,
        routes: &Arc<dyn ConfirmedRoutes>,
        key: &ShardKey,
        incarnation: u64,
        fresh: Duration,
    ) -> Option<Confirmed> {
        let slot_key = (key.clone(), incarnation);
        let now = Instant::now();
        let mut slots = self.slots.lock();
        if slots.len() > PRUNE_ABOVE {
            slots.retain(|_, slot| slot.reading || now.duration_since(slot.wanted) < FRESH);
        }
        let slot = slots.entry(slot_key.clone()).or_insert(Slot {
            answer: None,
            reading: false,
            fresh,
            wanted: now,
        });
        slot.wanted = now;
        slot.fresh = fresh;
        let answer = slot
            .answer
            .as_ref()
            .filter(|(_, at)| now.duration_since(*at) < fresh)
            .map(|(answer, _)| answer.clone());
        if !slot.reading {
            slot.reading = true;
            tokio::spawn(Arc::clone(self).read(Arc::clone(routes), slot_key));
        }
        answer
    }

    /// Read until nobody has asked for [`WANTED_FOR`], each answer replacing
    /// the last before it goes stale.
    async fn read(self: Arc<Self>, routes: Arc<dyn ConfirmedRoutes>, slot_key: (ShardKey, u64)) {
        let mut reading = Reading {
            reads: &self,
            slot_key: &slot_key,
            ended: false,
        };
        loop {
            let answer = routes.confirmed(&slot_key.0).await;
            let next = {
                let mut slots = self.slots.lock();
                let Some(slot) = slots.get_mut(&slot_key) else {
                    return;
                };
                let next = match answer {
                    // Nothing known; dropped so no older answer outlives it.
                    Confirmed::Unavailable | Confirmed::Unassigned => {
                        slot.answer = None;
                        UNANSWERED_RETRY
                    }
                    answer => {
                        slot.answer = Some((answer, Instant::now()));
                        slot.fresh / 2
                    }
                };
                if slot.wanted.elapsed() >= WANTED_FOR {
                    slot.reading = false;
                    reading.ended = true;
                    return;
                }
                next
            };
            tokio::time::sleep(next).await;
        }
    }
}

/// Clears a slot's reading flag if its task panics or is dropped, so the
/// next request starts another.
struct Reading<'a> {
    reads: &'a ConfirmedReads,
    slot_key: &'a (ShardKey, u64),
    /// Set once the task cleared the flag itself.
    ended: bool,
}

impl Drop for Reading<'_> {
    fn drop(&mut self) {
        if !self.ended
            && let Some(slot) = self.reads.slots.lock().get_mut(self.slot_key)
        {
            slot.reading = false;
        }
    }
}
