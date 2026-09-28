//! The per-shard write fence: the line between the writes a broker commits
//! for a shard and the ones it refuses once it has stopped serving it.
//!
//! Admission checks ownership, but an admitted write can then wait in a queue
//! for as long as the queue is deep. So every write enters the fence again at
//! the moment it claims its place in the log, and stays counted until it is
//! durable and fanned out. Once the fence is closed, a closed fence with
//! nothing in flight means nothing more can land: that is what makes the
//! drained report exact rather than a guess about how long a queue can hold a
//! write. See "Planned handoff" in `docs/replication-design.md`.
//!
//! [`ShardLifecycle`](super::ShardLifecycle) is the only thing that opens and
//! closes it: open while the shard is `Active` at a generation, closed in
//! every other phase.
//!
//! The fence is also the commit gate for the lease. Every write path already
//! has to enter it -- publishes, forwarded writes, cache puts, counter adds,
//! group acks -- so checking the lease here, against the clock, is what makes
//! a lapsed lease refuse all of them rather than whichever paths remembered to
//! ask. See "Leases" in `docs/replication-design.md`. The exception is a
//! `Quorum` stream shard whose acknowledgements its followers decide: its
//! writes get in without the lease, and only group state still asks for it
//! ([`ShardFence::require_lease`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, OnceLock};

use parking_lot::RwLock;
use tokio::sync::Notify;

use crate::cluster::lease::LeaseState;
use crate::cluster::lease::metrics as lease_metrics;
use crate::shards::ShardKey;
use crate::shards::routing::IngressRouter;

/// `Gate::open_at` for a closed fence. No assignment reaches this generation.
const CLOSED: u64 = u64::MAX;

/// Write fences for every shard this broker has served, keyed by kind as well
/// as name: a cache and a stream of the same name are different shards.
#[derive(Debug, Default)]
pub struct ShardFence {
    gates: RwLock<HashMap<ShardKey, Arc<Gate>>>,
    /// The broker's lease, once membership has one. Unset only where there is
    /// no control plane to grant one (unit tests of the fence on its own).
    lease: OnceLock<Arc<LeaseState>>,
    /// Promoted shards waiting for a majority to take their generation, and
    /// the generation each waits at. Closed meanwhile, like any shard not
    /// `Active`; kept here so replication, which runs the fence, can read it
    /// without the lifecycle's lock.
    promotions: RwLock<HashMap<ShardKey, u64>>,
}

impl ShardFence {
    /// Gate every write on `lease` as well as on the generation. Called once,
    /// at startup, before any shard is opened; a second call is ignored.
    pub fn bind_lease(&self, lease: Arc<LeaseState>) {
        let _ = self.lease.set(lease);
    }

    /// The lease bound with [`Self::bind_lease`], if any.
    pub fn lease(&self) -> Option<&Arc<LeaseState>> {
        self.lease.get()
    }

    /// Whether the lease, if one is bound, is valid right now against the
    /// clock. The same check a write makes when it enters, for callers that
    /// must re-check before acknowledging.
    pub fn lease_valid(&self) -> bool {
        self.lease.get().is_none_or(|lease| lease.is_valid_now())
    }

    /// Note that `key` waits for the promotion fence at `generation`.
    pub fn await_promotion(&self, key: &ShardKey, generation: u64) {
        self.promotions.write().insert(key.clone(), generation);
    }

    /// `key` no longer waits for a promotion fence.
    pub fn promotion_settled(&self, key: &ShardKey) {
        self.promotions.write().remove(key);
    }

    /// The generation `key` waits at for the promotion fence, if it does.
    pub fn awaiting_promotion(&self, key: &ShardKey) -> Option<u64> {
        self.promotions.read().get(key).copied()
    }

    /// Let writes to `key` in, if they were admitted at `generation`.
    pub fn open(&self, key: &ShardKey, generation: u64) {
        let gate = Arc::clone(self.gates.write().entry(key.clone()).or_default());
        gate.open_at.store(generation, SeqCst);
    }

    /// Refuse every write to `key` that has not entered yet. Writes already in
    /// finish; [`Self::quiesced`] says when they have.
    pub fn close(&self, key: &ShardKey) {
        if let Some(gate) = self.gates.read().get(key) {
            gate.open_at.store(CLOSED, SeqCst);
        }
    }

    /// Enter the fence for one write admitted at `generation`, right before it
    /// claims its place in the log. `None` means the shard stopped serving
    /// since admission and the write must be refused. Hold the guard until the
    /// write is durable and fanned out.
    pub fn enter(&self, key: &ShardKey, generation: u64) -> Option<FenceGuard> {
        self.admit(key, generation).ok()
    }

    /// [`Self::enter`], with the refusal and its reason as an error.
    ///
    /// The lease is read from the clock here, not from the cached admission
    /// flag: a write can sit in a queue, or the process can be suspended,
    /// for longer than the lease had left when the write was admitted.
    pub fn admit(&self, key: &ShardKey, generation: u64) -> Result<FenceGuard, Fenced> {
        let gate = Arc::clone(self.gates.read().get(key).ok_or(Fenced::NotServing)?);
        // Counted before the check. A close that lands between the two then
        // still sees this write in flight, so `quiesced` cannot miss a write
        // that got in; one that gets refused only delays it a moment.
        gate.in_flight.fetch_add(1, SeqCst);
        let guard = FenceGuard { gate, generation };
        if guard.gate.open_at.load(SeqCst) != generation {
            return Err(Fenced::NotServing);
        }
        if !guard.lease_free() {
            self.check_lease()?;
        }
        Ok(guard)
    }

    /// Refuse a write that already holds its place in the fence if the lease
    /// lapsed since it entered. The generation is not re-checked: a write in
    /// the fence is one a close waits for, which is what makes it safe to
    /// finish.
    pub fn recheck(&self, held: &FenceGuard) -> Result<(), Fenced> {
        if held.lease_free() {
            return Ok(());
        }
        self.check_lease()
    }

    /// Refuse unless the lease is valid, whatever the shard's writes need.
    /// Group state is acknowledged on the leader alone, so a deposed leader
    /// taking it without the lease would lose it at the failover.
    pub fn require_lease(&self) -> Result<(), Fenced> {
        self.check_lease()
    }

    fn check_lease(&self) -> Result<(), Fenced> {
        if self.lease_valid() {
            Ok(())
        } else {
            lease_metrics::record_refusal(lease_metrics::BOUNDARY_COMMIT);
            Err(Fenced::LeaseLapsed)
        }
    }

    /// Whether `key` is closed with no write in flight, so its log cannot grow
    /// until it is opened again. True for a shard that was never opened here.
    pub fn quiesced(&self, key: &ShardKey) -> bool {
        self.gates
            .read()
            .get(key)
            .is_none_or(|gate| gate.quiesced())
    }

    /// Wait until [`Self::quiesced`] holds for `key`.
    pub async fn quiesce(&self, key: &ShardKey) {
        let Some(gate) = self.gates.read().get(key).cloned() else {
            return;
        };
        loop {
            let idle = gate.idle.notified();
            tokio::pin!(idle);
            // Registered before the check, so a last write leaving in between
            // still wakes this.
            idle.as_mut().enable();
            if gate.quiesced() {
                return;
            }
            idle.await;
        }
    }
}

impl felix_replication::driver::WriteFence for ShardFence {
    fn quiesced(&self, key: &ShardKey) -> bool {
        ShardFence::quiesced(self, key)
    }

    fn serve_without_lease(&self, key: &ShardKey, generation: u64) {
        if let Some(gate) = self.gates.read().get(key) {
            gate.lease_free_at.store(generation, SeqCst);
        }
    }
}

/// Enter the fence for a write about to claim its place in `key`'s log, having
/// been admitted at `generation`.
///
/// `Ok(None)` on a single-node broker: no router, so no fence and nothing to
/// refuse. A cluster member with no shard to name refuses rather than skip the
/// fence.
pub fn enter(
    ingress: Option<&IngressRouter>,
    key: Option<&ShardKey>,
    generation: u64,
) -> Result<Option<FenceGuard>, Fenced> {
    match (ingress, key) {
        (None, _) => Ok(None),
        (Some(ingress), Some(key)) => ingress.fence().admit(key, generation).map(Some),
        (Some(_), None) => Err(Fenced::NotServing),
    }
}

/// [`enter`], for a write that may already hold a guard from admission.
pub fn enter_or_keep(
    held: &mut Option<FenceGuard>,
    ingress: Option<&IngressRouter>,
    key: Option<&ShardKey>,
    generation: u64,
) -> Result<Option<FenceGuard>, Fenced> {
    match held.take() {
        Some(guard) => {
            // Admission may have been a queue ago. The lease has to hold at
            // the claim, not just when the write came in.
            if let Some(ingress) = ingress {
                ingress.fence().recheck(&guard)?;
            }
            Ok(Some(guard))
        }
        None => enter(ingress, key, generation),
    }
}

/// A write refused at its claim. Nothing was written, so it is safe to send
/// again, to this broker once it holds the lease or to the shard's new owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fenced {
    /// The shard stopped serving here, or never was, at the write's generation.
    NotServing,
    /// This broker's lease lapsed: another broker may be leading the shard.
    LeaseLapsed,
}

impl std::fmt::Display for Fenced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotServing => write!(
                f,
                "shard is not servable here: this broker stopped serving it after the write was admitted"
            ),
            Self::LeaseLapsed => write!(
                f,
                "lease lapsed: this broker may no longer lead the shard, so it commits no writes until the lease is renewed"
            ),
        }
    }
}

impl std::error::Error for Fenced {}

/// One write inside the fence. Dropping it lets the fence quiesce.
#[derive(Debug)]
pub struct FenceGuard {
    gate: Arc<Gate>,
    generation: u64,
}

impl FenceGuard {
    /// Whether the write's generation acknowledges by its followers, so it
    /// needs no lease.
    pub(crate) fn lease_free(&self) -> bool {
        self.gate.lease_free_at.load(SeqCst) == self.generation
    }
}

impl Drop for FenceGuard {
    fn drop(&mut self) {
        if self.gate.in_flight.fetch_sub(1, SeqCst) == 1 {
            self.gate.idle.notify_waiters();
        }
    }
}

#[derive(Debug)]
struct Gate {
    /// The generation writes must have been admitted at, or [`CLOSED`].
    open_at: AtomicU64,
    /// The generation whose writes need no lease, or [`CLOSED`]: see
    /// [`felix_replication::driver::WriteFence::serve_without_lease`].
    lease_free_at: AtomicU64,
    in_flight: AtomicUsize,
    idle: Notify,
}

impl Default for Gate {
    fn default() -> Self {
        Self {
            open_at: AtomicU64::new(CLOSED),
            lease_free_at: AtomicU64::new(CLOSED),
            in_flight: AtomicUsize::new(0),
            idle: Notify::new(),
        }
    }
}

impl Gate {
    fn quiesced(&self) -> bool {
        self.open_at.load(SeqCst) == CLOSED && self.in_flight.load(SeqCst) == 0
    }
}

#[cfg(test)]
mod tests;
