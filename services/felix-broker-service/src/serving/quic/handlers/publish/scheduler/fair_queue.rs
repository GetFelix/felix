//! Per-tenant deficit round robin over per-lane FIFO queues.
//!
//! The queue owns three rules and nothing else, so it can be tested without a
//! runtime:
//!
//! - **A lane is served in order, one job at a time.** A popped job's lane is
//!   busy until [`FairQueue::complete`] is called for it; the next job on that
//!   lane is not handed out before then. This is what keeps one shard's claims
//!   in arrival order while different shards run side by side.
//! - **Tenants take turns by cost.** Each tenant with ready work earns
//!   `quantum` per round and spends a job's cost to run it, so a tenant sending
//!   many or large batches gets the same share of turns-by-bytes as one sending
//!   a few, not the whole queue.
//! - **The queue is bounded, and part of it is kept back.** A tenant may hold
//!   up to `share` jobs whenever there is room; past that it may borrow idle
//!   capacity, but never the last `share` slots. A tenant flooding the queue is
//!   refused while a quiet one still gets in.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

/// Queued jobs, grouped into lanes and served tenant by tenant.
pub(crate) struct FairQueue<K, T> {
    tenants: HashMap<Arc<str>, Tenant<K>>,
    /// Tenants with at least one ready lane, in turn order.
    active: VecDeque<Arc<str>>,
    lanes: HashMap<K, Lane<T>>,
    queued: usize,
    capacity: usize,
    share: usize,
    quantum: usize,
}

impl<K: Clone + Eq + Hash, T> FairQueue<K, T> {
    /// `capacity` bounds queued jobs in all; `share` is what every tenant is
    /// guaranteed room for; `quantum` is the cost a tenant earns per turn.
    pub(crate) fn new(capacity: usize, share: usize, quantum: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            tenants: HashMap::new(),
            active: VecDeque::new(),
            lanes: HashMap::new(),
            queued: 0,
            capacity,
            share: share.clamp(1, capacity),
            quantum: quantum.max(1),
        }
    }

    /// Jobs queued and not yet handed out.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.queued
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.queued == 0
    }

    /// Jobs `tenant` has queued.
    #[cfg(test)]
    pub(crate) fn queued_for(&self, tenant: &str) -> usize {
        self.tenants.get(tenant).map_or(0, |tenant| tenant.queued)
    }

    /// Queue `item` at the back of `lane`, charging `tenant` for it.
    ///
    /// `Err` hands the item back when the queue has no room for this tenant.
    pub(crate) fn push(&mut self, tenant: &str, lane: K, item: T, cost: usize) -> Result<(), T> {
        let held = self.tenants.get(tenant).map_or(0, |tenant| tenant.queued);
        if !self.admits(held) {
            return Err(item);
        }
        // A lane belongs to one tenant: it is a shard of one tenant's stream.
        let tenant = match self.lanes.get(&lane) {
            Some(existing) => Arc::clone(&existing.tenant),
            None => self
                .tenants
                .get_key_value(tenant)
                .map_or_else(|| Arc::from(tenant), |(name, _)| Arc::clone(name)),
        };
        let entry = self.lanes.entry(lane.clone()).or_insert_with(|| Lane {
            tenant: Arc::clone(&tenant),
            jobs: VecDeque::new(),
            busy: false,
        });
        let becomes_ready = entry.jobs.is_empty() && !entry.busy;
        entry.jobs.push_back((item, cost.max(1)));
        self.queued += 1;
        self.tenants
            .entry(Arc::clone(&tenant))
            .or_insert_with(Tenant::new)
            .queued += 1;
        if becomes_ready {
            self.make_ready(tenant, lane);
        }
        Ok(())
    }

    /// The next job to run, and its lane, which is busy until
    /// [`Self::complete`].
    pub(crate) fn pop(&mut self) -> Option<(K, T)> {
        loop {
            let name = Arc::clone(self.active.front()?);
            let tenant = self.tenants.get_mut(&name).expect("active tenants exist");
            let lane_key = tenant
                .ready
                .front()
                .expect("active tenants have a ready lane");
            let lane = self.lanes.get_mut(lane_key).expect("ready lanes exist");
            let cost = lane.jobs.front().expect("ready lanes have a job").1;
            if !tenant.in_turn {
                tenant.in_turn = true;
                tenant.deficit += self.quantum;
            }
            if tenant.deficit < cost {
                // Not enough for this job yet: keep the credit, wait a turn.
                tenant.in_turn = false;
                self.active.rotate_left(1);
                continue;
            }
            tenant.deficit -= cost;
            tenant.queued -= 1;
            let lane_key = tenant.ready.pop_front().expect("checked above");
            if tenant.ready.is_empty() {
                tenant.in_turn = false;
                self.active.pop_front();
                if tenant.queued == 0 {
                    // Idle. Carrying credit into an idle spell would let a
                    // tenant bank turns while others waited.
                    self.tenants.remove(&name);
                }
                // Otherwise its lanes are busy, not empty: it keeps its credit
                // and rejoins the rotation when one of them is done.
            }
            let (item, _) = lane.jobs.pop_front().expect("checked above");
            lane.busy = true;
            self.queued -= 1;
            return Some((lane_key, item));
        }
    }

    /// The job popped from `lane` has finished its ordered part; the lane's
    /// next job may run.
    pub(crate) fn complete(&mut self, lane: &K) {
        let Some(entry) = self.lanes.get_mut(lane) else {
            return;
        };
        entry.busy = false;
        if entry.jobs.is_empty() {
            self.lanes.remove(lane);
        } else {
            let tenant = Arc::clone(&entry.tenant);
            self.make_ready(tenant, lane.clone());
        }
    }

    /// Whether a tenant already holding `held` jobs may queue one more.
    fn admits(&self, held: usize) -> bool {
        if self.queued >= self.capacity {
            return false;
        }
        held < self.share || self.queued + self.share < self.capacity
    }

    fn make_ready(&mut self, name: Arc<str>, lane: K) {
        let tenant = self
            .tenants
            .entry(Arc::clone(&name))
            .or_insert_with(Tenant::new);
        if tenant.ready.is_empty() {
            self.active.push_back(name);
        }
        tenant.ready.push_back(lane);
    }
}

/// One tenant's place in the rotation.
struct Tenant<K> {
    /// Its lanes that have a job and are not running one, in turn order.
    ready: VecDeque<K>,
    /// Jobs it has queued, running lanes included.
    queued: usize,
    deficit: usize,
    /// It has had this turn's quantum.
    in_turn: bool,
}

impl<K> Tenant<K> {
    fn new() -> Self {
        Self {
            ready: VecDeque::new(),
            queued: 0,
            deficit: 0,
            in_turn: false,
        }
    }
}

/// One ordering domain: a shard's jobs, in arrival order.
struct Lane<T> {
    tenant: Arc<str>,
    jobs: VecDeque<(T, usize)>,
    /// A job from this lane is running; the next waits for it.
    busy: bool,
}

#[cfg(test)]
mod tests;
