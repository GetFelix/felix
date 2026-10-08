//! Local shard ownership: what this broker does about what the control plane
//! decided.
//!
//! The watch says who *should* own each shard. This says what this node has
//! actually done about it — because becoming an owner is not instant. The
//! durable log has to be opened and recovered first, and a broker that served
//! writes in that window would acknowledge records it could not yet persist.
//!
//! Ownership is therefore two-phase in both directions: `Opening` before
//! `Active`, and `Draining` before `Closed`. A shard serves only in `Active`,
//! and only at the generation the control plane currently names.
//!
//! The decision half is a pure state machine and the I/O half is a driver, for
//! the same reason placement is split that way: every interesting rule is then
//! testable without a disk.
//!
//! The phase also drives the [`fence::ShardFence`] every write passes through
//! when it claims its place in the log: open only while `Active`.
//!
//! A broker named as the destination of a move prepares the shard before it
//! is handed over, so the cut-over only has to record the new generation.
pub mod fence;
pub mod metrics;
pub mod promotion;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::shards::lifecycle::metrics as mm;
use crate::shards::{ShardKey, ShardKind, watch::ShardAssignment};

/// How long ending a shard's readers waits for writes already in flight. A
/// write inside the fence is a commit and a fanout, normally milliseconds.
const QUIESCE_BOUND: std::time::Duration = std::time::Duration::from_secs(2);

/// Where this broker is with one shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Phase {
    /// Not ours, and nothing open.
    Unassigned,
    /// Ours, but the local log is not ready. Not serving.
    Opening,
    /// Ours and recovered, and waiting for a majority of the replicas to
    /// take this broker's generation before it serves. Only after a
    /// promotion, and only when every replica offers the fence.
    Fencing,
    /// Ours and ready.
    Active,
    /// No longer ours. In-flight work is finishing; no new writes.
    Draining,
    /// Drained and closed.
    Closed,
    /// The local log could not be opened. Not serving, and not retried until
    /// the assignment changes -- a failure that repeats every poll is noise,
    /// and the operator needs the state to stay visible.
    Failed,
}

impl Phase {
    pub fn label(self) -> &'static str {
        match self {
            Phase::Unassigned => "unassigned",
            Phase::Opening => "opening",
            Phase::Fencing => "fencing",
            Phase::Active => "active",
            Phase::Draining => "draining",
            Phase::Closed => "closed",
            Phase::Failed => "failed",
        }
    }

    /// Whether a shard in this phase may accept writes.
    ///
    /// Only `Active`. `Opening` is the whole reason this type exists: the
    /// assignment has arrived but the log has not been recovered, and
    /// acknowledging a write there would promise durability that is not set up.
    pub fn may_serve(self) -> bool {
        matches!(self, Phase::Active)
    }
}

/// What this broker holds for one shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalShard {
    pub phase: Phase,
    /// The assignment generation this state was reached for.
    pub generation: u64,
    /// The assignment at this generation is `draining`: the shard is being
    /// moved away and must not serve here again, however many times the
    /// assignment is re-delivered.
    pub draining: bool,
}

/// What recording a successful open did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Opened {
    /// Serving.
    Activated,
    /// Recovered, and waiting for the promotion fence before it serves.
    Fencing,
    /// Recovered but not serving: the assignment is draining.
    Draining,
    /// The generation moved on while the log was opening.
    Stale,
}

/// Work the driver must do to catch up with a decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Open and recover the local log, then report back.
    Open {
        key: ShardKey,
        generation: u64,
        /// This broker was not serving the shard when this generation
        /// arrived, so another may have led it since. What it remembers from
        /// serving it before is stale and has to go before it serves again.
        new_term: bool,
        /// A promotion: the shard waits in `Fencing` once open, and where
        /// this generation begins is recorded when it serves, not here.
        fence: bool,
    },
    /// Stop writes, drain, flush, and close.
    Release { key: ShardKey, generation: u64 },
    /// A move is bringing the shard here. Get ready to serve it; nothing
    /// serves yet.
    Prepare { key: ShardKey },
    /// This broker is no longer the shard's leader, a replica, or a move's
    /// destination. Close what it holds open for it.
    Close { key: ShardKey },
    /// Nothing to do.
    None,
}

/// This broker's view of the shards it holds.
#[derive(Debug)]
pub struct ShardLifecycle {
    node_id: String,
    shards: HashMap<ShardKey, LocalShard>,
    fence: Arc<fence::ShardFence>,
    /// Moves naming this broker as the destination, until it serves the shard
    /// or stops being named.
    incoming: HashMap<ShardKey, Incoming>,
    /// Where each shard is headed by the latest assignment: the successor
    /// while this broker leads it, the new leader once it does not. What a
    /// reader ended by the move is told.
    headed: HashMap<ShardKey, felix_broker::ShardHandoff>,
    /// Shards the last assignment set gave this broker any role in. Leaving
    /// this set is what closes a shard's logs: until then a replica or a
    /// move's destination still needs them.
    held: std::collections::HashSet<ShardKey>,
    /// Whether a promoted shard waits for the fence before it serves. Set
    /// when a replication driver is there to fence it.
    fence_promotions: bool,
    /// Shards opening for a promotion, which go to `Fencing` once open.
    promoting: std::collections::HashSet<ShardKey>,
}

/// A move toward this broker, timed from when this broker first saw each step.
#[derive(Debug, Clone, Copy)]
struct Incoming {
    named: Instant,
    fenced: Option<Instant>,
}

impl ShardLifecycle {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            shards: HashMap::new(),
            fence: Arc::default(),
            incoming: HashMap::new(),
            headed: HashMap::new(),
            held: std::collections::HashSet::new(),
            fence_promotions: false,
            promoting: std::collections::HashSet::new(),
        }
    }

    /// Hold a promoted shard, stream or cache, in `Fencing` until
    /// [`Self::fenced`]. For a broker whose replication driver fences
    /// promotions.
    pub fn fence_promotions(&mut self) {
        self.fence_promotions = true;
    }

    /// The promotion fence for `key` is done at `generation`, or the shard
    /// opens on the lease: serve it. False if the shard has moved on.
    pub fn fenced(&mut self, key: &ShardKey, generation: u64) -> bool {
        match self.shards.get(key) {
            Some(shard) if shard.phase == Phase::Fencing && shard.generation == generation => {
                self.set(key, Phase::Active, generation, false);
                true
            }
            _ => false,
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// The write fence this lifecycle opens and closes. Every write path
    /// enters it; see [`fence`].
    pub fn fence(&self) -> &Arc<fence::ShardFence> {
        &self.fence
    }

    /// Whether this broker may serve writes for `key` right now.
    pub fn may_serve(&self, key: &ShardKey) -> bool {
        self.shards
            .get(key)
            .is_some_and(|shard| shard.phase.may_serve())
    }

    /// Whether this broker may serve `key` at exactly `generation`.
    ///
    /// The stricter gate, for a caller that read an assignment and wants to act
    /// on it: serving at a generation the control plane has moved past is how
    /// two brokers end up believing they lead the same shard.
    pub fn may_serve_at(&self, key: &ShardKey, generation: u64) -> bool {
        self.shards
            .get(key)
            .is_some_and(|shard| shard.phase.may_serve() && shard.generation == generation)
    }

    pub fn phase(&self, key: &ShardKey) -> Phase {
        self.shards
            .get(key)
            .map_or(Phase::Unassigned, |shard| shard.phase)
    }

    pub fn generation(&self, key: &ShardKey) -> Option<u64> {
        self.shards.get(key).map(|shard| shard.generation)
    }

    /// Where the latest assignment sends a shard this broker is giving up,
    /// without the address, which the lifecycle does not know. `None` when the
    /// shard has no assignment at all.
    pub fn headed(&self, key: &ShardKey) -> Option<felix_broker::ShardHandoff> {
        self.headed.get(key).cloned()
    }

    /// Shards this broker is currently serving.
    pub fn active(&self) -> impl Iterator<Item = &ShardKey> {
        self.shards
            .iter()
            .filter(|(_, shard)| shard.phase.may_serve())
            .map(|(key, _)| key)
    }

    /// React to what the control plane now says about one shard.
    ///
    /// `assignment` is `None` when the shard has no assignment at all. Returns
    /// the work the driver must do; repeated calls with the same or an older
    /// generation return [`Action::None`], so a duplicate delivery or a replayed
    /// snapshot costs nothing.
    pub fn observe(&mut self, key: &ShardKey, assignment: Option<&ShardAssignment>) -> Action {
        let ours = assignment.is_some_and(|a| a.leader == self.node_id);
        let generation = assignment.map_or(0, |a| a.generation);
        let draining = assignment.is_some_and(ShardAssignment::is_draining);
        let current = self.shards.get(key).cloned();
        let incoming = !ours
            && assignment.is_some_and(|a| a.successor.as_deref() == Some(self.node_id.as_str()));
        if !ours && !incoming {
            self.incoming.remove(key);
        }
        match assignment {
            Some(a) => {
                let node_id = if ours {
                    a.successor.clone()
                } else {
                    Some(a.leader.clone())
                };
                self.headed.insert(
                    key.clone(),
                    felix_broker::ShardHandoff {
                        node_id,
                        addr: None,
                        generation: a.generation,
                    },
                );
            }
            None => {
                self.headed.remove(key);
            }
        }

        match (ours, current) {
            // Newly ours: open before serving.
            (true, None) => self.begin_open(key, generation, draining),

            (true, Some(existing)) => {
                if generation < existing.generation {
                    // Older than what we already acted on: a duplicate, a
                    // reordered poll, or a snapshot replay.
                    mm::record_stale_event();
                    return Action::None;
                }
                match existing.phase {
                    // The shard is moving away. Stop serving now; the log
                    // stays open for replication to ship the tail.
                    Phase::Active | Phase::Fencing
                        if draining && generation == existing.generation =>
                    {
                        self.set(key, Phase::Draining, generation, true);
                        Action::Release {
                            key: key.clone(),
                            generation,
                        }
                    }
                    Phase::Opening if draining && generation == existing.generation => {
                        // `opened` reads this and closes instead of serving.
                        self.set(key, Phase::Opening, generation, true);
                        Action::None
                    }
                    // Already serving this generation: nothing to do. This is
                    // the common case on every poll.
                    Phase::Active | Phase::Opening | Phase::Fencing
                        if generation == existing.generation =>
                    {
                        Action::None
                    }
                    // A new generation for a shard we already hold. Reopening is
                    // what makes the generation meaningful: the control plane
                    // moved the shard away and back, and the local state has to
                    // be re-established rather than assumed.
                    Phase::Active | Phase::Opening | Phase::Fencing => {
                        self.begin_open(key, generation, draining)
                    }
                    // A failed open is not retried by the same assignment
                    // arriving again. Every poll re-delivers it, and retrying
                    // each time buries the failure in noise while hammering a
                    // log that is not opening. A new generation is real news.
                    Phase::Failed if generation == existing.generation => Action::None,
                    // Released for a drain at this generation: stays released.
                    Phase::Draining | Phase::Closed
                        if existing.draining && generation == existing.generation =>
                    {
                        Action::None
                    }
                    // Ours again after we let it go, or after a failure at an
                    // older generation.
                    Phase::Draining | Phase::Closed | Phase::Unassigned | Phase::Failed => {
                        self.begin_open(key, generation, draining)
                    }
                }
            }

            // Not ours, and we hold nothing.
            (false, None) if incoming => self.expect(key, draining),
            (false, None) => Action::None,

            (false, Some(existing)) => match existing.phase {
                // Already given up.
                Phase::Closed | Phase::Unassigned | Phase::Draining if incoming => {
                    self.expect(key, draining)
                }
                Phase::Closed | Phase::Unassigned | Phase::Draining => Action::None,
                // A failed open never served, so there is nothing to drain.
                Phase::Failed => {
                    self.set(key, Phase::Closed, existing.generation, false);
                    if incoming {
                        self.expect(key, draining)
                    } else {
                        Action::None
                    }
                }
                Phase::Active | Phase::Opening | Phase::Fencing => {
                    self.set(key, Phase::Draining, existing.generation, false);
                    Action::Release {
                        key: key.clone(),
                        generation: existing.generation,
                    }
                }
            },
        }
    }

    /// Note a move naming this broker as the destination. Prepares once per
    /// move; `fenced` records when the old leader was told to stop.
    fn expect(&mut self, key: &ShardKey, fenced: bool) -> Action {
        let now = Instant::now();
        let mut first = false;
        let entry = self.incoming.entry(key.clone()).or_insert_with(|| {
            first = true;
            Incoming {
                named: now,
                fenced: None,
            }
        });
        if fenced && entry.fenced.is_none() {
            entry.fenced = Some(now);
        }
        if first {
            Action::Prepare { key: key.clone() }
        } else {
            Action::None
        }
    }

    /// Whether a move naming this broker as its destination is in progress
    /// for `key`.
    #[cfg(test)]
    pub(crate) fn is_incoming(&self, key: &ShardKey) -> bool {
        self.incoming.contains_key(key)
    }

    /// Forget moves toward this broker whose shard no longer has an
    /// assignment at all.
    fn forget_incoming_except(&mut self, present: &HashMap<ShardKey, ShardAssignment>) {
        self.incoming.retain(|key, _| present.contains_key(key));
    }

    /// A [`Action::Close`] for every shard this broker had a role in and, by
    /// `assignments`, no longer does -- including one whose assignment is gone.
    ///
    /// Resource cleanup only: it changes no phase and no fence. Leading is
    /// already given up by [`Self::observe`]; this is the later step of
    /// letting go of the files once nothing here reads them either.
    pub fn relinquished(
        &mut self,
        assignments: &HashMap<ShardKey, ShardAssignment>,
    ) -> Vec<Action> {
        let node = self.node_id.as_str();
        let mut closes = Vec::new();
        for (key, assignment) in assignments {
            let has_role = assignment.leader == node
                || assignment.replicas.iter().any(|replica| replica == node)
                || assignment.successor.as_deref() == Some(node);
            if has_role {
                self.held.insert(key.clone());
            } else if self.held.remove(key) {
                closes.push(Action::Close { key: key.clone() });
            }
        }
        self.held.retain(|key| {
            let assigned = assignments.contains_key(key);
            if !assigned {
                closes.push(Action::Close { key: key.clone() });
            }
            assigned
        });
        closes
    }

    /// Every shard this broker holds that the given assignment set no longer
    /// mentions.
    ///
    /// A snapshot replaces the whole picture, so a shard that vanished from it
    /// has to be released as surely as one reassigned away — otherwise a broker
    /// keeps serving a shard whose assignment was deleted.
    pub fn missing_from<'a>(
        &self,
        present: impl IntoIterator<Item = &'a ShardKey>,
    ) -> Vec<ShardKey> {
        let present: std::collections::HashSet<&ShardKey> = present.into_iter().collect();
        self.shards
            .iter()
            .filter(|(key, shard)| {
                !present.contains(*key) && !matches!(shard.phase, Phase::Closed | Phase::Unassigned)
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Record that the driver opened the log successfully.
    ///
    /// Ignored if the generation has moved on since the open began: the shard
    /// was reassigned while we were recovering it, and activating now would
    /// serve a generation we no longer hold. An open for a draining
    /// assignment lands in `Closed`: the log was recovered so replication can
    /// ship from it, but the shard is leaving and must not serve.
    pub fn opened(&mut self, key: &ShardKey, generation: u64) -> Opened {
        match self.shards.get(key) {
            Some(shard) if shard.phase == Phase::Opening && shard.generation == generation => {
                if shard.draining {
                    self.set(key, Phase::Closed, generation, true);
                    Opened::Draining
                } else if self.promoting.remove(key) {
                    self.set(key, Phase::Fencing, generation, false);
                    Opened::Fencing
                } else {
                    self.set(key, Phase::Active, generation, false);
                    if let Some(incoming) = self.incoming.remove(key) {
                        mm::record_move_arrived(
                            incoming.named.elapsed(),
                            incoming.fenced.map(|at| at.elapsed()),
                        );
                    }
                    Opened::Activated
                }
            }
            _ => {
                mm::record_stale_event();
                Opened::Stale
            }
        }
    }

    /// Record that the driver could not open the log.
    pub fn open_failed(&mut self, key: &ShardKey, generation: u64) {
        if let Some(shard) = self.shards.get(key)
            && shard.phase == Phase::Opening
            && shard.generation == generation
        {
            self.set(key, Phase::Failed, generation, shard.draining);
        }
    }

    /// Record that the driver finished draining and closed the log.
    pub fn released(&mut self, key: &ShardKey, generation: u64) {
        if let Some(shard) = self.shards.get(key)
            && shard.phase == Phase::Draining
            && shard.generation == generation
        {
            self.set(key, Phase::Closed, generation, shard.draining);
        }
    }

    /// Shards this broker can serve, and the generation each was opened at.
    ///
    /// The form ingress reads on every publish. Handing out a snapshot rather
    /// than exposing the lock keeps the hot path off any mutex this holds.
    pub fn servable(&self) -> HashMap<ShardKey, u64> {
        self.shards
            .iter()
            .filter(|(_, shard)| shard.phase.may_serve())
            .map(|(key, shard)| (key.clone(), shard.generation))
            .collect()
    }

    /// How many shards sit in each phase, for the gauge.
    pub fn counts(&self) -> HashMap<Phase, usize> {
        let mut counts = HashMap::new();
        for shard in self.shards.values() {
            *counts.entry(shard.phase).or_default() += 1;
        }
        counts
    }

    fn begin_open(&mut self, key: &ShardKey, generation: u64, draining: bool) -> Action {
        // Only a reopen of a shard it is serving continues a term. Anything
        // else -- released, fenced, failed -- may have let another broker lead
        // in between, and a generation gap cannot rule that out: assignments
        // arrive as a coalesced set, not one write at a time.
        let new_term = self.phase(key) != Phase::Active;
        // A promotion is the one new term the model fences (`FenceOnPromote`):
        // a move's destination takes over from a leader that drained into it,
        // and a cancelled move hands the shard back to the leader that had it.
        // A hand-back is the generation right after the draining one. A later
        // one may have had another leader in between, as when the move
        // finished and that leader then failed, and only the fence keeps it
        // out if it is still writing.
        let handed_back = self
            .shards
            .get(key)
            .is_some_and(|shard| shard.draining && generation == shard.generation + 1);
        let fence = self.fence_promotions
            && new_term
            && !draining
            && !self.incoming.contains_key(key)
            && !handed_back;
        if fence {
            self.promoting.insert(key.clone());
        } else {
            self.promoting.remove(key);
        }
        self.set(key, Phase::Opening, generation, draining);
        Action::Open {
            key: key.clone(),
            generation,
            new_term,
            fence,
        }
    }

    fn set(&mut self, key: &ShardKey, phase: Phase, generation: u64, draining: bool) {
        // The one place a phase changes, so the fence cannot disagree with it.
        // Closing here, as the decision is made, is what puts the close ahead of
        // `publish_servable` and of whatever the driver does next -- including
        // when a move arrives as a new draining generation, which reopens the
        // shard for shipping rather than releasing it.
        if phase.may_serve() {
            self.fence.open(key, generation);
        } else {
            self.fence.close(key);
        }
        // A reopen keeps an unfinished promotion pending: until it finishes,
        // the published routes still name the generation being fenced, and
        // replication would otherwise ship the unfenced log at it.
        if phase == Phase::Fencing {
            self.fence.await_promotion(key, generation);
        } else if phase != Phase::Opening {
            self.fence.promotion_settled(key);
        }
        let previous = self.shards.get(key).map(|shard| shard.phase);
        self.shards.insert(
            key.clone(),
            LocalShard {
                phase,
                generation,
                draining,
            },
        );
        if previous != Some(phase) {
            mm::record_transition(previous.unwrap_or(Phase::Unassigned), phase);
            mm::record_phase_counts(self.counts());
        }
    }
}

/// What a driver needs to open and close local shard state.
///
/// A trait so the lifecycle can be exercised without a disk, and so the durable
/// and non-durable cases are one code path: an in-memory stream has no log to
/// open, and saying so is cheaper than branching everywhere.
#[async_trait::async_trait]
pub trait ShardStore: Send + Sync {
    /// Open and recover local state for a shard this broker now leads at
    /// `generation`.
    ///
    /// Runs before the shard goes `Active`, so the log's tail here is exactly
    /// where this leadership begins — the one moment that is true, since the
    /// next thing to touch the log is a write under this generation.
    ///
    /// `begins_here` is false for a promotion that will be fenced: the
    /// fence may take a replica's tail first, so where this generation
    /// begins is recorded when the shard serves ([`record_term_start`]).
    async fn open(&self, key: &ShardKey, generation: u64, begins_here: bool) -> anyhow::Result<()>;
    /// Flush everything accepted for a shard, before ownership is given up.
    ///
    /// This is the point at which "no accepted write is unaccounted for" is
    /// either true or not.
    async fn release(&self, key: &ShardKey) -> anyhow::Result<()>;
    /// End the subscriptions and cache watches this broker serves for a shard
    /// it has stopped serving: released, or reopened only to hand it off.
    ///
    /// The broker stays up, so without this a reader's feed simply goes quiet
    /// and the client has no reason to look for the new leader. Each reader
    /// gets what was already queued for it first.
    ///
    /// `handoff` says where the shard went, when the assignment says; each
    /// reader is then told where to resume. `quiet` is whether the writes in
    /// flight when the shard stopped serving had all landed first.
    async fn end_readers(
        &self,
        _key: &ShardKey,
        _handoff: Option<felix_broker::ShardHandoff>,
        _quiet: bool,
    ) {
    }
    /// Drop what this broker kept from an earlier term of leading `key`,
    /// before it serves the shard again. Runs while the shard is not serving.
    async fn begin_term(&self, _key: &ShardKey) {}
    /// Get ready to serve a shard a move is bringing here: open its log and
    /// load what serving it needs, so taking it over is quick.
    ///
    /// Best effort and must not block: whatever this does not finish, the
    /// open at the cut-over does.
    fn prepare(&self, _key: &ShardKey) {}
    /// Close the logs and drop the in-memory state held for a shard this
    /// broker has no role in any more. A later open recovers it from disk.
    async fn close(&self, _key: &ShardKey) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Where a reader of a shard this broker stopped serving resumes: nowhere
/// named, so the client finds the shard again through its entry broker.
fn no_owner(generation: u64) -> felix_broker::ShardHandoff {
    felix_broker::ShardHandoff {
        node_id: None,
        addr: None,
        generation,
    }
}

/// Ends the readers of a released shard, for the stores that serve them.
#[derive(Clone)]
pub struct ShardReaders {
    broker: std::sync::Arc<felix_broker::Broker>,
    endpoints: Option<Arc<crate::cluster::client_endpoints::ClientEndpoints>>,
}

impl ShardReaders {
    pub fn new(broker: std::sync::Arc<felix_broker::Broker>) -> Self {
        Self {
            broker,
            endpoints: None,
        }
    }

    /// Where to look up the next owner's client address for a moved reader.
    pub fn with_endpoints(
        mut self,
        endpoints: Arc<crate::cluster::client_endpoints::ClientEndpoints>,
    ) -> Self {
        self.endpoints = Some(endpoints);
        self
    }

    /// End the readers of the shards this broker may no longer serve, until
    /// `shutdown`.
    ///
    /// A lapsed lease means another broker may lead these shards by now, so a
    /// reader here could be following a log the new leader has moved past.
    /// Each subscription and watch ends the way a moved one does, with where
    /// to resume and no named owner, and the client finds the shard again
    /// through its entry broker. New reads are refused until the lease comes
    /// back (`redirect_for`). What a `Quorum` stream held back stays held: if
    /// the lease is renewed at the same generation the mark releases it, and
    /// if the shard goes, releasing it drops it.
    ///
    /// A shard whose sessions need no lease ([`ShardFence::sessions_lease_free`])
    /// is left serving at the lapse. Its readers end instead when a follower
    /// refuses it for a newer leader ([`ShardFence::depose`]), or when no
    /// majority has confirmed it for a lease duration. The rounds that check
    /// run only while the lease is lapsed; while it holds, a new assignment
    /// reaches this broker the usual way.
    ///
    /// [`ShardFence::sessions_lease_free`]: fence::ShardFence::sessions_lease_free
    /// [`ShardFence::depose`]: fence::ShardFence::depose
    pub async fn watch_leadership(
        self,
        fence: Arc<fence::ShardFence>,
        marks: Arc<felix_replication::quorum::QuorumMarks>,
        lifecycle: Arc<tokio::sync::Mutex<ShardLifecycle>>,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        let Some(lease) = fence.lease().cloned() else {
            return;
        };
        // Lease-free shards kept serving past the lapse, and when a majority
        // last confirmed each.
        let mut unleased: HashMap<ShardKey, (u64, tokio::time::Instant)> = HashMap::new();
        loop {
            let probe = (lease.usable() / 4).max(std::time::Duration::from_millis(100));
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = lease.lapsed() => {
                    let led: Vec<(ShardKey, u64)> = {
                        let lifecycle = lifecycle.lock().await;
                        lifecycle
                            .active()
                            .filter_map(|key| Some((key.clone(), lifecycle.generation(key)?)))
                            .collect()
                    };
                    let now = tokio::time::Instant::now();
                    for (key, generation) in led {
                        if fence.sessions_lease_free(&key, generation) {
                            unleased.entry(key).or_insert((generation, now));
                        } else {
                            self.end(&key, Some(no_owner(generation)), false).await;
                        }
                    }
                }
                _ = fence.deposed() => {
                    for (key, generation) in fence.take_deposals() {
                        unleased.remove(&key);
                        let current = lifecycle.lock().await.generation(&key);
                        if current == Some(generation) {
                            self.end(&key, Some(no_owner(generation)), false).await;
                        }
                    }
                }
                _ = tokio::time::sleep(probe), if !unleased.is_empty() => {
                    if lease.is_valid_now() {
                        unleased.clear();
                        continue;
                    }
                    self.probe(&fence, &marks, &mut unleased, lease.usable()).await;
                }
            }
        }
    }

    /// One round for each lease-free shard kept past the lapse. A shard no
    /// majority has confirmed for `silence` is deposed, which ends its readers
    /// on the next turn of [`Self::watch_leadership`].
    async fn probe(
        &self,
        fence: &fence::ShardFence,
        marks: &felix_replication::quorum::QuorumMarks,
        unleased: &mut HashMap<ShardKey, (u64, tokio::time::Instant)>,
        silence: std::time::Duration,
    ) {
        let check = marks.reads_by_round();
        let rounds = unleased.iter().map(|(key, (generation, _))| async move {
            let confirmed = match check {
                Some(check) => check.confirm(key, *generation).await.is_ok(),
                None => false,
            };
            (key.clone(), confirmed)
        });
        let now = tokio::time::Instant::now();
        for (key, confirmed) in futures::future::join_all(rounds).await {
            let Some((generation, heard)) = unleased.get_mut(&key) else {
                continue;
            };
            if confirmed {
                *heard = now;
            } else if now.saturating_duration_since(*heard) >= silence {
                fence.depose(&key, *generation);
            }
        }
    }

    /// Forget the consumer groups' in-flight state for a stream shard, so it
    /// is rebuilt from the cursors that came back with the shard.
    async fn begin_term(&self, key: &ShardKey) {
        if key.kind != ShardKind::Stream {
            return;
        }
        if let Some(groups) = self.broker.group_reader() {
            groups
                .reset_shard(&key.tenant_id, &key.namespace, &key.stream, key.shard)
                .await;
        }
    }

    /// Open the shard's log and its in-memory state in the background.
    ///
    /// A destination that has received part of the copy has both already;
    /// this covers one that has received nothing yet, and a cache shard,
    /// whose log is otherwise opened by the first request after the cut-over.
    fn prepare(&self, key: &ShardKey) {
        let broker = Arc::clone(&self.broker);
        let key = key.clone();
        tokio::spawn(async move {
            let prepared = match key.kind {
                ShardKind::Stream => broker
                    .resolve_stream_handle(&key.tenant_id, &key.namespace, &key.stream, key.shard)
                    .await
                    .map(drop)
                    .map_err(|err| err.to_string()),
                ShardKind::Cache => broker
                    .shard_log(
                        felix_broker::LogKind::Cache,
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                    )
                    .await
                    .map(drop)
                    .ok_or_else(|| "no cache log".to_string()),
            };
            // Not a failure of the move: the cut-over opens the shard anyway.
            if let Err(err) = prepared {
                tracing::debug!(
                    kind = ?key.kind,
                    name = %key.stream,
                    shard = key.shard,
                    error = %err,
                    "could not prepare an incoming shard ahead of its cut-over",
                );
            }
        });
    }

    /// Close everything the broker holds open for the shard.
    async fn close(&self, key: &ShardKey) -> anyhow::Result<()> {
        let closed = match key.kind {
            ShardKind::Stream => {
                self.broker
                    .close_stream_shard(&key.tenant_id, &key.namespace, &key.stream, key.shard)
                    .await
            }
            ShardKind::Cache => {
                self.broker
                    .close_cache_shard(&key.tenant_id, &key.namespace, &key.stream, key.shard)
                    .await
            }
        };
        closed.map_err(|err| anyhow::anyhow!("close shard: {err}"))
    }

    /// End a shard's readers because this broker stopped serving it, and
    /// drop what a `Quorum` stream held back from them: it was never
    /// committed under this broker's leadership, and the readers resume on
    /// the next owner's log.
    async fn stop(&self, key: &ShardKey, handoff: Option<felix_broker::ShardHandoff>, quiet: bool) {
        self.end(key, handoff, quiet).await;
        if key.kind == ShardKind::Stream {
            let dropped = self
                .broker
                .discard_uncommitted(&key.tenant_id, &key.namespace, &key.stream, key.shard)
                .await;
            if dropped > 0 {
                tracing::info!(
                    name = %key.stream,
                    shard = key.shard,
                    dropped,
                    "dropped batches held for a quorum that this broker no longer leads",
                );
            }
        }
    }

    async fn end(&self, key: &ShardKey, handoff: Option<felix_broker::ShardHandoff>, quiet: bool) {
        let handoff = handoff.map(|mut to| {
            if to.addr.is_none()
                && let (Some(node_id), Some(endpoints)) = (&to.node_id, &self.endpoints)
            {
                to.addr = endpoints.redirect_addr(node_id);
            }
            to
        });
        let ended = match key.kind {
            ShardKind::Stream => {
                self.broker
                    .end_subscriptions(
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        handoff,
                    )
                    .await
            }
            ShardKind::Cache => {
                let moved = match handoff {
                    Some(to) => Some(felix_broker::ShardMoved {
                        resume_from: self.cache_tail(key, quiet).await,
                        to,
                    }),
                    None => None,
                };
                self.broker.cache_watches().map_or(0, |hub| {
                    hub.end_shard(
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        moved,
                    )
                })
            }
        };
        if ended > 0 {
            tracing::info!(
                kind = ?key.kind,
                name = %key.stream,
                shard = key.shard,
                ended,
                "ended readers of a shard this broker no longer serves",
            );
        }
    }

    /// Where a moved cache watch resumes: the shard log's tail, but only once
    /// the writes in flight have landed. A change still being applied could
    /// otherwise sit below the tail and reach no watcher, and resuming from
    /// the watch's own last offset is gapless anyway.
    async fn cache_tail(&self, key: &ShardKey, quiet: bool) -> Option<u64> {
        use felix_storage::log::AppendOnlyLog;
        if !quiet {
            return None;
        }
        let log = self
            .broker
            .cache()
            .shard_log(&key.tenant_id, &key.namespace, &key.stream, key.shard)
            .await?;
        log.tail_offset().await.ok()
    }
}

/// A [`ShardStore`] over the broker's durable storage.
///
/// Opening recovers the shard's log from disk, which is the work `Opening`
/// exists to wait for. Releasing flushes it, so nothing acknowledged here is
/// still only in the page cache when another node takes the shard.
pub struct DurableShardStore {
    storage: std::sync::Arc<felix_broker::DurableStorage>,
    readers: Option<ShardReaders>,
    generation_starts: Option<GenerationStarts>,
    /// This broker's node id, when it keeps ballots: leading a generation is
    /// accepting it from itself.
    ballot: Option<String>,
}

/// What a leader needs to write its generation-start record: the broker it
/// appends through and the fleet that decides whether it does.
#[derive(Clone)]
pub struct GenerationStarts {
    pub broker: std::sync::Arc<felix_broker::Broker>,
    pub fleet: std::sync::Arc<felix_common::fleet::FleetGate>,
}

impl GenerationStarts {
    /// Whether the fleet finalized `generation_start`.
    pub fn enabled(&self) -> bool {
        self.fleet.supports(felix_common::fleet::GENERATION_START)
    }
}

impl DurableShardStore {
    pub fn new(storage: std::sync::Arc<felix_broker::DurableStorage>) -> Self {
        Self {
            storage,
            readers: None,
            generation_starts: None,
            ballot: None,
        }
    }

    /// Accept each generation this broker leads as a ballot naming `node_id`,
    /// so no other node is answered at it here.
    pub fn with_ballots(mut self, node_id: impl Into<String>) -> Self {
        self.ballot = Some(node_id.into());
        self
    }

    /// Write a generation-start record whenever a leadership begins
    /// here, once the fleet has finalized `generation_start`.
    pub fn with_generation_starts(mut self, starts: GenerationStarts) -> Self {
        self.generation_starts = Some(starts);
        self
    }

    /// End the shard's readers on release, as [`ShardStore::end_readers`] says.
    pub fn with_readers(mut self, readers: ShardReaders) -> Self {
        self.readers = Some(readers);
        self
    }
}

impl DurableShardStore {
    /// Open a cache shard's record and counter logs, record where this
    /// leadership begins in each, and accept the generation.
    ///
    /// Without the term start, a leader that dies holding an uncommitted
    /// record comes back as a follower with no generation history, so the
    /// divergence at its tail cannot be told apart from one reaching further
    /// back and the replica halts instead of dropping the suffix (#863).
    ///
    /// Once the fleet finalized `generation_start` each log also gets its
    /// generation-start record, as a stream's does, since the cache and
    /// counter marks then count only past it. A promotion's are written when
    /// its fence opens the shard.
    async fn open_cache(
        &self,
        key: &ShardKey,
        generation: u64,
        begins_here: bool,
    ) -> anyhow::Result<()> {
        // Only a broker-backed store has the cache store to open.
        let Some(broker) = self.generation_starts.as_ref().map(|starts| &starts.broker) else {
            return Ok(());
        };
        let starts = self
            .generation_starts
            .as_ref()
            .filter(|starts| starts.enabled());
        for kind in [
            felix_broker::LogKind::Cache,
            felix_broker::LogKind::Counters,
        ] {
            // No counter store configured means no counter log to mark.
            let Some(log) = broker
                .shard_log(kind, &key.tenant_id, &key.namespace, &key.stream, key.shard)
                .await
            else {
                if kind == felix_broker::LogKind::Cache {
                    anyhow::bail!("open cache shard log");
                }
                continue;
            };
            if begins_here {
                record_term_start(&log, key, generation, starts.is_some()).await?;
            }
            accept_led_generation(&log, generation, self.ballot.as_deref()).await?;
            if begins_here && let Some(starts) = starts {
                start_generation(&starts.broker, &log, key, generation).await?;
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ShardStore for DurableShardStore {
    async fn open(&self, key: &ShardKey, generation: u64, begins_here: bool) -> anyhow::Result<()> {
        // A cache's logs live in the cache and counter stores, which only the
        // broker reaches; opening a stream log here would create an empty
        // directory nothing reads.
        if key.kind == ShardKind::Cache {
            return self.open_cache(key, generation, begins_here).await;
        }
        // Recovery happens here: validating the tail and rebuilding indexes is
        // exactly the cost the `Opening` phase is holding writes back for.
        let log = self
            .storage
            .open_stream(&key.tenant_id, &key.namespace, &key.stream, key.shard)
            .map_err(|err| anyhow::anyhow!("open shard log: {err}"))?;

        // Where this leadership begins. Followers record what they accept, and
        // until now a leader recorded nothing — so a broker's history had a
        // hole exactly over the stretch it led, and replication had no bound to
        // resume a follower from other than offset zero.
        //
        // Here and nowhere later: the tail stops being the generation's start
        // the instant this broker serves its first write, which is what the
        // `Opening` phase is still holding back.
        // Once the fleet counts marks from a generation's start, where that
        // start is has to be known, so failing to record it fails the open.
        let starts = self
            .generation_starts
            .as_ref()
            .filter(|starts| starts.enabled());
        if begins_here {
            record_term_start(&log, key, generation, starts.is_some()).await?;
        }
        // Leading at a generation accepts it, as following does: once this
        // broker has led at it, a leader older than it is not taken as a
        // follower, even after a restart.
        accept_led_generation(&log, generation, self.ballot.as_deref()).await?;
        // A promotion's record is written when its fence opens the shard.
        if begins_here && let Some(starts) = starts {
            start_generation(&starts.broker, &log, key, generation).await?;
        }
        Ok(())
    }

    async fn release(&self, key: &ShardKey) -> anyhow::Result<()> {
        if key.kind == ShardKind::Cache {
            return Ok(());
        }
        let log = self
            .storage
            .open_stream(&key.tenant_id, &key.namespace, &key.stream, key.shard)
            .map_err(|err| anyhow::anyhow!("open shard log to flush it: {err}"))?;
        log.sync()
            .await
            .map_err(|err| anyhow::anyhow!("flush shard log: {err}"))
    }

    async fn end_readers(
        &self,
        key: &ShardKey,
        handoff: Option<felix_broker::ShardHandoff>,
        quiet: bool,
    ) {
        if let Some(readers) = &self.readers {
            readers.stop(key, handoff, quiet).await;
        }
    }

    async fn begin_term(&self, key: &ShardKey) {
        if let Some(readers) = &self.readers {
            readers.begin_term(key).await;
        }
    }

    fn prepare(&self, key: &ShardKey) {
        if let Some(readers) = &self.readers {
            readers.prepare(key);
        }
    }

    async fn close(&self, key: &ShardKey) -> anyhow::Result<()> {
        match &self.readers {
            Some(readers) => readers.close(key).await,
            // Without the broker there is only the stream log to close; a
            // cache's lives in the cache store, which only the broker reaches.
            None if key.kind == ShardKind::Stream => self
                .storage
                .close_stream(&key.tenant_id, &key.namespace, &key.stream, key.shard)
                .await
                .map_err(|err| anyhow::anyhow!("close shard log: {err}")),
            None => Ok(()),
        }
    }
}

/// A [`ShardStore`] for a broker with no durable storage.
///
/// Taking a shard is bookkeeping only: there is no log to open and nothing to
/// flush, so both operations succeed immediately. Kept explicit rather than
/// making the store optional, so the lifecycle has one code path. Readers
/// are still ended on release: an ephemeral stream has subscribers too.
#[derive(Default)]
pub struct EphemeralShardStore {
    readers: Option<ShardReaders>,
}

impl EphemeralShardStore {
    pub fn with_readers(readers: ShardReaders) -> Self {
        Self {
            readers: Some(readers),
        }
    }
}

#[async_trait::async_trait]
impl ShardStore for EphemeralShardStore {
    async fn open(
        &self,
        _key: &ShardKey,
        _generation: u64,
        _begins_here: bool,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn release(&self, _key: &ShardKey) -> anyhow::Result<()> {
        Ok(())
    }

    async fn end_readers(
        &self,
        key: &ShardKey,
        handoff: Option<felix_broker::ShardHandoff>,
        quiet: bool,
    ) {
        if let Some(readers) = &self.readers {
            readers.stop(key, handoff, quiet).await;
        }
    }

    /// No logs, but the stream's in-memory state still goes.
    async fn close(&self, key: &ShardKey) -> anyhow::Result<()> {
        match &self.readers {
            Some(readers) => readers.close(key).await,
            None => Ok(()),
        }
    }
}

/// Drive one decision to completion.
pub async fn apply(
    lifecycle: &tokio::sync::Mutex<ShardLifecycle>,
    store: &dyn ShardStore,
    action: Action,
) {
    match action {
        Action::None => {}
        Action::Prepare { key } => store.prepare(&key),
        Action::Close { key } => match store.close(&key).await {
            Ok(()) => tracing::info!(
                kind = ?key.kind,
                name = %key.stream,
                shard = key.shard,
                "closed a shard this broker no longer has a role in",
            ),
            // Handles are fenced even when the final flush fails, so there is
            // nothing to retry; the next open recovers from disk.
            Err(err) => tracing::error!(
                kind = ?key.kind,
                name = %key.stream,
                shard = key.shard,
                error = %err,
                "could not cleanly close a shard this broker no longer has a role in",
            ),
        },
        Action::Open {
            key,
            generation,
            new_term,
            fence,
        } => match open(store, &key, generation, new_term, !fence).await {
            Ok(()) => {
                // Bound first: matching on the locked call would hold the guard
                // through the arms, and a handoff ends its readers under the lock.
                let opened = lifecycle.lock().await.opened(&key, generation);
                match opened {
                    Opened::Activated => tracing::info!(
                        stream = %key.stream,
                        shard = key.shard,
                        generation,
                        "shard opened and now serving",
                    ),
                    Opened::Fencing => tracing::info!(
                        stream = %key.stream,
                        shard = key.shard,
                        generation,
                        "shard opened after a promotion; fencing its replicas before it serves",
                    ),
                    // How a move usually reaches the old leader: every assignment
                    // write bumps the generation, so the fence arrives as a new,
                    // draining generation rather than as a release of the current
                    // one. This broker has stopped serving the shard either way, so
                    // its readers end here too.
                    Opened::Draining => {
                        end_readers(lifecycle, store, &key).await;
                        tracing::info!(
                            stream = %key.stream,
                            shard = key.shard,
                            generation,
                            "shard opened for a move; shipping to its successor, not serving",
                        );
                    }
                    // Reassigned while we were recovering it. Not an error, and
                    // deliberately not activated -- see `ShardLifecycle::opened`.
                    Opened::Stale => tracing::info!(
                        stream = %key.stream,
                        shard = key.shard,
                        generation,
                        "shard was reassigned while opening; not activating",
                    ),
                }
            }
            Err(err) => {
                mm::record_open_failure();
                lifecycle.lock().await.open_failed(&key, generation);
                tracing::error!(
                    stream = %key.stream,
                    shard = key.shard,
                    generation,
                    error = %err,
                    "could not open local shard state; this broker is not serving it",
                );
            }
        },
        Action::Release { key, generation } => {
            if let Err(err) = store.release(&key).await {
                // Still closed: ownership has moved regardless, and continuing
                // to serve would be worse than an unflushed tail. The error is
                // the operator's to see.
                tracing::error!(
                    stream = %key.stream,
                    shard = key.shard,
                    generation,
                    error = %err,
                    "could not flush local shard state while releasing it",
                );
            }
            end_readers(lifecycle, store, &key).await;
            lifecycle.lock().await.released(&key, generation);
            tracing::info!(
                stream = %key.stream,
                shard = key.shard,
                generation,
                "shard released",
            );
        }
    }
}

async fn open(
    store: &dyn ShardStore,
    key: &ShardKey,
    generation: u64,
    new_term: bool,
    begins_here: bool,
) -> anyhow::Result<()> {
    if new_term {
        store.begin_term(key).await;
    }
    store.open(key, generation, begins_here).await
}

/// Note that this leadership begins at the log's tail.
///
/// Here and nowhere later: the tail stops being the generation's start the
/// instant this broker serves its first write.
///
/// With `required`, a failure is returned rather than logged: the quorum mark
/// counts from this start once the fleet finalized `generation_start`.
pub async fn record_term_start(
    log: &felix_broker::StreamLog,
    key: &ShardKey,
    generation: u64,
    required: bool,
) -> anyhow::Result<()> {
    let tail = log
        .tail_offset()
        .await
        .map_err(|err| anyhow::anyhow!("read shard tail: {err}"))?;
    if let Err(err) = log.record_generation(generation, tail) {
        if required {
            anyhow::bail!("record where generation {generation} begins: {err}");
        }
        // Not fatal. Replication falls back to comparing from the start of
        // the log, which is slow rather than wrong.
        tracing::warn!(
            stream = %key.stream,
            shard = key.shard,
            generation,
            error = %err,
            "could not record where this leadership begins",
        );
    }
    Ok(())
}

/// Accept `generation` on a log this broker is about to lead, failing if the
/// log already took a newer leader's, or this generation from another node.
async fn accept_led_generation(
    log: &felix_broker::StreamLog,
    generation: u64,
    ballot: Option<&str>,
) -> anyhow::Result<()> {
    match log.accept_generation(generation, ballot).await {
        Ok(felix_storage::disk_log::GenerationCheck::Superseded { accepted }) => {
            anyhow::bail!(
                "this broker already accepted generation {accepted} for the shard, \
                 newer than the {generation} it was assigned to lead"
            );
        }
        Ok(felix_storage::disk_log::GenerationCheck::Promised { leader }) => {
            anyhow::bail!(
                "this broker already accepted generation {generation} for the shard \
                 from {leader}, so it cannot lead it"
            );
        }
        Ok(_) => Ok(()),
        Err(err) => anyhow::bail!("persist the accepted generation: {err}"),
    }
}

/// Write this leader's generation-start record, unless the log already starts
/// the generation with one (an earlier attempt to open got that far), the
/// generation already has records of its own, or the stream is in-memory.
///
/// It goes at the generation's recorded start, before any client write, so
/// the quorum mark can cover records this leader inherited once a majority
/// holds it. See `docs/replication-design.md`. For a cache shard `log` is its
/// cache log or its counter log, each with a mark of its own.
pub async fn write_generation_start(
    broker: &felix_broker::Broker,
    log: &felix_broker::StreamLog,
    key: &ShardKey,
    generation: u64,
) -> anyhow::Result<()> {
    // An in-memory stream's publishes never reach the shard's log: they take
    // no offsets, nothing ships, and no `Quorum` write waits on a mark. So
    // there is no inherited record for a start record to cover, and the
    // broker has no log to append one to.
    if key.kind == ShardKind::Stream {
        let durable = broker
            .resolve_stream_handle(&key.tenant_id, &key.namespace, &key.stream, key.shard)
            .await
            .map_err(|err| anyhow::anyhow!("resolve the stream: {err}"))?
            .is_durable();
        if !durable {
            return Ok(());
        }
    }
    if felix_replication::quorum::generation_start(log, generation)
        .await
        .is_some()
    {
        return Ok(());
    }
    let tail = log
        .tail_offset()
        .await
        .map_err(|err| anyhow::anyhow!("read shard tail: {err}"))?;
    match log.generations().last() {
        Some(epoch) if epoch.generation == generation && epoch.start_offset == tail => {}
        // This broker led at this generation before and reopens it after a
        // restart or a lost lease, which is always the case for a generation
        // that began before the fleet finalized `generation_start`. The mark
        // already counts from the recorded start and every record past it is
        // this generation's own, so there is nothing inherited to cover.
        Some(epoch) if epoch.generation == generation && epoch.start_offset < tail => {
            return Ok(());
        }
        // Anywhere else, the records before it would not be this
        // generation's, and the mark would count them as if they were.
        _ => anyhow::bail!("generation {generation} does not begin at the tail {tail}"),
    }
    match key.kind {
        // Through the broker, so the stream's commit order and its
        // subscribers move past the record's offset too.
        ShardKind::Stream => broker
            .append_generation_start(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                generation,
            )
            .await
            .map(drop),
        // Straight to the log. Nothing writes to the shard before it serves,
        // and a cache's index and a counter shard's sums catch up with
        // records that arrive underneath them, skipping this one.
        ShardKind::Cache => log.append_generation_start(generation).await.map(drop),
    }
    .map_err(|err| anyhow::anyhow!("append the generation-start record: {err}"))?;
    Ok(())
}

/// Write the generation-start record for a leadership that begins without a
/// promotion's fence: a fresh placement, either end of a move, a cancelled
/// move handing the shard back, or a promotion on a broker that does not
/// fence.
///
/// Skipped when the log holds nothing (there is nothing to inherit) and when
/// the generation already has records, which is a reopen after a restart or
/// an earlier attempt that got this far. The mark stays safe either way: it
/// never counts past the generation's start until a record of its own is on
/// a majority, and this record only gets it there without a client write.
async fn start_generation(
    broker: &felix_broker::Broker,
    log: &felix_broker::StreamLog,
    key: &ShardKey,
    generation: u64,
) -> anyhow::Result<()> {
    let tail = log
        .tail_offset()
        .await
        .map_err(|err| anyhow::anyhow!("read shard tail: {err}"))?;
    let begins = log
        .generations()
        .iter()
        .rev()
        .find(|epoch| epoch.generation == generation)
        .map(|epoch| epoch.start_offset);
    if tail == log.base_offset() || begins != Some(tail) {
        return Ok(());
    }
    write_generation_start(broker, log, key, generation).await
}

/// End a shard's readers once the writes already inside its fence are done,
/// so each reader is handed every record this broker committed for the shard.
///
/// Bounded: a write that hangs must not hold up the feed that every other
/// shard's lifecycle runs on. A reader ended early resumes by offset on the
/// new owner, which has the record once the drained report goes out.
async fn end_readers(
    lifecycle: &tokio::sync::Mutex<ShardLifecycle>,
    store: &dyn ShardStore,
    key: &ShardKey,
) {
    let (fence, handoff) = {
        let lifecycle = lifecycle.lock().await;
        (Arc::clone(lifecycle.fence()), lifecycle.headed(key))
    };
    let quiet = tokio::time::timeout(QUIESCE_BOUND, fence.quiesce(key))
        .await
        .is_ok();
    if !quiet {
        tracing::warn!(
            stream = %key.stream,
            shard = key.shard,
            "ending a shard's readers with writes still in flight",
        );
    }
    store.end_readers(key, handoff, quiet).await;
}

/// Bring local state in line with a full assignment set.
///
/// Takes the whole set rather than a delta because a snapshot replaces the
/// picture: a shard that vanished from it must be released as surely as one
/// reassigned away, and only the full set can say which vanished.
pub async fn reconcile(
    lifecycle: &tokio::sync::Mutex<ShardLifecycle>,
    store: &dyn ShardStore,
    assignments: &HashMap<ShardKey, ShardAssignment>,
) {
    let mut actions = Vec::new();
    {
        let mut owned = lifecycle.lock().await;
        for (key, assignment) in assignments {
            actions.push(owned.observe(key, Some(assignment)));
        }
        for key in owned.missing_from(assignments.keys()) {
            actions.push(owned.observe(&key, None));
        }
        owned.forget_incoming_except(assignments);
        // Last, so a shard being released is flushed and its readers ended
        // before its logs close under them.
        actions.extend(owned.relinquished(assignments));
    }
    for action in actions {
        apply(lifecycle, store, action).await;
    }
}

#[cfg(test)]
mod tests;
