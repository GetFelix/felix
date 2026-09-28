//! What a publish waits on before a `Quorum` stream acknowledges it.
//!
//! The replication driver owns the cursors; a publish must not. So the driver
//! publishes one number per shard — the highest offset a majority of the
//! replica set holds durably — and a publish waits for that number to reach
//! past its own last offset.
//!
//! A `watch` channel rather than a lock and a condition variable: every waiter
//! wants the same number, latest-wins is exactly right for a monotonic
//! high-water mark, and a waiter that arrives after the mark has already passed
//! sees the current value immediately rather than waiting for the next change.
//!
//! # Generations
//!
//! The mark is reset to zero when a shard's generation changes. An offset that
//! a majority held under the previous leadership says nothing about the current
//! one: the replica set may be different, and the design note is explicit that
//! an acknowledgement from a replica at an older generation does not count
//! toward a newer generation's quorum. Resetting is what enforces that here.
use std::collections::HashMap;

use parking_lot::Mutex;
use tokio::sync::watch;

use super::FollowerCursor;
use crate::ShardKey;

/// The quorum-durable high-water mark for each shard this broker leads.
///
/// Two tables: the shard's own log, and the counter log that rides a cache
/// shard. A counter add on a `Quorum` cache waits on the second, published by
/// the same pass and under the same rule (after a report the control plane
/// stored), since a counter is acknowledged as a write to the shard.
#[derive(Debug, Default)]
pub struct QuorumMarks {
    shards: MarkTable,
    counters: MarkTable,
}

impl QuorumMarks {
    pub fn new() -> Self {
        Self::default()
    }

    /// The marks of the counter logs that ride cache shards.
    pub fn counters(&self) -> &MarkTable {
        &self.counters
    }

    /// See [`MarkTable::publish`].
    pub fn publish(&self, key: &ShardKey, generation: u64, offset: u64) {
        self.shards.publish(key, generation, offset);
    }

    /// Stop tracking a shard this broker no longer leads, in both tables.
    pub fn forget(&self, key: &ShardKey) {
        self.shards.forget(key);
        self.counters.forget(key);
    }

    /// Keep only the shards named, in both tables.
    pub fn retain(&self, live: &[ShardKey]) {
        self.shards.retain(live);
        self.counters.retain(live);
    }

    /// See [`MarkTable::offset`].
    pub fn offset(&self, key: &ShardKey, generation: u64) -> Option<u64> {
        self.shards.offset(key, generation)
    }

    /// See [`MarkTable::wait_for`].
    pub async fn wait_for(
        &self,
        key: &ShardKey,
        generation: u64,
        offset: u64,
        timeout: std::time::Duration,
    ) -> QuorumWait {
        self.shards.wait_for(key, generation, offset, timeout).await
    }

    /// See [`MarkTable::wait_while_leading`].
    pub async fn wait_while_leading(
        &self,
        key: &ShardKey,
        leading: impl Fn() -> Option<u64>,
        offset: u64,
        timeout: std::time::Duration,
    ) -> QuorumWait {
        self.shards
            .wait_while_leading(key, leading, offset, timeout)
            .await
    }
}

/// One high-water mark per shard, for one of the logs a shard has.
#[derive(Debug, Default)]
pub struct MarkTable {
    shards: Mutex<HashMap<ShardKey, ShardMark>>,
    /// Bumped whenever a shard gets a mark at a new generation, so a wait for
    /// one that does not exist yet wakes when it does.
    started: watch::Sender<u64>,
}

#[derive(Debug)]
struct ShardMark {
    generation: u64,
    offset: watch::Sender<u64>,
}

impl MarkTable {
    /// Record how far the majority has got for `key` at `generation`.
    ///
    /// A generation change restarts the mark at zero rather than carrying the
    /// old one forward.
    pub fn publish(&self, key: &ShardKey, generation: u64, offset: u64) {
        let mut shards = self.shards.lock();
        match shards.get_mut(key) {
            Some(mark) if mark.generation == generation => {
                // Monotonic within a generation: the mark is a high-water mark,
                // and a pass that saw less than the last one saw a follower
                // mid-answer rather than a record becoming un-stored.
                mark.offset.send_if_modified(|current| {
                    if offset > *current {
                        *current = offset;
                        true
                    } else {
                        false
                    }
                });
            }
            _ => {
                shards.insert(
                    key.clone(),
                    ShardMark {
                        generation,
                        offset: watch::Sender::new(offset),
                    },
                );
                self.started.send_modify(|n| *n = n.wrapping_add(1));
            }
        }
    }

    /// Stop tracking a shard this broker no longer leads.
    ///
    /// Dropping the sender ends every wait on it, which is what turns a
    /// leadership loss into a failed publish rather than one that hangs until
    /// its timeout.
    pub fn forget(&self, key: &ShardKey) {
        self.shards.lock().remove(key);
    }

    /// Keep only the shards named, forgetting the rest.
    pub fn retain(&self, live: &[ShardKey]) {
        self.shards.lock().retain(|key, _| live.contains(key));
    }

    /// The mark as it stands for `key` at `generation`, or `None` when this
    /// broker is not tracking the shard at that generation.
    pub fn offset(&self, key: &ShardKey, generation: u64) -> Option<u64> {
        let shards = self.shards.lock();
        let mark = shards.get(key)?;
        (mark.generation == generation).then(|| *mark.offset.borrow())
    }

    /// Wait until a majority holds every offset below `offset`.
    ///
    /// `offset` is one past the last record of the batch, so this returns when
    /// the batch itself is on a majority.
    pub async fn wait_for(
        &self,
        key: &ShardKey,
        generation: u64,
        offset: u64,
        timeout: std::time::Duration,
    ) -> QuorumWait {
        let Some(mut watcher) = self.watcher(key, generation) else {
            // Nothing is tracking this shard at this generation: either this
            // broker does not lead it, or the leadership has already moved.
            // Either way it cannot promise a quorum for this write.
            return QuorumWait::NotLeading;
        };

        if *watcher.borrow_and_update() >= offset {
            return QuorumWait::Reached;
        }

        let reached = tokio::time::timeout(timeout, async {
            // `changed` also errors when the sender is dropped, which is how a
            // shard released mid-publish ends the wait instead of running it
            // out.
            while watcher.changed().await.is_ok() {
                if *watcher.borrow_and_update() >= offset {
                    return true;
                }
            }
            false
        })
        .await;

        match reached {
            Ok(true) => QuorumWait::Reached,
            Ok(false) => QuorumWait::NotLeading,
            Err(_) => QuorumWait::TimedOut,
        }
    }

    /// Wait until a majority holds every offset below `offset`, at whatever
    /// generation this broker leads `key` at while it waits.
    ///
    /// `leading` is asked for that generation, and `None` from it ends the
    /// wait as [`QuorumWait::NotLeading`]. Unlike [`MarkTable::wait_for`], a
    /// generation this broker leads but has no mark for yet is waited out:
    /// a new generation has none until its first replication pass reports,
    /// and a move staging a destination starts one under the same leader.
    /// Neither is leadership moving. A write taken at an older generation is
    /// covered by the newer one's mark too, since that counts a majority of
    /// the newer replica set holding the log up to it.
    pub async fn wait_while_leading(
        &self,
        key: &ShardKey,
        leading: impl Fn() -> Option<u64>,
        offset: u64,
        timeout: std::time::Duration,
    ) -> QuorumWait {
        /// How often a wait with no mark to watch looks again at whether this
        /// broker still leads; nothing signals a leadership change here.
        const RECHECK: std::time::Duration = std::time::Duration::from_millis(50);

        let deadline = tokio::time::Instant::now() + timeout;
        let mut started = self.started.subscribe();
        loop {
            // Marked seen before the table is read, so a mark that appears
            // after the read still wakes the wait below.
            started.borrow_and_update();
            let Some(generation) = leading() else {
                return QuorumWait::NotLeading;
            };
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self.watcher(key, generation) {
                Some(mut watcher) => {
                    if *watcher.borrow_and_update() >= offset {
                        return QuorumWait::Reached;
                    }
                    let reached = tokio::time::timeout(left, async {
                        while watcher.changed().await.is_ok() {
                            if *watcher.borrow_and_update() >= offset {
                                return true;
                            }
                        }
                        false
                    })
                    .await;
                    match reached {
                        Ok(true) => return QuorumWait::Reached,
                        // The mark was dropped: forgotten, or replaced at a
                        // newer generation. `leading` says which.
                        Ok(false) => {}
                        Err(_) => return QuorumWait::TimedOut,
                    }
                }
                None => {
                    if left.is_zero() {
                        return QuorumWait::TimedOut;
                    }
                    let _ = tokio::time::timeout(left.min(RECHECK), started.changed()).await;
                }
            }
        }
    }

    /// A receiver for `key` at `generation`, if this broker is tracking it.
    fn watcher(&self, key: &ShardKey, generation: u64) -> Option<watch::Receiver<u64>> {
        let shards = self.shards.lock();
        let mark = shards.get(key)?;
        (mark.generation == generation).then(|| mark.offset.subscribe())
    }
}

/// How a wait for a majority ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuorumWait {
    /// A majority holds the batch. The publish may be acknowledged.
    Reached,
    /// No majority within the budget. The record may still be on disk here and
    /// may yet reach a majority, so this is not "the write failed" — it is
    /// "this broker cannot say that it succeeded", which is the honest answer
    /// and the one the client can act on.
    TimedOut,
    /// This broker is not the leader of that shard at that generation any more.
    NotLeading,
}

/// Why a `Quorum` write could not be acknowledged. Either way the leader has
/// already written it, so the client is told the outcome is unknown rather than
/// that the write failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QuorumError {
    /// No majority within the budget.
    #[error("the {what} is durable here but did not reach a majority within {timeout:?}")]
    TimedOut {
        what: &'static str,
        timeout: std::time::Duration,
    },
    /// This broker stopped leading the shard before a majority held the write.
    #[error("{detail} before the {what} could reach a quorum")]
    LeadershipLost {
        what: &'static str,
        detail: &'static str,
    },
}

/// What the quorum waits need to know about this broker's hold on a shard.
///
/// The broker's ingress router answers it; it lives behind a trait so this
/// crate does not depend on the broker service.
pub trait ShardServing: Send + Sync {
    /// Whether `key` has followers, so a `Quorum` write has to wait for them.
    fn replicated(&self, key: &ShardKey) -> bool;
    /// The generation this broker serves `key` at, or `None` when it does not
    /// serve it here.
    fn generation(&self, key: &ShardKey) -> Option<u64>;
    /// Whether this broker's lease is valid right now, against the clock.
    fn lease_valid(&self) -> bool;
    /// Count a `Quorum` write held back from its acknowledgement because the
    /// lease lapsed while it waited.
    fn record_ack_refusal(&self);
}

/// Hold a `Quorum` publish until a majority of the shard's replica set has it.
///
/// A `Leader` stream returns at once: local durability is the guarantee it
/// offers, and it has already been reached by the time this is called.
///
/// The wait is bounded. A timeout is **not** "the write failed" — the records
/// are on this broker's disk and may yet reach a majority — it is "this broker
/// cannot say that it succeeded", which is the honest answer and the one a
/// client can act on. Reporting success instead would make an acknowledgement
/// mean less than the stream promises.
pub async fn await_quorum<S: ShardServing + ?Sized>(
    handle: &felix_broker::StreamHandle,
    shard: Option<&crate::ShardKey>,
    outcome: &felix_broker::PublishOutcome,
    marks: Option<&crate::quorum::QuorumMarks>,
    ingress: Option<&S>,
    timeout: std::time::Duration,
) -> Result<(), anyhow::Error> {
    if handle.consistency() != felix_broker::ConsistencyLevel::Quorum {
        return Ok(());
    }
    let (Some(shard), Some(marks), Some(ingress)) = (shard, marks, ingress) else {
        // A single-node broker has no replica set. `Quorum` on a stream nobody
        // replicates is satisfied by the leader alone, which has already
        // written the record.
        return Ok(());
    };
    // An ephemeral stream has no offsets, so there is nothing to replicate and
    // nothing to wait for.
    let Some((_, last_offset)) = outcome.offsets else {
        return Ok(());
    };
    if ingress.generation(shard).is_none() {
        return Err(QuorumError::LeadershipLost {
            what: "batch",
            detail: "shard ownership changed",
        }
        .into());
    }

    // Placed with no replica, the leader is the majority and already holds
    // the batch. Nothing ships for such a shard, so no mark will come.
    if !ingress.replicated(shard) {
        return Ok(());
    }

    // `last_offset` is inclusive, and the mark is one past what is held.
    match marks
        .wait_while_leading(
            shard,
            || ingress.generation(shard),
            last_offset + 1,
            timeout,
        )
        .await
    {
        crate::quorum::QuorumWait::Reached => release(ingress, "batch"),
        crate::quorum::QuorumWait::TimedOut => {
            crate::metrics::record_quorum(crate::metrics::QUORUM_TIMED_OUT);
            Err(QuorumError::TimedOut {
                what: "batch",
                timeout,
            }
            .into())
        }
        crate::quorum::QuorumWait::NotLeading => {
            crate::metrics::record_quorum(crate::metrics::QUORUM_NOT_LEADING);
            Err(QuorumError::LeadershipLost {
                what: "batch",
                detail: "shard leadership moved",
            }
            .into())
        }
    }
}

/// Hold a write to a `Quorum` cache until a majority of its shard's replica set
/// has it. The cache-side mirror of [`await_quorum`].
///
/// A cache put does not report the offset it took, so this waits for the
/// shard's tail as read after the write. That is at or past the write, so a
/// mark at the tail covers it; a concurrent later write can only make the wait
/// longer, never make it end before this write is on a majority.
///
/// A read of a `Quorum` cache waits the same way, after it has read its
/// value: every write that value reflects is below the tail read afterwards,
/// so the answer is never one a failover could take back. `what` names which.
pub async fn await_cache_quorum<S: ShardServing + ?Sized>(
    broker: &felix_broker::Broker,
    shard: &crate::ShardKey,
    marks: Option<&QuorumMarks>,
    ingress: Option<&S>,
    timeout: std::time::Duration,
    what: &'static str,
) -> Result<(), anyhow::Error> {
    let consistency = broker
        .cache_consistency(&shard.tenant_id, &shard.namespace, &shard.stream)
        .await;
    if consistency != Some(felix_broker::ConsistencyLevel::Quorum) {
        return Ok(());
    }
    // As for a stream: a broker with no replica set is satisfied by itself.
    let (Some(marks), Some(ingress)) = (marks, ingress) else {
        return Ok(());
    };
    // A cache with no log keeps nothing to replicate.
    let Some(log) = broker
        .shard_log(
            felix_broker::LogKind::Cache,
            &shard.tenant_id,
            &shard.namespace,
            &shard.stream,
            shard.shard,
        )
        .await
    else {
        return Ok(());
    };
    let tail = log.tail_offset().await?;
    if ingress.generation(shard).is_none() {
        return Err(QuorumError::LeadershipLost {
            what,
            detail: "shard ownership changed",
        }
        .into());
    }
    if !ingress.replicated(shard) {
        return Ok(());
    }
    match marks
        .wait_while_leading(shard, || ingress.generation(shard), tail, timeout)
        .await
    {
        QuorumWait::Reached => release(ingress, what),
        QuorumWait::TimedOut => {
            crate::metrics::record_quorum(crate::metrics::QUORUM_TIMED_OUT);
            Err(QuorumError::TimedOut { what, timeout }.into())
        }
        QuorumWait::NotLeading => {
            crate::metrics::record_quorum(crate::metrics::QUORUM_NOT_LEADING);
            Err(QuorumError::LeadershipLost {
                what,
                detail: "shard leadership moved",
            }
            .into())
        }
    }
}

/// How far readers may see into a shard: the committed high-water mark.
///
/// A `Quorum` shard's commit point is the quorum mark, not the local tail. A
/// record past the mark may still be lost at failover and its offset reused
/// for a different record, so a reader that saw it would hold a position the
/// new leader's log contradicts. `None` means no bound: a `Leader` stream,
/// whose commit point is local durability, a single-node broker, or a shard
/// placed without replicas, where the leader alone is the majority -- the
/// same cases [`await_quorum`] acknowledges without waiting.
///
/// Zero while this broker has no mark for the shard at its current
/// generation: nothing is known to be on a majority yet.
pub fn read_bound<S: ShardServing + ?Sized>(
    consistency: Option<felix_broker::ConsistencyLevel>,
    shard: &crate::ShardKey,
    marks: Option<&QuorumMarks>,
    ingress: Option<&S>,
) -> Option<u64> {
    if consistency != Some(felix_broker::ConsistencyLevel::Quorum) {
        return None;
    }
    let (Some(marks), Some(ingress)) = (marks, ingress) else {
        return None;
    };
    if !ingress.replicated(shard) {
        return None;
    }
    let Some(generation) = ingress.generation(shard) else {
        return Some(0);
    };
    Some(marks.offset(shard, generation).unwrap_or(0))
}

/// Hold a counter add on a `Quorum` cache until a majority of the shard's
/// replica set holds the counter log up to `end`, or answer a counter read
/// only once it does (`end` `None`: the counter log's tail as read now, after
/// the value was).
///
/// Counters ride the cache shard's replica set in their own log, with their
/// own mark in [`QuorumMarks::counters`], published only after a report the
/// control plane stored names no follower missing them as caught up.
#[allow(clippy::too_many_arguments)]
pub async fn await_counter_quorum<S: ShardServing + ?Sized>(
    broker: &felix_broker::Broker,
    shard: &crate::ShardKey,
    marks: Option<&QuorumMarks>,
    ingress: Option<&S>,
    timeout: std::time::Duration,
    end: Option<u64>,
    what: &'static str,
) -> Result<(), anyhow::Error> {
    let consistency = broker
        .cache_consistency(&shard.tenant_id, &shard.namespace, &shard.stream)
        .await;
    if consistency != Some(felix_broker::ConsistencyLevel::Quorum) {
        return Ok(());
    }
    let (Some(marks), Some(ingress)) = (marks, ingress) else {
        return Ok(());
    };
    let end = match end {
        Some(end) => end,
        None => {
            let Some(log) = broker
                .shard_log(
                    felix_broker::LogKind::Counters,
                    &shard.tenant_id,
                    &shard.namespace,
                    &shard.stream,
                    shard.shard,
                )
                .await
            else {
                return Ok(());
            };
            log.tail_offset().await?
        }
    };
    if ingress.generation(shard).is_none() {
        return Err(QuorumError::LeadershipLost {
            what,
            detail: "shard ownership changed",
        }
        .into());
    }
    if !ingress.replicated(shard) {
        return Ok(());
    }
    // The counter log ships on a replication pass; start one now rather than
    // wait out the tick.
    broker.appended().notify_one();
    match marks
        .counters()
        .wait_while_leading(shard, || ingress.generation(shard), end, timeout)
        .await
    {
        QuorumWait::Reached => release(ingress, what),
        QuorumWait::TimedOut => {
            crate::metrics::record_quorum(crate::metrics::QUORUM_TIMED_OUT);
            Err(QuorumError::TimedOut { what, timeout }.into())
        }
        QuorumWait::NotLeading => {
            crate::metrics::record_quorum(crate::metrics::QUORUM_NOT_LEADING);
            Err(QuorumError::LeadershipLost {
                what,
                detail: "shard leadership moved",
            }
            .into())
        }
    }
}

/// The committed mark for readers of one shard, as the broker core asks for
/// it (see `felix_broker::ReadBound`).
///
/// [`read_bound`] with two more answers. `Settling` while this broker leads
/// the shard but has no mark for its generation: zero would tell a new reader
/// to start at the beginning of the log. `Refused` once it may not serve the
/// shard at all -- its lease lapsed, or the shard is led elsewhere -- so a
/// reader waiting on the mark stops and resumes where the shard is served.
/// Only asked for `Quorum` shards.
pub fn committed_bound<S: ShardServing + ?Sized>(
    key: &crate::ShardKey,
    marks: &QuorumMarks,
    ingress: &S,
) -> felix_broker::ReadBound {
    use felix_broker::ReadBound;
    if !ingress.replicated(key) {
        return ReadBound::Unbounded;
    }
    let Some(generation) = ingress.generation(key) else {
        return ReadBound::Refused;
    };
    if !ingress.lease_valid() {
        return ReadBound::Refused;
    }
    match marks.offset(key, generation) {
        Some(mark) => ReadBound::Committed(mark),
        None => ReadBound::Settling,
    }
}

/// [`committed_bound`] for the broker core's stream readers.
pub struct CommittedReads {
    marks: std::sync::Arc<QuorumMarks>,
    ingress: std::sync::Arc<dyn ShardServing>,
}

impl CommittedReads {
    pub fn new(
        marks: std::sync::Arc<QuorumMarks>,
        ingress: std::sync::Arc<dyn ShardServing>,
    ) -> Self {
        Self { marks, ingress }
    }
}

impl std::fmt::Debug for CommittedReads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommittedReads").finish_non_exhaustive()
    }
}

impl felix_broker::ReadBounds for CommittedReads {
    fn stream_bound(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> felix_broker::ReadBound {
        let key = crate::ShardKey {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
            kind: crate::ShardKind::Stream,
        };
        committed_bound(&key, &self.marks, &*self.ingress)
    }
}

/// Wait until every offset below `end` of a `Quorum` shard is committed, for
/// a reader about to hand out something that `end` bounds. Returns at once
/// when the shard is unbounded.
///
/// Errors when the shard stops being served here, and after `timeout` with
/// the mark short of `end`: either way the reader has nothing it can stand
/// behind, and says so rather than answering from records a failover may
/// replace.
pub async fn await_readable<S: ShardServing + ?Sized>(
    consistency: Option<felix_broker::ConsistencyLevel>,
    key: &crate::ShardKey,
    marks: Option<&QuorumMarks>,
    ingress: Option<&S>,
    end: u64,
    timeout: std::time::Duration,
) -> Result<(), QuorumError> {
    if consistency != Some(felix_broker::ConsistencyLevel::Quorum) {
        return Ok(());
    }
    let (Some(marks), Some(ingress)) = (marks, ingress) else {
        return Ok(());
    };
    use felix_broker::ReadBound;
    /// How long to wait before looking again for a mark this generation has
    /// not published yet; there is no watch to wake on until it has.
    const SETTLING_RECHECK: std::time::Duration = std::time::Duration::from_millis(50);

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        // Read again after every wait: the lease or the leadership can go
        // while the mark comes.
        let settling = match committed_bound(key, marks, ingress) {
            ReadBound::Unbounded => return Ok(()),
            ReadBound::Committed(mark) if mark >= end => return Ok(()),
            ReadBound::Refused => {
                return Err(QuorumError::LeadershipLost {
                    what: "read",
                    detail: "this broker is not serving the shard",
                });
            }
            ReadBound::Committed(_) => false,
            ReadBound::Settling => true,
        };
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            return Err(QuorumError::TimedOut {
                what: "read",
                timeout,
            });
        }
        match ingress.generation(key) {
            Some(generation) if !settling => {
                let _ = marks.wait_for(key, generation, end, left).await;
            }
            _ => tokio::time::sleep(left.min(SETTLING_RECHECK)).await,
        }
    }
}

/// The lease re-check at ack release.
///
/// The mark says a majority held the write when the control plane stored the
/// report, but this broker may have lost its lease while it waited, and an
/// acknowledgement from a broker that may no longer lead is the one the model
/// (`AckQuorum` requires `LeaseValid`) forbids. The write is on a majority or
/// may yet be, so the answer is "unknown", never "failed".
fn release<S: ShardServing + ?Sized>(ingress: &S, what: &'static str) -> Result<(), anyhow::Error> {
    if ingress.lease_valid() {
        return Ok(());
    }
    ingress.record_ack_refusal();
    Err(QuorumError::LeadershipLost {
        what,
        detail: "the lease lapsed",
    }
    .into())
}

/// How many copies must hold a record for a majority, the leader included.
///
/// `replicas` is the follower count, so the replica set is one larger. A set of
/// three needs two, a set of five needs three — and a set of one needs one,
/// which is the leader alone and is why `replication_factor: 1` costs nothing.
pub fn majority_of(replicas: usize) -> usize {
    replicas.div_ceil(2) + 1
}

/// The highest offset a majority of the replica set holds durably.
///
/// The leader is counted as holding everything up to `leader_tail`: it wrote the
/// records, and a record it has not written is not a candidate for a quorum in
/// the first place.
///
/// **A halted follower counts for nothing.** It is not slow, it has stopped —
/// its log has diverged, or this broker has been superseded — and letting a
/// stale position count toward a majority is how an acknowledgement comes to
/// mean less than it says.
///
/// Cursors belong to one generation. The caller passes the set for the
/// generation it is publishing at, which is what stops an older generation's
/// acknowledgements satisfying a newer generation's quorum.
pub fn quorum_offset(leader_tail: u64, followers: &[FollowerCursor]) -> u64 {
    quorum_offset_without(leader_tail, followers, None)
}

/// [`quorum_offset`] over the replica set without `learner`.
///
/// The learner is a move's destination that this leader saw added to the
/// replica set, still copying the log. It is not counted toward the majority
/// or toward its size: the set the stream asked for is the one without it, so
/// a majority of that set is the promise, and counting a node that is still
/// copying would make every `Quorum` publish wait for the copy. Its copy is
/// complete before it can lead, because the cut-over waits for it to be level.
pub fn quorum_offset_without(
    leader_tail: u64,
    followers: &[FollowerCursor],
    learner: Option<&str>,
) -> u64 {
    let voters = || {
        followers
            .iter()
            .filter(move |follower| Some(follower.node_id.as_str()) != learner)
    };
    let needed = majority_of(voters().count());
    // The leader is one of them, and it holds the most.
    let mut held: Vec<u64> = std::iter::once(leader_tail)
        .chain(
            voters()
                .filter(|follower| follower.halted.is_none())
                .map(|follower| follower.next_offset.min(leader_tail)),
        )
        .collect();
    // Descending, so the `needed`-th is the highest offset that many hold.
    held.sort_unstable_by(|a, b| b.cmp(a));
    held.get(needed - 1).copied().unwrap_or(0)
}

#[cfg(test)]
mod tests;
