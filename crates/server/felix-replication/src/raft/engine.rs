//! The Raft engine as the publish and read paths see it. The broker's
//! `StreamEngine` (`felix-broker-service`, `shards/engine.rs`) sends a
//! shard's writes and reads here when this broker holds a member. See `docs/per-shard-raft.md`, "Writing through openraft"
//! and "Write fence, leases and read bound under Raft".

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use super::cache::{Answer, CacheCommand};
use super::group::Group;
use super::propose::{self, AckAt, Batch, Heard, Unacknowledged};
use super::runtime::GroupRuntime;
use crate::ShardKey;
use crate::engine::{CommitError, Written};
use crate::quorum::Unreadable;

mod cache;

pub use cache::{
    CacheCondition, CacheOp, CacheReplay, CacheReplayed, CacheReply, CacheWatchPoint,
    ProducerRequest,
};

const NO_GROUP: &str = "this broker holds no group for the shard";

/// Writes to the Raft shards a [`GroupRuntime`] holds.
///
/// Cheap to clone; every clone is the same engine.
#[derive(Clone)]
pub struct RaftEngine {
    runtime: GroupRuntime,
    anonymous: std::sync::Arc<cache::Anonymous>,
}

/// Why [`RaftEngine::propose_group`] has no write to show.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{error}")]
pub struct GroupWriteError {
    pub error: CommitError,
    /// False only when the entry provably was not appended: refused before
    /// it reached the proposer, or by openraft on a lapsed lease. Otherwise
    /// it may commit, under this leader or the next.
    pub may_commit: bool,
}

impl RaftEngine {
    pub fn new(runtime: GroupRuntime) -> Self {
        Self {
            runtime,
            anonymous: std::sync::Arc::new(cache::Anonymous::new()),
        }
    }

    /// Write `batch` on this leader. openraft gives each record the next
    /// index, which is its offset, and the batch's records are the offsets
    /// just below the `end` returned. Returns when they are on this
    /// leader's log as far as `batch.ack` asks: written, or fsynced.
    ///
    /// Call it in the shard's commit order. The batch takes its place when
    /// this is called, before the future is polled, and keeps it if the
    /// future is dropped: it is still written, and the outcome is unknown
    /// to that caller.
    ///
    /// Written is not committed. A `Quorum` write waits for the commit
    /// index next, and so does a `Leader` write while
    /// [`Self::admit_local_ack`] refuses.
    ///
    /// An error means this broker cannot say where the batch landed. It may
    /// be in the log and may commit, so the caller must not propose it
    /// again as if nothing was written.
    pub fn propose(
        &self,
        shard: &ShardKey,
        batch: Batch,
    ) -> impl Future<Output = Result<Written, CommitError>> + Send + use<> {
        let proposed = self.propose_reaching(shard, batch, Heard::Written);
        async move { proposed.await.map_err(|unacked| unacked.error) }
    }

    /// [`Self::propose`], and on an error how far the batch is known to be
    /// written. `heard` says what wakes the caller.
    pub(crate) fn propose_reaching(
        &self,
        shard: &ShardKey,
        batch: Batch,
        heard: Heard,
    ) -> impl Future<Output = Result<Written, Unacknowledged>> + Send + use<> {
        propose_to(self.runtime.group(shard).as_ref(), batch, heard)
    }

    /// `shard`'s group, looked up once for a write that uses it at every
    /// step. See [`held`].
    pub(crate) fn looked_up(&self, shard: &ShardKey) -> Option<Arc<Group>> {
        self.runtime.group(shard)
    }

    /// How many times a shard was looked up in this engine's registry.
    /// Not an API: tests count lookups per publish with it.
    #[cfg(any(test, feature = "testing"))]
    pub fn lookups(&self) -> u64 {
        self.runtime.lookups()
    }

    /// How many stream batches `shard`'s group on this broker was handed.
    /// Not an API: tests count proposals with it.
    #[cfg(any(test, feature = "testing"))]
    pub fn proposals(&self, shard: &ShardKey) -> u64 {
        self.runtime.group(shard).map_or(0, |group| {
            group.proposed.load(std::sync::atomic::Ordering::Relaxed)
        })
    }

    /// Whether a write at `generation`, the term [`Self::propose`] wrote
    /// it in, may be acknowledged on this leader's log alone. That is the
    /// leader lease: this broker leads that term, a majority answered a
    /// message it sent less than [`super::RuntimeConfig::lease`] ago (a
    /// sole voter needs no answer), and it has neither given the term up
    /// nor just woken from a suspend. A zero lease refuses every write.
    ///
    /// On an error the write waits for its commit instead
    /// ([`Self::await_ack`]). A refusal never closes the shard.
    pub fn admit_local_ack(&self, shard: &ShardKey, generation: u64) -> Result<(), CommitError> {
        let group = self.runtime.group(shard);
        group
            .ok_or_else(|| propose::lost(NO_GROUP))?
            .admit_local_ack(generation)
    }

    /// [`Self::admit_local_ack`] for `written`, noted as acknowledged once
    /// admitted, so a backup point read after the answer covers it.
    pub(crate) fn admit_local_ack_of(
        &self,
        shard: &ShardKey,
        written: Written,
    ) -> Result<(), CommitError> {
        admit_local_ack_on(self.runtime.group(shard).as_ref(), written)
    }

    /// The acknowledgement of a `Leader` write that [`Self::propose`]
    /// returned as `written`: at once while the lease holds, and otherwise
    /// when the write is committed, as [`Self::await_commit`] says it.
    /// [`Self::publish`] asks the same check itself, since its batch waits
    /// for the commit either way before it is delivered.
    pub fn await_ack(
        &self,
        shard: &ShardKey,
        written: Written,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + use<> {
        let admitted = self.admit_local_ack_of(shard, written);
        let committed = admitted
            .is_err()
            .then(|| self.await_commit(shard, written, timeout));
        async move {
            match committed {
                Some(committed) => committed.await,
                None => Ok(()),
            }
        }
    }

    /// Wait until `written`, as [`Self::propose`] returned it, is
    /// committed: the commit index has passed it and the entry at its last
    /// offset has the term it was written at.
    ///
    /// An error means this broker cannot say so, not that the write is
    /// lost. A leader deposed while it waits gets leadership lost, and so
    /// does a write whose offset another leader's entry took.
    pub fn await_commit(
        &self,
        shard: &ShardKey,
        written: Written,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + use<> {
        let committed = self
            .runtime
            .group(shard)
            .map(|group| group.await_commit(written, timeout))
            .ok_or_else(|| propose::lost(NO_GROUP));
        async move { committed?.await }
    }

    /// [`Self::await_commit`] with no deadline. It still ends when this
    /// broker stops leading the term that wrote `written`, or the shard's
    /// member here stops or closes.
    pub(crate) fn await_commit_unbounded(
        &self,
        shard: &ShardKey,
        written: Written,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + use<> {
        let committed = self
            .runtime
            .group(shard)
            .map(|group| group.await_commit_within(written, None))
            .ok_or_else(|| propose::lost(NO_GROUP));
        async move { committed?.await }
    }

    /// Wait until a `Quorum` write of `written`, as [`Self::propose`]
    /// returned it, may be acknowledged. `ack` is the batch's.
    ///
    /// On a stream that fsyncs on commit ([`AckAt::Flushed`]) this is
    /// [`Self::await_commit`]. On any other stream it also returns once a
    /// majority of the shard's voters has written the record, each on its
    /// own word and before its fsync, when that comes before the commit. It
    /// is lost only if a majority of copies each lose their unsynced tail, by
    /// a power cut or a failed flush: such a copy restarts under a new copy id
    /// and does not vote with its cut log (`docs/per-shard-raft.md`, "A
    /// poisoned log"). Outside that case it survives
    /// process crashes, a change of leader and a membership change, which
    /// the model checks to the end; two terms with two writes and a
    /// membership change across a change of leader have not finished
    /// (`docs/formal/README.md`).
    ///
    /// The first such wait on a shard makes its leader ask the followers
    /// for their written positions from then on.
    pub fn await_quorum_ack(
        &self,
        shard: &ShardKey,
        written: Written,
        ack: AckAt,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + use<> {
        await_quorum_on(self.runtime.group(shard).as_ref(), written, ack, timeout)
    }

    /// One past the highest offset this broker knows committed, for every
    /// reader at every consistency level. It is the commit index plus one
    /// as this member has applied it, so it can trail the leader's and
    /// never passes it. A follower has one too. It never sits inside a
    /// conditional batch: while one is undecided it stays at the batch's
    /// first record.
    pub fn read_bound(&self, shard: &ShardKey) -> Result<u64, Unreadable> {
        let group = self.runtime.group(shard).ok_or(Unreadable::NotServed)?;
        Ok(group.read_bound())
    }

    /// `end`, a read bound of `shard`, less the entries just below it that
    /// hold no record: a leader's blank entry or a membership change. `end`
    /// when the shard is not served here.
    pub fn records_end(&self, shard: &ShardKey, end: u64) -> u64 {
        self.runtime
            .group(shard)
            .map_or(end, |group| group.records_end(end))
    }

    /// Confirm that this broker still leads `term`, by one read-index
    /// round: a majority answers a heartbeat sent after the call, and this
    /// broker has applied the read index. A reader of `shard`'s state takes
    /// [`Self::open_term`] first, then calls this with that term, then takes
    /// its value. A value taken before this returns can miss a write
    /// answered before it was applied. A broker deposed and elected again
    /// in between is refused: it can miss what the leader in between
    /// committed.
    ///
    /// No answer from a majority is a timeout, not a lost leadership, and
    /// does not close the shard.
    pub async fn confirm_read(&self, shard: &ShardKey, term: u64) -> Result<(), CommitError> {
        let Some(group) = self.runtime.group(shard) else {
            return Err(CommitError::LeadershipLost {
                what: "read",
                detail: NO_GROUP,
            });
        };
        group.confirm_read(term).await
    }

    /// A transfer of `shard`'s leadership away from this broker is about
    /// to be asked for. The lease for the term is given up and the shard
    /// closes (`deposed`) before this returns. Both come back only in a
    /// later term. Returns the term that was closed, read from the group's
    /// core. `None` means this broker leads no term of the shard.
    pub async fn transfer_starting(&self, shard: &ShardKey) -> Option<u64> {
        self.runtime.group(shard)?.transfer_starting().await
    }

    /// Hand `shard`'s leadership to `to`: [`Self::transfer_starting`], then
    /// the request to openraft. Every transfer goes through here. Returns
    /// the term given up, or `None` if this broker leads none, and then
    /// nothing was asked. The transfer itself may still not go through.
    pub async fn transfer(&self, shard: &ShardKey, to: super::MemberId) -> Option<u64> {
        self.runtime.group(shard)?.transfer(to).await
    }

    /// Tell this broker's member of `shard` the control plane's target.
    /// See [`GroupRuntime::retarget`].
    pub fn retarget(&self, shard: &ShardKey, target: super::Target, term: u64) -> bool {
        self.runtime.retarget(shard, target, term)
    }

    /// See [`GroupRuntime::untarget`].
    pub fn untarget(&self, shard: &ShardKey, term: u64) -> bool {
        self.runtime.untarget(shard, term)
    }

    /// See [`GroupRuntime::ack_incarnation`].
    pub fn ack_incarnation(&self, incarnation: u64) {
        self.runtime.ack_incarnation(incarnation);
    }

    /// The members a target on `brokers` names, by the copy each broker
    /// holds. Tests only; see [`GroupRuntime::target_members`].
    #[cfg(any(test, feature = "testing"))]
    pub async fn target_members(
        &self,
        shard: &ShardKey,
        brokers: impl IntoIterator<Item = &str>,
        within: Duration,
    ) -> Result<std::collections::BTreeSet<super::MemberId>, super::NotBlank> {
        self.runtime.target_members(shard, brokers, within).await
    }

    /// Where `shard`'s membership stands against its target, as this
    /// broker's member last saw. Only the leader's answer says anything:
    /// every other member is idle.
    pub fn membership(&self, shard: &ShardKey) -> Option<super::MembershipProgress> {
        Some(self.runtime.group(shard)?.membership_progress())
    }

    /// Write one consumer-group write of `shard` while this broker leads
    /// `term`. Returns when it is on this leader's log, not committed.
    /// Every member folds it into its group table once applied
    /// ([`Self::groups`]).
    ///
    /// Refused unless this member leads `term`. An error says whether the
    /// entry may commit ([`GroupWriteError::may_commit`]). Where it may,
    /// the term is closed and leadership handed to another voter before the
    /// error returns, so only a later term's read index can see the entry.
    pub fn propose_group(
        &self,
        shard: &ShardKey,
        term: u64,
        timestamp_micros: u64,
        entry: &felix_broker::GroupEntry,
    ) -> impl Future<Output = Result<Written, GroupWriteError>> + Send + use<> {
        let group = self.runtime.group(shard);
        let sent = match &group {
            Some(group) => group.propose_group(entry.encode(), timestamp_micros, term),
            None => Err(propose::lost(NO_GROUP)),
        };
        async move {
            let acknowledged = sent.map_err(|error| GroupWriteError {
                error,
                may_commit: false,
            })?;
            let unacked = match acknowledged.await {
                Ok(written) => return Ok(written),
                Err(unacked) => unacked,
            };
            // A lease refusal is openraft's, before it appended anything.
            let may_commit = !unacked.refused_on_lease();
            // A no-op unless this member still leads `term`. Stepping down,
            // not only closing, so the shard is served again in a new term
            // instead of staying closed until something else moves it.
            if may_commit && let Some(group) = group {
                group.step_down_in(term).await;
            }
            Err(GroupWriteError {
                error: unacked.error,
                may_commit,
            })
        }
    }

    /// This broker's member of `shard`'s group as it last saw the group.
    pub fn status(&self, shard: &ShardKey) -> Option<super::GroupStatus> {
        self.runtime.status(shard)
    }

    /// Replication to every other member of `shard`'s group, while this
    /// broker's member leads it.
    pub fn followers(&self, shard: &ShardKey) -> Option<Vec<super::FollowerProgress>> {
        self.runtime.followers(shard)
    }

    /// The members that vote in `shard`'s group, by the latest membership
    /// this broker's member holds. `None` without a member.
    pub fn voters(&self, shard: &ShardKey) -> Option<std::collections::BTreeSet<super::MemberId>> {
        self.runtime.voters(shard)
    }

    /// `shard`'s consumer groups as this broker's member has applied them.
    /// `None` without a member, or for a cache shard.
    pub fn groups(
        &self,
        shard: &ShardKey,
    ) -> Option<std::sync::Arc<parking_lot::RwLock<felix_broker::ShardGroups>>> {
        self.runtime.group(shard)?.groups()
    }

    /// Where `shard`'s group runs its tasks, or the current runtime when
    /// this broker holds no member of it.
    pub(crate) fn group_runtime(&self, shard: &ShardKey) -> tokio::runtime::Handle {
        self.runtime
            .group(shard)
            .map_or_else(tokio::runtime::Handle::current, |group| {
                group.runtime().clone()
            })
    }

    /// The broker runtime, for work that must not hold up a group thread.
    pub(crate) fn home_runtime(&self) -> tokio::runtime::Handle {
        self.runtime.home()
    }

    /// `shard`'s commit table on this broker, if it holds a member.
    pub(crate) fn commits(
        &self,
        shard: &ShardKey,
    ) -> Option<tokio::sync::watch::Receiver<super::commit::Commits>> {
        Some(self.runtime.group(shard)?.commits())
    }

    /// Commit a cache write on this leader and return what it answered
    /// when it was first applied. A retry of the same request gets that
    /// answer and is not applied again. `ttl` is in microseconds and only
    /// a put has one; zero is none.
    ///
    /// An error is an unknown outcome: the write may still commit.
    pub(crate) async fn cache_write(
        &self,
        shard: &ShardKey,
        command: CacheCommand,
        ttl: u64,
        timeout: Duration,
    ) -> Result<Answer, CommitError> {
        let group = self.runtime.group(shard);
        let group = group.ok_or_else(|| propose::lost(NO_GROUP))?;
        group.cache_write(command, ttl, timeout).await
    }

    /// `key`'s value and version on this leader, never one that has
    /// expired by its clock or by the log's time.
    pub async fn cache_read(
        &self,
        shard: &ShardKey,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<(Bytes, u64)>, CommitError> {
        let group = self.runtime.group(shard);
        let group = group.ok_or_else(|| propose::lost(NO_GROUP))?;
        group.cache_read(key, timeout).await
    }

    /// Where a cache watch on `shard` joins this member: the offset its live
    /// changes start at, and each value `keep` takes below it. Register the
    /// watch before calling this. `None` when no cache group of `shard` runs
    /// here.
    pub fn cache_watch_point(
        &self,
        shard: &ShardKey,
        keep: &dyn Fn(&str) -> bool,
    ) -> Option<CacheWatchPoint> {
        let group = self.runtime.group(shard)?;
        let (tail, entries) = group.cache_watch_point(keep)?;
        let mut entries: Vec<_> = entries
            .into_iter()
            .map(|(key, held)| felix_storage::CacheSnapshotEntry {
                key,
                value: held.value,
                offset: held.version,
                expires_at_millis: held.expires_at.div_ceil(1000),
            })
            .collect();
        entries.sort_by_key(|entry| entry.offset);
        Some(CacheWatchPoint { tail, entries })
    }

    /// Where a watch resuming from `from` joins `shard` on this member, and
    /// the changes it missed, if this member still keeps them all. Register
    /// the watch before calling this. `None` when no cache group of `shard`
    /// runs here.
    pub fn cache_replay(
        &self,
        shard: &ShardKey,
        from: u64,
        keep: &dyn Fn(&str) -> bool,
    ) -> Option<CacheReplay> {
        let group = self.runtime.group(shard)?;
        let (tail, changes) = group.cache_replay(from, keep)?;
        let changes = changes.map(|changes| {
            changes
                .into_iter()
                .map(|change| CacheReplayed {
                    key: change.key,
                    value: change.value,
                    offset: change.offset,
                    expires_at_millis: change.expires_at.div_ceil(1000),
                })
                .collect()
        });
        Some(CacheReplay { tail, changes })
    }

    /// A cache watch of `shard` on this member ended. The member keeps
    /// recording the shard's changes for a while, so a resume can be
    /// replayed rather than resnapshotted.
    pub fn cache_watch_ended(&self, shard: &ShardKey) {
        if let Some(group) = self.runtime.group(shard) {
            group.cache_watch_ended();
        }
    }

    /// [`Self::cache_watch_point`] for a watch that hands out the current
    /// state, read once this member has applied everything committed when
    /// the watch began. Followers serve watches, and a follower's applied
    /// edge can trail a put its leader already acknowledged. `Ok(None)` when
    /// no cache group of `shard` runs here; an error is retryable.
    pub async fn cache_retained_point(
        &self,
        shard: &ShardKey,
        keep: &(dyn Fn(&str) -> bool + Sync),
    ) -> Result<Option<CacheWatchPoint>, &'static str> {
        if self.runtime.group(shard).is_none() {
            return Ok(None);
        }
        self.runtime.read_barrier(shard).await?;
        Ok(self.cache_watch_point(shard, keep))
    }

    /// End every cache watch `observer` holds on `shard` as lagged at this
    /// member's live edge, for a member that can no longer say its state is
    /// current. A no-op when no cache group of `shard` runs here.
    pub fn end_cache_watches(&self, shard: &ShardKey, observer: &dyn felix_storage::CacheObserver) {
        if let Some(group) = self.runtime.group(shard) {
            group.end_cache_watches(observer);
        }
    }

    /// A counter's sum on this leader.
    pub async fn counter_read(
        &self,
        shard: &ShardKey,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<i64>, CommitError> {
        let group = self.runtime.group(shard);
        let group = group.ok_or_else(|| propose::lost(NO_GROUP))?;
        group.counter_read(key, timeout).await
    }

    /// The term this broker has `shard` open in, for a reader to take
    /// before it reads state and hand to [`Self::confirm_read`].
    pub fn open_term(&self, shard: &ShardKey) -> Option<u64> {
        self.runtime.group(shard)?.open_term()
    }
}

/// `group` as a write looked it up, while the registry still holds it
/// open. A group closed or replaced since refuses here as a lookup finding
/// none would, so a write that started on one member never reaches, or is
/// answered by, another.
fn held(group: Option<&Arc<Group>>) -> Result<&Arc<Group>, CommitError> {
    group
        .filter(|group| group.held())
        .ok_or_else(|| propose::lost(NO_GROUP))
}

/// [`RaftEngine::propose_reaching`] on a group already looked up.
pub(crate) fn propose_to(
    group: Option<&Arc<Group>>,
    batch: Batch,
    heard: Heard,
) -> impl Future<Output = Result<Written, Unacknowledged>> + Send + use<> {
    let proposed = held(group).map(|group| group.propose(batch, heard));
    async move { proposed?.await }
}

/// [`RaftEngine::admit_local_ack_of`] on a group already looked up.
pub(crate) fn admit_local_ack_on(
    group: Option<&Arc<Group>>,
    written: Written,
) -> Result<(), CommitError> {
    held(group)?.admit_local_ack_of(written)
}

/// [`RaftEngine::await_commit`] on a group already looked up, with no
/// deadline. It still ends when the member stops leading the term that
/// wrote `written`, or stops or closes.
pub(crate) fn await_commit_on(
    group: Option<&Arc<Group>>,
    written: Written,
) -> impl Future<Output = Result<(), CommitError>> + Send + use<> {
    let committed = held(group).map(|group| group.await_commit_within(written, None));
    async move { committed?.await }
}

/// [`RaftEngine::await_quorum_ack`] on a group already looked up.
pub(crate) fn await_quorum_on(
    group: Option<&Arc<Group>>,
    written: Written,
    ack: AckAt,
    timeout: Duration,
) -> impl Future<Output = Result<(), CommitError>> + Send + use<> {
    let acked = held(group).map(|group| group.await_quorum(written, ack, timeout));
    async move { acked?.await }
}

/// Hold the group's skip counts for a batch about to be proposed, so its
/// publisher can count the skips below it after the commit.
pub(crate) fn claim_skips_on(group: Option<&Arc<Group>>) -> Option<super::commit::SkipClaim> {
    held(group).ok().map(|group| group.claim_skips())
}
