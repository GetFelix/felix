//! The state machine of a shard group. A stream shard's state is its log,
//! so this keeps what openraft asks every state machine for (the last
//! applied entry and the membership in force) and the shard's consumer
//! groups, folded from its group entries ([`ShardGroups`]). A cache shard's
//! group folds its entries into a [`CacheState`] instead.
//!
//! All of it is kept in memory. A member starts from the snapshot in the
//! group's [`SnapshotStore`] and openraft applies the log above it again.
//! A stream shard's snapshot holds no records, only the cut batches its log
//! refuses by name and the group table as of its last entry
//! ([`StreamSnapshot`]); a cache shard's is its projection as of the
//! snapshot's last entry.

use std::io;
use std::num::NonZeroUsize;
use std::sync::{Arc, LazyLock};

use bytes::Bytes;

use felix_broker::{GroupEntry, GroupTable, ShardGroups};
use felix_storage::DiskLog;
use felix_storage::disk_log::CarriedFloor;
use felix_storage::log::FsyncMode;
use felix_storage::raft_log::{LogPoint, LogReader, SnapshotId, SnapshotStore, StoredSnapshot};
use felix_storage::waiters::{Queue, Waiters};
use futures::{Stream, StreamExt};
use openraft::storage::{EntryResponder, RaftLogReader, RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::{EntryPayload, OptionalSend};
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, watch};

use super::cache::{CacheCommand, CacheState, Cut};
use super::codec::{ShardCodec, StoredMembership, invalid};
use super::commit::Commits;
use super::stream_snapshot::{PurgeFloors, StreamSnapshot};
use super::types::{Applied, ShardTypes};
use crate::ShardKind;

/// A cache group's projection, shared by its state machine, which writes
/// it, and the member's read and answer paths.
pub(crate) type Cache = Arc<parking_lot::RwLock<CacheState>>;

/// A stream shard's consumer groups, shared by its state machine, which
/// folds them, and the member's group paths.
pub(crate) type Groups = Arc<parking_lot::RwLock<ShardGroups>>;

/// The entry the next stream snapshot is to be written at, set by the purge
/// driver (`Group::purge_below_floor`) before it asks openraft for one.
pub(crate) type PurgeAt = Arc<parking_lot::Mutex<Option<u64>>>;

/// Entries read from the log per chunk while folding up to a purge point.
const FOLD_READ: u64 = 1024;

#[derive(Clone, Default)]
struct State {
    last_applied: Option<LogIdOf<ShardTypes>>,
    membership: StoredMembershipOf<ShardTypes>,
}

pub(crate) struct Machine {
    snapshots: SnapshotStore,
    state: State,
    /// What this member has applied, and so knows committed.
    commits: Commits,
    /// `commits` as the read bound and the commit waits see it, moved once
    /// per run of applied entries.
    published: watch::Sender<Commits>,
    /// Woken by the applied index, beside `published`.
    applied: Queue<u64>,
    /// `None` in a stream shard's group.
    cache: Option<Cache>,
    /// `None` in a cache shard's group.
    groups: Option<Groups>,
    /// A stream shard's log, `None` in a cache shard's group.
    log: Option<DiskLog>,
    /// The Raft log, of either kind of shard.
    raft_log: Option<DiskLog>,
    /// Where a purge asked for the next stream snapshot, and the log it
    /// folds from to get there.
    purge_at: PurgeAt,
    reader: Option<LogReader<ShardTypes, ShardCodec>>,
    /// Held by a test to keep this member from applying.
    #[cfg(test)]
    pub(crate) hold: Arc<tokio::sync::Mutex<()>>,
    /// Held by a test to keep apply from folding.
    #[cfg(test)]
    pub(crate) fold_hold: Arc<tokio::sync::Mutex<()>>,
}

impl Machine {
    /// The state machine over the snapshot `snapshots` holds, if any, and
    /// what it has applied as that changes. `log` is the group's, which a
    /// stream shard's snapshots read and install into.
    pub(crate) async fn open(
        snapshots: SnapshotStore,
        kind: ShardKind,
        log: Option<&DiskLog>,
    ) -> io::Result<Opened> {
        let stored = snapshots.current().await?;
        let state = match &stored {
            Some(stored) => StoredMeta::state(&stored.meta)?,
            None => State::default(),
        };
        let data = stored.map(|stored| stored.data).unwrap_or_default();
        let (cache, groups) = match kind {
            // The table as of the snapshot's last entry; openraft applies
            // the group entries above it again.
            ShardKind::Stream => {
                let mut groups = ShardGroups::default();
                groups.reset(StreamSnapshot::decode(&data)?.groups);
                (None, Some(Arc::new(parking_lot::RwLock::new(groups))))
            }
            ShardKind::Cache => {
                let restored = restore(data, state.last_applied.is_some()).await?;
                (Some(Arc::new(parking_lot::RwLock::new(restored))), None)
            }
        };
        let commits = Commits::resumed(state.last_applied.as_ref().map(point));
        let (published, watched) = watch::channel(commits.clone());
        let applied = Queue::new();
        let waiters = applied.waiters();
        let machine = Self {
            snapshots,
            state,
            commits,
            published,
            applied,
            cache: cache.clone(),
            groups: groups.clone(),
            log: log.filter(|_| matches!(kind, ShardKind::Stream)).cloned(),
            raft_log: log.cloned(),
            purge_at: PurgeAt::default(),
            reader: None,
            #[cfg(test)]
            hold: Arc::default(),
            #[cfg(test)]
            fold_hold: Arc::default(),
        };
        Ok(Opened {
            machine,
            commits: watched,
            applied: waiters,
            cache,
            groups,
        })
    }

    fn publish(&self) {
        let moved = self.published.send_if_modified(|published| {
            let moved = *published != self.commits;
            if moved {
                published.clone_from(&self.commits);
            }
            moved
        });
        if moved {
            self.applied
                .advance(self.commits.applied().map(|applied| applied.index));
        }
    }
}

/// A state machine as [`Machine::open`] opened it, and what the member's
/// other paths share with it.
pub(crate) struct Opened {
    pub(crate) machine: Machine,
    pub(crate) commits: watch::Receiver<Commits>,
    pub(crate) applied: Waiters<u64>,
    pub(crate) cache: Option<Cache>,
    pub(crate) groups: Option<Groups>,
}

/// A snapshot's data as openraft carries it.
pub(crate) struct SnapshotData {
    pub(crate) bytes: Bytes,
    /// The state `bytes` hold, when a receiver has already parsed them.
    /// Install takes it instead of parsing a second time.
    pub(crate) restored: Option<CacheState>,
}

impl SnapshotData {
    fn bytes(bytes: Bytes) -> Self {
        Self {
            bytes,
            restored: None,
        }
    }
}

/// Parse a cache snapshot on the blocking pool: near the size cap it is
/// seconds of work that would otherwise hold a runtime thread.
pub(crate) async fn restore(data: Bytes, applied: bool) -> io::Result<CacheState> {
    cpu(move || CacheState::restore(&data, applied)).await?
}

/// How many snapshot encodes and parses run at once, across every group.
#[cfg(test)]
pub(crate) fn cpu_jobs() -> usize {
    CPU_JOBS.1
}

static CPU_JOBS: LazyLock<(Arc<Semaphore>, usize)> = LazyLock::new(|| {
    let cores = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    let jobs = (cores / 2).max(1);
    (Arc::new(Semaphore::new(jobs)), jobs)
});

/// Run CPU-bound snapshot work on the blocking pool, at most
/// [`cpu_jobs`] at a time. Hundreds of groups restarting or snapshotting
/// together would otherwise fill the pool that disk flushes wait on.
pub(crate) async fn cpu<F, R>(work: F) -> io::Result<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    // Under the simulator the work runs on the runtime, and a bound taken
    // from this machine's core count would change a seed's schedule.
    let permit = if felix_common::blocking::on_runtime() {
        None
    } else {
        let jobs = Arc::clone(&CPU_JOBS.0);
        Some(jobs.acquire_owned().await.map_err(io::Error::other)?)
    };
    // The permit goes with the work, so a caller that stops waiting does
    // not free a slot the work still uses.
    felix_common::blocking::spawn(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(io::Error::other)
}

fn point(id: &LogIdOf<ShardTypes>) -> LogPoint {
    let (term, index) = term_index(id);
    LogPoint { term, index }
}

/// A snapshot's metadata as stored, in JSON.
#[derive(Serialize, Deserialize)]
struct StoredMeta {
    /// `(term, index)` of the last applied entry.
    last_applied: Option<(u64, u64)>,
    /// `(term, index)` of the entry that holds the membership.
    membership_at: Option<(u64, u64)>,
    membership: StoredMembership,
}

fn log_id((term, index): (u64, u64)) -> LogIdOf<ShardTypes> {
    LogIdOf::<ShardTypes>::new(term.into(), index)
}

fn term_index(id: &LogIdOf<ShardTypes>) -> (u64, u64) {
    (**id.committed_leader_id(), id.index)
}

impl StoredMeta {
    fn of(meta: &SnapshotMetaOf<ShardTypes>) -> Self {
        Self {
            last_applied: meta.last_log_id.as_ref().map(term_index),
            membership_at: meta.last_membership.log_id().as_ref().map(term_index),
            membership: StoredMembership::of(meta.last_membership.membership()),
        }
    }

    fn state(bytes: &[u8]) -> io::Result<State> {
        let meta: Self = serde_json::from_slice(bytes)
            .map_err(|err| invalid(format!("a snapshot's metadata does not parse: {err}")))?;
        Ok(State {
            last_applied: meta.last_applied.map(log_id),
            membership: StoredMembershipOf::<ShardTypes>::new(
                meta.membership_at.map(log_id),
                meta.membership.membership()?,
            ),
        })
    }
}

async fn store(
    snapshots: &SnapshotStore,
    meta: &SnapshotMetaOf<ShardTypes>,
    data: Bytes,
) -> io::Result<()> {
    let stored = StoredSnapshot {
        last: meta.last_log_id.as_ref().map(|id| {
            let (term, index) = term_index(id);
            SnapshotId { term, index }
        }),
        meta: serde_json::to_vec(&StoredMeta::of(meta))
            .map_err(io::Error::other)?
            .into(),
        data,
    };
    // False means a later snapshot is already stored, which is fine.
    snapshots.install(stored).await.map(|_| ())
}

/// The snapshot a voter of a switch from the hybrid starts from: everything
/// up to the base's entry applied, and `voters` as the membership, held at
/// that entry.
pub(crate) fn base_snapshot(
    base: LogPoint,
    voters: &std::collections::BTreeSet<super::types::MemberId>,
) -> io::Result<StoredSnapshot> {
    let at = Some((base.term, base.index));
    let meta = StoredMeta {
        last_applied: at,
        membership_at: at,
        membership: StoredMembership {
            version: super::codec::MEMBERSHIP_VERSION,
            voters: vec![voters.clone()],
            learners: std::collections::BTreeSet::new(),
            target: Some(super::membership::TargetVersion::default()),
        },
    };
    Ok(StoredSnapshot {
        last: Some(SnapshotId {
            term: base.term,
            index: base.index,
        }),
        meta: serde_json::to_vec(&meta).map_err(io::Error::other)?.into(),
        data: bytes::Bytes::new(),
    })
}

fn meta(state: &State) -> SnapshotMetaOf<ShardTypes> {
    SnapshotMetaOf::<ShardTypes> {
        last_log_id: state.last_applied,
        last_membership: state.membership.clone(),
    }
}

pub(crate) struct Builder {
    snapshots: SnapshotStore,
    state: State,
    /// The projection as of `state.last_applied`. Let go, the apply task
    /// folds the writes made since back into the live maps.
    cut: Option<Cut>,
    /// A stream shard's group table as of `state.last_applied`.
    groups: Option<GroupTable>,
    /// A stream shard's log.
    log: Option<DiskLog>,
    /// The Raft log, synced before a snapshot is stored.
    raft_log: Option<DiskLog>,
    /// Set for a purge snapshot: the entry it is written at, below the
    /// applied one, and the log it folds the state up to there from.
    purge: Option<(u64, LogReader<ShardTypes, ShardCodec>)>,
}

/// Overlay writes folded per hold of the cache lock, so apply and reads
/// wait out at most one chunk.
const FOLD_CHUNK: usize = 256;

/// Overlay writes folded per entry applied. A write adds at most four
/// (value, expiry added and removed, producer answer), so the overlay
/// shrinks under any write rate.
const FOLD_PER_ENTRY: usize = 8;

impl RaftSnapshotBuilder<ShardTypes> for Builder {
    type SnapshotData = SnapshotData;

    async fn build_snapshot(&mut self) -> io::Result<SnapshotOf<ShardTypes, SnapshotData>> {
        if let (Some((at, reader)), Some(log)) = (self.purge.take(), self.log.clone()) {
            return self.build_purge(at, reader, &log).await;
        }
        let meta = meta(&self.state);
        // A panic or JoinError in the encode fails the build, which stops
        // the member. A cache member whose parse fails in `Group::dispatch`
        // refuses the snapshot and keeps running. A stream member parses
        // in `install_snapshot` and stops: without the refusals it would
        // write a cut batch again.
        let data = match (self.cut.take(), &self.log) {
            (Some(cut), _) => cpu(move || cut.snapshot()).await?,
            (None, Some(log)) => {
                let groups = self.groups.take().unwrap_or_default();
                stream_data(log, &self.state, groups).await?
            }
            (None, None) => Bytes::new(),
        };
        // A log that counts written entries can have applied one it has not
        // synced. The snapshot is synced, so the log below it is synced
        // first, and a purge after it never drops an unsynced entry.
        if let Some(log) = &self.raft_log
            && log.config().fsync_mode != FsyncMode::OnCommit
        {
            log.sync().await.map_err(io::Error::other)?;
        }
        store(&self.snapshots, &meta, data.clone()).await?;
        Ok(SnapshotOf::<ShardTypes, SnapshotData> {
            meta,
            snapshot: SnapshotData::bytes(data),
        })
    }
}

impl Builder {
    /// A purge snapshot at entry `at`, at or below the applied one and below
    /// the floor: the group table and membership folded from the stored
    /// snapshot over the log up to `at`, the floor as of `at` for the
    /// tables, and the highest applied floor as the carried one. Written at
    /// `at` rather than at the applied entry so a member behind the purge can
    /// install it: one at or above the floor would skip records still
    /// served. When there is nothing to write (the stored snapshot is at or
    /// past `at`, or `at` is not below the floor) the stored one is returned.
    async fn build_purge(
        &mut self,
        at: u64,
        mut reader: LogReader<ShardTypes, ShardCodec>,
        log: &DiskLog,
    ) -> io::Result<SnapshotOf<ShardTypes, SnapshotData>> {
        let stored = self.snapshots.current().await?;
        let (mut state, data) = match &stored {
            Some(stored) => (StoredMeta::state(&stored.meta)?, stored.data.clone()),
            None => (State::default(), Bytes::new()),
        };
        let carried = log.head_trim();
        let from = state.last_applied.map_or(0, |id| id.index + 1);
        let below_floor = carried.as_ref().is_some_and(|trim| at < trim.floor);
        if from > at || !below_floor {
            return Ok(SnapshotOf::<ShardTypes, SnapshotData> {
                meta: meta(&state),
                snapshot: SnapshotData::bytes(data),
            });
        }
        let previous = StreamSnapshot::decode(&data)?;
        let mut groups = ShardGroups::default();
        groups.reset(previous.groups);
        let mut next = from;
        while next <= at {
            let end = at.min(next.saturating_add(FOLD_READ - 1));
            let entries = reader.try_get_log_entries(next..end + 1).await?;
            if entries.len() as u64 != end + 1 - next {
                return Err(io::Error::other(format!(
                    "the log has no entries {next}..={end} to fold a purge snapshot from"
                )));
            }
            for entry in entries {
                match &entry.payload {
                    EntryPayload::Normal(command) if command.group => {
                        let decoded = GroupEntry::decode(&command.payload).map_err(|err| {
                            invalid(format!("a group entry does not decode: {err}"))
                        })?;
                        let (term, index) = term_index(&entry.log_id);
                        groups.apply(term, index, &decoded);
                    }
                    EntryPayload::Membership(membership) => {
                        state.membership = StoredMembershipOf::<ShardTypes>::new(
                            Some(entry.log_id),
                            membership.clone(),
                        );
                    }
                    _ => {}
                }
                state.last_applied = Some(entry.log_id);
            }
            next = end + 1;
        }
        let cuts = log
            .forgotten_cuts_from(at + 1)
            .await
            .map_err(io::Error::other)?;
        let tables = log
            .floor_as_of(at)
            .max(previous.purge.map_or(0, |purge| purge.tables));
        let trim = carried.expect("checked below the floor");
        let data = StreamSnapshot {
            cuts,
            groups: groups.table().clone(),
            purge: Some(PurgeFloors {
                tables,
                carried: CarriedFloor {
                    offset: trim.offset,
                    floor: trim.floor,
                    headers: trim.headers,
                },
            }),
        }
        .encode();
        if let Some(raft_log) = &self.raft_log
            && raft_log.config().fsync_mode != FsyncMode::OnCommit
        {
            raft_log.sync().await.map_err(io::Error::other)?;
        }
        let meta = meta(&state);
        store(&self.snapshots, &meta, data.clone()).await?;
        Ok(SnapshotOf::<ShardTypes, SnapshotData> {
            meta,
            snapshot: SnapshotData::bytes(data),
        })
    }
}

/// A stream shard's snapshot data: the cut batches a member whose log starts
/// right after the snapshot's last entry has to refuse by name, and
/// `groups`, the group table folded up to that entry. A member that
/// installs the snapshot holds no record below it, so the batches the log
/// forgot, or would forget once purged there, and the group writes below
/// it are known to it only from here. Built from the committed records up
/// to the snapshot's last entry and what earlier purges and installs
/// carried, never from refusals only this member has, so every member's
/// snapshot at one entry carries the same. Empty when there is nothing.
///
/// A member that keeps a carried floor (it installed a purge snapshot)
/// passes on the floor it serves, the higher of its own and any it
/// installed, never only the bytes it was sent: a member that installs from
/// it skips the trim records too.
async fn stream_data(log: &DiskLog, state: &State, groups: GroupTable) -> io::Result<Bytes> {
    let base = state.last_applied.map_or(0, |id| id.index + 1);
    let cuts = log
        .forgotten_cuts_from(base)
        .await
        .map_err(io::Error::other)?;
    // Taken at the applied entry, so the tables' floor is the floor now.
    let purge = log
        .carried_floor()
        .and(log.head_trim())
        .map(|trim| PurgeFloors {
            tables: trim.floor,
            carried: CarriedFloor {
                offset: trim.offset,
                floor: trim.floor,
                headers: trim.headers,
            },
        });
    Ok(StreamSnapshot {
        cuts,
        groups,
        purge,
    }
    .encode())
}

/// Before a stream snapshot replaces this member's state: keep as its floor
/// the higher of the one it serves and the one the snapshot carries, with
/// that floor's headers. The install drops the log below the snapshot, and
/// with it the trim record behind this member's own floor, so both go to
/// the raise-only floor file, synced, before the snapshot is stored. A crash
/// at any point leaves a durable floor no lower than before: the file never
/// goes down, and the old snapshot and log still hold what they held.
/// The snapshot's tables keep their own floor; only the carried one is
/// raised.
async fn keep_floor(log: &DiskLog, purge: Option<PurgeFloors>) -> io::Result<()> {
    let log = log.clone();
    cpu(move || {
        if let Some(own) = log.head_trim() {
            log.carry_floor(CarriedFloor {
                offset: own.offset,
                floor: own.floor,
                headers: own.headers,
            })
            .map_err(io::Error::other)?;
        }
        if let Some(purge) = purge {
            log.carry_floor(purge.carried).map_err(io::Error::other)?;
        }
        Ok::<_, io::Error>(())
    })
    .await?
}

impl Machine {
    /// A builder for the state as of the last applied entry, `cut` being
    /// the projection's.
    ///
    /// The state and the id it is as of are taken in one step, where no
    /// entry is being applied. Taken when the snapshot is built, the state
    /// could hold entries past the id, and a member that restored it would
    /// apply them twice. The cut copies nothing; encoding it is the cost,
    /// and happens in the build, off this task.
    fn builder(&self, cut: Option<Cut>) -> Builder {
        let purge = self
            .purge_at
            .lock()
            .take()
            .zip(self.reader.clone())
            .filter(|_| self.log.is_some());
        Builder {
            purge,
            snapshots: self.snapshots.clone(),
            state: self.state.clone(),
            cut,
            groups: self
                .groups
                .as_ref()
                .map(|groups| groups.read().table().clone()),
            log: self.log.clone(),
            raft_log: self.raft_log.clone(),
        }
    }

    /// Let the purge driver ask for a stream snapshot below the applied
    /// entry, folded from `reader`. Returns where it asks.
    pub(crate) fn purging(&mut self, reader: LogReader<ShardTypes, ShardCodec>) -> PurgeAt {
        self.reader = Some(reader);
        Arc::clone(&self.purge_at)
    }

    /// Fold part of the overlay a released cut left back into the live
    /// maps, after a run that applied `written` cache entries. On this task rather than a
    /// thread of its own: the overlay's nodes are freed where they were
    /// made, and no second thread competes with apply for the lock or a
    /// core. A cut still held, or no overlay, makes this a no-op.
    fn fold_back(&self, written: usize) {
        let Some(cache) = &self.cache else {
            return;
        };
        #[cfg(test)]
        let Ok(_held) = self.fold_hold.try_lock() else {
            return;
        };
        let mut budget = FOLD_CHUNK.max(written.saturating_mul(FOLD_PER_ENTRY));
        while budget > 0 {
            let chunk = budget.min(FOLD_CHUNK);
            let mut state = cache.write();
            let more = state.fold(chunk);
            parking_lot::RwLockWriteGuard::unlock_fair(state);
            if !more {
                return;
            }
            budget -= chunk;
        }
    }
}

impl RaftStateMachine<ShardTypes> for Machine {
    type SnapshotData = SnapshotData;
    type SnapshotBuilder = Builder;

    async fn applied_state(
        &mut self,
    ) -> io::Result<(Option<LogIdOf<ShardTypes>>, StoredMembershipOf<ShardTypes>)> {
        Ok((self.state.last_applied, self.state.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
    where
        Strm: Stream<Item = io::Result<EntryResponder<ShardTypes>>> + Unpin + OptionalSend,
    {
        #[cfg(test)]
        let _held = Arc::clone(&self.hold).lock_owned().await;
        let mut applied = Ok(());
        let mut written = 0;
        while let Some(next) = entries.next().await {
            let (entry, responder) = match next {
                Ok(next) => next,
                Err(err) => {
                    applied = Err(err);
                    break;
                }
            };
            if let (Some(cache), EntryPayload::Normal(command)) = (&self.cache, &entry.payload) {
                // An entry this build cannot read stops the member. Skipping
                // it would leave this member's state unlike the others'.
                let command = match CacheCommand::decode(&command.payload) {
                    Ok(decoded) => (command.timestamp_micros, decoded),
                    Err(err) => {
                        applied = Err(err);
                        break;
                    }
                };
                // Before the commit table moves, so a write that waits on
                // its commit finds its answer.
                cache
                    .write()
                    .apply(entry.log_id.index, command.0, &command.1);
                written += 1;
            }
            if let (Some(groups), EntryPayload::Normal(command)) = (&self.groups, &entry.payload)
                && command.group
            {
                // Refused like a cache entry: every member folds every
                // group write, or their tables differ.
                let decoded = match GroupEntry::decode(&command.payload) {
                    Ok(decoded) => decoded,
                    Err(err) => {
                        applied = Err(invalid(format!("a group entry does not decode: {err}")));
                        break;
                    }
                };
                // Before the commit table moves, so a write that waits on
                // its apply finds it folded.
                let (term, index) = term_index(&entry.log_id);
                groups.write().apply(term, index, &decoded);
            }
            // openraft applies entries read back from the log, where a
            // conditional publish off its offset is already a blank. Checked
            // again in case one ever reaches apply as the command.
            let record = match &entry.payload {
                EntryPayload::Normal(command) => command.holds_record_at(entry.log_id.index),
                _ => false,
            };
            if let EntryPayload::Membership(membership) = entry.payload {
                self.state.membership =
                    StoredMembershipOf::<ShardTypes>::new(Some(entry.log_id), membership);
            }
            self.commits.advance(point(&entry.log_id));
            if !record {
                self.commits.holds_no_record(entry.log_id.index);
            }
            self.state.last_applied = Some(entry.log_id);
            if let Some(responder) = responder {
                responder.send(Applied);
            }
        }
        // Once for the run, whatever ended it: a waiter is woken per append,
        // not per record.
        self.publish();
        // After the publish, so the run's waiters are not held by it.
        self.fold_back(written);
        applied
    }

    async fn try_create_snapshot_builder(&mut self, force: bool) -> Option<Self::SnapshotBuilder> {
        // While the last build's overlay is still folding back, a cut would
        // have to finish the fold on this task, holding apply and reads.
        // Deferring is safe: openraft asks again `cache_snapshot_every`
        // entries later and the log is purged later. Only a forced build
        // (one a follower or a restart needs) takes the cut regardless.
        let cut = match &self.cache {
            Some(cache) => {
                let mut state = cache.write();
                if !force && state.layered() {
                    return None;
                }
                Some(state.cut())
            }
            None => None,
        };
        Some(self.builder(cut))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let cut = self.cache.as_ref().map(|cache| cache.write().cut());
        self.builder(cut)
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<ShardTypes>,
        snapshot: SnapshotData,
    ) -> io::Result<()> {
        let SnapshotData {
            bytes: data,
            restored,
        } = snapshot;
        // Read before it is stored: a snapshot that does not parse must not
        // become the one this member restarts from.
        let restored = match (&self.cache, restored) {
            (Some(_), Some(restored)) => Some(restored),
            (Some(_), None) => Some(restore(data.clone(), meta.last_log_id.is_some()).await?),
            (None, _) => None,
        };
        let carried = match &self.log {
            Some(_) => Some(StreamSnapshot::decode(&data)?),
            None => None,
        };
        // Saved in the log before the snapshot is stored, so this member
        // refuses a re-send of a batch it was never sent from the moment
        // it holds nothing below the snapshot. A crash in between leaves
        // the old snapshot and only more refusals, which the next install
        // repeats.
        let groups = match (&self.log, carried) {
            (
                Some(log),
                Some(StreamSnapshot {
                    cuts,
                    groups,
                    purge,
                }),
            ) => {
                keep_floor(log, purge).await?;
                let base = meta.last_log_id.map_or(0, |id| id.index + 1);
                log.remember_forgotten_cuts(base, cuts)
                    .await
                    .map_err(io::Error::other)?;
                Some(groups)
            }
            _ => None,
        };
        store(&self.snapshots, meta, data).await?;
        if let (Some(cache), Some(restored)) = (&self.cache, restored) {
            *cache.write() = restored;
        }
        // The table as of the snapshot's last entry, which openraft applies
        // above from here. Trackers go with the old table.
        if let (Some(live), Some(groups)) = (&self.groups, groups) {
            live.write().reset(groups);
        }
        self.state = State {
            last_applied: meta.last_log_id,
            membership: meta.last_membership.clone(),
        };
        // Only the snapshot's last entry is known by term now.
        let resumed = Commits::resumed(meta.last_log_id.as_ref().map(point));
        if resumed.read_bound() > self.commits.read_bound() {
            self.commits = resumed;
            self.publish();
        }
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> io::Result<Option<SnapshotOf<ShardTypes, SnapshotData>>> {
        let Some(stored) = self.snapshots.current().await? else {
            return Ok(None);
        };
        let state = StoredMeta::state(&stored.meta)?;
        Ok(Some(SnapshotOf::<ShardTypes, SnapshotData> {
            meta: meta(&state),
            snapshot: SnapshotData::bytes(stored.data),
        }))
    }
}
