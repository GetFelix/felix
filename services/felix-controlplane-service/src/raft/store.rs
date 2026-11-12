//! Persistent Raft state: log, vote, and the current snapshot.
//!
//! Everything lives in one redb file per instance. redb gives crash-safe,
//! fsynced-on-commit transactions, which is exactly the guarantee Raft
//! storage must not fake: a vote or a log entry acknowledged to the core and
//! then lost is how a cluster elects two leaders for one term. Choosing an
//! embedded ACID store over hand-rolled files is deliberate — consensus
//! durability plumbing is the last place Felix should be inventive
//! (`docs/metadata-raft-design.md#library`).
//!
//! What is written is Felix's own JSON (`super::wire`), the form openraft 0.9
//! stored, so a member can be rolled back to a release that ran 0.9.
//!
//! This log is not `felix-storage` yet. The segment store now has a Raft
//! log (`felix_storage::raft_log`, which cuts uncommitted suffixes without
//! rewriting a record), and the control plane moves onto it
//! (`docs/replication-engine.md`, "Raft log on the segment store").
//!
//! The state machine itself is volatile and rebuilt on startup from the
//! persisted snapshot plus log replay — the "persistent snapshot" option in
//! openraft's contract. That keeps [`super::AppStateMachine`] free of any
//! durability obligation, which is what lets #338 plug the in-memory
//! metadata store in unchanged.

use std::io::{self, Cursor};
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::{Stream, TryStreamExt};
use openraft::OptionalSend;
use openraft::storage::{
    EntryResponder, IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder,
    RaftStateMachine,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tokio::sync::Mutex;

use super::AppStateMachine;
use super::types::{
    Entry, EntryPayload, LogId, Snapshot, SnapshotData, SnapshotMeta, StoredMembership, TypeConfig,
    Vote,
};
use super::wire;

const LOGS: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_logs");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

const KEY_VOTE: &str = "vote";
const KEY_COMMITTED: &str = "committed";
const KEY_LAST_PURGED: &str = "last_purged";
const KEY_SNAPSHOT_META: &str = "snapshot_meta";
const KEY_SNAPSHOT_DATA: &str = "snapshot_data";
/// Set while this member must not vote: from the moment it started empty
/// until it has formed the group or caught up with it (`super::join`).
const KEY_VOTE_WITHHELD: &str = "vote_withheld";
/// The cluster this data dir belongs to (`FELIX_RAFT_CLUSTER_ID`).
const KEY_CLUSTER_ID: &str = "cluster_id";

/// A member's store, opened: what its Raft node is made from.
pub(super) struct Opened {
    pub(super) log_store: LogStore,
    pub(super) state_machine: StateMachineStore,
    pub(super) may_vote: bool,
    /// What this node had committed before it stopped: the floor its state
    /// machine must be replayed back to before anything serves from it.
    /// Read before the node starts, because the node moves it.
    pub(super) committed_floor: Option<u64>,
    pub(super) member: MemberState,
}

/// Open (or create) the member's store under `settings.data_dir`.
pub(super) async fn open_member(
    settings: &super::RaftSettings,
    app: Arc<dyn AppStateMachine>,
) -> Result<Opened> {
    std::fs::create_dir_all(&settings.data_dir).context("create raft data dir")?;
    let db = open(&settings.data_dir.join("raft.redb"))?;
    claim_cluster_id(&db, &settings.security.cluster_id)?;
    Ok(Opened {
        log_store: LogStore::new(Arc::clone(&db)),
        state_machine: StateMachineStore::open(Arc::clone(&db), app).await?,
        may_vote: !vote_withheld(&db)?,
        committed_floor: persisted_committed_index(&db),
        member: MemberState {
            db: Arc::downgrade(&db),
        },
    })
}

/// What a running member reads and changes in its store outside the log.
///
/// Weak, so that a stopped node releases its store even while clones of the
/// handle live on, and a restart in the same process can reopen it.
#[derive(Clone)]
pub(super) struct MemberState {
    db: std::sync::Weak<Database>,
}

impl MemberState {
    fn db(&self) -> Result<Arc<Database>> {
        self.db
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("raft node has stopped"))
    }

    /// Whether the store holds any consensus state.
    pub(super) fn holds_state(&self) -> Result<bool> {
        holds_state(&*self.db()?)
    }

    /// Durable before it returns.
    pub(super) async fn set_vote_withheld(&self, withheld: bool) -> Result<()> {
        set_vote_withheld(&*self.db()?, withheld)
    }

    /// The node has stopped. The file closes when its last user is dropped.
    pub(super) async fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// Leave a store in `dir` as a member that held `vote` and `entries` would
/// have, with its vote withheld or not.
#[cfg(test)]
pub(super) async fn seed(
    dir: &Path,
    _cluster_id: &str,
    vote: Vote,
    entries: Vec<Entry>,
    vote_withheld: bool,
) {
    let db = open(&dir.join("raft.redb")).expect("open store");
    let mut log = LogStore::new(Arc::clone(&db));
    log.save_vote(&vote).await.expect("save vote");
    log.append(entries, IOFlushed::Noop).await.expect("append");
    set_vote_withheld(&db, vote_withheld).expect("vote flag");
}

/// The committed index this store had persisted when it last ran — what a
/// restarting node must re-apply before it is fit to serve.
fn persisted_committed_index(db: &Database) -> Option<u64> {
    read_meta::<Option<wire::LogId>>(db, KEY_COMMITTED)
        .ok()
        .flatten()
        .flatten()
        .map(|log_id| log_id.index)
}

/// Whether this store holds any consensus state: a vote, a log entry, or a
/// snapshot. An empty one is a first boot or a wiped volume.
fn holds_state(db: &Database) -> Result<bool> {
    let txn = db.begin_read().context("read raft store")?;
    let logs = txn.open_table(LOGS).context("open log table")?;
    if logs.first().context("read log table")?.is_some() {
        return Ok(true);
    }
    let meta = txn.open_table(META).context("open meta table")?;
    for key in [KEY_VOTE, KEY_COMMITTED, KEY_LAST_PURGED, KEY_SNAPSHOT_META] {
        if meta.get(key).context("read raft meta")?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn vote_withheld(db: &Database) -> Result<bool> {
    Ok(read_meta::<bool>(db, KEY_VOTE_WITHHELD)
        .map_err(|err| anyhow::anyhow!("read vote flag: {err}"))?
        .unwrap_or(false))
}

/// Durable before it returns, so a crash never forgets that a half-filled
/// log is not yet fit to vote.
fn set_vote_withheld(db: &Database, withheld: bool) -> Result<()> {
    write_meta(db, KEY_VOTE_WITHHELD, &withheld)
        .map_err(|err| anyhow::anyhow!("write vote flag: {err}"))
}

/// Record that this data dir belongs to `cluster_id`, or refuse if it
/// already belongs to another cluster.
///
/// A store without a recorded id takes the configured one, whether it is
/// empty or predates the id: either way this is the first start that knows
/// it. After that, a member pointed at the wrong group's peers, or a volume
/// mounted into the wrong cluster, fails here instead of mixing two logs.
fn claim_cluster_id(db: &Database, cluster_id: &str) -> Result<()> {
    let recorded = read_meta::<String>(db, KEY_CLUSTER_ID)
        .map_err(|err| anyhow::anyhow!("read cluster id: {err}"))?;
    match recorded {
        Some(recorded) if recorded == cluster_id => Ok(()),
        Some(recorded) => anyhow::bail!(
            "raft data dir belongs to cluster '{recorded}', not the configured \
             FELIX_RAFT_CLUSTER_ID '{cluster_id}'"
        ),
        None => write_meta(db, KEY_CLUSTER_ID, &cluster_id.to_string())
            .map_err(|err| anyhow::anyhow!("write cluster id: {err}")),
    }
}

/// Whether the stopped member in `data_dir` holds consensus state. Refuses
/// a store that belongs to a cluster other than `cluster_id`. Creates and
/// records nothing; no store is no state.
pub(super) async fn stopped_member_holds_state(data_dir: &Path, cluster_id: &str) -> Result<bool> {
    let path = data_dir.join("raft.redb");
    if !path.exists() {
        return Ok(false);
    }
    // Fails while the member runs: its process holds the file.
    let db = open(&path).context("open the raft store; stop the member first")?;
    let recorded = read_meta::<String>(&db, KEY_CLUSTER_ID)
        .map_err(|err| anyhow::anyhow!("read cluster id: {err}"))?;
    if let Some(recorded) = recorded.filter(|recorded| recorded != cluster_id) {
        anyhow::bail!("raft data dir belongs to cluster '{recorded}', not '{cluster_id}'");
    }
    holds_state(&db)
}

/// Open (or create) the store file and make sure both tables exist, so
/// every later read can assume them.
pub(super) fn open(path: &Path) -> Result<Arc<Database>> {
    let db = Database::create(path).context("open raft store")?;
    let txn = db.begin_write().context("init raft store")?;
    txn.open_table(LOGS).context("init log table")?;
    txn.open_table(META).context("init meta table")?;
    txn.commit().context("init raft store commit")?;
    Ok(Arc::new(db))
}

fn io_err(err: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(err)
}

fn read_meta<T: serde::de::DeserializeOwned>(
    db: &Database,
    key: &str,
) -> Result<Option<T>, io::Error> {
    let txn = db.begin_read().map_err(io_err)?;
    let table = txn.open_table(META).map_err(io_err)?;
    let Some(guard) = table.get(key).map_err(io_err)? else {
        return Ok(None);
    };
    let value = serde_json::from_slice(guard.value()).map_err(io_err)?;
    Ok(Some(value))
}

fn write_meta<T: serde::Serialize>(db: &Database, key: &str, value: &T) -> Result<(), io::Error> {
    let bytes = serde_json::to_vec(value).map_err(io_err)?;
    let txn = db.begin_write().map_err(io_err)?;
    {
        let mut table = txn.open_table(META).map_err(io_err)?;
        table.insert(key, bytes.as_slice()).map_err(io_err)?;
    }
    txn.commit().map_err(io_err)?;
    Ok(())
}

/// Runs a durable write on the blocking pool and waits for it.
///
/// Every redb commit fsyncs. Run inline, a slow disk would hold an async
/// worker for the whole flush and stall every task queued on it, heartbeats
/// and RPC handlers included. Openraft still awaits each call before issuing
/// the next, so writes keep their order and are durable when this returns.
async fn blocking_write<T, F>(db: &Arc<Database>, write: F) -> Result<T, io::Error>
where
    T: Send + 'static,
    F: FnOnce(&Database) -> Result<T, io::Error> + Send + 'static,
{
    let db = Arc::clone(db);
    tokio::task::spawn_blocking(move || write(&db))
        .await
        .map_err(io_err)?
}

/// The Raft log and vote. Clones share the database; openraft hands clones
/// to its replication tasks as log readers.
#[derive(Clone)]
pub(super) struct LogStore {
    db: Arc<Database>,
}

impl LogStore {
    pub(super) fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn owned_bound(bound: std::ops::Bound<&u64>) -> std::ops::Bound<u64> {
        match bound {
            std::ops::Bound::Included(index) => std::ops::Bound::Included(*index),
            std::ops::Bound::Excluded(index) => std::ops::Bound::Excluded(*index),
            std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        }
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, io::Error> {
        let txn = self.db.begin_read().map_err(io_err)?;
        let table = txn.open_table(LOGS).map_err(io_err)?;
        let start = Self::owned_bound(range.start_bound());
        let end = Self::owned_bound(range.end_bound());
        let mut entries = Vec::new();
        for item in table.range((start, end)).map_err(io_err)? {
            let (_, value) = item.map_err(io_err)?;
            let entry: wire::Entry = serde_json::from_slice(value.value()).map_err(io_err)?;
            entries.push(entry.into());
        }
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        Ok(read_meta::<wire::Vote>(&self.db, KEY_VOTE)?.map(Vote::from))
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let last_purged: Option<LogId> =
            read_meta::<wire::LogId>(&self.db, KEY_LAST_PURGED)?.map(LogId::from);
        let txn = self.db.begin_read().map_err(io_err)?;
        let table = txn.open_table(LOGS).map_err(io_err)?;
        let last = match table.last().map_err(io_err)? {
            Some((_, value)) => {
                let entry: wire::Entry = serde_json::from_slice(value.value()).map_err(io_err)?;
                Some(entry.log_id.into())
            }
            None => last_purged,
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        // The commit fsyncs; a vote that is acknowledged but not durable
        // lets this node vote twice for one term after a crash.
        let vote = wire::Vote::from(vote);
        blocking_write(&self.db, move |db| write_meta(db, KEY_VOTE, &vote)).await
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        let committed = wire::log_id(&committed);
        blocking_write(&self.db, move |db| {
            write_meta(db, KEY_COMMITTED, &committed)
        })
        .await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        let committed = read_meta::<Option<wire::LogId>>(&self.db, KEY_COMMITTED)?;
        Ok(wire::lib_log_id(committed.flatten()))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let rows = entries
            .into_iter()
            .map(|entry| {
                Ok((
                    entry.log_id.index,
                    serde_json::to_vec(&wire::Entry::from(&entry)).map_err(io_err)?,
                ))
            })
            .collect::<Result<Vec<_>, io::Error>>()?;
        blocking_write(&self.db, move |db| {
            let txn = db.begin_write().map_err(io_err)?;
            {
                let mut table = txn.open_table(LOGS).map_err(io_err)?;
                for (index, bytes) in rows {
                    table.insert(index, bytes.as_slice()).map_err(io_err)?;
                }
            }
            txn.commit().map_err(io_err)
        })
        .await?;
        // Only after the durable commit: this callback is openraft's "these
        // entries survive a crash" signal, and everything downstream
        // (commit, apply, acknowledgements) leans on it being true.
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        // Conflict resolution: a deposed leader's uncommitted suffix goes
        // away. `last_log_id` is the last entry kept.
        let start = last_log_id.map_or(0, |id| id.index + 1);
        blocking_write(&self.db, move |db| {
            let txn = db.begin_write().map_err(io_err)?;
            {
                let mut table = txn.open_table(LOGS).map_err(io_err)?;
                let doomed: Vec<u64> = table
                    .range(start..)
                    .map_err(io_err)?
                    .map(|item| item.map(|(key, _)| key.value()))
                    .collect::<Result<_, _>>()
                    .map_err(io_err)?;
                for index in doomed {
                    table.remove(index).map_err(io_err)?;
                }
            }
            txn.commit().map_err(io_err)
        })
        .await
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        // Compaction behind a snapshot. The purge marker commits in the same
        // transaction as the deletions so a crash between them cannot leave
        // the log claiming a hole it does not have.
        let bytes = serde_json::to_vec(&wire::LogId::from(&log_id)).map_err(io_err)?;
        blocking_write(&self.db, move |db| {
            let txn = db.begin_write().map_err(io_err)?;
            {
                let mut meta = txn.open_table(META).map_err(io_err)?;
                meta.insert(KEY_LAST_PURGED, bytes.as_slice())
                    .map_err(io_err)?;
                let mut table = txn.open_table(LOGS).map_err(io_err)?;
                let doomed: Vec<u64> = table
                    .range(..=log_id.index)
                    .map_err(io_err)?
                    .map(|item| item.map(|(key, _)| key.value()))
                    .collect::<Result<_, _>>()
                    .map_err(io_err)?;
                for index in doomed {
                    table.remove(index).map_err(io_err)?;
                }
            }
            txn.commit().map_err(io_err)
        })
        .await
    }
}

/// Bookkeeping the snapshot must capture atomically with the app state.
struct Applied {
    last_applied: Option<LogId>,
    membership: StoredMembership,
}

struct SmInner {
    db: Arc<Database>,
    app: Arc<dyn AppStateMachine>,
    /// One lock over the bookkeeping *and* every app mutation: a snapshot
    /// built under it pairs `(last_applied, membership)` with exactly the
    /// app state those describe, never half a batch later. Async because it
    /// is held across the app's own async calls.
    applied: Mutex<Applied>,
}

/// The state machine adapter: volatile app state, persistent snapshots.
#[derive(Clone)]
pub(super) struct StateMachineStore {
    inner: Arc<SmInner>,
}

impl StateMachineStore {
    /// Rebuild the app state from the last persisted snapshot, if any. Log
    /// entries after it are replayed by openraft on startup — that pairing
    /// is what makes a volatile state machine safe.
    pub(super) async fn open(db: Arc<Database>, app: Arc<dyn AppStateMachine>) -> Result<Self> {
        let mut applied = Applied {
            last_applied: None,
            membership: StoredMembership::default(),
        };
        let meta: Option<wire::SnapshotMeta> =
            read_meta(&db, KEY_SNAPSHOT_META).map_err(|err| anyhow::anyhow!("{err}"))?;
        if let Some(meta) = meta.map(SnapshotMeta::from) {
            let data: Option<Vec<u8>> =
                read_meta(&db, KEY_SNAPSHOT_DATA).map_err(|err| anyhow::anyhow!("{err}"))?;
            let data = data.context("snapshot meta without snapshot data")?;
            app.restore(&data).await;
            applied.last_applied = meta.last_log_id;
            applied.membership = meta.last_membership;
        }
        Ok(Self {
            inner: Arc::new(SmInner {
                db,
                app,
                applied: Mutex::new(applied),
            }),
        })
    }

    /// Store `meta` and `data` as the current snapshot unless the stored one
    /// is at the same log index or past it. Returns whether it wrote.
    ///
    /// Openraft builds snapshots in a detached task, so a build that captured
    /// its state before an install of a newer snapshot can reach here after
    /// it (#1745). Writing it would leave a snapshot behind the purged log,
    /// and the member could not restart. The check reads the stored meta in
    /// the same write transaction, and redb runs one writer at a time, so no
    /// other snapshot can land between the check and the write.
    async fn persist_snapshot(&self, meta: &SnapshotMeta, data: &[u8]) -> Result<bool, io::Error> {
        // Meta and data commit together: a snapshot that exists without its
        // descriptor (or the reverse) reads as no snapshot at all.
        let index = meta.last_log_id.map(|id| id.index);
        let meta_bytes = serde_json::to_vec(&wire::SnapshotMeta::from(meta)).map_err(io_err)?;
        let data_bytes = serde_json::to_vec(&data).map_err(io_err)?;
        blocking_write(&self.inner.db, move |db| {
            let txn = db.begin_write().map_err(io_err)?;
            {
                let mut table = txn.open_table(META).map_err(io_err)?;
                if let Some(stored) = table.get(KEY_SNAPSHOT_META).map_err(io_err)? {
                    let stored: wire::SnapshotMeta =
                        serde_json::from_slice(stored.value()).map_err(io_err)?;
                    if stored.last_log_id.map(|id| id.index) >= index {
                        return Ok(false);
                    }
                }
                table
                    .insert(KEY_SNAPSHOT_META, meta_bytes.as_slice())
                    .map_err(io_err)?;
                table
                    .insert(KEY_SNAPSHOT_DATA, data_bytes.as_slice())
                    .map_err(io_err)?;
            }
            txn.commit().map_err(io_err)?;
            Ok(true)
        })
        .await
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotData = SnapshotData;
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
        let applied = self.inner.applied.lock().await;
        Ok((applied.last_applied, applied.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        while let Some((entry, responder)) = entries.try_next().await? {
            // Taken per entry, not across the wait for the next one, so a
            // snapshot build is never held up by a slow log read.
            let mut applied = self.inner.applied.lock().await;
            let response = match entry.payload {
                EntryPayload::Blank => Vec::new(),
                EntryPayload::Normal(ref command) => self.inner.app.apply(&command.0).await,
                EntryPayload::Membership(ref membership) => {
                    applied.membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    Vec::new()
                }
            };
            applied.last_applied = Some(entry.log_id);
            drop(applied);
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            inner: Arc::clone(&self.inner),
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: SnapshotData,
    ) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        // Persist before replacing live state, so open rebuilds from this
        // snapshot after a crash. An older snapshot is not written, so what
        // is on disk only ever moves forward. Openraft purges the log behind
        // this snapshot without waiting for it to be persisted, so a crash
        // before this commit can leave the log purged past the stored one.
        self.persist_snapshot(meta, &data).await?;
        let mut applied = self.inner.applied.lock().await;
        // Openraft only installs past its committed index, so this does not
        // happen. If it did, restoring would move the applied cursor back
        // and the entries in between would be applied twice; failing stops
        // the member instead of letting it diverge.
        let applied_index = applied.last_applied.map(|id| id.index);
        if applied_index > meta.last_log_id.map(|id| id.index) {
            return Err(io::Error::other(format!(
                "snapshot at {:?} is behind the applied state at {:?}",
                meta.last_log_id, applied.last_applied
            )));
        }
        self.inner.app.restore(&data).await;
        applied.last_applied = meta.last_log_id;
        applied.membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, io::Error> {
        let meta: Option<wire::SnapshotMeta> = read_meta(&self.inner.db, KEY_SNAPSHOT_META)?;
        let Some(meta) = meta else {
            return Ok(None);
        };
        let data: Option<Vec<u8>> = read_meta(&self.inner.db, KEY_SNAPSHOT_DATA)?;
        let Some(data) = data else {
            return Ok(None);
        };
        Ok(Some(Snapshot {
            meta: meta.into(),
            snapshot: Cursor::new(data),
        }))
    }
}

pub(super) struct SnapshotBuilder {
    inner: Arc<SmInner>,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    type SnapshotData = SnapshotData;

    async fn build_snapshot(&mut self) -> Result<Snapshot, io::Error> {
        let (data, meta) = {
            let applied = self.inner.applied.lock().await;
            let data = self.inner.app.snapshot().await;
            let meta = SnapshotMeta {
                last_log_id: applied.last_applied,
                last_membership: applied.membership.clone(),
            };
            (data, meta)
        };

        let store = StateMachineStore {
            inner: Arc::clone(&self.inner),
        };
        // Skipped when a newer snapshot was installed meanwhile; openraft
        // drops this older one when it sees it.
        store.persist_snapshot(&meta, &data).await?;

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

#[cfg(test)]
mod tests;
