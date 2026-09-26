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
//! This log is deliberately **not** `felix-storage`: Raft truncates
//! divergent uncommitted suffixes, and the segment store's never-rewritten
//! invariant exists precisely to forbid that. Separate log, separate rules.
//!
//! The state machine itself is volatile and rebuilt on startup from the
//! persisted snapshot plus log replay — the "persistent snapshot" option in
//! openraft's contract. That keeps [`super::AppStateMachine`] free of any
//! durability obligation, which is what lets #338 plug the in-memory
//! metadata store in unchanged.

// The error type and its size are openraft's, fixed by the storage traits.
#![allow(clippy::result_large_err)]

use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use openraft::storage::{
    LogFlushed, LogState, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
};
use openraft::{AnyError, ErrorSubject, ErrorVerb, RaftLogReader, StorageIOError};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tokio::sync::Mutex;

use super::AppStateMachine;
use super::types::{
    Entry, LogId, Snapshot, SnapshotMeta, StorageError, StoredMembership, TypeConfig,
};

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

/// The committed index this store had persisted when it last ran — what a
/// restarting node must re-apply before it is fit to serve.
pub(super) fn persisted_committed_index(db: &Database) -> Option<u64> {
    read_meta::<Option<LogId>>(db, KEY_COMMITTED)
        .ok()
        .flatten()
        .flatten()
        .map(|log_id| log_id.index)
}

/// Whether this store holds any consensus state: a vote, a log entry, or a
/// snapshot. An empty one is a first boot or a wiped volume.
pub(super) fn holds_state(db: &Database) -> Result<bool> {
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

pub(super) fn vote_withheld(db: &Database) -> Result<bool> {
    Ok(read_meta::<bool>(db, KEY_VOTE_WITHHELD)
        .map_err(|err| anyhow::anyhow!("read vote flag: {err}"))?
        .unwrap_or(false))
}

/// Durable before it returns, so a crash never forgets that a half-filled
/// log is not yet fit to vote.
pub(super) fn set_vote_withheld(db: &Database, withheld: bool) -> Result<()> {
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
pub(super) fn claim_cluster_id(db: &Database, cluster_id: &str) -> Result<()> {
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

fn read_err(err: impl std::error::Error + 'static) -> StorageError {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&err)).into()
}

fn write_err(err: impl std::error::Error + 'static) -> StorageError {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&err)).into()
}

fn read_meta<T: serde::de::DeserializeOwned>(
    db: &Database,
    key: &str,
) -> Result<Option<T>, StorageError> {
    let txn = db.begin_read().map_err(read_err)?;
    let table = txn.open_table(META).map_err(read_err)?;
    let Some(guard) = table.get(key).map_err(read_err)? else {
        return Ok(None);
    };
    let value = serde_json::from_slice(guard.value()).map_err(read_err)?;
    Ok(Some(value))
}

fn write_meta<T: serde::Serialize>(
    db: &Database,
    key: &str,
    value: &T,
) -> Result<(), StorageError> {
    let bytes = serde_json::to_vec(value).map_err(write_err)?;
    let txn = db.begin_write().map_err(write_err)?;
    {
        let mut table = txn.open_table(META).map_err(write_err)?;
        table.insert(key, bytes.as_slice()).map_err(write_err)?;
    }
    txn.commit().map_err(write_err)?;
    Ok(())
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
    ) -> Result<Vec<Entry>, StorageError> {
        let txn = self.db.begin_read().map_err(read_err)?;
        let table = txn.open_table(LOGS).map_err(read_err)?;
        let start = Self::owned_bound(range.start_bound());
        let end = Self::owned_bound(range.end_bound());
        let mut entries = Vec::new();
        for item in table.range((start, end)).map_err(read_err)? {
            let (_, value) = item.map_err(read_err)?;
            entries.push(serde_json::from_slice(value.value()).map_err(read_err)?);
        }
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError> {
        let last_purged: Option<LogId> = read_meta(&self.db, KEY_LAST_PURGED)?;
        let txn = self.db.begin_read().map_err(read_err)?;
        let table = txn.open_table(LOGS).map_err(read_err)?;
        let last = match table.last().map_err(read_err)? {
            Some((_, value)) => {
                let entry: Entry = serde_json::from_slice(value.value()).map_err(read_err)?;
                Some(entry.log_id)
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

    async fn save_vote(&mut self, vote: &openraft::Vote<u64>) -> Result<(), StorageError> {
        // The commit below fsyncs; a vote that is acknowledged but not
        // durable lets this node vote twice for one term after a crash.
        write_meta(&self.db, KEY_VOTE, vote)
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<u64>>, StorageError> {
        read_meta(&self.db, KEY_VOTE)
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), StorageError> {
        write_meta(&self.db, KEY_COMMITTED, &committed)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, StorageError> {
        Ok(read_meta(&self.db, KEY_COMMITTED)?.flatten())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = txn.open_table(LOGS).map_err(write_err)?;
            for entry in entries {
                let bytes = serde_json::to_vec(&entry).map_err(write_err)?;
                table
                    .insert(entry.log_id.index, bytes.as_slice())
                    .map_err(write_err)?;
            }
        }
        txn.commit().map_err(write_err)?;
        // Only after the durable commit: this callback is openraft's "these
        // entries survive a crash" signal, and everything downstream
        // (commit, apply, acknowledgements) leans on it being true.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        // Conflict resolution: a deposed leader's uncommitted suffix goes
        // away. This is the operation the felix-storage segment log
        // deliberately cannot do, and why this log is not that log.
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = txn.open_table(LOGS).map_err(write_err)?;
            let doomed: Vec<u64> = table
                .range(log_id.index..)
                .map_err(write_err)?
                .map(|item| item.map(|(key, _)| key.value()))
                .collect::<Result<_, _>>()
                .map_err(write_err)?;
            for index in doomed {
                table.remove(index).map_err(write_err)?;
            }
        }
        txn.commit().map_err(write_err)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        // Compaction behind a snapshot. The purge marker commits in the same
        // transaction as the deletions so a crash between them cannot leave
        // the log claiming a hole it does not have.
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut meta = txn.open_table(META).map_err(write_err)?;
            let bytes = serde_json::to_vec(&log_id).map_err(write_err)?;
            meta.insert(KEY_LAST_PURGED, bytes.as_slice())
                .map_err(write_err)?;
            let mut table = txn.open_table(LOGS).map_err(write_err)?;
            let doomed: Vec<u64> = table
                .range(..=log_id.index)
                .map_err(write_err)?
                .map(|item| item.map(|(key, _)| key.value()))
                .collect::<Result<_, _>>()
                .map_err(write_err)?;
            for index in doomed {
                table.remove(index).map_err(write_err)?;
            }
        }
        txn.commit().map_err(write_err)?;
        Ok(())
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
        let meta: Option<SnapshotMeta> =
            read_meta(&db, KEY_SNAPSHOT_META).map_err(|err| anyhow::anyhow!("{err}"))?;
        if let Some(meta) = meta {
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

    fn persist_snapshot(&self, meta: &SnapshotMeta, data: &[u8]) -> Result<(), StorageError> {
        // Meta and data commit together: a snapshot that exists without its
        // descriptor (or the reverse) reads as no snapshot at all.
        let meta_bytes = serde_json::to_vec(meta).map_err(write_err)?;
        let data_bytes = serde_json::to_vec(&data).map_err(write_err)?;
        let txn = self.inner.db.begin_write().map_err(write_err)?;
        {
            let mut table = txn.open_table(META).map_err(write_err)?;
            table
                .insert(KEY_SNAPSHOT_META, meta_bytes.as_slice())
                .map_err(write_err)?;
            table
                .insert(KEY_SNAPSHOT_DATA, data_bytes.as_slice())
                .map_err(write_err)?;
        }
        txn.commit().map_err(write_err)?;
        Ok(())
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        let applied = self.inner.applied.lock().await;
        Ok((applied.last_applied, applied.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Vec<u8>>, StorageError>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        let mut applied = self.inner.applied.lock().await;
        for entry in entries {
            let response = match entry.payload {
                openraft::EntryPayload::Blank => Vec::new(),
                openraft::EntryPayload::Normal(ref command) => self.inner.app.apply(command).await,
                openraft::EntryPayload::Membership(ref membership) => {
                    applied.membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    Vec::new()
                }
            };
            applied.last_applied = Some(entry.log_id);
            responses.push(response);
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            inner: Arc::clone(&self.inner),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError> {
        let data = snapshot.into_inner();
        // Persist before replacing live state: a crash between the two
        // leaves either the old state (snapshot re-sent) or the new
        // (rebuilt on open) — never neither.
        self.persist_snapshot(meta, &data)?;
        let mut applied = self.inner.applied.lock().await;
        self.inner.app.restore(&data).await;
        applied.last_applied = meta.last_log_id;
        applied.membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError> {
        let meta: Option<SnapshotMeta> = read_meta(&self.inner.db, KEY_SNAPSHOT_META)?;
        let Some(meta) = meta else {
            return Ok(None);
        };
        let data: Option<Vec<u8>> = read_meta(&self.inner.db, KEY_SNAPSHOT_DATA)?;
        let Some(data) = data else {
            return Ok(None);
        };
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

pub(super) struct SnapshotBuilder {
    inner: Arc<SmInner>,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError> {
        let (data, meta) = {
            let applied = self.inner.applied.lock().await;
            let data = self.inner.app.snapshot().await;
            let snapshot_id = format!(
                "{}-{}",
                applied.last_applied.map(|id| id.index).unwrap_or(0),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            );
            let meta = SnapshotMeta {
                last_log_id: applied.last_applied,
                last_membership: applied.membership.clone(),
                snapshot_id,
            };
            (data, meta)
        };

        let store = StateMachineStore {
            inner: Arc::clone(&self.inner),
        };
        store.persist_snapshot(&meta, &data)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[cfg(test)]
mod tests;
