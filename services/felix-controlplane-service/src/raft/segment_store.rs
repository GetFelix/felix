//! Persistent Raft state on `felix-storage`'s metadata store: the log, the
//! vote and the committed index in a `DiskLog` opened as a Raft log, the
//! snapshot in one file, and the member file.
//!
//! This is the store of a build with `--cfg felix_segment_store`, in place
//! of the redb one (`store.rs`). No release sets it.
//! `felix_storage::metadata_store::MetadataStore::open` owns the start
//! rules, the lock and the purge rule; this module adds what only the
//! control plane knows: how a command and a membership are stored, and the
//! state machine. The design is `docs/metadata-raft-design.md`, "Moving to
//! the segment store".
//!
//! The state machine is volatile, as on redb: rebuilt at start from the
//! stored snapshot, then the log.

use std::io;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use felix_storage::log::{AppendRecord, LogRecord, RecordMark};
use felix_storage::metadata_store::{MetadataStore, STORE_DIR};
use felix_storage::raft_log::{
    EntryCodec, RaftLog, SnapshotId, SnapshotStore, StoredSnapshot, transfer,
};
use futures_util::{Stream, TryStreamExt};
use openraft::OptionalSend;
use openraft::storage::{EntryResponder, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};
use tokio::sync::Mutex;

use super::AppStateMachine;
use super::types::{
    Command, EntryPayload, LogId, Membership, Snapshot, SnapshotData, SnapshotMeta,
    StoredMembership, TypeConfig,
};
use super::wire;

type Store = MetadataStore<TypeConfig, Codec>;
pub(super) type LogStore = RaftLog<TypeConfig, Codec>;

/// A member's store, opened and marked as started: what its Raft node is
/// made from.
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

/// Open (or create) the member's store under `settings.data_dir` and set
/// its started mark.
///
/// The mark is on disk before this returns, so before the Raft node exists:
/// a node campaigns on its own timer, and from its first vote the member
/// can no longer go back to its redb file alone.
pub(super) async fn open_member(
    settings: &super::RaftSettings,
    app: Arc<dyn AppStateMachine>,
) -> Result<Opened> {
    let store = Store::open(
        &settings.data_dir,
        &settings.security.cluster_id,
        Codec,
        "metadata raft",
    )
    .await
    .context("open the metadata store")?;
    match start(&store, &settings.data_dir, app, settings.snapshot_max_bytes).await {
        Ok((state_machine, committed_floor)) => Ok(Opened {
            log_store: store.log(),
            state_machine,
            may_vote: !store.member_file().member().vote_withheld,
            committed_floor,
            member: MemberState {
                raft_state: store.has_raft_state(),
                store: Arc::new(Mutex::new(Some(store))),
            },
        }),
        Err(err) => {
            let _ = store.shutdown().await;
            Err(err)
        }
    }
}

/// Whether the stopped member in `data_dir` holds Raft state. Refuses a
/// store that belongs to a cluster other than `cluster_id`. No store is no
/// state, and none is created.
pub(super) async fn stopped_member_holds_state(
    data_dir: &std::path::Path,
    cluster_id: &str,
) -> Result<bool> {
    use felix_storage::metadata_store::REDB_FILE;

    if !data_dir.join(STORE_DIR).exists() {
        // An older release's store may hold a group this build cannot read.
        anyhow::ensure!(
            !data_dir.join(REDB_FILE).exists(),
            "the data directory holds {REDB_FILE} from an older release; this is not a first start"
        );
        return Ok(false);
    }
    // Fails while the member runs: its process holds the store's lock.
    let store = Store::open(data_dir, cluster_id, Codec, "form-without")
        .await
        .context("open the metadata store; stop the member first")?;
    let holds = store.has_raft_state();
    store.shutdown().await.context("close the metadata store")?;
    Ok(holds)
}

async fn start(
    store: &Store,
    data_dir: &std::path::Path,
    app: Arc<dyn AppStateMachine>,
    snapshot_max_bytes: u64,
) -> Result<(StateMachineStore, Option<u64>)> {
    let started = store.mark_started().await.context("set the started mark")?;
    // The mark of this store, not of some other one.
    started.check(&data_dir.join(STORE_DIR))?;
    // A member that votes from here on has the flag on disk first, whatever
    // path brought it here (a crash after `initialize`, a converted store).
    let member = store.member_file();
    if store.has_raft_state() && !member.member().vote_withheld {
        member
            .mark_reached()
            .await
            .context("write the formation file")?;
    }
    let committed = store
        .log()
        .read_committed()
        .await
        .context("read the committed index")?;
    // A clone of the store's own: the log measures its purges against it.
    let state_machine =
        StateMachineStore::open(store.snapshots().clone(), app, snapshot_max_bytes).await?;
    Ok((state_machine, committed.map(|id| id.index)))
}

/// Leave a store in `dir` as a member that held `vote` and `entries` would
/// have, with its vote withheld or not.
#[cfg(test)]
pub(super) async fn seed(
    dir: &std::path::Path,
    cluster_id: &str,
    vote: super::types::Vote,
    entries: Vec<super::types::Entry>,
    vote_withheld: bool,
) {
    use openraft::storage::IOFlushed;

    let store = Store::open(dir, cluster_id, Codec, "seed")
        .await
        .expect("open store");
    let mut log = store.log();
    log.save_vote(&vote).await.expect("save vote");
    log.append(entries, IOFlushed::Noop).await.expect("append");
    store
        .member_file()
        .set_vote_withheld(vote_withheld)
        .await
        .expect("vote flag");
    store.shutdown().await.expect("shutdown");
}

/// What a running member reads and changes in its store outside the log.
///
/// Holds the store until [`Self::close`], which the node's stop calls, so a
/// restart in the same process can reopen it while clones of the handle
/// live on.
#[derive(Clone)]
pub(super) struct MemberState {
    store: Arc<Mutex<Option<Store>>>,
    raft_state: bool,
}

impl MemberState {
    /// Whether the open found a vote, an entry, a purged id or a snapshot.
    /// Without one this member is treated as one with an empty store, which
    /// is also what a crash between the started mark and the first vote
    /// leaves.
    pub(super) fn holds_state(&self) -> Result<bool> {
        Ok(self.raft_state)
    }

    /// Durable before it returns.
    pub(super) async fn set_vote_withheld(&self, withheld: bool) -> Result<()> {
        let member = match self.store.lock().await.as_ref() {
            Some(store) => store.member_file().clone(),
            None => anyhow::bail!("raft node has stopped"),
        };
        member
            .set_vote_withheld(withheld)
            .await
            .context("write the member file")
    }

    /// The store's snapshots, until the node stops.
    pub(super) async fn snapshots(&self) -> Option<SnapshotStore> {
        let store = self.store.lock().await;
        store.as_ref().map(|store| store.snapshots().clone())
    }

    /// Whether this store holds the member's own mark that it set out to
    /// form the group.
    pub(super) async fn forming_mark(&self) -> Result<bool> {
        Ok(self.formation().await?.forming)
    }

    /// Set that mark. Durable before it returns.
    pub(super) async fn mark_forming(&self) -> Result<()> {
        // The store stays locked to the end of the write, so a stop waits
        // for it and nothing is written into a store that has closed.
        let written = match self.store.lock().await.as_ref() {
            Some(store) => store.member_file().mark_forming().await,
            None => anyhow::bail!("raft node has stopped"),
        };
        written.context("write the formation file")
    }

    /// Whether this store records that the member has voted from it.
    pub(super) async fn reached(&self) -> Result<bool> {
        Ok(self.formation().await?.reached)
    }

    /// Record that the member may vote from this store. Durable before it
    /// returns.
    pub(super) async fn mark_reached(&self) -> Result<()> {
        let written = match self.store.lock().await.as_ref() {
            Some(store) => store.member_file().mark_reached().await,
            None => anyhow::bail!("raft node has stopped"),
        };
        written.context("write the formation file")
    }

    /// The members this one recorded as having set out to form the group.
    pub(super) async fn formed(&self) -> Result<std::collections::BTreeSet<super::NodeId>> {
        Ok(self.formation().await?.formed)
    }

    /// Record that `member` set out to form the group. Durable before it
    /// returns.
    pub(super) async fn record_formed(&self, member: super::NodeId) -> Result<()> {
        let written = match self.store.lock().await.as_ref() {
            Some(store) => store.member_file().record_formed(member).await,
            None => anyhow::bail!("raft node has stopped"),
        };
        written.context("write the formation file")
    }

    async fn formation(&self) -> Result<felix_storage::metadata_store::Formation> {
        match self.store.lock().await.as_ref() {
            Some(store) => Ok(store.member_file().formation()),
            None => anyhow::bail!("raft node has stopped"),
        }
    }

    /// The node has stopped: flush and close the log.
    pub(super) async fn close(&self) -> Result<()> {
        let store = self.store.lock().await.take();
        match store {
            Some(store) => store.shutdown().await.context("close the metadata store"),
            None => Ok(()),
        }
    }
}

/// How the control plane's commands, memberships and node ids are stored.
#[derive(Clone, Copy)]
pub(super) struct Codec;

impl EntryCodec<TypeConfig> for Codec {
    fn encode_data(&self, data: &Command) -> io::Result<AppendRecord> {
        Ok(AppendRecord {
            payload: Bytes::copy_from_slice(&data.0),
            timestamp_micros: crate::clock::now_millis().saturating_mul(1000),
            mark: RecordMark::None,
            publisher: None,
            headers: None,
        })
    }

    fn decode_data(&self, record: LogRecord) -> io::Result<Command> {
        Ok(Command(record.payload.to_vec()))
    }

    fn encode_membership(&self, membership: &Membership) -> io::Result<Bytes> {
        serde_json::to_vec(&wire::Membership::from(membership))
            .map(Bytes::from)
            .map_err(io::Error::other)
    }

    fn decode_membership(&self, payload: &[u8]) -> io::Result<Membership> {
        serde_json::from_slice::<wire::Membership>(payload)
            .map(Membership::from)
            .map_err(io::Error::other)
    }

    fn encode_node_id(&self, node_id: &u64) -> String {
        node_id.to_string()
    }

    fn decode_node_id(&self, leader: &str) -> io::Result<u64> {
        leader.parse().map_err(io::Error::other)
    }
}

/// Bookkeeping the snapshot must capture atomically with the app state.
struct Applied {
    last_applied: Option<LogId>,
    membership: StoredMembership,
}

struct SmInner {
    snapshots: SnapshotStore,
    app: Arc<dyn AppStateMachine>,
    /// One lock over the bookkeeping *and* every app mutation: a snapshot
    /// built under it pairs `(last_applied, membership)` with exactly the
    /// app state those describe, never half a batch later. Async because it
    /// is held across the app's own async calls.
    applied: Mutex<Applied>,
    /// The largest snapshot a member accepts. A build past half of it is
    /// logged, long before a follower can no longer be caught up.
    snapshot_max_bytes: u64,
}

/// The state machine adapter: volatile app state, persistent snapshots.
#[derive(Clone)]
pub(super) struct StateMachineStore {
    inner: Arc<SmInner>,
}

impl StateMachineStore {
    /// Rebuild the app state from the stored snapshot, if any. Log entries
    /// after it are replayed by openraft on startup.
    ///
    /// `snapshots` must be a clone of the store the log was opened with:
    /// the log decides what a purge may remove from what this one holds.
    pub(super) async fn open(
        snapshots: SnapshotStore,
        app: Arc<dyn AppStateMachine>,
        snapshot_max_bytes: u64,
    ) -> Result<Self> {
        let mut applied = Applied {
            last_applied: None,
            membership: StoredMembership::default(),
        };
        if let Some(stored) = snapshots.current().await.context("read the snapshot")? {
            let meta = decode_meta(&stored.meta).context("read the snapshot's meta")?;
            app.restore(&stored.data).await;
            applied.last_applied = meta.last_log_id;
            applied.membership = meta.last_membership;
        }
        Ok(Self {
            inner: Arc::new(SmInner {
                snapshots,
                app,
                applied: Mutex::new(applied),
                snapshot_max_bytes,
            }),
        })
    }

    /// Store `meta` and `data` as the current snapshot, in one file. A
    /// snapshot behind the stored one is not written: openraft builds in a
    /// detached task, so a build can finish after a newer install (#1745).
    async fn persist_snapshot(&self, meta: &SnapshotMeta, data: Bytes) -> io::Result<bool> {
        self.inner
            .snapshots
            .install(StoredSnapshot {
                last: meta.last_log_id.map(|id| SnapshotId {
                    term: *id.leader_id,
                    index: id.index,
                }),
                meta: serde_json::to_vec(&wire::SnapshotMeta::from(meta))
                    .map_err(io::Error::other)?
                    .into(),
                data,
            })
            .await
    }

    /// Store a snapshot of what has been applied. openraft purges only what
    /// a stored snapshot covers; its storage suite purges what it has only
    /// applied, and stores this first.
    #[cfg(test)]
    pub(super) async fn store_applied(&self) -> io::Result<()> {
        RaftSnapshotBuilder::build_snapshot(&mut SnapshotBuilder {
            inner: Arc::clone(&self.inner),
        })
        .await
        .map(|_| ())
    }
}

fn decode_meta(bytes: &[u8]) -> io::Result<SnapshotMeta> {
    serde_json::from_slice::<wire::SnapshotMeta>(bytes)
        .map(SnapshotMeta::from)
        .map_err(io::Error::other)
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
        // A received snapshot is read back from its file and checked before
        // anything is replaced: the one whole copy a receive makes.
        let (data, staged) = match snapshot {
            SnapshotData::Staged(mut staged) => (staged.read().await?.data, Some(staged)),
            mut other => (read_all(&mut other).await?, None),
        };
        // Held from the check to the restore, so nothing is applied in
        // between.
        let mut applied = self.inner.applied.lock().await;
        // Openraft only installs past its committed index, so this does not
        // happen. If it did, restoring would move the applied cursor back
        // and the entries in between would be applied twice; failing stops
        // the member instead of letting it diverge. Checked before the file
        // is written, so a refused snapshot is not what a restart finds.
        let applied_index = applied.last_applied.map(|id| id.index);
        if applied_index > meta.last_log_id.map(|id| id.index) {
            return Err(io::Error::other(format!(
                "snapshot at {:?} is behind the applied state at {:?}",
                meta.last_log_id, applied.last_applied
            )));
        }
        // On disk before the live state is replaced, so a restart rebuilds
        // from this snapshot. Not written when the stored one is newer.
        match staged {
            Some(staged) => staged.commit().await?,
            None => self.persist_snapshot(meta, data.clone()).await?,
        };
        self.inner.app.restore(&data).await;
        applied.last_applied = meta.last_log_id;
        applied.membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, io::Error> {
        let Some(stored) = self.inner.snapshots.open_current().await? else {
            return Ok(None);
        };
        Ok(Some(Snapshot {
            meta: decode_meta(stored.meta())?,
            snapshot: SnapshotData::Stored(stored),
        }))
    }
}

/// The whole of a snapshot's data.
async fn read_all(snapshot: &mut SnapshotData) -> io::Result<Bytes> {
    if let SnapshotData::Memory(data) = snapshot {
        return Ok(std::mem::take(data));
    }
    let mut data = Vec::new();
    while let Some(chunk) = snapshot.next_chunk(transfer::CHUNK).await? {
        data.extend_from_slice(&chunk);
    }
    Ok(data.into())
}

/// Past the cap no follower can be caught up by a snapshot, so a build
/// past half of it is an error to see long before then.
fn note_size(data_len: usize, max: u64) {
    metrics::gauge!("felix_meta_raft_snapshot_bytes").set(data_len as f64);
    if data_len as u64 > max / 2 {
        tracing::error!(
            bytes = data_len,
            max,
            "the metadata snapshot is past half the largest a member accepts; raise \
             FELIX_RAFT_SNAPSHOT_MAX_BYTES on every member before it gets there"
        );
    }
}

pub(super) struct SnapshotBuilder {
    inner: Arc<SmInner>,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    type SnapshotData = SnapshotData;

    async fn build_snapshot(&mut self) -> Result<Snapshot, io::Error> {
        // The state and its id are taken under the apply lock. The file is
        // written outside it, so applying goes on meanwhile.
        let (data, meta) = {
            let applied = self.inner.applied.lock().await;
            let data = Bytes::from(self.inner.app.snapshot().await);
            let meta = SnapshotMeta {
                last_log_id: applied.last_applied,
                last_membership: applied.membership.clone(),
            };
            (data, meta)
        };
        let store = StateMachineStore {
            inner: Arc::clone(&self.inner),
        };
        note_size(data.len(), self.inner.snapshot_max_bytes);
        // Skipped when a newer snapshot was installed meanwhile; openraft
        // drops this older one when it sees it.
        store.persist_snapshot(&meta, data.clone()).await?;
        Ok(Snapshot {
            meta,
            snapshot: SnapshotData::Memory(data),
        })
    }
}

#[cfg(test)]
mod tests;
