use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use felix_storage::metadata_store::{Start, decide_start};
use felix_storage::raft_log::LogReader;
use openraft::storage::{IOFlushed, LogState, RaftLogReader};
use openraft::testing::log::{StoreBuilder, Suite};
use openraft::type_config::TypeConfigExt;

use super::*;
use crate::raft::types::{Entry, Vote, test_log_id};
use crate::raft::{InitialClusterState, PeerSecurity, RaftHandle, RaftSettings};

const CLUSTER: &str = "test-cluster";

/// Keeps what it was last restored from, and snapshots as those bytes.
#[derive(Default)]
struct BytesApp(std::sync::Mutex<Vec<u8>>);

#[async_trait::async_trait]
impl AppStateMachine for BytesApp {
    async fn apply(&self, _command: &[u8]) -> Vec<u8> {
        Vec::new()
    }
    async fn snapshot(&self) -> Vec<u8> {
        self.0.lock().expect("state").clone()
    }
    async fn restore(&self, snapshot: &[u8]) {
        *self.0.lock().expect("state") = snapshot.to_vec();
    }
}

fn settings(dir: &std::path::Path) -> RaftSettings {
    let mut settings = RaftSettings::new(
        1,
        dir.into(),
        PeerSecurity {
            cluster_id: CLUSTER.to_string(),
            token: Some("0123456789abcdef0123456789abcdef".to_string()),
            tls: None,
        },
    );
    settings.heartbeat_interval = std::time::Duration::from_millis(50);
    settings.election_timeout = (
        std::time::Duration::from_millis(150),
        std::time::Duration::from_millis(300),
    );
    settings
}

async fn open(dir: &std::path::Path) -> Store {
    Store::open(dir, CLUSTER, Codec, "test")
        .await
        .expect("open store")
}

const MAX: u64 = crate::raft::DEFAULT_SNAPSHOT_MAX_BYTES;

fn meta(index: u64) -> SnapshotMeta {
    SnapshotMeta {
        last_log_id: Some(test_log_id(1, index)),
        last_membership: StoredMembership::default(),
    }
}

fn names(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .expect("read dir")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .into_string()
                .expect("name")
        })
        .collect();
    names.sort();
    names
}

/// The log and the state machine as openraft's suite wants them: one type
/// that is the log and its reader.
///
/// The suite purges what it has only applied, where openraft purges what a
/// stored snapshot covers. So each purge here first stores the snapshot of
/// what was applied, as it is under openraft.
struct SuiteLog {
    log: LogStore,
    machine: StateMachineStore,
}

impl RaftLogReader<TypeConfig> for SuiteLog {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> io::Result<Vec<Entry>>
    where
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        self.log.reader().try_get_log_entries(range).await
    }

    async fn limited_get_log_entries(&mut self, start: u64, end: u64) -> io::Result<Vec<Entry>> {
        self.log.reader().limited_get_log_entries(start, end).await
    }

    async fn read_vote(&mut self) -> io::Result<Option<Vote>> {
        self.log.reader().read_vote().await
    }
}

impl RaftLogStorage<TypeConfig> for SuiteLog {
    type LogReader = LogReader<TypeConfig, Codec>;

    async fn get_log_state(&mut self) -> io::Result<LogState<TypeConfig>> {
        self.log.get_log_state().await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.log.reader()
    }

    async fn save_vote(&mut self, vote: &Vote) -> io::Result<()> {
        self.log.save_vote(vote).await
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> io::Result<()> {
        self.log.save_committed(committed).await
    }

    async fn read_committed(&mut self) -> io::Result<Option<LogId>> {
        self.log.read_committed().await
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>) -> io::Result<()>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.log.append(entries, callback).await
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> io::Result<()> {
        self.log.truncate_after(last_log_id).await
    }

    async fn purge(&mut self, log_id: LogId) -> io::Result<()> {
        self.machine.store_applied().await?;
        self.log.purge(log_id).await
    }
}

struct Builder;

impl StoreBuilder<TypeConfig, SuiteLog, StateMachineStore, (tempfile::TempDir, Store)> for Builder {
    async fn build(
        &self,
    ) -> Result<
        ((tempfile::TempDir, Store), SuiteLog, StateMachineStore),
        openraft::StorageError<TypeConfig>,
    > {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = open(dir.path()).await;
        let machine = StateMachineStore::open(
            store.snapshots().clone(),
            Arc::new(BytesApp::default()),
            MAX,
        )
        .await
        .expect("open state machine");
        let log = SuiteLog {
            log: store.log(),
            machine: machine.clone(),
        };
        Ok(((dir, store), log, machine))
    }
}

/// openraft's own storage suite over the metadata store as the control
/// plane opens it: purges hidden past the
/// snapshot, the append rule on, and the state machine on the snapshot file.
#[test]
fn satisfies_openrafts_storage_contract() {
    TypeConfig::run(async {
        Suite::test_all(Builder).await.expect("storage suite");
    });
}

/// A node campaigns on its own timer, so the mark must be on disk before
/// there is a node: when the open returns.
#[tokio::test]
async fn the_started_mark_is_on_disk_before_the_node_is_made() {
    let dir = tempfile::tempdir().expect("tempdir");
    let opened = open_member(&settings(dir.path()), Arc::new(BytesApp::default()))
        .await
        .expect("open");
    match decide_start(dir.path()).expect("decide") {
        Start::Existing(member) => assert!(member.started, "{member:?}"),
        Start::New => panic!("no store"),
    }
    opened.member.close().await.expect("close");
}

/// A crash between the started mark and the first vote leaves a started
/// member with a blank log. Nothing says whether it voted, so it enters the
/// group like a member with an empty store: without a vote.
#[tokio::test]
async fn a_started_member_with_no_raft_state_withholds_its_vote() {
    let dir = tempfile::tempdir().expect("tempdir");
    let app = || Arc::new(BytesApp::default());
    let handle = RaftHandle::start(settings(dir.path()), app())
        .await
        .expect("first start");
    handle.shutdown().await.expect("shutdown");
    match decide_start(dir.path()).expect("decide") {
        Start::Existing(member) => assert!(member.started && !member.vote_withheld),
        Start::New => panic!("no store"),
    }

    let handle = RaftHandle::start(settings(dir.path()), app())
        .await
        .expect("second start");
    assert!(handle.may_vote.load(Ordering::SeqCst));
    // Nothing listens on these, so the member keeps asking.
    let peers: BTreeMap<u64, String> = (1..=3).map(|id| (id, format!("127.0.0.1:{id}"))).collect();
    let shutdown = tokio_util::sync::CancellationToken::new();
    handle
        .enter_group(peers, InitialClusterState::Existing, shutdown.clone())
        .await
        .expect("enter group");
    assert!(!handle.may_vote.load(Ordering::SeqCst), "may vote");
    shutdown.cancel();
    handle.shutdown().await.expect("shutdown");
    match decide_start(dir.path()).expect("decide") {
        Start::Existing(member) => assert!(member.vote_withheld, "not on disk"),
        Start::New => panic!("no store"),
    }
}

/// The start rule reads names only: a redb file with no store beside it is
/// refused before anything is written.
#[tokio::test]
async fn a_redb_file_beside_no_store_refuses_the_start_and_writes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("raft.redb"), b"an older release's store").expect("write");
    let err = RaftHandle::start(settings(dir.path()), Arc::new(BytesApp::default()))
        .await
        .err()
        .expect("refused");
    assert!(format!("{err:#}").contains("not converted"), "{err:#}");
    assert_eq!(names(dir.path()), ["raft.redb"]);
}

/// The same for a store the conversion did not build.
#[tokio::test]
async fn a_redb_file_beside_an_unconverted_store_refuses_the_start() {
    let dir = tempfile::tempdir().expect("tempdir");
    open(dir.path()).await.shutdown().await.expect("shutdown");
    std::fs::write(dir.path().join("raft.redb"), b"an older release's store").expect("write");
    let err = RaftHandle::start(settings(dir.path()), Arc::new(BytesApp::default()))
        .await
        .err()
        .expect("refused");
    assert!(format!("{err:#}").contains("not converted"), "{err:#}");
    match decide_start(dir.path()) {
        Err(_) => {}
        Ok(start) => panic!("{start:?}"),
    }
}

/// An installed snapshot is what a restart rebuilds from, and its data is
/// stored as the bytes the app gave.
#[tokio::test]
async fn an_installed_snapshot_is_restored_at_the_next_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = open(dir.path()).await;
    let mut machine = StateMachineStore::open(
        store.snapshots().clone(),
        Arc::new(BytesApp::default()),
        MAX,
    )
    .await
    .expect("open");
    machine
        .install_snapshot(
            &meta(7),
            SnapshotData::Memory(Bytes::from_static(b"state at seven")),
        )
        .await
        .expect("install");
    let stored = store
        .snapshots()
        .current()
        .await
        .expect("read")
        .expect("stored");
    assert_eq!(stored.data.as_ref(), b"state at seven");
    drop(machine);
    store.shutdown().await.expect("shutdown");

    let store = open(dir.path()).await;
    let app = Arc::new(BytesApp::default());
    let mut machine =
        StateMachineStore::open(store.snapshots().clone(), Arc::clone(&app) as _, MAX)
            .await
            .expect("reopen");
    assert_eq!(*app.0.lock().expect("state"), b"state at seven");
    let (applied, _) = machine.applied_state().await.expect("applied");
    assert_eq!(applied, meta(7).last_log_id);
    let current = machine
        .get_current_snapshot()
        .await
        .expect("read")
        .expect("snapshot");
    assert_eq!(current.meta, meta(7));
    let mut data = current.snapshot;
    assert_eq!(
        data.next_chunk(1024).await.expect("read").as_deref(),
        Some(&b"state at seven"[..])
    );
    assert_eq!(data.next_chunk(1024).await.expect("read"), None);
    store.shutdown().await.expect("shutdown");
}

/// A snapshot received from a peer is installed from its own file: it is
/// the stored snapshot and the live state after, and no receive file is
/// left.
#[tokio::test]
async fn a_received_snapshot_is_installed_from_its_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = open(dir.path()).await;
    let app = Arc::new(BytesApp::default());
    let mut machine =
        StateMachineStore::open(store.snapshots().clone(), Arc::clone(&app) as _, MAX)
            .await
            .expect("open");
    let meta_bytes = serde_json::to_vec(&wire::SnapshotMeta::from(&meta(7))).expect("meta");
    let mut staged = store
        .snapshots()
        .begin_receive(
            Some(SnapshotId { term: 1, index: 7 }),
            meta_bytes.into(),
            14,
        )
        .await
        .expect("begin");
    staged
        .write(Bytes::from_static(b"state at seven"))
        .await
        .expect("write");
    staged.finish().await.expect("finish");
    machine
        .install_snapshot(&meta(7), SnapshotData::Staged(staged))
        .await
        .expect("install");
    assert_eq!(*app.0.lock().expect("state"), b"state at seven");
    let current = machine
        .get_current_snapshot()
        .await
        .expect("read")
        .expect("snapshot");
    assert_eq!(current.meta, meta(7));
    assert!(
        !dir.path()
            .join(STORE_DIR)
            .join("raft-snapshot.recv")
            .exists()
    );
    store.shutdown().await.expect("shutdown");
}

/// A snapshot behind the applied state would apply the entries in between
/// twice. It is refused before the file is written, so it is not what a
/// restart finds either.
#[tokio::test]
async fn a_snapshot_behind_the_applied_state_is_refused_and_not_stored() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = open(dir.path()).await;
    let app = Arc::new(BytesApp::default());
    let mut machine =
        StateMachineStore::open(store.snapshots().clone(), Arc::clone(&app) as _, MAX)
            .await
            .expect("open");
    let entries = (0..=5).map(|index| {
        Ok((
            Entry {
                log_id: test_log_id(1, index),
                payload: EntryPayload::Blank,
            },
            None,
        ))
    });
    machine
        .apply(futures_util::stream::iter(entries))
        .await
        .expect("apply");

    machine
        .install_snapshot(
            &meta(3),
            SnapshotData::Memory(Bytes::from_static(b"behind")),
        )
        .await
        .expect_err("behind the applied state");
    assert_eq!(store.snapshots().current().await.expect("read"), None);
    assert!(app.0.lock().expect("state").is_empty(), "restored");
    let (applied, _) = machine.applied_state().await.expect("applied");
    assert_eq!(applied, Some(test_log_id(1, 5)));
    store.shutdown().await.expect("shutdown");
}

/// A build that finishes after a newer install is dropped: the stored
/// snapshot never goes back.
#[tokio::test]
async fn a_build_does_not_replace_a_newer_stored_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = open(dir.path()).await;
    let machine = StateMachineStore::open(
        store.snapshots().clone(),
        Arc::new(BytesApp::default()),
        MAX,
    )
    .await
    .expect("open");
    assert!(
        machine
            .persist_snapshot(&meta(9), Bytes::from_static(b"newer"))
            .await
            .expect("store")
    );
    assert!(
        !machine
            .persist_snapshot(&meta(4), Bytes::from_static(b"older"))
            .await
            .expect("store")
    );
    let stored = store
        .snapshots()
        .current()
        .await
        .expect("read")
        .expect("stored");
    assert_eq!(stored.data.as_ref(), b"newer");
    store.shutdown().await.expect("shutdown");
}
