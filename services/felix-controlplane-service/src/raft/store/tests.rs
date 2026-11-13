use openraft::testing::log::{StoreBuilder, Suite};
use openraft::type_config::TypeConfigExt;

use super::*;

/// A state machine that remembers nothing; the storage suite never
/// applies app commands (openraft cannot construct a `C::D`), it
/// exercises logs, votes, membership, and snapshots.
struct NullApp;

#[async_trait::async_trait]
impl AppStateMachine for NullApp {
    async fn apply(&self, _command: &[u8]) -> Vec<u8> {
        Vec::new()
    }
    async fn snapshot(&self) -> Vec<u8> {
        Vec::new()
    }
    async fn restore(&self, _snapshot: &[u8]) {}
}

struct Builder;

/// Keeps the store's directory alive for the duration of one suite case.
struct Guard(#[allow(dead_code)] tempfile::TempDir);

impl StoreBuilder<TypeConfig, LogStore, StateMachineStore, Guard> for Builder {
    async fn build(
        &self,
    ) -> Result<(Guard, LogStore, StateMachineStore), openraft::StorageError<TypeConfig>> {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = open(&dir.path().join("raft.redb")).expect("open store");
        let log_store = LogStore::new(Arc::clone(&db));
        let state_machine = StateMachineStore::open(db, Arc::new(NullApp))
            .await
            .expect("open state machine");
        Ok((Guard(dir), log_store, state_machine))
    }
}

/// openraft's own storage conformance suite, run against the redb
/// store — the same idea as running the node/shard contract suites
/// against every metadata backend: the trait's semantics are the
/// author's tests, not ours to re-invent.
#[test]
fn satisfies_openrafts_storage_contract() {
    TypeConfig::run(async {
        Suite::test_all(Builder).await.expect("storage suite");
    });
}

/// The one property the suite cannot see: state survives closing and
/// reopening the file. A vote that does not is a double vote waiting
/// for a crash.
#[test]
fn vote_and_purge_marker_survive_reopen() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("raft.redb");

        let vote = Vote::new(7, 3);
        let purged = LogId::new(leader(7, 3), 11);
        {
            let db = open(&path).expect("open");
            let mut store = LogStore::new(db);
            store.save_vote(&vote).await.expect("save vote");
            store.purge(purged).await.expect("purge");
        }

        let db = open(&path).expect("reopen");
        let mut store = LogStore::new(db);
        assert_eq!(store.read_vote().await.expect("read vote"), Some(vote));
        let state = store.get_log_state().await.expect("log state");
        assert_eq!(state.last_purged_log_id, Some(purged));
        assert_eq!(state.last_log_id, Some(purged));
    });
}

#[test]
fn a_data_dir_keeps_the_cluster_id_it_first_saw() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("raft.redb");
    {
        let db = open(&path).expect("open store");
        claim_cluster_id(&db, "prod-a").expect("first claim records the id");
        claim_cluster_id(&db, "prod-a").expect("the same id again is fine");
    }
    let db = open(&path).expect("reopen store");
    let err = claim_cluster_id(&db, "prod-b").expect_err("another cluster's id is refused");
    assert!(err.to_string().contains("prod-a"), "{err}");
}

fn leader(term: u64, node_id: u64) -> super::super::types::LeaderId {
    super::super::types::LeaderId { term, node_id }
}

fn log_id(term: u64, index: u64) -> LogId {
    LogId::new(leader(term, 1), index)
}

fn snapshot_meta(last: LogId) -> SnapshotMeta {
    SnapshotMeta {
        last_log_id: Some(last),
        last_membership: StoredMembership::default(),
    }
}

fn current_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

/// A state machine holding one opaque value, whose `snapshot` can be made to
/// stop until the test lets it go. That pause is the window between a build
/// capturing its state and persisting it.
#[derive(Default)]
struct HeldApp {
    state: std::sync::Mutex<Vec<u8>>,
    hold: std::sync::Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}

impl HeldApp {
    /// The next `snapshot` signals the first returned channel once it has
    /// captured its state, then waits for the second.
    fn hold_next_snapshot(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *self.hold.lock().unwrap() = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }

    fn state(&self) -> Vec<u8> {
        self.state.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl AppStateMachine for HeldApp {
    async fn apply(&self, _command: &[u8]) -> Vec<u8> {
        Vec::new()
    }
    async fn snapshot(&self) -> Vec<u8> {
        let data = self.state();
        let hold = self.hold.lock().unwrap().take();
        if let Some((entered, release)) = hold {
            let _ = entered.send(());
            let _ = release.await;
        }
        data
    }
    async fn restore(&self, snapshot: &[u8]) {
        *self.state.lock().unwrap() = snapshot.to_vec();
    }
}

async fn install(store: &mut StateMachineStore, last: LogId, data: &[u8]) -> Result<(), io::Error> {
    store
        .install_snapshot(&snapshot_meta(last), Cursor::new(data.to_vec()))
        .await
}

async fn stored_snapshot(store: &mut StateMachineStore) -> (Option<LogId>, Vec<u8>) {
    let snapshot = store
        .get_current_snapshot()
        .await
        .expect("read snapshot")
        .expect("a snapshot is stored");
    (snapshot.meta.last_log_id, snapshot.snapshot.into_inner())
}

/// Waits, bounded, until the store's snapshot is the one at `index`.
async fn until_stored(db: &Database, index: u64) {
    for _ in 0..1000 {
        let meta: Option<wire::SnapshotMeta> = read_meta(db, KEY_SNAPSHOT_META).expect("read meta");
        if meta.and_then(|meta| meta.last_log_id).map(|id| id.index) == Some(index) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("snapshot at {index} was never stored");
}

/// #1745: openraft builds snapshots in a detached task, so a build that
/// captured its state before an install of a newer snapshot can finish
/// after it. The stored snapshot must stay the newer one, and so must what
/// a restart loads once the log is purged behind it.
#[test]
fn a_build_that_finishes_after_a_newer_install_keeps_the_install() {
    current_thread().block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("raft.redb");
        let older = log_id(1, 5);
        let newer = log_id(2, 10);
        {
            let db = open(&path).expect("open");
            let app = Arc::new(HeldApp::default());
            let mut store = StateMachineStore::open(Arc::clone(&db), app.clone())
                .await
                .expect("open state machine");
            install(&mut store, older, b"older")
                .await
                .expect("first install");

            let (entered, release) = app.hold_next_snapshot();
            let mut builder = store.get_snapshot_builder().await;
            let build = tokio::spawn(async move { builder.build_snapshot().await });
            entered.await.expect("build captured its state");

            let mut installer = store.clone();
            let install_task =
                tokio::spawn(async move { install(&mut installer, newer, b"newer").await });
            until_stored(&db, newer.index).await;

            release.send(()).expect("release the build");
            let built = build.await.expect("build task").expect("build");
            assert_eq!(
                built.meta.last_log_id,
                Some(older),
                "the build captured the older state"
            );
            install_task.await.expect("install task").expect("install");

            assert_eq!(
                stored_snapshot(&mut store).await,
                (Some(newer), b"newer".to_vec())
            );
            let (applied, _) = store.applied_state().await.expect("applied state");
            assert_eq!(applied, Some(newer));
            assert_eq!(app.state(), b"newer");

            // Openraft purges the log behind an installed snapshot.
            LogStore::new(db).purge(newer).await.expect("purge");
        }

        // A restart must resume from a snapshot at or past the purge point,
        // or log replay would ask for entries that are gone.
        let db = open(&path).expect("reopen");
        let purged = LogStore::new(Arc::clone(&db))
            .get_log_state()
            .await
            .expect("log state")
            .last_purged_log_id;
        let app = Arc::new(HeldApp::default());
        let mut store = StateMachineStore::open(db, app.clone())
            .await
            .expect("reopen state machine");
        let (applied, _) = store.applied_state().await.expect("applied state");
        assert!(
            applied >= purged,
            "restart resumes at {applied:?}, behind the purge at {purged:?}"
        );
        assert_eq!(applied, Some(newer));
        assert_eq!(app.state(), b"newer");
    });
}

/// The other order: the build persists first and the newer install still
/// replaces it.
#[test]
fn an_install_after_a_build_replaces_it() {
    current_thread().block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = open(&dir.path().join("raft.redb")).expect("open");
        let app = Arc::new(HeldApp::default());
        let mut store = StateMachineStore::open(db, app.clone())
            .await
            .expect("open state machine");
        install(&mut store, log_id(1, 5), b"older")
            .await
            .expect("first install");

        let built = store
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .expect("build");
        assert_eq!(built.meta.last_log_id, Some(log_id(1, 5)));
        install(&mut store, log_id(2, 10), b"newer")
            .await
            .expect("install");

        assert_eq!(
            stored_snapshot(&mut store).await,
            (Some(log_id(2, 10)), b"newer".to_vec())
        );
        assert_eq!(app.state(), b"newer");
    });
}

/// An install never moves the applied cursor backwards: replaying from an
/// older point would apply the entries in between a second time.
#[test]
fn an_install_older_than_the_applied_state_is_refused() {
    current_thread().block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = open(&dir.path().join("raft.redb")).expect("open");
        let app = Arc::new(HeldApp::default());
        let mut store = StateMachineStore::open(db, app.clone())
            .await
            .expect("open state machine");
        install(&mut store, log_id(2, 10), b"newer")
            .await
            .expect("install");

        install(&mut store, log_id(1, 5), b"older")
            .await
            .expect_err("an install behind the applied state is refused");

        let (applied, _) = store.applied_state().await.expect("applied state");
        assert_eq!(applied, Some(log_id(2, 10)));
        assert_eq!(app.state(), b"newer");
        assert_eq!(
            stored_snapshot(&mut store).await,
            (Some(log_id(2, 10)), b"newer".to_vec())
        );
    });
}

/// A crash before the snapshot transaction commits leaves the previous
/// snapshot whole: meta and data are one redb commit, so there is no point
/// where one is written and the other is not.
#[test]
fn a_crash_before_the_snapshot_commit_keeps_the_previous_snapshot() {
    current_thread().block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("raft.redb");
        {
            let db = open(&path).expect("open");
            let mut store = StateMachineStore::open(Arc::clone(&db), Arc::new(HeldApp::default()))
                .await
                .expect("open state machine");
            install(&mut store, log_id(1, 5), b"older")
                .await
                .expect("install");

            // The write a crash interrupts: staged, never committed.
            let txn = db.begin_write().expect("begin");
            {
                let mut table = txn.open_table(META).expect("meta table");
                let meta = serde_json::to_vec(&snapshot_meta(log_id(2, 10))).expect("encode");
                table
                    .insert(KEY_SNAPSHOT_META, meta.as_slice())
                    .expect("stage meta");
            }
            drop(txn);
        }

        let db = open(&path).expect("reopen");
        let app = Arc::new(HeldApp::default());
        let mut store = StateMachineStore::open(db, app.clone())
            .await
            .expect("reopen state machine");
        assert_eq!(
            stored_snapshot(&mut store).await,
            (Some(log_id(1, 5)), b"older".to_vec())
        );
        assert_eq!(app.state(), b"older");
    });
}

fn fixture(name: &str) -> Vec<u8> {
    crate::raft::wire::tests::fixture(name)
}

fn raw_meta(db: &Database, key: &str) -> Vec<u8> {
    let txn = db.begin_read().expect("read");
    let table = txn.open_table(META).expect("meta table");
    let value = table.get(key).expect("get").expect("the key is stored");
    value.value().to_vec()
}

fn raw_log(db: &Database, index: u64) -> Vec<u8> {
    let txn = db.begin_read().expect("read");
    let table = txn.open_table(LOGS).expect("log table");
    let value = table.get(index).expect("get").expect("the entry is stored");
    value.value().to_vec()
}

const ENTRY_FIXTURES: [(u64, &str); 4] = [
    (16, "stored_entry_blank"),
    (17, "stored_entry_normal"),
    (18, "stored_entry_membership"),
    (19, "stored_entry_joint_membership"),
];

/// A member rolled back to a release that ran openraft 0.9 opens the store
/// this build wrote. So what is written must be what 0.9 wrote, byte for
/// byte, snapshot id included.
#[test]
fn the_store_writes_the_bytes_0_9_wrote() {
    current_thread().block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = open(&dir.path().join("raft.redb")).expect("open");
        let mut log = LogStore::new(Arc::clone(&db));

        let vote: wire::Vote = serde_json::from_slice(&fixture("stored_vote")).expect("vote");
        log.save_vote(&vote.into()).await.expect("save vote");
        assert_eq!(raw_meta(&db, KEY_VOTE), fixture("stored_vote"));

        for (index, name) in ENTRY_FIXTURES {
            let entry: wire::Entry = serde_json::from_slice(&fixture(name)).expect("entry");
            log.append([entry.into()], IOFlushed::Noop)
                .await
                .expect("append");
            assert_eq!(raw_log(&db, index), fixture(name), "{name}");
        }

        let committed: Option<wire::LogId> =
            serde_json::from_slice(&fixture("stored_committed")).expect("committed");
        log.save_committed(wire::lib_log_id(committed))
            .await
            .expect("save committed");
        assert_eq!(raw_meta(&db, KEY_COMMITTED), fixture("stored_committed"));

        let purged: wire::LogId =
            serde_json::from_slice(&fixture("stored_last_purged")).expect("purged");
        log.purge(purged.into()).await.expect("purge");
        assert_eq!(
            raw_meta(&db, KEY_LAST_PURGED),
            fixture("stored_last_purged")
        );

        // The snapshot meta differs from 0.9's in the id alone: 0.9 made one
        // up, this build derives it from the position.
        let theirs: wire::SnapshotMeta =
            serde_json::from_slice(&fixture("stored_snapshot_meta")).expect("meta");
        let mut machine = StateMachineStore::open(Arc::clone(&db), Arc::new(NullApp))
            .await
            .expect("open state machine");
        machine
            .install_snapshot(&theirs.clone().into(), Cursor::new(vec![1, 2]))
            .await
            .expect("install");
        let ours: wire::SnapshotMeta =
            serde_json::from_slice(&raw_meta(&db, KEY_SNAPSHOT_META)).expect("stored meta");
        assert_eq!(ours.snapshot_id, "3-2-18");
        assert_eq!(
            wire::SnapshotMeta {
                snapshot_id: theirs.snapshot_id.clone(),
                ..ours
            },
            theirs
        );
    });
}

/// The other direction: a member upgraded from a release that ran 0.9 opens
/// the store that release wrote.
#[test]
fn the_store_reads_what_0_9_wrote() {
    current_thread().block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = open(&dir.path().join("raft.redb")).expect("open");
        {
            let txn = db.begin_write().expect("write");
            {
                let mut logs = txn.open_table(LOGS).expect("log table");
                for (index, name) in ENTRY_FIXTURES {
                    logs.insert(index, fixture(name).as_slice())
                        .expect("insert");
                }
                let mut meta = txn.open_table(META).expect("meta table");
                let data = serde_json::to_vec(&vec![1u8, 2]).expect("data");
                for (key, bytes) in [
                    (KEY_VOTE, fixture("stored_vote")),
                    (KEY_COMMITTED, fixture("stored_committed")),
                    (KEY_SNAPSHOT_META, fixture("stored_snapshot_meta")),
                    (KEY_SNAPSHOT_DATA, data),
                ] {
                    meta.insert(key, bytes.as_slice()).expect("insert");
                }
            }
            txn.commit().expect("commit");
        }

        let mut log = LogStore::new(Arc::clone(&db));
        let vote = log.read_vote().await.expect("read vote").expect("a vote");
        assert_eq!(
            serde_json::to_vec(&wire::Vote::from(&vote)).expect("vote"),
            fixture("stored_vote")
        );
        assert_eq!(persisted_committed_index(&db), Some(17));
        let entries = log.try_get_log_entries(16..20).await.expect("read entries");
        assert_eq!(entries.len(), 4);
        for (entry, (_, name)) in entries.iter().zip(ENTRY_FIXTURES) {
            assert_eq!(
                serde_json::to_vec(&wire::Entry::from(entry)).expect("entry"),
                fixture(name),
                "{name}"
            );
        }

        let mut machine = StateMachineStore::open(Arc::clone(&db), Arc::new(NullApp))
            .await
            .expect("open state machine");
        let (applied, membership) = machine.applied_state().await.expect("applied state");
        assert_eq!(applied.map(|id| id.index), Some(18));
        assert_eq!(membership.voter_ids().collect::<Vec<_>>(), vec![1, 2, 3]);
        let (last, data) = stored_snapshot(&mut machine).await;
        assert_eq!(last.map(|id| id.index), Some(18));
        assert_eq!(data, vec![1, 2]);
    });
}

fn settings(dir: &Path) -> super::super::RaftSettings {
    super::super::RaftSettings::new(
        1,
        dir.into(),
        super::super::PeerSecurity {
            cluster_id: "test-cluster".to_string(),
            token: None,
            tls: None,
        },
    )
}

/// A data dir as a segment-store build leaves it: `store/` with its member
/// file, and no `raft.redb`.
fn segment_store_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("store/log")).expect("store dir");
    std::fs::write(dir.path().join("store/member"), b"member").expect("member file");
    dir
}

#[test]
fn a_segment_store_data_dir_is_refused_before_anything_is_written() {
    let dir = segment_store_dir();
    let err = current_thread()
        .block_on(open_member(&settings(dir.path()), Arc::new(NullApp)))
        .err()
        .expect("a segment store is refused");
    assert!(err.to_string().contains("felix_segment_store"), "{err}");
    assert!(
        !dir.path().join("raft.redb").exists(),
        "the refusal must not create an empty redb store"
    );
}

#[test]
fn form_without_refuses_a_segment_store_data_dir() {
    let dir = segment_store_dir();
    let err = current_thread()
        .block_on(stopped_member_holds_state(dir.path(), "test-cluster"))
        .expect_err("a segment store is not an empty member");
    assert!(err.to_string().contains("felix_segment_store"), "{err}");
    assert!(!dir.path().join("raft.redb").exists());
}
