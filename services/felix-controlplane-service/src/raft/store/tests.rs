use openraft::testing::{StoreBuilder, Suite};

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
    async fn build(&self) -> Result<(Guard, LogStore, StateMachineStore), StorageError> {
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
    Suite::test_all(Builder).expect("storage suite");
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

        let vote = openraft::Vote::new(7, 3);
        let purged = LogId::new(openraft::CommittedLeaderId::new(7, 3), 11);
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

fn log_id(term: u64, index: u64) -> LogId {
    LogId::new(openraft::CommittedLeaderId::new(term, 1), index)
}

fn snapshot_meta(last: LogId) -> SnapshotMeta {
    SnapshotMeta {
        last_log_id: Some(last),
        last_membership: StoredMembership::default(),
        snapshot_id: format!("{}-test", last.index),
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

async fn install(
    store: &mut StateMachineStore,
    last: LogId,
    data: &[u8],
) -> Result<(), StorageError> {
    store
        .install_snapshot(&snapshot_meta(last), Box::new(Cursor::new(data.to_vec())))
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
        let meta: Option<SnapshotMeta> = read_meta(db, KEY_SNAPSHOT_META).expect("read meta");
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
