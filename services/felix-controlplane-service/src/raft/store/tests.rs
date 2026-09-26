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
