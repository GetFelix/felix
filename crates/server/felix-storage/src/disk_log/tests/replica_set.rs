//! The replica set through the log: newer sets replace older ones and never
//! the reverse, a kept set survives a restart, and the open reconciles a set
//! ahead of the ballot.
use super::*;
use crate::StorageId;

fn set_at(generation: u64) -> ReplicaSet {
    ReplicaSet {
        current: Membership {
            generation,
            members: ["a", "b", "c"]
                .iter()
                .enumerate()
                .map(|(i, node)| Member {
                    node: (*node).to_string(),
                    storage: Some(StorageId::from_bytes([i as u8 + 1; 16])),
                    learner: false,
                })
                .collect(),
        },
        leaving: None,
    }
}

#[tokio::test]
async fn a_set_is_raised_only_by_a_newer_one() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.set_generation(), 0);
    assert_eq!(log.replica_set(), None);
    assert_eq!(
        log.accept_set(set_at(5)).await.expect("accept"),
        SetCheck::Raised
    );
    assert_eq!(
        log.accept_set(set_at(5)).await.expect("accept"),
        SetCheck::Current
    );
    assert_eq!(
        log.accept_set(set_at(3)).await.expect("accept"),
        SetCheck::Stale { known: 5 }
    );
    assert_eq!(log.set_generation(), 5);
    assert_eq!(log.replica_set(), Some(set_at(5)));
    // The stale set wrote nothing.
    assert_eq!(set::load(dir.path()).expect("load"), Some(set_at(5)));
}

#[tokio::test]
async fn an_accepted_set_survives_a_restart() {
    let dir = tempdir().expect("tempdir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.accept_generation(4, Some("a")).await.expect("ballot");
        log.accept_set(set_at(4)).await.expect("accept");
    }
    // Reopened without a shutdown: the set was on disk before it returned.
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.set_generation(), 4);
    assert_eq!(log.replica_set(), Some(set_at(4)));
    assert_eq!(log.accepted_generation(), 4);
    assert_eq!(log.accepted_leader().as_deref(), Some("a"));
}

#[tokio::test]
async fn a_set_newer_than_the_ballot_raises_the_accepted_generation_at_open() {
    let dir = tempdir().expect("tempdir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.accept_generation(3, Some("a")).await.expect("ballot");
    }
    // The ballot is written first, so this order cannot happen; the open
    // still refuses everything older than the set.
    set::store(dir.path(), &set_at(7)).expect("store");
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.set_generation(), 7);
    assert_eq!(log.accepted_generation(), 7);
    assert_eq!(log.accepted_leader(), None);
    assert_eq!(
        log.accept_generation(5, Some("a")).await.expect("accept"),
        GenerationCheck::Superseded { accepted: 7 }
    );
    // The first leader named at the raised generation is taken.
    assert_eq!(
        log.accept_generation(7, Some("b")).await.expect("accept"),
        GenerationCheck::Current
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sets_leave_the_newer_on_disk() {
    for _ in 0..20 {
        let dir = tempdir().expect("tempdir");
        let log = open(&dir, FsyncMode::OnCommit);
        let (older, newer) = tokio::join!(
            tokio::spawn({
                let log = log.clone();
                async move { log.accept_set(set_at(6)).await }
            }),
            tokio::spawn({
                let log = log.clone();
                async move { log.accept_set(set_at(9)).await }
            }),
        );
        let older = older.expect("join").expect("accept");
        assert!(
            matches!(older, SetCheck::Raised | SetCheck::Stale { known: 9 }),
            "{older:?}"
        );
        assert_eq!(newer.expect("join").expect("accept"), SetCheck::Raised);
        assert_eq!(log.set_generation(), 9);
        drop(log);
        assert_eq!(set::load(dir.path()).expect("load"), Some(set_at(9)));
    }
}

#[tokio::test]
async fn leftover_temporaries_are_swept_at_open_once_a_set_exists() {
    let dir = tempdir().expect("tempdir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.accept_generation(4, Some("a")).await.expect("ballot");
        log.accept_set(set_at(4)).await.expect("accept");
    }
    let leftover = dir.path().join("replica-set.tmp.1.1");
    std::fs::write(&leftover, b"half a set").expect("write");
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.replica_set(), Some(set_at(4)));
    assert!(!leftover.exists());
}

/// A shard that never kept a set does not scan its directory for temporaries.
#[tokio::test]
async fn a_shard_without_a_set_does_not_scan_for_temporaries() {
    let dir = tempdir().expect("tempdir");
    drop(open(&dir, FsyncMode::OnCommit));
    let leftover = dir.path().join("replica-set.tmp.1.1");
    std::fs::write(&leftover, b"half a set").expect("write");
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.replica_set(), None);
    assert!(leftover.exists());
}
