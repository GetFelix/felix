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
                    wiped: false,
                })
                .collect(),
        },
        leaving: None,
        earlier: Vec::new(),
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
    assert_eq!(
        set::load(dir.path()).expect("load"),
        Some((set_at(5), None))
    );
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
    set::store(dir.path(), &set_at(7), None).expect("store");
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
        assert_eq!(
            set::load(dir.path()).expect("load"),
            Some((set_at(9), None))
        );
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

/// Admitting a learner is written before it returns, so a restart cannot
/// bring the mark back, and it changes only that member at the same generation.
#[tokio::test]
async fn admitting_a_learner_is_durable_and_touches_only_it() {
    let dir = tempdir().expect("tempdir");
    let mut set = set_at(4);
    set.current.members[1].learner = true;
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.accept_set(set.clone()).await.expect("accept");
        assert_eq!(log.member_is_learner("b"), Some(true));
        assert!(
            !log.admit_member("a", false).await.expect("admit"),
            "a is no learner"
        );
        assert!(log.admit_member("b", false).await.expect("admit"));
        assert!(!log.admit_member("b", false).await.expect("admit"), "once");
        assert!(
            !log.admit_member("z", false).await.expect("admit"),
            "not a member"
        );
    }
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.replica_set(), Some(set_at(4)));
    assert_eq!(log.set_generation(), 4);
}

/// A wiped mark is kept apart from the learner flag, survives a restart, and
/// outlives an admission that is not settled.
#[tokio::test]
async fn a_wiped_mark_is_kept_until_settled() {
    let dir = tempdir().expect("tempdir");
    let mut set = set_at(4);
    set.current.members[1].learner = true;
    set.current.members[1].wiped = true;
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.accept_set(set).await.expect("accept");
        assert!(log.admit_member("b", false).await.expect("admit"));
    }
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.member_is_learner("b"), Some(false));
    assert!(log.member_is_wiped("b"), "an unsettled admission keeps it");
    assert!(log.admit_member("b", true).await.expect("settle"));
    drop(log);
    let log = open(&dir, FsyncMode::OnCommit);
    assert!(!log.member_is_wiped("b"));
    assert_eq!(log.replica_set(), Some(set_at(4)));
}

/// A newer set keeps a member's wiped mark whatever it says: a leader that
/// writes its route's set must not end it before the copy has settled.
#[tokio::test]
async fn a_newer_set_keeps_the_wiped_mark() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    let mut set = set_at(4);
    set.current.members[1].wiped = true;
    log.accept_set(set).await.expect("accept");
    assert_eq!(
        log.accept_set(set_at(5)).await.expect("accept"),
        SetCheck::Raised
    );
    assert!(log.member_is_wiped("b"));
    assert!(!log.member_is_wiped("a"));
    assert!(log.admit_member("b", true).await.expect("settle"));
    log.accept_set(set_at(6)).await.expect("accept");
    assert!(
        !log.member_is_wiped("b"),
        "settled, so a later set is clean"
    );
}

/// **A set that lists the wiped copy only as leaving, or not at all, does
/// not end its mark**, so a later set naming it again still finds it marked.
/// A move away and back would otherwise hand a voter's place to a copy that
/// still lacks what it lost.
#[tokio::test]
async fn the_wiped_mark_outlives_sets_that_leave_the_copy_out() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    let mut set = set_at(4);
    set.current.members[1].learner = true;
    set.current.members[1].wiped = true;
    log.accept_set(set).await.expect("accept");

    let without_b = |generation| {
        let mut set = set_at(generation);
        set.current.members.retain(|member| member.node != "b");
        set
    };
    let mut moved = without_b(5);
    moved.leaving = Some(set_at(4).current);
    log.accept_set(moved).await.expect("accept");
    assert!(log.member_is_wiped("b"), "listed only as leaving");
    assert_eq!(log.member_is_learner("b"), Some(true));
    log.accept_set(without_b(6)).await.expect("accept");
    assert!(log.member_is_wiped("b"), "not listed at all");

    drop(log);
    let log = open(&dir, FsyncMode::OnCommit);
    log.accept_set(set_at(7)).await.expect("accept");
    assert!(log.member_is_wiped("b"), "named again, after a restart");
    assert_eq!(log.member_is_learner("b"), Some(true));
    assert!(log.replica_set().unwrap().current.members[1].wiped);

    assert!(log.admit_member("b", false).await.expect("admit"));
    assert_eq!(log.member_is_learner("b"), Some(false));
    assert!(log.member_is_wiped("b"), "admitted, not settled");
    assert!(log.admit_member("b", true).await.expect("settle"));
    log.accept_set(without_b(8)).await.expect("accept");
    log.accept_set(set_at(9)).await.expect("accept");
    assert!(!log.member_is_wiped("b"));
}

fn named(generation: u64, nodes: &[&str]) -> Membership {
    Membership {
        generation,
        members: nodes
            .iter()
            .map(|node| Member {
                node: (*node).to_string(),
                storage: None,
                learner: false,
                wiped: false,
            })
            .collect(),
    }
}

fn joint(current: Membership, leaving: Membership) -> ReplicaSet {
    ReplicaSet {
        current,
        leaving: Some(leaving),
        earlier: Vec::new(),
    }
}

/// Two fences in a row that never opened: the second must not overwrite
/// the older set the first was leaving, or a failover from this log would
/// open without the writes only that set holds (#1881).
#[tokio::test]
async fn sets_of_unopened_generations_are_carried_into_a_newer_set() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.accept_set_keeping_earlier(joint(
        named(2, &["b", "c", "d"]),
        named(1, &["a", "b", "c"]),
    ))
    .await
    .expect("accept");
    log.accept_set_keeping_earlier(joint(
        named(3, &["a", "c", "d"]),
        named(2, &["b", "c", "d"]),
    ))
    .await
    .expect("accept");
    let kept = log.replica_set().expect("set");
    assert_eq!(kept.leaving, Some(named(2, &["b", "c", "d"])));
    assert_eq!(kept.earlier, vec![named(1, &["a", "b", "c"])]);
    // On disk before it returned.
    drop(log);
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.replica_set().expect("set").earlier, kept.earlier);

    // Lease churn at the same set changes nothing but the generation.
    log.accept_set_keeping_earlier(joint(
        named(4, &["a", "c", "d"]),
        named(3, &["a", "c", "d"]),
    ))
    .await
    .expect("accept");
    let kept = log.replica_set().expect("set");
    assert_eq!(
        kept.earlier,
        vec![named(2, &["b", "c", "d"]), named(1, &["a", "b", "c"])]
    );
}

/// A record of the kept set's generation is the evidence it opened: the
/// sets before it are dropped, and only then.
#[tokio::test]
async fn earlier_sets_are_dropped_once_the_log_holds_a_record_of_the_kept_generation() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.accept_set_keeping_earlier(joint(
        named(2, &["b", "c", "d"]),
        named(1, &["a", "b", "c"]),
    ))
    .await
    .expect("accept");
    // An epoch with nothing written in it is no evidence.
    log.record_generation(2, 0).expect("generation");
    assert!(!log.holds_records_of(2));
    log.record_generation(3, 0).expect("generation");
    log.append(&records(&["at three"])).await.expect("append");
    assert!(!log.holds_records_of(2));
    assert!(log.holds_records_of(3));
    log.accept_set_keeping_earlier(joint(
        named(3, &["a", "c", "d"]),
        named(2, &["b", "c", "d"]),
    ))
    .await
    .expect("accept");
    assert_eq!(
        log.replica_set().expect("set").earlier,
        vec![named(1, &["a", "b", "c"])]
    );
    log.accept_set_keeping_earlier(joint(
        named(4, &["a", "b", "d"]),
        named(3, &["a", "c", "d"]),
    ))
    .await
    .expect("accept");
    assert_eq!(log.replica_set().expect("set").earlier, Vec::new());
}

/// Without `joint_fence` nothing reads earlier sets, and a file holding them
/// is one an older build refuses, so a rollback could not open the shard.
#[tokio::test]
async fn a_set_kept_without_earlier_sets_writes_none() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.accept_set(joint(
        named(2, &["b", "c", "d"]),
        named(1, &["a", "b", "c"]),
    ))
    .await
    .expect("accept");
    log.accept_set(ReplicaSet {
        earlier: vec![named(1, &["a", "b", "c"])],
        ..joint(named(3, &["a", "c", "d"]), named(2, &["b", "c", "d"]))
    })
    .await
    .expect("accept");
    assert_eq!(log.replica_set().expect("set").earlier, Vec::new());
    let bytes = std::fs::read(dir.path().join("replica-set")).expect("file");
    let flags = u16::from_be_bytes(bytes[6..8].try_into().expect("flags"));
    assert_eq!(flags, 1, "only the set being left follows");
}

/// A record a crash could still lose is no evidence: dropping the sets on it
/// would leave the log with neither the record nor the sets.
#[tokio::test]
async fn a_record_not_yet_durable_drops_nothing() {
    let dir = tempdir().expect("tempdir");
    let log = open(
        &dir,
        FsyncMode::Periodic {
            interval: std::time::Duration::from_secs(3600),
        },
    );
    log.accept_set_keeping_earlier(joint(
        named(2, &["b", "c", "d"]),
        named(1, &["a", "b", "c"]),
    ))
    .await
    .expect("accept");
    log.record_generation(2, 0).expect("generation");
    log.append(&records(&["at two"])).await.expect("append");
    assert!(log.holds_records_of(2));
    log.accept_set_keeping_earlier(joint(
        named(3, &["a", "c", "d"]),
        named(2, &["b", "c", "d"]),
    ))
    .await
    .expect("accept");
    assert_eq!(
        log.replica_set().expect("set").earlier,
        vec![named(1, &["a", "b", "c"])]
    );
}

/// Past the file's limit a raise adding a set fails and nothing changes:
/// refusing the fence that asked is safe, forgetting a set is not. One
/// naming only kept sets still fits, even without the set being left.
#[tokio::test]
async fn more_earlier_sets_than_the_file_keeps_fail_the_raise() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    let nodes = |g: u64| vec![format!("n{g}"), "x".to_string(), "y".to_string()];
    let mut generation = 1;
    let result = loop {
        let names = nodes(generation);
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let result = log
            .accept_set_keeping_earlier(ReplicaSet {
                current: named(generation, &names),
                leaving: (generation > 1).then(|| {
                    let before = nodes(generation - 1);
                    let before: Vec<&str> = before.iter().map(String::as_str).collect();
                    named(generation - 1, &before)
                }),
                earlier: Vec::new(),
            })
            .await;
        if result.is_err() || generation > set::MAX_EARLIER_SETS as u64 + 2 {
            break result;
        }
        generation += 1;
    };
    assert!(result.is_err(), "a set past the limit was kept");
    let kept = log.replica_set().expect("set");
    assert_eq!(kept.current.generation, generation - 1);
    assert_eq!(kept.earlier.len(), set::MAX_EARLIER_SETS - 1);

    // A fence naming only kept sets still fits, so one can open and end it.
    for nodes in [&kept.current, kept.earlier.last().expect("oldest")] {
        log.accept_set_keeping_earlier(ReplicaSet {
            current: Membership {
                generation,
                members: nodes.members.clone(),
            },
            leaving: None,
            earlier: Vec::new(),
        })
        .await
        .expect("a fence on a kept set is taken");
        generation += 1;
    }
    assert_eq!(
        log.replica_set().expect("set").earlier.len(),
        set::MAX_EARLIER_SETS
    );
}

/// Admitting a learner can make the current set one an earlier set names;
/// the repeat goes rather than fail every admission.
#[tokio::test]
async fn admitting_a_learner_drops_an_earlier_set_it_now_repeats() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    let mut current = named(2, &["a", "b"]);
    current.members[1].learner = true;
    log.accept_set_keeping_earlier(ReplicaSet {
        current,
        leaving: None,
        earlier: vec![named(1, &["a", "b"])],
    })
    .await
    .expect("accept");
    assert!(log.admit_member("b", false).await.expect("admit"));
    let kept = log.replica_set().expect("set");
    assert_eq!(
        (kept.current, kept.earlier),
        (named(2, &["a", "b"]), Vec::new())
    );
}

/// A first set is held to the same cap as any other, so the guarantee that
/// a raise naming only kept sets fits holds from the start.
#[tokio::test]
async fn a_first_set_past_the_cap_is_refused() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    let earlier = (1..=set::MAX_EARLIER_SETS as u64)
        .rev()
        .map(|g| named(g, &[&format!("n{g}"), "x"]))
        .collect();
    let result = log
        .accept_set_keeping_earlier(ReplicaSet {
            current: named(20, &["a", "x"]),
            leaving: Some(named(19, &["b", "x"])),
            earlier,
        })
        .await;
    assert!(result.is_err(), "kept more sets than a later raise fits");
    assert!(log.replica_set().is_none());
}

/// **A candidate's ballot is taken only on the set it read** (#1970): once
/// the copy keeps a newer set, the ballot is refused and nothing is written.
#[tokio::test]
async fn a_ballot_on_a_set_is_refused_once_the_set_moved() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.accept_generation(3, Some("a")).await.expect("ballot");
    log.accept_set(set_at(3)).await.expect("set");
    let read = log.replica_set().expect("kept");

    log.accept_set(set_at(4)).await.expect("set");
    assert_eq!(
        log.accept_generation_on_set(6, "b", read)
            .await
            .expect("ballot"),
        None
    );
    assert_eq!(log.accepted_generation(), 3);

    let read = log.replica_set().expect("kept");
    assert_eq!(
        log.accept_generation_on_set(6, "b", read)
            .await
            .expect("ballot"),
        Some(GenerationCheck::Raised)
    );
    assert_eq!(log.accepted_generation(), 6);
}

/// **A set from a sender this copy promised past is not kept** (#1970): a
/// candidate's ballot landing between a fence's ballot and its set leaves
/// the candidate's set alone. A set from the ballot's own leader, or from
/// ahead of it, is kept.
#[tokio::test]
async fn a_set_from_a_sender_promised_past_is_refused() {
    let dir = tempdir().expect("tempdir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.accept_generation(4, Some("d")).await.expect("ballot");
    log.accept_generation(6, Some("b")).await.expect("ballot");

    assert_eq!(
        log.accept_set_from(set_at(4), false, 4, Some("d"))
            .await
            .expect("set"),
        Err(GenerationCheck::Superseded { accepted: 6 })
    );
    assert_eq!(
        log.accept_set_from(set_at(4), false, 6, Some("d"))
            .await
            .expect("set"),
        Err(GenerationCheck::Promised {
            leader: "b".to_string()
        })
    );
    assert_eq!(log.replica_set(), None, "nothing written");

    assert_eq!(
        log.accept_set_from(set_at(4), true, 6, Some("b"))
            .await
            .expect("set"),
        Ok(SetCheck::Raised)
    );
    assert_eq!(
        log.accept_set_from(set_at(5), false, 7, Some("c"))
            .await
            .expect("set"),
        Ok(SetCheck::Raised)
    );
}
