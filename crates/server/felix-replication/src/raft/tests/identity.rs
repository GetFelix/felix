//! Members opened as copies of their shard directories, through a root that
//! passed the gate.

use felix_storage::raft_copy::{CopyRoot, Evidence, Gate, RotateReason, check, kept_copies};
use felix_storage::storage_identity;
use openraft::async_runtime::watch::WatchReceiver;

use super::*;

/// One data root per broker.
struct Roots {
    dir: TempDir,
    roots: HashMap<&'static str, CopyRoot>,
}

impl Roots {
    fn new() -> Self {
        let dir = tempdir().expect("dir");
        let mut roots = HashMap::new();
        for (n, broker) in BROKERS.iter().enumerate() {
            let root = dir.path().join(broker);
            std::fs::create_dir_all(&root).expect("root");
            storage_identity::load_or_create(&root, || [n as u8 + 1; 16]).expect("id");
            let Gate::Passed(passed) = check(&root, Evidence::default()).expect("gate") else {
                panic!("a new root did not pass");
            };
            let next = std::sync::atomic::AtomicU64::new(100 * (n as u64 + 1));
            let copies = CopyRoot::open(&root, passed, move || {
                next.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            });
            roots.insert(*broker, copies);
        }
        Self { dir, roots }
    }

    fn shard_dir(&self, broker: &str) -> std::path::PathBuf {
        self.dir.path().join(broker).join("shards").join("0")
    }
}

/// Set in the process that poisons a log for
/// [`a_copy_whose_flush_failed_does_not_vote_with_the_cut_log`].
const POISONING_PROCESS: &str = "FELIX_TEST_POISONING_PROCESS";

/// A copy whose directory was wiped comes back under a new id. A request
/// for the old id is refused, the group still counts the old id as its
/// voter, and the new copy never votes: the old promise is not made again
/// from an empty log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wiped_copy_comes_back_under_a_new_id_that_does_not_vote() {
    let mut cluster = Cluster::new(true);
    let roots = Roots::new();
    let mut members = BTreeSet::new();
    let mut logs = HashMap::new();
    for broker in BROKERS {
        let opened = cluster
            .runtime(broker)
            .open_copy(
                group(0),
                &roots.shard_dir(broker),
                log_config(),
                &roots.roots[broker],
            )
            .await
            .expect("open copy");
        assert_eq!(
            opened.rotation.as_ref().map(|rotation| rotation.reason),
            Some(RotateReason::Empty)
        );
        members.insert(opened.member);
        logs.insert(broker, opened.log);
    }
    for runtime in cluster.runtimes() {
        runtime
            .initialize(&shard(0), members.clone())
            .await
            .expect("initialize");
    }
    cluster.write(0, b"before").await;

    let old = cluster
        .runtime("c")
        .status(&shard(0))
        .expect("status")
        .member;
    // Out of the cluster's view: a member that hears from no leader would
    // keep `elected` waiting.
    cluster.net.cut.lock().insert("c".into());
    assert!(cluster.runtime("c").close(&shard(0)).await);
    logs.remove("c").expect("log").close().await.expect("close");
    std::fs::remove_dir_all(roots.shard_dir("c")).expect("wipe");
    let reopened = cluster
        .runtime("c")
        .open_copy(
            group(0),
            &roots.shard_dir("c"),
            log_config(),
            &roots.roots["c"],
        )
        .await
        .expect("reopen");
    assert_eq!(
        reopened.rotation.as_ref().map(|rotation| rotation.reason),
        Some(RotateReason::Empty)
    );
    let new = reopened.member.clone();
    assert_eq!(new.broker, old.broker);
    assert_ne!(new.copy, old.copy);

    let request = RaftRequest {
        group: group(0),
        to: old.clone(),
        beat: 0,
        report: false,
        deadline: Duration::from_secs(1),
        body: {
            let candidate = LeaderIdOf::<ShardTypes>::new(99, MemberId::new("b", 1));
            let vote = <VoteOf<ShardTypes> as RaftVote>::from_leader_id(candidate, false);
            Request::Vote(VoteRequest::new(vote, None))
        },
    };
    assert!(matches!(
        cluster.runtime("c").handle(request).await,
        Err(Refused::NoSuchMember(_))
    ));

    // The group goes on with the two copies kept.
    cluster.write(0, b"after").await;
    let leader = cluster.elected(0).await;
    let member = cluster
        .runtime(&leader.broker)
        .group(&shard(0))
        .expect("group");
    let voters: BTreeSet<MemberId> = member
        .raft()
        .metrics()
        .borrow_watched()
        .membership_config
        .membership()
        .voter_ids()
        .collect();
    assert!(
        voters.contains(&old) && !voters.contains(&new),
        "{voters:?}"
    );
    let status = cluster.runtime("c").status(&shard(0)).expect("status");
    assert_eq!(status.member, new);
    assert_eq!(status.role, Role::Learner);
    assert_eq!(status.term, 0, "the new copy voted");

    reopened.log.close().await.ok();
    cluster.stop().await;
    for (_, log) in logs {
        log.close().await.ok();
    }
}

/// Issue #2469. The leader and follower F1 write record R, and R is
/// acknowledged on the written count while F2 is cut off. F1's flush fails,
/// so a restart would cut its log back to the last sync, without R. It comes
/// back under a new copy id instead. With the leader cut off, F1 and F2
/// cannot elect a leader without R, and once the leader is back R commits.
///
/// The body runs in a process of its own. A poisoned log is counted for the
/// whole process and never forgotten, and the replica tests in this binary
/// attest no trusted copy once one is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_whose_flush_failed_does_not_vote_with_the_cut_log() {
    const NAME: &str =
        "raft::tests::identity::a_copy_whose_flush_failed_does_not_vote_with_the_cut_log";
    if std::env::var_os(POISONING_PROCESS).is_none() {
        let output = tokio::task::spawn_blocking(|| {
            std::process::Command::new(std::env::current_exe().expect("this binary"))
                .args(["--exact", NAME, "--nocapture"])
                .env(POISONING_PROCESS, "1")
                .output()
                .expect("child")
        })
        .await
        .expect("join");
        let stdout = String::from_utf8_lossy(&output.stdout);
        // "1 passed": a filter that matched nothing also exits 0.
        assert!(
            output.status.success() && stdout.contains("1 passed"),
            "{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let mut cluster = Cluster::new(true);
    let roots = Roots::new();
    let mut members = BTreeSet::new();
    let mut logs = HashMap::new();
    for broker in BROKERS {
        let opened = cluster
            .runtime(broker)
            .open_copy(
                group(0),
                &roots.shard_dir(broker),
                log_config(),
                &roots.roots[broker],
            )
            .await
            .expect("open copy");
        members.insert(opened.member);
        logs.insert(broker, opened.log);
    }
    for runtime in cluster.runtimes() {
        runtime
            .initialize(&shard(0), members.clone())
            .await
            .expect("initialize");
    }
    let before = cluster.write(0, b"before").await;
    cluster.committed_everywhere(0, before).await;

    let leader = cluster.elected(0).await;
    let engine = RaftEngine::new(cluster.runtime(&leader.broker));
    let batch = |payload: &'static [u8]| Batch {
        records: vec![Record {
            timestamp_micros: 1_700_000_000,
            payload: Bytes::from_static(payload),
        }],
        ack: AckAt::Written,
        term: None,
        producer: None,
    };
    // A wait on the written count makes the leader ask for reports.
    let primed = engine
        .propose(&shard(0), batch(b"primed"))
        .await
        .expect("written");
    engine
        .await_quorum_ack(&shard(0), primed, AckAt::Written, PATIENCE)
        .await
        .expect("acknowledged");
    engine
        .await_commit(&shard(0), primed, PATIENCE)
        .await
        .expect("committed");
    cluster.committed_everywhere(0, primed.end - 1).await;

    let mut followers = BROKERS
        .iter()
        .copied()
        .filter(|broker| *broker != &*leader.broker);
    let failing = followers.next().expect("a follower");
    let other = followers.next().expect("another follower");
    let old = cluster
        .runtime(failing)
        .status(&shard(0))
        .expect("status")
        .member;
    let log = logs.remove(failing).expect("log");
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while log.durable_offset() < log.tail_offset().await.expect("tail") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the follower never synced"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Only the flush of R fails: an idle flush in between passes and leaves
    // the failure armed.
    log.fail_flush_past(log.tail_offset().await.expect("tail"));
    cluster.net.cut.lock().insert(other.into());
    cluster.net.muted.lock().insert(other.into());
    let r = engine
        .propose(&shard(0), batch(b"R"))
        .await
        .expect("written");
    engine
        .await_quorum_ack(&shard(0), r, AckAt::Written, PATIENCE)
        .await
        .expect("R acknowledged on the written count");
    let committed = cluster
        .runtime(&leader.broker)
        .status(&shard(0))
        .expect("status")
        .committed;
    assert!(
        committed.is_none_or(|at| at < r.end - 1),
        "R committed, so it was not acknowledged on the written count alone"
    );
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while !log.is_poisoned() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the flush never failed"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        log.tail_offset().await.expect("tail") > log.durable_offset(),
        "R was written past the last sync"
    );

    // F1 restarts.
    cluster.net.cut.lock().insert(failing.into());
    assert!(cluster.runtime(failing).close(&shard(0)).await);
    // A poisoned log has nothing left to flush, so its close may fail.
    let _ = log.close().await;
    let reopened = cluster
        .runtime(failing)
        .open_copy(
            group(0),
            &roots.shard_dir(failing),
            log_config(),
            &roots.roots[failing],
        )
        .await
        .expect("reopen");

    // The leader goes away and F2 comes back. A cut copy that kept its id
    // would elect a leader with F2, and neither holds R.
    {
        let mut cut = cluster.net.cut.lock();
        cut.clear();
        cut.insert(leader.broker.to_string());
    }
    {
        let mut muted = cluster.net.muted.lock();
        muted.clear();
        muted.insert(leader.broker.to_string());
    }
    let settle = tokio::time::Instant::now() + 3 * timers().election_max;
    while tokio::time::Instant::now() < settle {
        for broker in [failing, other] {
            let status = cluster.runtime(broker).status(&shard(0));
            assert!(
                status.is_none_or(|status| status.role != Role::Leader),
                "{broker} was elected without R"
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The leader comes back, and a write in whatever term follows commits R
    // where it was written. The new copy is left out: nothing is sent to it.
    cluster.net.muted.lock().clear();
    {
        let mut cut = cluster.net.cut.lock();
        cut.clear();
        cut.insert(failing.into());
    }
    let after = cluster.write(0, b"after").await;
    assert!(after > r.end - 1);
    for broker in [&*leader.broker, other] {
        let range = felix_storage::log::ReadRange {
            start: r.end - 1,
            max_bytes: 1 << 20,
        };
        let records = logs[broker]
            .read_range_with_trims(range)
            .await
            .expect("read");
        assert_eq!(
            records.first().map(|record| &record.payload[..]),
            Some(&b"R"[..]),
            "{broker} lost R"
        );
    }

    assert_eq!(
        reopened.rotation.as_ref().map(|rotation| rotation.reason),
        Some(RotateReason::Poisoned)
    );
    let new = reopened.member.clone();
    assert_eq!(new.broker, old.broker);
    assert_ne!(new.copy, old.copy);
    let request = RaftRequest {
        group: group(0),
        to: old.clone(),
        beat: 0,
        report: false,
        deadline: Duration::from_secs(1),
        body: {
            let candidate = LeaderIdOf::<ShardTypes>::new(99, MemberId::new("b", 1));
            let vote = <VoteOf<ShardTypes> as RaftVote>::from_leader_id(candidate, false);
            Request::Vote(VoteRequest::new(vote, None))
        },
    };
    assert!(matches!(
        cluster.runtime(failing).handle(request).await,
        Err(Refused::NoSuchMember(_))
    ));
    let member = cluster
        .runtime(&leader.broker)
        .group(&shard(0))
        .expect("group");
    let voters: BTreeSet<MemberId> = member
        .raft()
        .metrics()
        .borrow_watched()
        .membership_config
        .membership()
        .voter_ids()
        .collect();
    assert!(
        voters.contains(&old) && !voters.contains(&new),
        "{voters:?}"
    );
    let status = cluster.runtime(failing).status(&shard(0)).expect("status");
    assert_eq!(status.member, new);
    assert_eq!(status.role, Role::Learner);
    assert_eq!(status.term, 0, "the new copy voted");

    reopened.log.close().await.ok();
    cluster.stop().await;
    for (_, log) in logs {
        log.close().await.ok();
    }
}

/// The marks' name is on disk: it stays the same across builds, and a `/`
/// in a user's name cannot make two shards share one.
#[test]
fn the_mark_name_is_stable_and_unambiguous() {
    use super::super::runtime::mark_name;
    let key = |tenant: &str, namespace: &str, stream: &str, kind| ShardKey {
        tenant_id: tenant.to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        shard: 3,
        kind,
    };
    assert_eq!(
        mark_name(&key("t", "ns", "orders", ShardKind::Stream)),
        "1:t/2:ns/6:orders/3/stream"
    );
    assert_eq!(
        mark_name(&key("t", "ns", "orders", ShardKind::Cache)),
        "1:t/2:ns/6:orders/3/cache"
    );
    assert_ne!(
        mark_name(&key("a/b", "c", "d", ShardKind::Stream)),
        mark_name(&key("a", "b/c", "d", ShardKind::Stream))
    );
}

/// A rotated copy's kept directory is held while the new copy has committed
/// less than it holds, and deleted once the group commits past it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kept_copy_is_reclaimed_once_the_new_copy_commits_past_it() {
    let mut cluster = Cluster::new(true);
    let roots = Roots::new();
    let dir = roots.shard_dir("a");
    let open = |runtime: GroupRuntime| {
        let (dir, roots) = (dir.clone(), &roots);
        async move {
            runtime
                .open_copy(group(0), &dir, log_config(), &roots.roots["a"])
                .await
        }
    };

    let first = open(cluster.runtime("a")).await.expect("open copy");
    let set = BTreeSet::from([first.member.clone()]);
    cluster
        .runtime("a")
        .initialize(&shard(0), set)
        .await
        .expect("initialize");
    let mut last = 0;
    for _ in 0..5 {
        last = cluster.write(0, b"before").await;
    }
    assert!(cluster.runtime("a").close(&shard(0)).await);
    first.log.close().await.expect("close");

    // Without its copy file the directory rotates, keeping these records.
    std::fs::remove_file(dir.join("raft-copy")).expect("copy file");
    let second = open(cluster.runtime("a")).await.expect("reopen");
    assert_eq!(
        second.rotation.as_ref().map(|rotation| rotation.reason),
        Some(RotateReason::NoCopyFile)
    );
    assert_eq!(second.kept.reclaimed, Vec::<std::path::PathBuf>::new());
    assert_eq!(second.kept.held.len(), 1, "{:?}", second.kept);
    assert!(second.kept.wait_for().is_some_and(|held| held >= 5));
    assert_eq!(kept_copies(&dir).expect("kept").len(), 1);

    let set = BTreeSet::from([second.member.clone()]);
    cluster
        .runtime("a")
        .initialize(&shard(0), set)
        .await
        .expect("initialize");
    while last < second.kept.wait_for().expect("held") + 1 {
        last = cluster.write(0, b"after").await;
    }
    eventually(|| kept_copies(&dir).expect("kept").is_empty().then_some(())).await;

    second.log.close().await.ok();
    cluster.stop().await;
}

/// The commit that covers a kept copy is the shard's last: no later write
/// wakes the reclaim task, so the save of that commit has to be polled for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kept_copy_is_reclaimed_when_the_last_commit_is_exactly_the_covering_one() {
    let mut cluster = Cluster::new(true);
    let roots = Roots::new();
    let dir = roots.shard_dir("a");
    let open = |runtime: GroupRuntime| {
        let (dir, roots) = (dir.clone(), &roots);
        async move {
            runtime
                .open_copy(group(0), &dir, log_config(), &roots.roots["a"])
                .await
        }
    };

    let first = open(cluster.runtime("a")).await.expect("open copy");
    let set = BTreeSet::from([first.member.clone()]);
    cluster
        .runtime("a")
        .initialize(&shard(0), set)
        .await
        .expect("initialize");
    for _ in 0..5 {
        cluster.write(0, b"before").await;
    }
    assert!(cluster.runtime("a").close(&shard(0)).await);
    first.log.close().await.expect("close");

    // Without its copy file the directory rotates, keeping these records.
    std::fs::remove_file(dir.join("raft-copy")).expect("copy file");
    let second = open(cluster.runtime("a")).await.expect("reopen");
    assert_eq!(
        second.rotation.as_ref().map(|rotation| rotation.reason),
        Some(RotateReason::NoCopyFile)
    );
    assert_eq!(second.kept.reclaimed, Vec::<std::path::PathBuf>::new());
    assert_eq!(second.kept.held.len(), 1, "{:?}", second.kept);
    assert!(second.kept.wait_for().is_some_and(|held| held >= 5));
    assert_eq!(kept_copies(&dir).expect("kept").len(), 1);

    let set = BTreeSet::from([second.member.clone()]);
    cluster
        .runtime("a")
        .initialize(&shard(0), set)
        .await
        .expect("initialize");
    // Stop at the wait index: nothing later wakes the watchers, so only the
    // save poll can notice the commit landing on disk.
    let wait = second.kept.wait_for().expect("held");
    let mut last = 0;
    while last < wait {
        last = cluster.write(0, b"after").await;
    }
    assert_eq!(last, wait, "the last write must be the covering one");
    tokio::time::timeout(
        Duration::from_secs(10),
        eventually(|| kept_copies(&dir).expect("kept").is_empty().then_some(())),
    )
    .await
    .expect("the kept copy was not reclaimed after the last commit");

    second.log.close().await.ok();
    cluster.stop().await;
}
