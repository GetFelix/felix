//! Members opened as copies of their shard directories, through a root that
//! passed the gate.

use felix_storage::raft_copy::{
    ControlPlaneCopy, CopyRoot, Evidence, Gate, RotateReason, check, kept_copies,
};
use felix_storage::{run_state, sim_fs, storage_identity};
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
            let Gate::Passed(passed) = check(
                &root,
                Evidence {
                    witness_dir: None,
                    control_plane: None,
                    restored: false,
                },
            )
            .expect("gate") else {
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

/// Set in the process that poisons a log for [`flush_failed_copy`].
const POISONING_PROCESS: &str = "FELIX_TEST_POISONING_PROCESS";

/// A copy id is minted where the copy is, so the broker that forms a group
/// asks each other broker for its member. The set it forms names the copy
/// every directory holds, a silent broker stops it, and so does a member a
/// group already reached.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_members_are_the_copies_the_brokers_hold() {
    let mut cluster = Cluster::new(true);
    let roots = Roots::new();
    let mut opened = BTreeSet::new();
    let mut logs = Vec::new();
    for broker in BROKERS {
        let member = cluster
            .runtime(broker)
            .open_copy(
                group(0),
                &roots.shard_dir(broker),
                log_config(),
                &roots.roots[broker],
            )
            .await
            .expect("open copy");
        opened.insert(member.member);
        logs.push(member.log);
    }
    let (a, key, within) = (cluster.runtime("a"), shard(0), Duration::from_secs(5));

    cluster.net.cut.lock().insert("c".into());
    match a.first_members(&key, BROKERS, within).await {
        Err(NotBlank::Unanswered(member, _)) => assert_eq!(&*member.broker, "c"),
        other => panic!("a silent broker counted as a member: {other:?}"),
    }
    cluster.net.cut.lock().clear();

    let members = a
        .first_members(&key, BROKERS, within)
        .await
        .expect("every broker is blank");
    assert_eq!(members, opened);
    assert!(members.iter().all(|member| member.copy != 0));
    // The questions are pre-votes: they leave the members as they were.
    for status in cluster.statuses(0) {
        assert_eq!(status.term, 0, "{status:?}");
    }

    a.initialize(&key, members).await.expect("initialize");
    cluster.elected(0).await;
    cluster.write(0, b"record").await;
    match cluster
        .runtime("b")
        .first_members(&key, BROKERS, within)
        .await
    {
        Err(NotBlank::Formed(_)) => {}
        other => panic!("a formed group looked blank: {other:?}"),
    }
    cluster.stop().await;
    for log in logs {
        log.close().await.expect("close");
    }
}

/// A broker a target adds joins by the copy its directory holds: the
/// leader asks for its id, as the first members are asked for, and the
/// driver adds that copy. A silent broker the group does not hold stops
/// the call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_target_names_the_copy_a_new_broker_holds() {
    let mut cluster = Cluster::new(true);
    let roots = Roots::new();
    let mut opened = HashMap::new();
    let mut logs = Vec::new();
    for broker in BROKERS {
        let member = cluster
            .runtime(broker)
            .open_copy(
                group(0),
                &roots.shard_dir(broker),
                log_config(),
                &roots.roots[broker],
            )
            .await
            .expect("open copy");
        opened.insert(broker, member.member);
        logs.push(member.log);
    }
    let (a, key, within) = (cluster.runtime("a"), shard(0), Duration::from_secs(5));
    let first = a
        .first_members(&key, ["a", "b"], within)
        .await
        .expect("blank");
    // c is cut while the group forms, since it is no member yet and would
    // never see the leader.
    cluster.net.cut.lock().insert("c".into());
    a.initialize(&key, first).await.expect("initialize");
    cluster.elected(0).await;
    cluster.write(0, b"record").await;

    match a.target_members(&key, BROKERS, within).await {
        Err(NotBlank::Unanswered(member, _)) => assert_eq!(&*member.broker, "c"),
        other => panic!("a silent new broker got a member id: {other:?}"),
    }
    // A silent broker the group holds keeps the member it has.
    cluster.net.cut.lock().insert("b".into());
    cluster.net.cut.lock().remove("c");
    let kept = a
        .target_members(&key, BROKERS, within)
        .await
        .expect("members");
    assert_eq!(kept, opened.values().cloned().collect());
    cluster.net.cut.lock().clear();

    let voters = a
        .target_members(&key, BROKERS, within)
        .await
        .expect("members");
    assert_eq!(voters, opened.values().cloned().collect());
    let target = Target {
        voters: voters.clone(),
        version: TargetVersion {
            incarnation: 0,
            counter: 1,
        },
    };
    for runtime in cluster.runtimes() {
        runtime.retarget(&key, target.clone());
    }
    eventually(|| {
        BROKERS
            .iter()
            .all(|broker| cluster.runtime(broker).group(&key).expect("group").voters() == voters)
            .then_some(())
    })
    .await;
    cluster.stop().await;
    for log in logs {
        log.close().await.expect("close");
    }
}

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
        Err(Refused::OtherCopy(_, copy)) if copy == new.copy
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

/// A copy syncs its root's vouched-unsynced marker before its written
/// position reaches a leader, and the leader does before it counts its own
/// unsynced write. A copy whose marker does not sync says nothing, so its
/// root still shows every record it was counted for as synced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_written_report_leaves_only_after_the_roots_marker_is_synced() {
    let mut cluster = Cluster::new(true);
    let roots = Roots::new();
    let mut members = BTreeSet::new();
    let mut logs = Vec::new();
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
        logs.push(opened.log);
    }
    for runtime in cluster.runtimes() {
        runtime
            .initialize(&shard(0), members.clone())
            .await
            .expect("initialize");
    }
    let before = cluster.write(0, b"before").await;
    cluster.committed_everywhere(0, before).await;
    let vouched = |broker: &str| {
        run_state::vouched_unsynced(&roots.dir.path().join(broker)).expect("read the marker")
    };
    for broker in BROKERS {
        assert!(!vouched(broker), "{broker} vouched with no report asked");
    }

    let leader = cluster.elected(0).await;
    let mut followers = BROKERS
        .iter()
        .copied()
        .filter(|broker| *broker != &*leader.broker);
    let silent = followers.next().expect("a follower");
    let telling = followers.next().expect("another follower");
    roots.roots[silent].mark_log().fail_vouch();

    let engine = RaftEngine::new(cluster.runtime(&leader.broker));
    let member_of = |broker: &str| {
        let status = cluster.runtime(broker).status(&shard(0));
        status.expect("status").member
    };
    let group = cluster
        .runtime(&leader.broker)
        .group(&shard(0))
        .expect("group");
    let deadline = tokio::time::Instant::now() + PATIENCE;
    // The first wait makes the leader ask; later appends carry the ask.
    while group.reports().from(&member_of(telling)).is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no written report arrived"
        );
        let batch = Batch {
            records: vec![Record {
                timestamp_micros: 1_700_000_000,
                payload: Bytes::from_static(b"relaxed"),
            }],
            ack: AckAt::Written,
            term: None,
            producer: None,
        };
        let written = engine.propose(&shard(0), batch).await.expect("written");
        engine
            .await_quorum_ack(&shard(0), written, AckAt::Written, PATIENCE)
            .await
            .expect("acknowledged");
        cluster.committed_everywhere(0, written.end - 1).await;
    }
    assert!(vouched(telling), "a report left before its marker");
    assert!(
        vouched(&leader.broker),
        "the leader counted its own unsynced write before its marker"
    );
    assert!(!vouched(silent));
    assert_eq!(
        group.reports().from(&member_of(silent)),
        None,
        "a copy whose marker did not sync reported"
    );

    cluster.stop().await;
    for log in logs {
        log.close().await.ok();
    }
}

/// The leader's own marker syncs before it counts its unsynced write, but a
/// slow sync holds up only the written count. The commit still acknowledges,
/// and a write that cannot commit still times out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_marker_sync_does_not_hold_the_commit_or_the_timeout() {
    let mut cluster = Cluster::new(true);
    let roots = Roots::new();
    let mut members = BTreeSet::new();
    let mut logs = Vec::new();
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
        logs.push(opened.log);
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
    let stalled = roots.roots[&*leader.broker].mark_log().stall_vouch().await;

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
    // Well inside the wait's own timeout, so a wait stuck behind the marker
    // fails here.
    let soon = Duration::from_secs(20);
    let written = engine
        .propose(&shard(0), batch(b"commits"))
        .await
        .expect("written");
    let acked = engine.await_quorum_ack(&shard(0), written, AckAt::Written, PATIENCE);
    tokio::time::timeout(soon, acked)
        .await
        .expect("the commit waited on the marker")
        .expect("acknowledged at its commit");

    // No follower answers, so nothing commits.
    for broker in BROKERS.iter().filter(|broker| **broker != &*leader.broker) {
        cluster.net.cut.lock().insert((*broker).into());
    }
    let stuck = engine
        .propose(&shard(0), batch(b"stuck"))
        .await
        .expect("written");
    let brief = Duration::from_millis(200);
    let timed_out = engine.await_quorum_ack(&shard(0), stuck, AckAt::Written, brief);
    let timed_out = tokio::time::timeout(soon, timed_out)
        .await
        .expect("the timeout waited on the marker");
    assert!(
        matches!(timed_out, Err(crate::engine::CommitError::TimedOut { .. })),
        "{timed_out:?}"
    );
    assert!(
        !run_state::vouched_unsynced(&roots.dir.path().join(&*leader.broker)).expect("read"),
        "the marker was written while stalled"
    );
    drop(stalled);

    cluster.stop().await;
    for log in logs {
        log.close().await.ok();
    }
}

/// Start a run on `root`, which is on the simulated filesystem, as a broker
/// whose own fsync mode is `OnCommit`, and open it as a
/// copy root. `control_plane` stands in for the control plane's copy of the
/// root sequence, which a restart compares against.
fn start_sim_run(root: &Path, mint_from: u64, control_plane: Option<ControlPlaneCopy>) -> CopyRoot {
    static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let relaxed = run_state::relaxed(&log_config().fsync_mode);
    run_state::begin(
        root,
        relaxed,
        None,
        Some(sim_fs::boot_id(root).expect("boot")),
        || false,
        || {
            let mut id = [0x5A; 16];
            let n = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            id[8..].copy_from_slice(&n.to_be_bytes());
            id
        },
    )
    .expect("begin a run");
    let Gate::Passed(passed) = check(
        root,
        Evidence {
            witness_dir: None,
            control_plane,
            restored: false,
        },
    )
    .expect("gate") else {
        panic!("{} did not pass the gate", root.display());
    };
    let next = std::sync::atomic::AtomicU64::new(mint_from);
    CopyRoot::open(root, passed, move || {
        next.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    })
}

/// Issue #2423. Every broker's own fsync mode is `OnCommit`, but a Raft copy
/// reports a record written before its fsync, and the leader counts that for
/// a relaxed stream. The leader and follower F1 write record R, and R is
/// acknowledged on the written count while F2 is cut off. Then F1 loses
/// power before R is synced. It synced its root's marker before the report
/// left, so it boots under a new storage id and its copy with it. With the leader cut off, F1
/// and F2 cannot elect a leader without R, and once the leader is back R
/// commits.
///
/// The roots are on the simulated filesystem. The cut lands in R's append on
/// F1, so R comes back torn and recovery drops it, and whatever F1 writes
/// after the cut, its log mark included, is lost at the reboot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_back_from_a_power_cut_does_not_vote_with_the_cut_log() {
    let dir = tempdir().expect("dir");
    let root_of = |broker: &str| dir.path().join(broker);
    let mut roots = HashMap::new();
    for (n, broker) in BROKERS.iter().enumerate() {
        sim_fs::register_root(&root_of(broker));
        roots.insert(
            *broker,
            start_sim_run(&root_of(broker), 100 * (n as u64 + 1), None),
        );
    }
    let shard_dir = |broker: &str| root_of(broker).join("shards").join("0");
    let mut cluster = Cluster::new(true);
    let mut members = BTreeSet::new();
    let mut logs = HashMap::new();
    for broker in BROKERS {
        let opened = cluster
            .runtime(broker)
            .open_copy(group(0), &shard_dir(broker), log_config(), &roots[broker])
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
    // A first report waits on its root's marker, and is dropped if the
    // append's answer is ready before it. Later ones do not wait.
    let old = cluster
        .runtime(failing)
        .status(&shard(0))
        .expect("status")
        .member;
    let heard = cluster
        .runtime(&leader.broker)
        .group(&shard(0))
        .expect("group");
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while heard.reports().from(&old).is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the follower never vouched"
        );
        let again = engine
            .propose(&shard(0), batch(b"primed"))
            .await
            .expect("written");
        // The leader asks for reports only once a wait has had to count
        // them, which a commit that is already there never does.
        engine
            .await_quorum_ack(&shard(0), again, AckAt::Written, PATIENCE)
            .await
            .expect("acknowledged");
        cluster.committed_everywhere(0, again.end - 1).await;
    }
    // The leader counts this copy from here on, so the marker has to be on
    // the device already, not only in the page cache.
    assert!(
        sim_fs::entry_is_durable(&root_of(failing), run_state::VOUCHED_UNSYNCED_FILE_NAME)
            .expect("simulated root"),
        "a report left before its marker was synced"
    );
    let log = logs.remove(failing).expect("log");
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while log.durable_offset() < log.tail_offset().await.expect("tail") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the follower never synced"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // What the control plane would hold: the root as of the cut.
    let held = ControlPlaneCopy {
        storage_id: roots[failing].storage_id(),
        sequence: roots[failing].mark_log().sequence(),
    };
    let cut = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let cut = Arc::clone(&cut);
        sim_fs::arm_power_cut(&root_of(failing), 7, move |_, _| {
            cut.store(true, std::sync::atomic::Ordering::Release);
        })
        .expect("arm the cut");
    }
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
    assert!(
        cut.load(std::sync::atomic::Ordering::Acquire),
        "the power was not cut in R's append"
    );

    // F1 boots again.
    cluster.net.cut.lock().insert(failing.into());
    assert!(cluster.runtime(failing).close(&shard(0)).await);
    let _ = log.close().await;
    drop(roots.remove(failing));
    assert!(sim_fs::reboot(&root_of(failing)).expect("reboot"));
    let root = start_sim_run(&root_of(failing), 900, Some(held));
    let reopened = cluster
        .runtime(failing)
        .open_copy(group(0), &shard_dir(failing), log_config(), &root)
        .await
        .expect("reopen");

    // The leader goes away and F2 comes back. A copy that kept its id would
    // elect a leader with F2, and neither holds R.
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

    // The leader comes back, and R commits where it was written.
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
        Some(RotateReason::StorageId)
    );
    assert_ne!(reopened.member.copy, old.copy);
    let status = cluster.runtime(failing).status(&shard(0)).expect("status");
    assert_eq!(status.role, Role::Learner);

    reopened.log.close().await.ok();
    cluster.stop().await;
    for (_, log) in logs {
        log.close().await.ok();
    }
    for broker in BROKERS {
        sim_fs::unregister_root(&root_of(broker));
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
    flush_failed_copy(
        "raft::tests::identity::a_copy_whose_flush_failed_does_not_vote_with_the_cut_log",
        false,
    )
    .await;
}

/// Issue #2475. As above, but the poison marker cannot be written either,
/// and F1 is reopened in the same process, so no run record rotates it. The
/// kernel may drop pages a failed fsync left behind: the test cuts F1's
/// segments back to their length at the last sync before the reopen. F1
/// still comes back under a new copy id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_reopened_after_a_failed_flush_and_marker_does_not_vote() {
    flush_failed_copy(
        "raft::tests::identity::a_copy_reopened_after_a_failed_flush_and_marker_does_not_vote",
        true,
    )
    .await;
}

/// The length of every segment file in `dir`.
fn segment_lengths(dir: &Path) -> Vec<(std::path::PathBuf, u64)> {
    std::fs::read_dir(dir)
        .expect("shard dir")
        .map(|entry| entry.expect("entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
        .map(|path| {
            let len = std::fs::metadata(&path).expect("segment").len();
            (path, len)
        })
        .collect()
}

/// The body of the two tests above. `marker_fails` fails the poison marker
/// write and cuts F1's log back to its last sync before the reopen.
async fn flush_failed_copy(name: &str, marker_fails: bool) {
    if std::env::var_os(POISONING_PROCESS).is_none() {
        let name = name.to_owned();
        let output = tokio::task::spawn_blocking(move || {
            std::process::Command::new(std::env::current_exe().expect("this binary"))
                .args(["--exact", &name, "--nocapture"])
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
    // A first report waits on its root's marker, and is dropped if the
    // append's answer is ready before it. Later ones do not wait.
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while !run_state::vouched_unsynced(&roots.dir.path().join(failing)).expect("read the marker") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the follower never vouched"
        );
        let again = engine
            .propose(&shard(0), batch(b"primed"))
            .await
            .expect("written");
        // The leader asks for reports only once a wait has had to count
        // them, which a commit that is already there never does.
        engine
            .await_quorum_ack(&shard(0), again, AckAt::Written, PATIENCE)
            .await
            .expect("acknowledged");
        cluster.committed_everywhere(0, again.end - 1).await;
    }
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
    let synced = segment_lengths(&roots.shard_dir(failing));
    if marker_fails {
        log.fail_poison_marker();
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
    if marker_fails {
        assert!(
            !roots.shard_dir(failing).join("poisoned").exists(),
            "the marker was written"
        );
        for (path, len) in &synced {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(path)
                .expect("segment");
            file.set_len(*len).expect("cut");
        }
    }
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
        Err(Refused::OtherCopy(_, copy)) if copy == new.copy
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
