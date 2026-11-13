//! Members opened as copies of their shard directories, through a root that
//! passed the gate.

use felix_storage::raft_copy::{CopyRoot, Evidence, Gate, RotateReason, check};
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
