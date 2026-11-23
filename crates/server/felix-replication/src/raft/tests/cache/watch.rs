//! Cache watches on a cache shard's group: each member reports the changes
//! its applied entries make, and a watch joins at the applied edge.

use std::sync::atomic::{AtomicBool, Ordering};

use felix_storage::{CacheChange, CacheObserver};

use super::super::super::cache::CacheWatch;
use super::*;

/// What a member told the watch hub, in the order it told it.
#[derive(Debug)]
struct Recorder {
    watching: AtomicBool,
    changes: parking_lot::Mutex<Vec<(String, Option<Bytes>, u64)>>,
    resets: parking_lot::Mutex<Vec<u64>>,
}

impl Recorder {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            watching: AtomicBool::new(true),
            changes: parking_lot::Mutex::default(),
            resets: parking_lot::Mutex::default(),
        })
    }

    fn changes(&self) -> Vec<(String, Option<Bytes>, u64)> {
        self.changes.lock().clone()
    }

    fn offsets(&self) -> Vec<u64> {
        self.changes().into_iter().map(|(_, _, at)| at).collect()
    }
}

impl CacheObserver for Recorder {
    fn cache_changed(&self, change: CacheChange) {
        self.changes
            .lock()
            .push((change.key, change.value, change.offset));
    }

    fn watching(&self) -> bool {
        self.watching.load(Ordering::Acquire)
    }

    fn cache_reset(&self, _: &str, _: &str, _: &str, _: u32, at: u64) {
        self.resets.lock().push(at);
    }
}

/// A cache group's state machine that reports to `recorder`.
async fn watched_machine(
    dir: &std::path::Path,
    recorder: &Arc<Recorder>,
) -> (
    super::super::super::machine::Machine,
    super::super::super::machine::Cache,
) {
    let snapshots = felix_storage::raft_log::SnapshotStore::open(dir).expect("store");
    let watch = CacheWatch::new(
        Arc::clone(recorder) as Arc<dyn CacheObserver>,
        Arc::new(shard(N)),
    );
    let opened =
        super::super::super::machine::Machine::open(snapshots, ShardKind::Cache, None, Some(watch))
            .await;
    let super::super::super::machine::Opened { machine, cache, .. } = opened.expect("open");
    (machine, cache.expect("a cache group"))
}

fn delete(key: &str, version: Option<u64>, sequence: u64) -> CacheCommand {
    CacheCommand::Delete {
        key: key.into(),
        version,
        request: request(sequence),
    }
}

fn put_if_absent(key: &str, value: &'static [u8], sequence: u64) -> CacheCommand {
    CacheCommand::Put {
        key: key.into(),
        value: Bytes::from_static(value),
        expires_at: 0,
        cond: Condition::Absent,
        request: request(sequence),
    }
}

/// **Each applied change reaches the hub at its entry's index, and nothing
/// else does**: a retry, a refused conditional write, a delete of an absent
/// key, an expiry of a key written since, and a counter add change no value
/// and report nothing.
#[tokio::test]
async fn applied_changes_reach_the_hub_and_no_others_do() {
    use openraft::storage::RaftStateMachine;
    let dir = tempdir().expect("dir");
    let recorder = Recorder::new();
    let (mut machine, cache) = watched_machine(dir.path(), &recorder).await;
    let commands = [
        put("k", b"one", 1),
        // The same request again: answered from the table, not applied.
        put("k", b"one", 1),
        put_if_absent("k", b"refused", 2),
        delete("k", Some(99), 3),
        CacheCommand::Expire {
            key: "k".into(),
            version: 99,
        },
        add("n", 5, 4),
        put("k", b"two", 5),
        delete("k", None, 6),
        delete("k", None, 7),
        put("other", b"x", 8),
    ];
    machine
        .apply(entries(1, commands.len() as u64, |index| {
            commands[index as usize - 1].clone()
        }))
        .await
        .expect("apply");
    let one = Some(Bytes::from_static(b"one"));
    let two = Some(Bytes::from_static(b"two"));
    let x = Some(Bytes::from_static(b"x"));
    assert_eq!(
        recorder.changes(),
        vec![
            ("k".to_string(), one, 1),
            ("k".to_string(), two, 7),
            ("k".to_string(), None, 8),
            ("other".to_string(), x, 10),
        ]
    );
    assert_eq!(cache.read().applied(), 10);
}

/// **A hub with no watchers costs one load per entry**, and a watch that
/// registers after reads the state, which holds what it skipped.
#[tokio::test]
async fn an_unwatched_member_reports_nothing() {
    use openraft::storage::RaftStateMachine;
    let dir = tempdir().expect("dir");
    let recorder = Recorder::new();
    recorder.watching.store(false, Ordering::Release);
    let (mut machine, cache) = watched_machine(dir.path(), &recorder).await;
    machine
        .apply(entries(1, 3, |index| keyed(index, 2)))
        .await
        .expect("apply");
    assert!(recorder.changes().is_empty());
    let held = cache.read().watched_entries(&|_| true);
    assert_eq!(held.len(), 2);
}

/// **A snapshot install ends every watch as lagged at the first entry it
/// was not told of**, and the state's edge moves to the snapshot's entry.
/// A watch that kept going would never hear of the writes the snapshot
/// folded in.
#[tokio::test]
async fn an_install_ends_watches_as_lagged_past_the_old_edge() {
    use super::super::super::cache::CacheState;
    use openraft::storage::RaftStateMachine;
    let dir = tempdir().expect("dir");
    let recorder = Recorder::new();
    let (mut machine, cache) = watched_machine(dir.path(), &recorder).await;
    machine
        .apply(entries(1, 3, |index| keyed(index, 2)))
        .await
        .expect("apply");
    let mut sent = CacheState::default();
    sent.apply(9, 90, &put("k", b"sent", 9));
    install(&mut machine, 9, &sent).await;
    assert_eq!(*recorder.resets.lock(), vec![4]);
    assert_eq!(cache.read().applied(), 9);

    machine
        .apply(entries(10, 10, |_| put("k", b"after", 10)))
        .await
        .expect("apply");
    assert_eq!(recorder.offsets(), vec![1, 2, 3, 10]);
}

/// **The join is at the applied edge, not the commit index.** A write
/// committed but not yet applied when the watch joins is in no value the
/// join hands out, so its change must arrive at or past the edge; with the
/// commit index as the edge it would arrive below it and be dropped as a
/// duplicate. A write applied between registering and joining arrives both
/// ways, below the edge, and the queued copy is the one dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_watch_joins_at_the_applied_edge_and_loses_no_write() {
    let mut cluster = Cluster::new(true);
    let recorder = Recorder::new();
    let leader_runtime = {
        // Every member reports; only the first member's are read.
        for runtime in cluster.runtimes() {
            runtime.report_cache_changes(Arc::clone(&recorder) as Arc<dyn CacheObserver>);
        }
        cluster.form(N).await;
        leading(&cluster).await
    };
    let (engine, group) = leader_runtime;
    // Registered from here: the recorder already watches.
    let landed = write(&cluster, 10, put("a", b"one", 1)).await;
    cluster.committed_everywhere(N, landed).await;

    let held = Arc::clone(&group.apply_hold).lock_owned().await;
    let pending = Command {
        timestamp_micros: 20,
        payload: put("b", b"two", 2).encode(),
        producer: None,
        expected: None,
        trim: None,
        commit: false,
        group: false,
        publisher: None,
        headers: None,
        written: None,
        cond: None,
    };
    let raft = group.raft().clone();
    let writing = tokio::spawn(async move { raft.client_write(pending).await.expect("write") });
    eventually(|| (group.status().committed > Some(landed)).then_some(())).await;

    let point = engine
        .cache_watch_point(&shard(N), &|_| true)
        .expect("a cache group");
    drop(held);
    let pending = writing.await.expect("task").log_id.index;

    assert!(point.tail <= pending, "the edge passed an unapplied write");
    let joined: Vec<_> = point.entries.iter().map(|entry| entry.offset).collect();
    assert_eq!(joined, vec![landed]);
    let leader = group.member.broker.clone();
    let reported = eventually(|| {
        let mine: Vec<_> = recorder
            .changes()
            .into_iter()
            .filter(|(key, _, _)| key == "b")
            .collect();
        (!mine.is_empty()).then_some(mine)
    })
    .await;
    // The live half: what arrives at or past the edge, as the handler keeps.
    let live: Vec<_> = reported
        .iter()
        .map(|(_, _, at)| *at)
        .filter(|at| *at >= point.tail)
        .collect();
    assert!(
        live.contains(&pending),
        "the write committed during the join was lost on {leader}: {reported:?}"
    );
    cluster.stop().await;
}

/// **A member keeps reporting across a change of leader**, in index order,
/// and every member reports the same changes: only committed entries apply,
/// so none a failover takes back is ever handed to a watch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_keeps_reporting_across_a_change_of_leader() {
    let mut cluster = Cluster::new(true);
    let recorders: HashMap<&str, Arc<Recorder>> = BROKERS
        .iter()
        .map(|broker| (*broker, Recorder::new()))
        .collect();
    for broker in BROKERS {
        cluster
            .runtime(broker)
            .report_cache_changes(Arc::clone(&recorders[broker]) as Arc<dyn CacheObserver>);
    }
    cluster.form(N).await;
    let first = write(&cluster, 10, put("k", b"one", 1)).await;
    cluster.committed_everywhere(N, first).await;
    let old = cluster.elected(N).await;
    cut_off(&cluster, &old.broker);
    let (_, second) = leading(&cluster).await;
    assert_ne!(second.member, old);
    let next = write(&cluster, 20, put("k", b"two", 2)).await;
    joined(&cluster, &old.broker);
    cluster.committed_everywhere(N, next).await;

    let follower = *BROKERS
        .iter()
        .find(|broker| ***broker != *old.broker && ***broker != *second.member.broker)
        .expect("a follower");
    let wanted = vec![
        ("k".to_string(), Some(Bytes::from_static(b"one")), first),
        ("k".to_string(), Some(Bytes::from_static(b"two")), next),
    ];
    for broker in [follower, &*old.broker, &*second.member.broker] {
        let recorder = &recorders[broker];
        eventually(|| (recorder.changes().len() >= 2).then_some(())).await;
        assert_eq!(recorder.changes(), wanted, "{broker}");
    }
    cluster.stop().await;
}

/// A retained watch on a follower whose apply trails the leader still
/// starts from every acknowledged put.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retained_point_on_a_lagging_follower_holds_every_acknowledged_put() {
    let mut cluster = Cluster::new(true);
    cluster.form(N).await;
    let leader = cluster.elected(N).await;
    let follower = BROKERS
        .iter()
        .find(|broker| ***broker != *leader.broker)
        .expect("a follower");
    let runtime = cluster.runtime(follower);
    let group = runtime.group(&shard(N)).expect("group");
    let engine = RaftEngine::new(runtime);

    let held = Arc::clone(&group.apply_hold).lock_owned().await;
    // Acknowledged: the leader has applied it, the follower has not.
    let landed = write(&cluster, 10, put("a", b"one", 1)).await;
    let joining = tokio::spawn(async move {
        engine
            .cache_retained_point(&shard(N), &|_| true)
            .await
            .map(|point| point.map(|point| point.entries))
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(held);

    let entries = joining
        .await
        .expect("task")
        .expect("a retained point")
        .expect("a cache group");
    let offsets: Vec<_> = entries.iter().map(|entry| entry.offset).collect();
    assert_eq!(offsets, vec![landed], "the retained state missed a put");
    cluster.stop().await;
}
