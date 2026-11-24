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
    fn cache_changed(&self, change: CacheChange) -> bool {
        self.changes
            .lock()
            .push((change.key, change.value, change.offset));
        true
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
    // Stamped as a leader stamps, on the members' clock: a member stops
    // recording once its clock passes the window after the last change.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after the epoch")
        .as_micros() as u64;
    let first = write(&cluster, now, put("k", b"one", 1)).await;
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

use super::super::super::cache::{CacheState, Change};

fn offsets_of(changes: &[Change]) -> Vec<u64> {
    changes.iter().map(|change| change.offset).collect()
}

/// **An unwatched member keeps nothing**, and a watched one keeps every
/// change from the first a watch of the shard took, so a resume from there
/// is replayed and one from before it resnapshots.
#[tokio::test]
async fn a_member_keeps_changes_only_once_its_shard_is_watched() {
    use openraft::storage::RaftStateMachine;
    let dir = tempdir().expect("dir");
    let recorder = Recorder::new();
    recorder.watching.store(false, Ordering::Release);
    let (mut machine, cache) = watched_machine(dir.path(), &recorder).await;
    let commands = [
        put("a", b"one", 1),
        add("n", 5, 2),
        delete("a", None, 3),
        put("a", b"two", 4),
        add("n", 1, 5),
        delete("a", None, 6),
    ];
    machine
        .apply(entries(1, 3, |index| commands[index as usize - 1].clone()))
        .await
        .expect("apply");
    assert!(cache.read().recent_since(0, &|_| true).is_none());

    recorder.watching.store(true, Ordering::Release);
    machine
        .apply(entries(4, 6, |index| commands[index as usize - 1].clone()))
        .await
        .expect("apply");
    let state = cache.read();
    assert!(
        state.recent_since(3, &|_| true).is_none(),
        "kept before a watch"
    );
    let kept = state
        .recent_since(4, &|_| true)
        .expect("from the first watched");
    assert_eq!(offsets_of(&kept), vec![4, 6]);
    assert_eq!(kept[1].value, None, "a delete is kept as a removal");
}

fn changed(report: bool) -> impl Fn(&Change) -> bool {
    move |_| report
}

/// **Recording lasts a window past the last watched change, then stops**,
/// on apply or on the group's tick, and a watch that ends starts it again
/// at the live edge. A resume from below where recording restarted is
/// resnapshotted, never replayed with a gap.
#[test]
fn recording_stops_a_window_after_the_last_watch() {
    let window = super::super::super::cache::WINDOW_MICROS;
    let mut state = CacheState::default();
    let seen = changed(true);
    let unseen = changed(false);
    state.apply_reporting(1, 100, &put("k", b"one", 1), Some(&seen));
    state.apply_reporting(2, 100 + window, &put("k", b"two", 2), Some(&unseen));
    state.apply_reporting(3, 100 + window, &put("k", b"three", 3), None);
    assert_eq!(
        offsets_of(&state.recent_since(1, &|_| true).expect("in the window")),
        vec![1, 2, 3]
    );
    state.apply_reporting(4, 101 + window, &put("k", b"four", 4), Some(&unseen));
    assert!(
        state.recent_since(1, &|_| true).is_none(),
        "past the window"
    );
    assert!(state.recent_since(5, &|_| true).is_none());

    state.watch_ended(200 + window);
    assert!(state.recent_since(4, &|_| true).is_none());
    state.apply_reporting(5, 300 + window, &put("k", b"five", 5), None);
    assert_eq!(
        offsets_of(&state.recent_since(5, &|_| true).expect("since it ended")),
        vec![5]
    );
    assert!(!state.recording_lapsed(200 + 2 * window));
    assert!(state.recording_lapsed(201 + 2 * window));
    state.stop_lapsed_recording(201 + 2 * window);
    assert!(
        state.recent_since(6, &|_| true).is_none(),
        "an idle shard frees it"
    );
}

/// **A live watch is told of a value too big to keep**: the live report
/// comes from the change itself, and the floor moves past it, so a resume
/// from it resnapshots.
#[tokio::test]
async fn a_live_watch_sees_a_value_too_big_to_keep() {
    use openraft::storage::RaftStateMachine;
    let dir = tempdir().expect("dir");
    let recorder = Recorder::new();
    let (mut machine, cache) = watched_machine(dir.path(), &recorder).await;
    let big: &'static [u8] = Box::leak(vec![7u8; 2 << 20].into_boxed_slice());
    machine
        .apply(entries(1, 2, |index| match index {
            1 => put("k", b"small", 1),
            _ => put("k", big, 2),
        }))
        .await
        .expect("apply");
    let told = recorder.changes();
    assert_eq!(told.len(), 2, "the big value reached no watch");
    assert_eq!(told[1].1.as_deref().map(<[u8]>::len), Some(big.len()));
    assert_eq!(told[1].2, 2);
    assert!(cache.read().recent_since(2, &|_| true).is_none());
    assert_eq!(cache.read().recent_since(3, &|_| true), Some(vec![]));
}

/// **A snapshot install and a restart start the kept changes past the
/// snapshot's entry**: the state was replaced, so a resume from at or below
/// it is resnapshotted, never replayed with the folded writes missing. A
/// restart keeps nothing until a watch takes a change again.
#[tokio::test]
async fn an_install_and_a_restart_forget_the_kept_changes() {
    use openraft::storage::RaftStateMachine;
    let dir = tempdir().expect("dir");
    let recorder = Recorder::new();
    {
        let (mut machine, cache) = watched_machine(dir.path(), &recorder).await;
        machine
            .apply(entries(1, 3, |index| keyed(index, 2)))
            .await
            .expect("apply");
        let mut sent = CacheState::default();
        sent.apply(9, 90, &put("k", b"sent", 9));
        install(&mut machine, 9, &sent).await;
        assert!(cache.read().recent_since(3, &|_| true).is_none());
        assert!(cache.read().recent_since(9, &|_| true).is_none());
        machine
            .apply(entries(10, 10, |_| put("k", b"after", 10)))
            .await
            .expect("apply");
        let kept = cache.read().recent_since(10, &|_| true).expect("past it");
        assert_eq!(offsets_of(&kept), vec![10]);
    }
    let (_machine, cache) = watched_machine(dir.path(), &recorder).await;
    assert_eq!(cache.read().applied(), 9);
    assert!(cache.read().recent_since(9, &|_| true).is_none());
    assert!(cache.read().recent_since(10, &|_| true).is_none());
}

/// **A resume is replayed from its offset on a new leader, then joins live
/// delivery with no gap or duplicate.** The first change replayed is the
/// one at the resume offset, which the new leader applied as a follower.
/// Writes land while the replay is read: what the replay returns is below
/// its edge, and the live half is what arrives at or past it, as the
/// handler keeps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_replays_from_its_offset_and_joins_live_with_no_gap_or_duplicate() {
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
    // Stamped as a leader stamps, on the members' clock: a member stops
    // recording once its clock passes the window after the last change.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after the epoch")
        .as_micros() as u64;
    let first = write(&cluster, now, put("k", b"one", 1)).await;
    cluster.committed_everywhere(N, first).await;
    let old = cluster.elected(N).await;
    cut_off(&cluster, &old.broker);
    let (engine, second) = leading(&cluster).await;
    assert_ne!(second.member, old);
    joined(&cluster, &old.broker);
    let recorder = &recorders[&*second.member.broker];

    let writes = async {
        let mut written = Vec::new();
        for n in 0..20 {
            written.push(write(&cluster, now + 1 + n, put("k", b"more", 2 + n)).await);
        }
        written
    };
    let replay = async {
        eventually(|| (recorder.changes().len() >= 5).then_some(())).await;
        engine
            .cache_replay(&shard(N), first, &|_| true)
            .expect("a cache group")
    };
    let (written, replay) = tokio::join!(writes, replay);
    let last = *written.last().expect("writes");
    eventually(|| recorder.offsets().contains(&last).then_some(())).await;

    let replayed: Vec<_> = replay
        .changes
        .expect("the resume offset is still kept")
        .iter()
        .map(|change| change.offset)
        .collect();
    assert_eq!(
        replayed.first(),
        Some(&first),
        "the replay starts at the offset"
    );
    assert!(replayed.iter().all(|at| *at < replay.tail));
    let mut delivered = replayed;
    delivered.extend(
        recorder
            .offsets()
            .into_iter()
            .filter(|at| *at >= replay.tail),
    );
    let mut wanted = vec![first];
    wanted.extend(written);
    assert_eq!(delivered, wanted, "a gap or a duplicate at the join");
    cluster.stop().await;
}
