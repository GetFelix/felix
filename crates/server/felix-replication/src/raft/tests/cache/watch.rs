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
        Arc::default(),
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
            .await
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

/// **A Watched entry starts recording at its own index on a member no watch
/// is served on**, keeps it going for its whole life whatever the member's
/// watches do, and a resume from below it resnapshots. The leader is asked
/// for another only once the last has less than a window left.
#[test]
fn a_mark_records_from_its_entry_for_its_life() {
    use super::super::super::cache::WINDOW_MICROS;
    let mark = 2 * WINDOW_MICROS;
    let mut state = CacheState::default();
    assert!(state.wants_mark(0));
    state.apply(1, 100, &put("k", b"one", 1));
    state.apply(2, 100, &CacheCommand::Watched);
    assert!(!state.wants_mark(100));
    assert!(state.wants_mark(101 + WINDOW_MICROS));
    state.apply(3, 100, &put("k", b"two", 2));
    state.apply(4, 99 + mark, &delete("k", None, 3));
    assert!(state.recent_since(1, &|_| true).is_none(), "below the mark");
    let kept = state.recent_since(2, &|_| true).expect("from the mark");
    assert_eq!(offsets_of(&kept), vec![3, 4]);
    assert!(state.recent_since(3, &|_| true).is_some());
    state.apply(5, 101 + mark, &put("k", b"three", 4));
    assert!(
        state.recent_since(2, &|_| true).is_none(),
        "past the mark's life"
    );
}

/// **A member that installs a snapshot, or restarts and applies its log
/// again, mid-window claims no change it does not hold**: an install
/// forgets what was kept, and a restart records again only from a mark it
/// applies, from that mark's entry on.
#[tokio::test]
async fn an_install_or_a_restart_mid_mark_keeps_a_true_floor() {
    use openraft::storage::RaftStateMachine;
    let dir = tempdir().expect("dir");
    let recorder = Recorder::new();
    recorder.watching.store(false, Ordering::Release);
    let marked = |index: u64| match index {
        2 | 7 => CacheCommand::Watched,
        _ => keyed(index, 2),
    };
    {
        let (mut machine, cache) = watched_machine(dir.path(), &recorder).await;
        machine.apply(entries(1, 4, marked)).await.expect("apply");
        let kept = cache.read().recent_since(2, &|_| true).expect("marked");
        assert_eq!(offsets_of(&kept), vec![3, 4]);
        let mut sent = CacheState::default();
        sent.apply(5, 50, &put("k", b"sent", 5));
        install(&mut machine, 5, &sent).await;
        for from in [2, 5, 6] {
            assert!(cache.read().recent_since(from, &|_| true).is_none());
        }
    }
    // openraft applies the log above the snapshot again on a restart.
    let (mut machine, cache) = watched_machine(dir.path(), &recorder).await;
    machine.apply(entries(6, 6, marked)).await.expect("apply");
    assert!(cache.read().recent_since(6, &|_| true).is_none());
    machine.apply(entries(7, 8, marked)).await.expect("apply");
    assert!(cache.read().recent_since(6, &|_| true).is_none());
    let kept = cache.read().recent_since(7, &|_| true).expect("marked");
    assert_eq!(offsets_of(&kept), vec![8]);
}

fn wall_clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after the epoch")
        .as_micros() as u64
}

/// A cluster whose members report to recorders that serve no watch.
async fn unwatched_cluster() -> (Cluster, HashMap<&'static str, Arc<Recorder>>) {
    let mut cluster = Cluster::new(true);
    let recorders: HashMap<&str, Arc<Recorder>> = BROKERS
        .iter()
        .map(|broker| {
            let recorder = Recorder::new();
            recorder.watching.store(false, Ordering::Release);
            (*broker, recorder)
        })
        .collect();
    for broker in BROKERS {
        cluster
            .runtime(broker)
            .report_cache_changes(Arc::clone(&recorders[broker]) as Arc<dyn CacheObserver>);
    }
    cluster.form(N).await;
    (cluster, recorders)
}

/// Wait until every member applied a mark: each records from its edge.
async fn every_member_records(cluster: &Cluster) {
    eventually(|| {
        BROKERS
            .iter()
            .all(|broker| {
                let group = cluster.runtime(broker).group(&shard(N)).expect("group");
                let state = group.cache().read();
                state.recent_since(state.applied() + 1, &|_| true).is_some()
            })
            .then_some(())
    })
    .await;
}

/// Five puts after the mark, the first of which is where a client resumes.
async fn writes_after_the_mark(cluster: &Cluster, now: u64) -> Vec<u64> {
    let mut written = Vec::new();
    for n in 0..5 {
        written.push(write(cluster, now + 10 + n, put("k", b"more", 10 + n)).await);
    }
    cluster
        .committed_everywhere(N, *written.last().expect("writes"))
        .await;
    written
}

fn replayed_offsets(replay: &crate::raft::CacheReplay) -> Option<Vec<u64>> {
    let changes = replay.changes.as_ref()?;
    Some(changes.iter().map(|change| change.offset).collect())
}

/// **A watch of the leader that resumes on the new leader after a failover
/// is replayed**, exactly the changes from its offset on, because the
/// leader appended a mark when its watch took a change and the follower
/// that took over recorded from it. A resume from before the mark is
/// replayed from the log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_on_a_new_leader_after_a_failover_is_replayed() {
    let (mut cluster, recorders) = unwatched_cluster().await;
    let now = wall_clock();
    let before = write(&cluster, now, put("k", b"zero", 1)).await;
    cluster.committed_everywhere(N, before).await;
    let old = cluster.elected(N).await;
    recorders[&*old.broker]
        .watching
        .store(true, Ordering::Release);
    let one = write(&cluster, now + 1, put("k", b"one", 2)).await;
    every_member_records(&cluster).await;
    let written = writes_after_the_mark(&cluster, now).await;

    cut_off(&cluster, &old.broker);
    let (engine, second) = leading(&cluster).await;
    assert_ne!(second.member, old);
    let resumed = engine
        .cache_replay(&shard(N), written[0], &|_| true)
        .await
        .expect("a cache group");
    assert_eq!(
        replayed_offsets(&resumed),
        Some(written.clone()),
        "the new leader kept no changes for the resume"
    );
    let early = engine
        .cache_replay(&shard(N), before, &|_| true)
        .await
        .expect("a cache group");
    let mut all = vec![before, one];
    all.extend(&written);
    assert_eq!(
        replayed_offsets(&early),
        Some(all),
        "below what it recorded, the resume is replayed from the log"
    );
    cluster.stop().await;
}

/// **A watch served by a follower has the leader mark the shard**, so a
/// resume on the other follower, which served no watch, is replayed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_watch_on_a_follower_makes_every_member_record() {
    let (mut cluster, recorders) = unwatched_cluster().await;
    let leader = cluster.elected(N).await;
    let mut followers = BROKERS.iter().filter(|broker| **broker != &*leader.broker);
    let (watched, other) = (
        followers.next().expect("one"),
        followers.next().expect("two"),
    );
    recorders[watched].watching.store(true, Ordering::Release);
    let now = wall_clock();
    write(&cluster, now, put("k", b"one", 1)).await;
    every_member_records(&cluster).await;
    let written = writes_after_the_mark(&cluster, now).await;

    let engine = RaftEngine::new(cluster.runtime(other));
    let resumed = engine
        .cache_replay(&shard(N), written[0], &|_| true)
        .await
        .expect("a cache group");
    assert_eq!(replayed_offsets(&resumed), Some(written));
    cluster.stop().await;
}

/// **An unwatched shard gets no mark**: no member records and the log
/// holds only the writes, however often the groups are ticked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unwatched_shard_appends_no_mark() {
    let (mut cluster, _recorders) = unwatched_cluster().await;
    let now = wall_clock();
    let mut last = 0;
    for n in 0..5 {
        last = write(&cluster, now + n, put("k", b"v", 1 + n)).await;
    }
    cluster.committed_everywhere(N, last).await;
    for _ in 0..5 {
        for broker in BROKERS {
            cluster
                .runtime(broker)
                .group(&shard(N))
                .expect("group")
                .tick()
                .await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for broker in BROKERS {
        let group = cluster.runtime(broker).group(&shard(N)).expect("group");
        let state = group.cache().read();
        assert_eq!(
            state.applied(),
            last,
            "{broker} applied an entry past the writes"
        );
        assert!(
            state.recent_since(last + 1, &|_| true).is_none(),
            "{broker} records an unwatched shard"
        );
    }
    cluster.stop().await;
}

/// **A member that opens from a snapshot inside a mark's life records from
/// past the snapshot's entry**, after an install and after a restart, so a
/// resume above that floor is replayed and one at or below it resnapshots.
/// A snapshot with no live mark starts no recording: an unwatched shard
/// still pays nothing after either.
#[tokio::test]
async fn a_member_opening_inside_a_mark_records_past_its_snapshot() {
    use super::super::super::cache::WINDOW_MICROS;
    use openraft::storage::RaftStateMachine;
    let recorder = Recorder::new();
    recorder.watching.store(false, Ordering::Release);
    for live in [true, false] {
        let dir = tempdir().expect("dir");
        let mut sent = CacheState::default();
        sent.apply(3, 30, &CacheCommand::Watched);
        // Without a live mark: a later entry stamped past its life.
        let stamp = if live { 40 } else { 31 + 2 * WINDOW_MICROS };
        sent.apply(5, stamp, &put("k", b"sent", 5));
        // As a receiver holds it: parsed from the bytes, recording nothing.
        let sent = CacheState::restore(&sent.snapshot(), true).expect("parse");
        let resumed = |cache: &super::super::super::machine::Cache| {
            let state = cache.read();
            (
                state.recent_since(5, &|_| true).is_some(),
                state
                    .recent_since(6, &|_| true)
                    .map(|kept| offsets_of(&kept)),
            )
        };
        {
            let (mut machine, cache) = watched_machine(dir.path(), &recorder).await;
            install(&mut machine, 5, &sent).await;
            let opened = match live {
                true => Some(vec![]),
                false => None,
            };
            assert_eq!(resumed(&cache), (false, opened), "after the install");
        }
        let (mut machine, cache) = watched_machine(dir.path(), &recorder).await;
        // openraft applies the log above the snapshot again.
        machine
            .apply(entries(6, 7, |index| keyed(index, 2)))
            .await
            .expect("apply");
        let kept = match live {
            true => Some(vec![6, 7]),
            false => None,
        };
        assert_eq!(resumed(&cache), (false, kept), "after the restart");
    }
}

/// **A snapshot carries the mark's end, and two states differ by it**; a
/// version 1 snapshot, which has none, reads as one with no live mark.
#[test]
fn a_snapshot_carries_the_mark() {
    let mut marked = CacheState::default();
    marked.apply(1, 10, &put("k", b"one", 1));
    marked.apply(2, 20, &CacheCommand::Watched);
    let mut unmarked = CacheState::default();
    unmarked.apply(1, 10, &put("k", b"one", 1));
    unmarked.apply(
        2,
        20,
        &CacheCommand::Expire {
            key: "k".into(),
            version: 99,
        },
    );
    assert_ne!(marked, unmarked, "the mark is state");
    let bytes = marked.snapshot();
    let restored = CacheState::restore(&bytes, true).expect("restore");
    assert_eq!(restored, marked);
    let mut old = bytes.to_vec();
    old[0] = 1;
    old.drain(9..17);
    let restored = CacheState::restore(&Bytes::from(old), true).expect("version 1");
    assert_eq!(restored, unmarked);
}

/// **A resume at the edge of a member that kept nothing is replayed, not
/// resnapshotted**, though entries land after it: the probe starts recording
/// at the edge it reads, so the mark the resume asks for, or a put, cannot
/// move the edge past an offset that missed no change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_at_the_edge_is_replayed_though_entries_land_after_it() {
    let (mut cluster, _recorders) = unwatched_cluster().await;
    let now = wall_clock();
    let first = write(&cluster, now, put("k", b"one", 1)).await;
    cluster.committed_everywhere(N, first).await;
    let (engine, _) = leading(&cluster).await;
    let edge = engine
        .cache_replay(&shard(N), first + 1, &|_| true)
        .await
        .expect("a cache group");
    assert_eq!(edge.tail, first + 1);
    assert_eq!(
        replayed_offsets(&edge),
        Some(vec![]),
        "resnapshotted at the edge"
    );
    let next = write(&cluster, now + 1, put("k", b"two", 2)).await;
    cluster.committed_everywhere(N, next).await;
    let again = engine
        .cache_replay(&shard(N), first + 1, &|_| true)
        .await
        .expect("a cache group");
    assert_eq!(replayed_offsets(&again), Some(vec![next]));
    let below = engine
        .cache_replay(&shard(N), first, &|_| true)
        .await
        .expect("a cache group");
    assert_eq!(
        replayed_offsets(&below),
        Some(vec![first, next]),
        "below where it began recording, replayed from the log"
    );
    cluster.stop().await;
}

/// What `replay` hands a resuming watch, as (offset, key, value).
fn replayed_changes(
    replay: &crate::raft::CacheReplay,
) -> Option<Vec<(u64, String, Option<Bytes>)>> {
    let changes = replay.changes.as_ref()?;
    Some(
        changes
            .iter()
            .map(|change| (change.offset, change.key.clone(), change.value.clone()))
            .collect(),
    )
}

/// **A resume on a shard no watch ever took a change of is replayed from
/// the log, as the hybrid replays its shard log**, on the leader and on
/// each follower alike: exactly the changes from its offset on, in order,
/// with the refused write, the counter add and the retry making none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_on_a_shard_nobody_watched_is_replayed_from_the_log() {
    let (mut cluster, _recorders) = unwatched_cluster().await;
    let now = wall_clock();
    let one = write(&cluster, now, put("k", b"one", 1)).await;
    write(&cluster, now + 1, put_if_absent("k", b"refused", 2)).await;
    write(&cluster, now + 2, add("n", 5, 3)).await;
    let jay = write(&cluster, now + 3, put("j", b"jay", 4)).await;
    let gone = write(&cluster, now + 4, delete("k", None, 5)).await;
    let two = write(&cluster, now + 5, put("k", b"two", 6)).await;
    let retry = write(&cluster, now + 6, put("k", b"one", 1)).await;
    cluster.committed_everywhere(N, retry).await;

    let value = |bytes: &'static [u8]| Some(Bytes::from_static(bytes));
    let all = vec![
        (one, "k".to_string(), value(b"one")),
        (jay, "j".to_string(), value(b"jay")),
        (gone, "k".to_string(), None),
        (two, "k".to_string(), value(b"two")),
    ];
    for broker in BROKERS {
        let engine = RaftEngine::new(cluster.runtime(broker));
        let from_start = engine
            .cache_replay(&shard(N), 0, &|_| true)
            .await
            .expect("a cache group");
        assert!(
            from_start.tail > retry,
            "{broker}: the edge is past the writes"
        );
        assert_eq!(replayed_changes(&from_start), Some(all.clone()), "{broker}");
        let from_jay = engine
            .cache_replay(&shard(N), jay, &|_| true)
            .await
            .expect("a cache group");
        assert_eq!(
            replayed_changes(&from_jay),
            Some(all[1..].to_vec()),
            "{broker}"
        );
        let only_j = engine
            .cache_replay(&shard(N), 0, &|key| key == "j")
            .await
            .expect("a cache group");
        assert_eq!(
            replayed_changes(&only_j),
            Some(vec![all[1].clone()]),
            "{broker}"
        );
    }
    cluster.stop().await;
}

/// **A resume from an offset the snapshot covers is resnapshotted**, as a
/// hybrid resume below retention is, while one from above the snapshot is
/// still replayed from the log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_from_below_the_snapshot_is_resnapshotted() {
    let (mut cluster, _recorders) = unwatched_cluster().await;
    let now = wall_clock();
    let first = write(&cluster, now, put("k", b"one", 1)).await;
    let mid = write(&cluster, now + 1, put("k", b"two", 2)).await;
    cluster.committed_everywhere(N, mid).await;
    for broker in BROKERS {
        cluster.snapshot(broker, N, mid).await;
    }
    let last = write(&cluster, now + 2, put("k", b"three", 3)).await;
    cluster.committed_everywhere(N, last).await;
    for broker in BROKERS {
        let engine = RaftEngine::new(cluster.runtime(broker));
        let below = engine
            .cache_replay(&shard(N), first, &|_| true)
            .await
            .expect("a cache group");
        assert!(
            below.changes.is_none(),
            "{broker}: compacted into the snapshot"
        );
        let above = engine
            .cache_replay(&shard(N), last, &|_| true)
            .await
            .expect("a cache group");
        assert_eq!(replayed_offsets(&above), Some(vec![last]), "{broker}");
    }
    cluster.stop().await;
}

/// **A resume replayed from the log joins live delivery with no gap or
/// duplicate while writes land during the replay.** The watch registers
/// first, so it records from a later change than the resume offset and
/// the replay has to come from the log. What the replay returns is below
/// its edge, and the live half is what arrives at or past it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_from_the_log_joins_live_with_no_gap_or_duplicate() {
    let (mut cluster, recorders) = unwatched_cluster().await;
    let now = wall_clock();
    let first = write(&cluster, now, put("k", b"one", 1)).await;
    cluster.committed_everywhere(N, first).await;
    let (engine, leader) = leading(&cluster).await;
    let recorder = &recorders[&*leader.member.broker];
    recorder.watching.store(true, Ordering::Release);

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
            .await
            .expect("a cache group")
    };
    let (written, replay) = tokio::join!(writes, replay);
    let last = *written.last().expect("writes");
    eventually(|| recorder.offsets().contains(&last).then_some(())).await;

    let mut delivered = replayed_offsets(&replay).expect("the log still holds the resume offset");
    assert!(delivered.iter().all(|at| *at < replay.tail));
    assert!(
        recorder.offsets().first().is_some_and(|at| *at > first),
        "the watch recorded nothing as far back as the resume"
    );
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

/// **A resume the snapshot covers is resnapshotted without reading the
/// snapshot file**, and without waiting behind replays at the bound: the
/// covered index is kept in memory. With the file made unreadable, a
/// resume above the snapshot fails its read, and one it covers is still
/// answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_the_snapshot_covers_reads_no_snapshot_file() {
    let (mut cluster, _recorders) = unwatched_cluster().await;
    let now = wall_clock();
    let first = write(&cluster, now, put("k", b"one", 1)).await;
    let mid = write(&cluster, now + 1, put("k", b"two", 2)).await;
    cluster.committed_everywhere(N, mid).await;
    let broker = BROKERS[0];
    cluster.snapshot(broker, N, mid).await;
    let group = cluster.runtime(broker).group(&shard(N)).expect("group");
    let (tail, _) = group.cache_replay(first, &|_| true).expect("a cache group");

    let file = cluster.dir(broker, N).join("raft-snapshot");
    let stored = std::fs::read(&file).expect("the snapshot file");
    std::fs::write(&file, b"not a snapshot").expect("damage the file");
    assert!(
        group.replay_from_log(mid + 1, tail).await.is_err(),
        "a resume above the snapshot reads the file"
    );
    let covered = group.replay_from_log(first, tail).await;
    assert!(matches!(covered, Ok(None)), "{covered:?}");
    let held = group.hold_log_replays().await;
    let decided = tokio::time::timeout(
        Duration::from_secs(1),
        group.cache_replay_from_log(first, tail, &|_| true),
    )
    .await;
    assert!(matches!(decided, Ok(None)), "{decided:?}");
    drop(held);
    std::fs::write(&file, stored).expect("restore the file");
    cluster.stop().await;
}

/// **Concurrent resumes from the log queue behind the bound** rather than
/// each restoring its own copy of the snapshot at once, and none of them
/// fails for waiting: once the bound frees, each is replayed in full.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replays_from_the_log_queue_behind_the_bound() {
    let (mut cluster, _recorders) = unwatched_cluster().await;
    let now = wall_clock();
    let first = write(&cluster, now, put("k", b"one", 1)).await;
    cluster.committed_everywhere(N, first).await;
    let group = cluster.runtime(BROKERS[0]).group(&shard(N)).expect("group");
    let (tail, _) = group.cache_replay(first, &|_| true).expect("a cache group");

    let held = group.hold_log_replays().await;
    let replays = futures::future::join_all(
        (0..=super::super::super::group::LOG_REPLAYS)
            .map(|_| group.cache_replay_from_log(first, tail, &|_| true)),
    );
    tokio::pin!(replays);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut replays)
            .await
            .is_err(),
        "a replay ran with the bound taken"
    );
    drop(held);
    let replayed = tokio::time::timeout(PATIENCE, replays)
        .await
        .expect("the queued replays ran once the bound freed");
    for changes in replayed {
        assert_eq!(changes.as_deref().map(offsets_of), Some(vec![first]));
    }
    cluster.stop().await;
}
