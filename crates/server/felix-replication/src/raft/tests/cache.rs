//! A cache shard's group: every member folds the same entries into the
//! same state, and a snapshot carries it across a restart and to a member
//! that is behind.

use super::super::cache::{Answer, CacheCommand, Condition, Request};
use super::*;

const N: u32 = CACHE;

fn request(sequence: u64) -> Request {
    Request {
        producer: 7,
        sequence,
        floor: 0,
    }
}

fn put(key: &str, value: &'static [u8], sequence: u64) -> CacheCommand {
    CacheCommand::Put {
        key: key.into(),
        value: Bytes::from_static(value),
        expires_at: 0,
        cond: Condition::None,
        request: request(sequence),
    }
}

fn add(key: &str, delta: i64, sequence: u64) -> CacheCommand {
    CacheCommand::Add {
        key: key.into(),
        delta,
        request: request(sequence),
    }
}

/// Commit `command` stamped `stamp` through the group and return its index.
async fn write(cluster: &Cluster, stamp: u64, command: CacheCommand) -> u64 {
    let leader = cluster.elected(N).await;
    let member = cluster
        .runtime(&leader.broker)
        .group(&shard(N))
        .expect("group");
    let command = Command {
        timestamp_micros: stamp,
        payload: command.encode(),
        written: None,
    };
    let written = member.raft().client_write(command).await.expect("write");
    written.log_id.index
}

fn state(cluster: &Cluster, broker: &str) -> Bytes {
    let member = cluster.runtime(broker).group(&shard(N)).expect("group");
    member.cache().read().snapshot()
}

/// Wait until every member holds the state the leader holds.
async fn agreed(cluster: &Cluster) -> Bytes {
    eventually(|| {
        let first = state(cluster, BROKERS[0]);
        BROKERS
            .iter()
            .all(|broker| state(cluster, broker) == first)
            .then_some(first)
    })
    .await
}

/// A member that restarts holds what its snapshot held plus the entries
/// above it, which is what it held before. A snapshot with no data, or one
/// whose entries were applied again from the start, would not add up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_resumes_from_its_snapshot() {
    let mut cluster = Cluster::new(true);
    cluster.form(N).await;
    write(&cluster, 10, put("k", b"one", 1)).await;
    write(&cluster, 20, add("n", 5, 2)).await;
    let mid = write(&cluster, 30, put("gone", b"x", 3)).await;
    cluster.committed_everywhere(N, mid).await;
    for broker in BROKERS {
        cluster.snapshot(broker, N, mid).await;
    }
    write(&cluster, 40, add("n", 2, 4)).await;
    let last = write(&cluster, 50, put("k", b"two", 5)).await;
    cluster.committed_everywhere(N, last).await;
    let before = agreed(&cluster).await;
    let leader = cluster.elected(N).await;
    let member = cluster
        .runtime(&leader.broker)
        .group(&shard(N))
        .expect("group");
    assert_eq!(member.cache().read().sum("n"), Some(7));
    assert_eq!(
        member.cache().read().answer(7, 5),
        Some(Answer::Written { version: last })
    );

    cluster.stop().await;
    cluster.start(true);
    cluster.open(N).await;
    cluster.elected(N).await;
    for broker in BROKERS {
        eventually(|| (state(&cluster, broker) == before).then_some(())).await;
    }
    cluster.stop().await;
}

/// A member that installs its leader's snapshot holds the leader's state as
/// of it, the answers included, so a retry that reaches it later is not
/// applied twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_that_installs_a_snapshot_holds_its_state_and_answers() {
    let mut cluster = Cluster::new(true);
    cluster.form(N).await;
    let first = write(&cluster, 10, put("k", b"one", 1)).await;
    cluster.committed_everywhere(N, first).await;
    let leader = cluster.elected(N).await;
    let behind = *BROKERS
        .iter()
        .find(|broker| ***broker != *leader.broker)
        .expect("a follower");
    cluster.net.cut.lock().insert(behind.into());
    write(&cluster, 20, add("n", 5, 2)).await;
    let last = write(&cluster, 30, put("k", b"two", 3)).await;
    let leader = cluster.elected(N).await;
    cluster.snapshot(&leader.broker, N, last).await;
    let leading = cluster
        .runtime(&leader.broker)
        .group(&shard(N))
        .expect("group");
    let snapshot = leading.raft().get_snapshot().await.expect("read");
    let snapshot = snapshot.expect("snapshot");
    let data = Bytes::from(snapshot.snapshot.clone());
    assert_ne!(state(&cluster, behind), data);

    let leads = LeaderIdOf::<ShardTypes>::new(leading.status().term, leader.clone());
    let vote = <VoteOf<ShardTypes> as RaftVote>::from_leader_id(leads, true);
    let member = cluster.runtime(behind).group(&shard(N)).expect("group");
    member
        .raft()
        .install_full_snapshot(vote, snapshot)
        .await
        .expect("install");
    eventually(|| (state(&cluster, behind) == data).then_some(())).await;
    assert_eq!(
        member.cache().read().answer(7, 3),
        Some(Answer::Written { version: last })
    );
    cluster.stop().await;
}

/// One settable clock for every broker, so whoever leads stamps by it.
fn clocked(cluster: &Cluster, at: u64) -> Arc<std::sync::atomic::AtomicU64> {
    let clock = Arc::new(std::sync::atomic::AtomicU64::new(at));
    for runtime in cluster.runtimes() {
        let clock = Arc::clone(&clock);
        runtime.set_clock(Arc::new(move || {
            clock.load(std::sync::atomic::Ordering::Relaxed)
        }));
    }
    clock
}

async fn leading(cluster: &Cluster) -> (RaftEngine, Arc<super::super::group::Group>) {
    let leader = cluster.elected(N).await;
    let runtime = cluster.runtime(&leader.broker);
    let group = runtime.group(&shard(N)).expect("group");
    eventually(|| group.open_term()).await;
    (RaftEngine::new(runtime), group)
}

/// A leader whose clock is behind the entries it inherited stamps above
/// them. The first entry here is written with its stamp given, as another
/// leader's would arrive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_leader_stamps_above_an_inherited_tail() {
    let mut cluster = Cluster::new(true);
    clocked(&cluster, 5);
    cluster.form(N).await;
    let (engine, group) = leading(&cluster).await;
    let inherited = write(&cluster, 1_000, put("k", b"one", 1)).await;
    cluster.committed_everywhere(N, inherited).await;
    let answer = engine
        .cache_write(&shard(N), put("k", b"two", 2), 0, PATIENCE)
        .await
        .expect("write");
    assert!(matches!(answer, Answer::Written { .. }));
    assert_eq!(group.cache().read().time(), 1_000);
    cluster.stop().await;
}

/// A write stamped while this member led one term is not appended once it
/// leads another: the leader in between may have written later stamps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stamp_taken_in_one_term_is_not_written_in_another() {
    let mut cluster = Cluster::new(true);
    cluster.form(N).await;
    let (_, group) = leading(&cluster).await;
    let term = group.open_term().expect("open");
    let command = |sequence| Command {
        timestamp_micros: 10,
        payload: put("k", b"one", sequence).encode(),
        written: None,
    };
    assert!(!group.write_in_term(command(1), term - 1).await);
    assert!(group.write_in_term(command(2), term).await);
    assert_eq!(group.cache().read().answer(7, 1), None);
    assert!(group.cache().read().answer(7, 2).is_some());
    cluster.stop().await;
}

/// A value the leader's clock says has expired is not read, and its delete
/// is committed: a leader with a slower clock does not find it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expired_key_is_deleted_for_every_member_before_it_reads_absent() {
    let mut cluster = Cluster::new(true);
    let clock = clocked(&cluster, 100);
    cluster.form(N).await;
    let (engine, _) = leading(&cluster).await;
    engine
        .cache_write(&shard(N), put("k", b"one", 1), 50, PATIENCE)
        .await
        .expect("write");
    let read = engine.cache_read(&shard(N), "k", PATIENCE).await;
    assert!(read.expect("read").is_some());
    clock.store(200, std::sync::atomic::Ordering::Relaxed);
    let read = engine.cache_read(&shard(N), "k", PATIENCE).await;
    assert_eq!(read.expect("read"), None);
    clock.store(100, std::sync::atomic::Ordering::Relaxed);
    let read = engine.cache_read(&shard(N), "k", PATIENCE).await;
    assert_eq!(read.expect("read"), None);
    let empty = agreed(&cluster).await;
    assert_eq!(state(&cluster, BROKERS[0]), empty);
    cluster.stop().await;
}

/// The leader deletes an expired value with nobody reading it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expired_key_is_deleted_with_no_reader() {
    let mut cluster = Cluster::new(true);
    let clock = clocked(&cluster, 100);
    cluster.form(N).await;
    let (engine, group) = leading(&cluster).await;
    engine
        .cache_write(&shard(N), put("k", b"one", 1), 50, PATIENCE)
        .await
        .expect("write");
    clock.store(200, std::sync::atomic::Ordering::Relaxed);
    // An applied entry wakes the expiry task, as any write would.
    engine
        .cache_write(&shard(N), add("n", 1, 2), 0, PATIENCE)
        .await
        .expect("write");
    let sum = engine.counter_read(&shard(N), "n", PATIENCE).await;
    assert_eq!(sum.expect("read"), Some(1));
    eventually(|| group.cache().read().next_expiry().is_none().then_some(())).await;
    cluster.stop().await;
}

/// A clock of its own for each broker.
fn clocks(cluster: &Cluster, at: u64) -> HashMap<&'static str, Arc<std::sync::atomic::AtomicU64>> {
    let clock = |broker: &&'static str| {
        let clock = Arc::new(std::sync::atomic::AtomicU64::new(at));
        let read = Arc::clone(&clock);
        cluster.runtime(broker).set_clock(Arc::new(move || {
            read.load(std::sync::atomic::Ordering::Relaxed)
        }));
        (*broker, clock)
    };
    BROKERS.iter().map(clock).collect()
}

/// Nothing reaches `broker` and nothing is heard from it.
fn cut_off(cluster: &Cluster, broker: &str) {
    cluster.net.cut.lock().insert(broker.into());
    cluster.net.muted.lock().insert(broker.into());
}

fn joined(cluster: &Cluster, broker: &str) {
    cluster.net.cut.lock().remove(broker);
    cluster.net.muted.lock().remove(broker);
}

/// A leader that wins an election stamps above what the leader before it
/// wrote, though its own clock is far behind that leader's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_elected_leader_with_a_slow_clock_stamps_above_the_last_leaders_entries() {
    use std::sync::atomic::Ordering::Relaxed;
    let mut cluster = Cluster::new(true);
    let clocks = clocks(&cluster, 100);
    cluster.form(N).await;
    let (engine, first) = leading(&cluster).await;
    clocks[&*first.member.broker].store(10_000, Relaxed);
    engine
        .cache_write(&shard(N), put("k", b"one", 1), 0, PATIENCE)
        .await
        .expect("write");
    assert_eq!(first.cache().read().time(), 10_000);

    cut_off(&cluster, &first.member.broker);
    let (engine, second) = leading(&cluster).await;
    assert_ne!(second.member, first.member);
    let answer = engine
        .cache_write(&shard(N), put("k", b"two", 2), 0, PATIENCE)
        .await
        .expect("write");
    assert!(matches!(answer, Answer::Written { .. }));
    assert!(second.cache().read().time() >= 10_000);
    cluster.stop().await;
}

/// A write is stamped while its member leads, the member is deposed before
/// openraft is handed the write, and it is elected again. The leader in
/// between wrote a later stamp, so the write must not be appended now: it
/// would land below that entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_stamped_before_a_deposal_is_not_written_after_a_re_election() {
    use std::sync::atomic::Ordering::Relaxed;
    let mut cluster = Cluster::new(true);
    let clocks = clocks(&cluster, 100);
    cluster.form(N).await;
    let (engine, first) = leading(&cluster).await;
    let term = first.open_term().expect("open");

    let gate = Arc::clone(&first.gate);
    let shut = gate.shut.lock().await;
    let stamped = gate.stamped.load(Relaxed);
    let held = RaftEngine::new(cluster.runtime(&first.member.broker));
    let held = tokio::spawn(async move {
        held.cache_write(&shard(N), put("k", b"held", 1), 0, PATIENCE)
            .await
    });
    eventually(|| (gate.stamped.load(Relaxed) > stamped).then_some(())).await;

    cut_off(&cluster, &first.member.broker);
    let (between, second) = leading(&cluster).await;
    assert_ne!(second.member, first.member);
    clocks[&*second.member.broker].store(10_000, Relaxed);
    between
        .cache_write(&shard(N), put("k", b"between", 2), 0, PATIENCE)
        .await
        .expect("write");
    joined(&cluster, &first.member.broker);
    agreed(&cluster).await;
    let to = first.member.clone();
    let transfer = second.raft().trigger().transfer_leader(to).await;
    transfer.expect("transfer");
    eventually(|| first.open_term().filter(|open| *open > term)).await;

    drop(shut);
    // Queued behind the held write, so it is answered after it.
    engine
        .cache_write(&shard(N), add("n", 1, 3), 0, PATIENCE)
        .await
        .expect("write");
    assert!(held.await.expect("task").is_err());
    {
        let cache = first.cache().read();
        assert_eq!(cache.answer(7, 1), None);
        assert!(cache.time() >= 10_000);
    }
    cluster.stop().await;
}

/// A read does not look at the leader's state until the leader has applied
/// what was committed when a majority confirmed it leads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_waits_until_the_leader_has_applied_what_is_committed() {
    let mut cluster = Cluster::new(true);
    cluster.form(N).await;
    let (engine, group) = leading(&cluster).await;
    let first = write(&cluster, 10, put("k", b"one", 1)).await;
    cluster.committed_everywhere(N, first).await;

    let held = Arc::clone(&group.apply_hold).lock_owned().await;
    let second = Command {
        timestamp_micros: 20,
        payload: put("k", b"two", 2).encode(),
        written: None,
    };
    let raft = group.raft().clone();
    let writing = tokio::spawn(async move { raft.client_write(second).await.is_ok() });
    // Committed by the followers; the leader cannot apply it yet.
    eventually(|| (group.status().committed > Some(first)).then_some(())).await;
    let key = shard(N);
    let read = engine.cache_read(&key, "k", PATIENCE);
    let read = tokio::time::timeout(Duration::from_millis(500), read).await;
    assert!(read.is_err(), "the read did not wait: {read:?}");

    drop(held);
    assert!(writing.await.expect("task"));
    let read = engine.cache_read(&shard(N), "k", PATIENCE).await;
    let (value, _) = read.expect("read").expect("a value");
    assert_eq!(&value[..], b"two");
    cluster.stop().await;
}

/// The data of a snapshot is the state as of the entry the snapshot names,
/// whatever is applied before it is built. With later entries in it, a
/// member that restored it would apply them twice.
#[tokio::test]
async fn a_snapshot_holds_the_state_as_of_the_entry_it_names() {
    use openraft::entry::RaftEntry;
    use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
    use openraft::type_config::alias::{EntryOf, LogIdOf};
    let dir = tempdir().expect("dir");
    let snapshots = felix_storage::raft_log::SnapshotStore::open(dir.path()).expect("store");
    let opened = super::super::machine::Machine::open(snapshots, ShardKind::Cache).await;
    let (mut machine, _, cache) = opened.expect("open");
    let cache = cache.expect("a cache group");
    let entry = |index: u64, command: CacheCommand| {
        let command = Command {
            timestamp_micros: index * 10,
            payload: command.encode(),
            written: None,
        };
        let id = LogIdOf::<ShardTypes>::new(1.into(), index);
        let entry = EntryOf::<ShardTypes>::new_normal(id, command);
        futures::stream::iter([Ok((entry, None))])
    };
    machine
        .apply(entry(1, put("k", b"one", 1)))
        .await
        .expect("apply");
    let named = cache.read().snapshot();
    let mut builder = machine.get_snapshot_builder().await;
    machine
        .apply(entry(2, put("k", b"two", 2)))
        .await
        .expect("apply");
    let built = builder.build_snapshot().await.expect("build");
    assert_eq!(built.meta.last_log_id.map(|id| id.index), Some(1));
    assert_eq!(Bytes::from(built.snapshot), named);
}

/// Entries a snapshot covers can leave the log, and no others. A member
/// that restarts after that holds what the purged entries wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_purge_stops_at_the_snapshot_and_a_restart_restores_it() {
    let mut cluster = Cluster::new(true);
    cluster.form(N).await;
    write(&cluster, 10, put("k", b"one", 1)).await;
    let mid = write(&cluster, 20, add("n", 5, 2)).await;
    cluster.committed_everywhere(N, mid).await;
    for broker in BROKERS {
        cluster.snapshot(broker, N, mid).await;
    }
    let last = write(&cluster, 30, add("n", 2, 3)).await;
    cluster.committed_everywhere(N, last).await;
    let before = agreed(&cluster).await;
    for broker in BROKERS {
        let member = cluster.runtime(broker).group(&shard(N)).expect("group");
        member
            .raft()
            .trigger()
            .purge_log(last)
            .await
            .expect("purge");
        let purged = eventually(|| cluster.purged(broker, N).filter(|purged| *purged >= mid));
        assert_eq!(purged.await, mid);
    }

    cluster.stop().await;
    cluster.start(true);
    cluster.open(N).await;
    cluster.elected(N).await;
    for broker in BROKERS {
        eventually(|| (state(&cluster, broker) == before).then_some(())).await;
    }
    cluster.stop().await;
}

/// A cluster whose cache shards snapshot every eight entries and keep two
/// under the snapshot.
fn snapshotting() -> Cluster {
    Cluster::with(
        RuntimeConfig {
            cache_snapshot_every: 8,
            cache_log_keep: 2,
            ..timers()
        },
        true,
    )
}

/// A cache shard's log does not grow with its writes: every member
/// snapshots by itself and drops the entries its snapshot covers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cache_shards_log_is_cut_to_its_snapshot() {
    let mut cluster = snapshotting();
    cluster.form(N).await;
    let mut last = 0;
    for sequence in 1..=40 {
        last = write(&cluster, 10 * sequence, add("n", 1, sequence)).await;
    }
    cluster.committed_everywhere(N, last).await;
    for broker in BROKERS {
        let purged = eventually(|| {
            cluster
                .purged(broker, N)
                .filter(|purged| *purged + 12 >= last)
        });
        assert!(purged.await < last);
    }
    agreed(&cluster).await;
    cluster.stop().await;
}

/// A member cut off until its leader has purged the entries it lacks gets
/// the leader's snapshot from the leader, over the transport, and then
/// holds what the others hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_behind_the_purge_catches_up_by_snapshot() {
    let mut cluster = snapshotting();
    cluster.form(N).await;
    let first = write(&cluster, 10, put("k", b"one", 1)).await;
    cluster.committed_everywhere(N, first).await;
    let leader = cluster.elected(N).await;
    let behind = *BROKERS
        .iter()
        .find(|broker| ***broker != *leader.broker)
        .expect("a follower");
    cut_off(&cluster, behind);
    for sequence in 2..=40 {
        write(&cluster, 10 * sequence, add("n", 1, sequence)).await;
    }
    let leader = cluster.elected(N).await;
    eventually(|| {
        cluster
            .purged(&leader.broker, N)
            .filter(|purged| *purged > first + 8)
    })
    .await;
    assert_ne!(state(&cluster, behind), state(&cluster, &leader.broker));

    joined(&cluster, behind);
    let after = write(&cluster, 500, put("k", b"two", 41)).await;
    cluster.committed_everywhere(N, after).await;
    agreed(&cluster).await;
    let member = cluster.runtime(behind).group(&shard(N)).expect("group");
    assert_eq!(member.cache().read().answer(7, 40), Some(Answer::Sum(39)));
    // It was sent the state, not the entries: its own log starts past them.
    assert!(
        cluster
            .purged(behind, N)
            .is_some_and(|purged| purged > first)
    );
    cluster.stop().await;
}

/// A snapshot arrives in pieces and is installed at the last. A piece from
/// a sender the member has outvoted is answered with the member's vote and
/// not kept, one over the cap is refused at its first piece, and one that
/// skips ahead is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_in_pieces_is_installed_at_the_last() {
    use super::super::install::MAX_SNAPSHOT_BYTES;
    let mut cluster = Cluster::new(true);
    cluster.form(N).await;
    let first = write(&cluster, 10, put("k", b"one", 1)).await;
    cluster.committed_everywhere(N, first).await;
    let leader = cluster.elected(N).await;
    let behind = *BROKERS
        .iter()
        .find(|broker| ***broker != *leader.broker)
        .expect("a follower");
    cut_off(&cluster, behind);
    let last = write(&cluster, 30, put("k", b"two", 2)).await;
    let leader = cluster.elected(N).await;
    cluster.snapshot(&leader.broker, N, last).await;
    let leading = cluster
        .runtime(&leader.broker)
        .group(&shard(N))
        .expect("group");
    let snapshot = leading.raft().get_snapshot().await.expect("read");
    let snapshot = snapshot.expect("snapshot");
    let data = Bytes::from(snapshot.snapshot.clone());
    let term = leading.status().term;
    let vote = |term| {
        let leads = LeaderIdOf::<ShardTypes>::new(term, leader.clone());
        <VoteOf<ShardTypes> as RaftVote>::from_leader_id(leads, true)
    };
    let member = cluster.runtime(behind).group(&shard(N)).expect("group");
    let runtime = cluster.runtime(behind);
    let piece = |term, data_len: u64, offset: usize, data: Bytes| {
        let request = RaftRequest {
            group: group(N),
            to: MemberId::new(behind, 1),
            beat: 0,
            deadline: Duration::from_secs(10),
            body: super::super::network::Request::Snapshot {
                vote: vote(term),
                meta: snapshot.meta.clone(),
                data_len,
                offset: offset as u64,
                data,
            },
        };
        runtime.handle(request)
    };
    let whole = data.len() as u64;
    let half = data.len() / 2;

    let stale = piece(term - 1, whole, 0, data.slice(..half)).await;
    let Ok(RaftReply(super::super::network::Reply::Snapshot(answer))) = stale else {
        panic!("a stale sender was not answered");
    };
    assert_eq!(answer.vote, vote(term));
    assert_eq!(member.incoming().held(), None);

    let over = piece(term, MAX_SNAPSHOT_BYTES + 1, 0, data.slice(..half)).await;
    assert!(matches!(over, Err(Refused::Unreachable(_))));
    assert_eq!(member.incoming().held(), None);

    assert!(piece(term, whole, 0, data.slice(..half)).await.is_ok());
    assert_eq!(member.incoming().held(), Some(half));
    let skipped = piece(term, whole, half + 1, data.slice(half + 1..)).await;
    assert!(matches!(skipped, Err(Refused::Unreachable(_))));
    assert_ne!(state(&cluster, behind), data);

    // The sender starts again, which replaces what was held.
    assert!(piece(term, whole, 0, data.slice(..half)).await.is_ok());
    assert!(piece(term, whole, half, data.slice(half..)).await.is_ok());
    assert_eq!(member.incoming().held(), None);
    eventually(|| (state(&cluster, behind) == data).then_some(())).await;
    cluster.stop().await;
}

/// A stream's batch is never written to a cache shard: its raw record
/// would fail to apply on every member and on every restart. It is refused
/// at the call and again by the publish task, and the group goes on taking
/// cache writes. A cache write to a stream shard is refused too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cache_shard_takes_no_stream_batch_and_a_stream_shard_no_cache_write() {
    use super::super::propose::{AckAt, Batch, Record};
    let mut cluster = Cluster::new(true);
    cluster.form(N).await;
    cluster.form(1).await;
    let (engine, group) = leading(&cluster).await;
    let batch = || Batch {
        records: vec![Record {
            timestamp_micros: 10,
            payload: Bytes::from_static(b"a stream record"),
        }],
        ack: AckAt::Written,
    };
    // Refused at the call: nothing is queued.
    let refused = futures::FutureExt::now_or_never(engine.propose(&shard(N), batch()));
    assert!(matches!(refused, Some(Err(_))));
    assert!(group.propose_unchecked(batch()).await.is_err());
    let answer = engine
        .cache_write(&shard(N), put("k", b"one", 1), 0, PATIENCE)
        .await
        .expect("write");
    let Answer::Written { version } = answer else {
        panic!("not written: {answer:?}");
    };
    cluster.committed_everywhere(N, version).await;
    agreed(&cluster).await;
    for broker in BROKERS {
        let member = cluster.runtime(broker).group(&shard(N)).expect("group");
        assert_ne!(member.status().role, Role::Stopped);
    }

    let leader = cluster.elected(1).await;
    let stream = RaftEngine::new(cluster.runtime(&leader.broker));
    let refused = stream
        .cache_write(&shard(1), put("k", b"one", 2), 0, PATIENCE)
        .await;
    assert!(refused.is_err());
    cluster.stop().await;
}

/// A snapshot this member cannot read is refused before openraft is handed
/// it, and the member keeps running and catches up later. openraft would
/// take the restore error as a storage failure and stop the member.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_this_member_cannot_read_is_refused_and_it_keeps_running() {
    let mut cluster = Cluster::new(true);
    cluster.form(N).await;
    let first = write(&cluster, 10, put("k", b"one", 1)).await;
    cluster.committed_everywhere(N, first).await;
    let leader = cluster.elected(N).await;
    let behind = *BROKERS
        .iter()
        .find(|broker| ***broker != *leader.broker)
        .expect("a follower");
    cut_off(&cluster, behind);
    let last = write(&cluster, 30, put("k", b"two", 2)).await;
    let leader = cluster.elected(N).await;
    cluster.snapshot(&leader.broker, N, last).await;
    let leading = cluster
        .runtime(&leader.broker)
        .group(&shard(N))
        .expect("group");
    let snapshot = leading.raft().get_snapshot().await.expect("read");
    let snapshot = snapshot.expect("snapshot");
    // What a later build's format looks like to this one.
    let mut newer = snapshot.snapshot.clone();
    newer[0] = 2;
    let leads = LeaderIdOf::<ShardTypes>::new(leading.status().term, leader.clone());
    let vote = <VoteOf<ShardTypes> as RaftVote>::from_leader_id(leads, true);
    let request = RaftRequest {
        group: group(N),
        to: MemberId::new(behind, 1),
        beat: 0,
        deadline: Duration::from_secs(10),
        body: super::super::network::Request::Snapshot {
            vote,
            meta: snapshot.meta.clone(),
            data_len: newer.len() as u64,
            offset: 0,
            data: Bytes::from(newer),
        },
    };
    let refused = cluster.runtime(behind).handle(request).await;
    let Err(Refused::Unreachable(why)) = refused else {
        panic!("the snapshot was not refused as unreadable");
    };
    assert!(why.contains("cannot read"), "{why}");
    let member = cluster.runtime(behind).group(&shard(N)).expect("group");
    assert_ne!(member.status().role, Role::Stopped);

    joined(&cluster, behind);
    let after = write(&cluster, 50, put("k", b"three", 3)).await;
    cluster.committed_everywhere(N, after).await;
    agreed(&cluster).await;
    cluster.stop().await;
}

/// A measurement, not a check: what one openraft call per cache entry
/// costs next to one call for the whole drain. Run it alone with
/// `--ignored --nocapture`; only the ratio means anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "prints a ratio and checks nothing"]
async fn what_a_write_call_per_entry_costs() {
    use futures::StreamExt;
    const ROUNDS: u64 = 40;
    let mut cluster = Cluster::new(true);
    cluster.form(N).await;
    let (_, group) = leading(&cluster).await;
    let term = group.open_term().expect("open");
    for entries in [1, 16, 256] {
        let mut took = Vec::new();
        for (mode, term) in [(0, Some(term)), (1, None)] {
            let started = std::time::Instant::now();
            for round in 0..ROUNDS {
                let commands = (0..entries).map(|entry| Command {
                    timestamp_micros: 10,
                    payload: put("k", b"value", (mode * ROUNDS + round) * 1_000 + entry).encode(),
                    written: None,
                });
                let commands = commands.collect();
                let sent = super::super::propose::write(group.raft(), commands, term).await;
                let results: Vec<_> = sent.expect("running").collect().await;
                assert!(results.iter().all(|result| matches!(result, Ok(Ok(_)))));
            }
            took.push(started.elapsed().as_secs_f64());
        }
        let ratio = took[0] / took[1];
        println!("{entries} entries a drain: a call per entry takes {ratio:.2}x one call");
    }
    cluster.stop().await;
}
