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
