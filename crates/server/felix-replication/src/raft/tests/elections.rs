//! The broker's bound on elections in flight
//! (`docs/per-shard-raft.md`, "Many groups electing at once").

use std::sync::atomic::Ordering::Relaxed;

use super::*;

/// The survivors of a lost broker `a`.
const LEFT: [&str; 2] = ["b", "c"];

fn bounded(elections: usize) -> RuntimeConfig {
    RuntimeConfig {
        elections,
        // Groups never slow down, so every follower stands 3 to 4 s after
        // its leader is lost.
        idle_heartbeat: timers().heartbeat,
        ..timers()
    }
}

/// `groups` groups, all led by `a`, which is then lost.
async fn leader_lost(config: RuntimeConfig, groups: u32) -> Cluster {
    let mut cluster = Cluster::with(config, true);
    for n in 0..groups {
        cluster.form(n).await;
        assert_eq!(&*cluster.elected(n).await.broker, "a");
    }
    cluster.net.cut.lock().insert("a".into());
    cluster.net.muted.lock().insert("a".into());
    cluster
}

fn candidates(cluster: &Cluster, broker: &str, groups: u32) -> usize {
    let runtime = cluster.runtime(broker);
    (0..groups)
        .filter_map(|n| runtime.status(&shard(n)))
        .filter(|status| status.role == Role::Candidate)
        .count()
}

/// When every group on a broker loses its leader at once, the broker has
/// only so many stand at a time, the rest wait, and all of them end up led.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_bounds_the_elections_it_starts() {
    const GROUPS: u32 = 60;
    const BOUND: usize = 4;
    let mut cluster = leader_lost(bounded(BOUND), GROUPS).await;
    let (mut waited, mut most) = (0, 0);
    eventually(|| {
        for broker in LEFT {
            let load = cluster.runtime(broker).election_load();
            assert!(load.in_flight <= BOUND && load.limit <= BOUND, "{load:?}");
            waited = waited.max(load.waiting);
            most = most.max(candidates(&cluster, broker, GROUPS));
        }
        (0..GROUPS)
            .all(|n| cluster.leader(n).is_some())
            .then_some(())
    })
    .await;
    assert!(waited > 0, "no group waited its turn");
    // A member whose vote split is still a candidate when its turn is
    // over, so the count can pass the bound, but not by much.
    assert!(
        most <= 2 * BOUND,
        "{most} members stood at once on one broker"
    );
    // Counted until the ticker next looks.
    eventually(|| {
        let idle = |broker: &&str| {
            let load = cluster.runtime(broker).election_load();
            (load.in_flight, load.waiting) == (0, 0)
        };
        LEFT.iter().all(idle).then_some(())
    })
    .await;
    cluster.stop().await;
}

/// Members that cannot win do not keep the others out: each is counted for
/// a while and then goes to the back of the queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_that_cannot_win_gives_up_its_turn() {
    const GROUPS: u32 = 3;
    let mut cluster = leader_lost(bounded(1), GROUPS).await;
    // No candidate is answered, so no election ends.
    cluster.net.lose_votes.store(true, Relaxed);
    let b = cluster.runtime("b");
    let stood = |n: u32| b.status(&shard(n)).is_some_and(|status| status.term > 1);
    eventually(|| {
        let load = b.election_load();
        assert!(load.in_flight <= 1, "{load:?}");
        (0..GROUPS).all(stood).then_some(())
    })
    .await;
    cluster.net.lose_votes.store(false, Relaxed);
    for n in 0..GROUPS {
        cluster.elected(n).await;
    }
    cluster.stop().await;
}

/// A group that loses its leader alone is never made to wait: with
/// nothing else in flight it is checked in the interval it comes due.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_group_is_not_held_back() {
    let mut cluster = leader_lost(bounded(1), 1).await;
    eventually(|| {
        for broker in LEFT {
            let load = cluster.runtime(broker).election_load();
            assert_eq!(load.waiting, 0, "{broker}: {load:?}");
        }
        cluster.leader(0)
    })
    .await;
    cluster.stop().await;
}

/// A follower that hears its leader is sent no timer check, and neither is
/// one waiting for its turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_of_a_live_leader_is_not_checked() {
    let mut cluster = Cluster::with(bounded(1), true);
    cluster.form(0).await;
    let leader = cluster.elected(0).await;
    let followers: Vec<_> = BROKERS
        .into_iter()
        .filter(|broker| *broker != &*leader.broker)
        .collect();
    let checks = |cluster: &Cluster| -> u64 {
        let checks = followers.iter().map(|broker| {
            let status = cluster.runtime(broker).status(&shard(0));
            status.expect("status").ticks
        });
        checks.sum()
    };
    tokio::time::sleep(timers().election_max).await;
    let before = checks(&cluster);
    tokio::time::sleep(3 * timers().election_max).await;
    assert_eq!(checks(&cluster), before, "a follower was checked");
    cluster.stop().await;
}
