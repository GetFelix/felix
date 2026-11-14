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

/// When every group on a broker loses its leader at once, the broker has
/// only so many stand at a time, the rest wait, and all of them end up led.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_bounds_the_elections_it_starts() {
    const GROUPS: u32 = 60;
    const BOUND: usize = 4;
    let mut cluster = leader_lost(bounded(BOUND), GROUPS).await;
    let mut waited = 0;
    eventually(|| {
        for broker in LEFT {
            waited = waited.max(cluster.runtime(broker).election_load().waiting);
        }
        (0..GROUPS)
            .all(|n| cluster.leader(n).is_some())
            .then_some(())
    })
    .await;
    assert!(waited > 0, "no group waited its turn");
    for broker in LEFT {
        // The ticker's own count. One more place is kept for a member
        // whose vote split.
        let load = cluster.runtime(broker).election_load();
        assert!(load.peak <= BOUND + 1, "{broker}: {load:?}");
    }
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

/// A member that lost gives its place up, so the others get to stand
/// while no candidate is answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_that_cannot_win_gives_up_its_turn() {
    const GROUPS: u32 = 3;
    let mut cluster = leader_lost(bounded(1), GROUPS).await;
    // No candidate is answered, so no election ends.
    cluster.net.lose_votes.store(true, Relaxed);
    let b = cluster.runtime("b");
    let stood = |n: u32| b.status(&shard(n)).is_some_and(|status| status.term > 1);
    eventually(|| (0..GROUPS).all(stood).then_some(())).await;
    cluster.net.lose_votes.store(false, Relaxed);
    for n in 0..GROUPS {
        cluster.elected(n).await;
    }
    cluster.stop().await;
}

/// Groups with no quorum in reach do not keep the others waiting, however
/// many there are, and do not lower the limit. Each costs one lost round:
/// a few intervals when its peers refuse, as here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn groups_that_cannot_win_do_not_starve_the_ones_that_can() {
    const STUCK: u32 = 40;
    const GROUPS: u32 = STUCK + 4;
    const BOUND: usize = 2;
    // `c` is not ticked, so it votes and never stands: every group waits
    // for its turn on `b`.
    let mut cluster = Cluster::with(bounded(BOUND), false);
    for broker in ["a", "b"] {
        cluster.runtime(broker).start_ticker();
    }
    for n in 0..GROUPS {
        cluster.form(n).await;
        assert_eq!(&*cluster.elected(n).await.broker, "a");
    }
    // Only `b` is left of the first groups: it stands and nobody answers.
    for n in 0..STUCK {
        assert!(cluster.runtime("c").close(&shard(n)).await);
    }
    cluster.net.cut.lock().insert("a".into());
    cluster.net.muted.lock().insert("a".into());
    let lost = tokio::time::Instant::now();
    let b = cluster.runtime("b");
    eventually(|| {
        // Only a slow vote save may lower the limit, and a busy test
        // machine can have one.
        let slow = timers().election_min.as_millis() as u64 / 4;
        let limit = b.election_load().limit;
        assert!(
            limit == BOUND || b.slowest_vote_save() > slow,
            "the limit fell to {limit}"
        );
        (STUCK..GROUPS)
            .all(|n| cluster.leader(n).is_some())
            .then_some(())
    })
    .await;
    // 3 to 4 s until the followers stand, and then the groups ahead: at
    // two places, far less than a second each. Without the rule that a
    // lost round gives its place up, 40 groups would take minutes.
    let took = lost.elapsed();
    assert!(took < Duration::from_secs(15), "led after {took:?}");
    let load = b.election_load();
    assert!(load.peak <= BOUND + 1, "{load:?}");
    assert!((0..STUCK).all(|n| cluster.leader(n).is_none()));
    cluster.stop().await;
}

/// A group that loses its leader alone is led as soon under the bound as
/// without one. Only `b` is ticked and its election timeouts are held to
/// a narrow range, so no vote splits and the two times can be compared.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_group_is_not_held_back() {
    async fn led_after(elections: usize) -> Duration {
        let config = RuntimeConfig {
            election_min: Duration::from_millis(1000),
            election_max: Duration::from_millis(1040),
            lease: Duration::from_millis(800),
            ..bounded(elections)
        };
        let mut cluster = Cluster::with(config, false);
        for broker in ["a", "b"] {
            cluster.runtime(broker).start_ticker();
        }
        cluster.form(0).await;
        assert_eq!(&*cluster.elected(0).await.broker, "a");
        cluster.net.cut.lock().insert("a".into());
        cluster.net.muted.lock().insert("a".into());
        let lost = tokio::time::Instant::now();
        eventually(|| cluster.leader(0)).await;
        let took = lost.elapsed();
        cluster.stop().await;
        took
    }
    // The quickest of three, so a busy machine does not decide it.
    let (mut under, mut free) = (Duration::MAX, Duration::MAX);
    for _ in 0..3 {
        under = under.min(led_after(1).await);
        free = free.min(led_after(0).await);
    }
    // The last heartbeat was up to one gap before the leader was lost.
    let slack = timers().heartbeat + 2 * timers().tick;
    assert!(
        under <= free + slack,
        "led after {under:?} under the bound and {free:?} without"
    );
}
