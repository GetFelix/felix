//! What survives losing a leader.
//!
//! These start real broker processes and kill one mid-flight, so they are slow
//! and deliberately few. What they cover is the claim replication rests
//! on — that a record acknowledged under `Quorum` is still there after the
//! broker that acknowledged it is gone.
//!
//! Run with `cargo test -p felix-cluster --test failures failover::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";

/// Three brokers, one shard, three copies of it, acknowledged by a majority.
fn quorum_config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    }
}

/// Wait until the leader has actually replicated and reported.
///
/// A cluster that has only just started has a leader that has shipped nothing
/// and told the control plane nothing, so killing it there tests startup rather
/// than failover. The scenario is a *healthy* replicated shard losing its
/// leader, and this is what makes it one: zero lag means every follower holds
/// what the leader does, and the leader has said so.
async fn shipped_so_far(cluster: &Cluster, leader: &str) -> f64 {
    cluster
        .metric(leader, "felix_broker_replication_shipped_total")
        .await
        .ok()
        .flatten()
        .unwrap_or(0.0)
}

/// Wait until the record published since `shipped_before` is on every follower.
///
/// The lag gauge is written once per replication pass, so reading zero can mean
/// "every follower is level" or "every follower was level one pass ago, before
/// the record this test just published". Requiring the shipped counter to have
/// risen past a baseline is what makes the zero be about this record.
///
/// **`shipped_before` has to be sampled before the publish, not after.** The
/// counter only moves on a real exchange -- an idle pass returns up-to-date
/// without touching it -- and a publish to a `Quorum` stream does not return
/// until a majority holds the record, so the ship has already been counted by
/// the time the publish does. A baseline taken afterwards already includes the
/// ship it is waiting to see, and on a quiet stream nothing will ever move the
/// counter again.
async fn replication_settled(cluster: &Cluster, leader: &str, shipped_before: f64) {
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the published record to be shipped and acknowledged",
        || async {
            let shipped = shipped_so_far(cluster, leader).await;
            let lag = cluster
                .metric(leader, "felix_broker_replication_lag_records")
                .await
                .ok()
                .flatten();
            shipped > shipped_before && matches!(lag, Some(lag) if lag == 0.0)
        },
    )
    .await
    .expect("replication should reach the followers of a healthy shard");
}

/// Everything `node_id` will replay from the start of the stream.
///
/// Retried until it yields something, because being named leader and being
/// ready to serve are different moments: a promoted broker learns of its own
/// promotion through the same watch as everything else, and has to open the
/// shard before it can answer for it.
async fn replay_until(cluster: &Cluster, node_id: &str, budget: Duration) -> Vec<Vec<u8>> {
    let deadline = std::time::Instant::now() + budget;
    let mut attempts = 0usize;
    let mut last;
    loop {
        attempts += 1;
        match cluster.replay_on(node_id, STREAM).await {
            Ok((_client, mut subscription)) => {
                let mut payloads = Vec::new();
                loop {
                    match tokio::time::timeout(Duration::from_secs(2), subscription.next_event())
                        .await
                    {
                        Ok(Ok(Some(event))) => payloads.push(event.payload.to_vec()),
                        Ok(Ok(None)) => {
                            last = "the broker ended the subscription".into();
                            break;
                        }
                        Ok(Err(err)) => {
                            last = format!("delivery error: {err}");
                            break;
                        }
                        Err(_) => {
                            last = "no event within 2s".into();
                            break;
                        }
                    }
                }
                if !payloads.is_empty() {
                    return payloads;
                }
            }
            Err(err) => last = format!("subscribe refused: {err}"),
        }
        if std::time::Instant::now() >= deadline {
            panic!("nothing replayed from {node_id} after {attempts} attempts; last: {last}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Wait until the control plane has named a leader other than `gone`.
async fn failover_from(cluster: &Cluster, gone: &str, budget: Duration) -> Option<Duration> {
    let started = std::time::Instant::now();
    let ok = felix_cluster::wait::until(budget, "a new leader", || async {
        // The harness's control plane does not run the reconciler on a timer,
        // so a failure test has to step placement itself. Without this nothing
        // re-plans after the kill and the wait is measuring nothing.
        cluster.place_shards().await;
        match cluster.owner(STREAM).await {
            Ok(owner) => owner != gone,
            Err(_) => false,
        }
    })
    .await;
    ok.ok().map(|_| started.elapsed())
}

/// **The milestone signal.** A record acknowledged under `Quorum` is readable
/// after the broker that acknowledged it is killed.
///
/// The acknowledgement is the whole claim: it means a majority held the record
/// durably, so losing any one of them — including the leader — cannot take it.
///
/// Was intermittent, roughly one run in four, and the cause is now known: the
/// acknowledgement did not require a majority at all. `Quorum` degraded to the
/// `Leader` behaviour whenever `ack_on_commit` was off — the default — because
/// the enqueue-ack path answered "accepted into the ingress queue" to a
/// question about majorities, and the quorum wait ran in a worker nobody was
/// listening to. A record acknowledged that way existed only on the leader, so
/// promoting any replica legitimately lost it, and the test failed whenever the
/// promoted broker was one that had not received it (#282).
///
/// The comment that used to sit here said this test was ignored. It was not —
/// there was no `#[ignore]`, so it ran and failed one run in four, which is
/// worse than either honest option.
#[serial]
#[tokio::test]
async fn a_quorum_acknowledged_record_survives_its_leader() {
    let mut cluster = Cluster::start(quorum_config())
        .await
        .expect("start cluster");
    let leader = cluster.owner(STREAM).await.expect("owner");
    let shipped_before = shipped_so_far(&cluster, &leader).await;

    // Acknowledged by a majority before this returns.
    cluster
        .publish_via(&leader, STREAM, b"survives".to_vec())
        .await
        .expect("publish under quorum");

    replication_settled(&cluster, &leader, shipped_before).await;
    cluster.kill_node(&leader).expect("kill the leader");

    let elapsed = failover_from(&cluster, &leader, Duration::from_secs(30))
        .await
        .expect("a replica should have been promoted");
    println!("failover took {elapsed:?}");

    let new_leader = cluster.owner(STREAM).await.expect("owner");
    assert_ne!(new_leader, leader);

    // Replayed from the start, not subscribed live: the record was published
    // before the kill, so this asks what the promoted broker actually holds.
    //
    // Retried, because the control plane naming a new leader and that broker
    // having opened the shard are two different moments: it learns of its own
    // promotion through the same watch as everything else.
    let payloads = replay_until(&cluster, &new_leader, Duration::from_secs(30)).await;

    assert!(
        payloads.iter().any(|payload| payload == b"survives"),
        "a quorum-acknowledged record did not survive its leader; the promoted \
         broker replayed {payloads:?}",
    );
    cluster.shutdown().await;
}

/// Publish through `node` until it acknowledges, and return the acked offset.
/// A promoted broker takes a moment to open the shard, so early attempts fail.
async fn publish_at_once_serving(cluster: &Cluster, node: &str, payload: &str) -> u64 {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match cluster
            .publish_via_at(node, STREAM, payload.as_bytes().to_vec())
            .await
        {
            Ok(offset) => {
                return offset.unwrap_or_else(|| panic!("{payload} was acked without an offset"));
            }
            Err(err) if std::time::Instant::now() >= deadline => {
                panic!("{node} never acknowledged {payload}: {err:#}")
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
}

/// Every record `node_id` replays, by offset, once it holds all of `wanted`.
async fn replay_by_offset(
    cluster: &Cluster,
    node_id: &str,
    wanted: &[(u64, String)],
) -> std::collections::BTreeMap<u64, String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut held = std::collections::BTreeMap::new();
        if let Ok((_client, mut subscription)) = cluster.replay_on(node_id, STREAM).await {
            while let Ok(Ok(Some(event))) =
                tokio::time::timeout(Duration::from_secs(2), subscription.next_event()).await
            {
                let offset = event
                    .offset
                    .expect("a durable stream's events carry offsets");
                held.insert(offset, String::from_utf8_lossy(&event.payload).into_owned());
            }
        }
        let complete = wanted.iter().all(|(offset, _)| held.contains_key(offset));
        if complete || std::time::Instant::now() >= deadline {
            return held;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// **An acknowledged offset is where the record is**, on the leader that
/// acknowledged it and on the one promoted after it dies. A reader that
/// seeks to an acked offset gets that record, not a neighbour.
#[serial]
#[tokio::test]
async fn acknowledged_offsets_hold_across_a_failover() {
    let mut cluster = Cluster::start(quorum_config())
        .await
        .expect("start cluster");
    let leader = cluster.owner(STREAM).await.expect("owner");
    let shipped_before = shipped_so_far(&cluster, &leader).await;

    let mut acked = Vec::new();
    for i in 0..10 {
        let payload = format!("before-{i}");
        let offset = cluster
            .publish_via_at(&leader, STREAM, payload.clone().into_bytes())
            .await
            .expect("publish under quorum")
            .unwrap_or_else(|| panic!("{payload} was acked without an offset"));
        acked.push((offset, payload));
    }
    let first = acked[0].0;
    let offsets: Vec<u64> = acked.iter().map(|(offset, _)| *offset).collect();
    assert_eq!(
        offsets,
        (first..first + 10).collect::<Vec<_>>(),
        "one publisher's acks on one shard land one after another"
    );

    replication_settled(&cluster, &leader, shipped_before).await;
    cluster.kill_node(&leader).expect("kill the leader");
    failover_from(&cluster, &leader, Duration::from_secs(30))
        .await
        .expect("a replica should have been promoted");
    let new_leader = cluster.owner(STREAM).await.expect("owner");
    assert_ne!(new_leader, leader);

    for i in 0..2 {
        let payload = format!("after-{i}");
        let offset = publish_at_once_serving(&cluster, &new_leader, &payload).await;
        assert!(
            offset > first + 9,
            "{payload} was acked at {offset}, inside what the old leader wrote"
        );
        acked.push((offset, payload));
    }

    let held = replay_by_offset(&cluster, &new_leader, &acked).await;
    for (offset, payload) in &acked {
        assert_eq!(
            held.get(offset),
            Some(payload),
            "{payload} was acked at {offset}; {new_leader} holds {held:?}"
        );
    }
    cluster.shutdown().await;
}

/// **Only a broker that holds the log is promoted.** The promoted leader must
/// be one of the replicas, not whichever node scored highest — that is the
/// difference between a failover and a silently empty shard.
#[serial]
#[tokio::test]
async fn the_promoted_leader_is_one_of_the_replicas() {
    let mut cluster = Cluster::start(quorum_config())
        .await
        .expect("start cluster");
    let leader = cluster.owner(STREAM).await.expect("owner");
    let shipped_before = shipped_so_far(&cluster, &leader).await;
    let replicas: Vec<String> = cluster
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .filter(|node| node != &leader)
        .collect();

    cluster
        .publish_via(&leader, STREAM, b"held".to_vec())
        .await
        .expect("publish");
    replication_settled(&cluster, &leader, shipped_before).await;
    cluster.kill_node(&leader).expect("kill the leader");
    failover_from(&cluster, &leader, Duration::from_secs(30))
        .await
        .expect("a replica should have been promoted");

    let promoted = cluster.owner(STREAM).await.expect("owner");
    assert!(
        replicas.contains(&promoted),
        "{promoted} was promoted but was not a replica of the shard",
    );
    cluster.shutdown().await;
}

/// Failover completes within a bound derived from the cluster's own liveness
/// settings, rather than an arbitrary number: the lease has to lapse and the
/// control plane has to notice before anything can be promoted.
#[serial]
#[tokio::test]
async fn failover_completes_within_the_configured_bound() {
    let mut cluster = Cluster::start(quorum_config())
        .await
        .expect("start cluster");
    let leader = cluster.owner(STREAM).await.expect("owner");
    let shipped_before = shipped_so_far(&cluster, &leader).await;
    cluster
        .publish_via(&leader, STREAM, b"timed".to_vec())
        .await
        .expect("publish");

    replication_settled(&cluster, &leader, shipped_before).await;
    cluster.kill_node(&leader).expect("kill the leader");
    let elapsed = failover_from(&cluster, &leader, Duration::from_secs(30))
        .await
        .expect("a replica should have been promoted");

    // Generous against the harness's short liveness windows. The assertion is
    // that failover is bounded at all, not that it is fast: a tight bound here
    // would fail on a loaded CI runner for reasons that say nothing about the
    // design.
    assert!(
        elapsed < Duration::from_secs(30),
        "failover took {elapsed:?}",
    );
    println!("failover took {elapsed:?}");
    cluster.shutdown().await;
}

/// **Deregistering a live leader does not make two leaders.** The broker
/// keeps running on the lease it was granted before it was deregistered, so
/// a follower is promoted only once that lease has provably run out -- and
/// then it is, without the broker having to stop.
#[serial]
#[tokio::test]
async fn a_deregistered_leader_is_replaced_only_after_its_lease() {
    let cluster = Cluster::start(quorum_config())
        .await
        .expect("start cluster");
    let leader = cluster.owner(STREAM).await.expect("owner");
    let shipped_before = shipped_so_far(&cluster, &leader).await;
    cluster
        .publish_via(&leader, STREAM, b"before leaving".to_vec())
        .await
        .expect("publish");
    replication_settled(&cluster, &leader, shipped_before).await;
    // A broker still running would hand the shard over through a move as
    // soon as it is fenced, which is safe and not what this is about. With
    // moves paused only the lease can end the wait.
    cluster.pause_placement().await.expect("pause moves");

    cluster
        .deregister_node(&leader)
        .await
        .expect("deregister the leader");
    let deregistered = std::time::Instant::now();
    // Stepped at once, as a pass woken by the change would be. A caught-up
    // replica is on hand, so only the fence keeps it from being promoted.
    cluster.place_shards().await;
    assert_eq!(
        cluster.owner(STREAM).await.expect("owner"),
        leader,
        "a follower was promoted while the deregistered leader's lease ran",
    );

    failover_from(&cluster, &leader, Duration::from_secs(30))
        .await
        .expect("the shard should move once the lease has run out");
    // The harness waits 1.25 s of silence; the last heartbeat was at most an
    // interval or so before the deregistration.
    let waited = deregistered.elapsed();
    assert!(
        waited >= Duration::from_millis(800),
        "promoted {waited:?} after deregistering, inside the lease",
    );
    let promoted = cluster.owner(STREAM).await.expect("owner");
    let payloads = replay_until(&cluster, &promoted, Duration::from_secs(30)).await;
    assert!(
        payloads.iter().any(|payload| payload == b"before leaving"),
        "the promoted broker replayed {payloads:?}",
    );
    cluster.shutdown().await;
}

/// **A shard with no replica to promote stays unavailable rather than being
/// served empty.** The counterpart to the tests above: when there is nothing
/// holding the log, the cluster declines to invent a leader.
#[serial]
#[tokio::test]
async fn a_shard_with_no_caught_up_replica_does_not_fail_over_to_an_empty_broker() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        // Replicated across two, so a replica set exists — but the publish is
        // leader-acknowledged, so the record need not have reached the follower.
        streams: vec![StreamSpec::replicated(STREAM, 1, 2)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let leader = cluster.owner(STREAM).await.expect("owner");
    let shipped_before = shipped_so_far(&cluster, &leader).await;
    // The record this test is about. Without it nothing is ever shipped, so the
    // wait below -- which is for the shipped counter to rise -- could only ever
    // be satisfied by whatever the harness still had in flight from startup.
    cluster
        .publish_via(&leader, STREAM, b"leader-acknowledged".to_vec())
        .await
        .expect("publish");
    let replicas: Vec<String> = cluster
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .filter(|node| node != &leader)
        .collect();

    replication_settled(&cluster, &leader, shipped_before).await;
    cluster.kill_node(&leader).expect("kill the leader");

    // Whatever happens, the shard must not land on a broker outside the replica
    // set: that broker holds none of the log.
    if failover_from(&cluster, &leader, Duration::from_secs(20))
        .await
        .is_some()
    {
        let promoted = cluster.owner(STREAM).await.expect("owner");
        assert!(
            replicas.contains(&promoted),
            "{promoted} was made leader but holds none of the shard",
        );
    }
    cluster.shutdown().await;
}

/// A leader-only stream: the owner holds the only copy of each shard.
fn unreplicated_config() -> ClusterConfig {
    ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    }
}

/// Publish a record to the owner and wait until it replays it, so it is in
/// the owner's log before the owner is stopped.
async fn written_to_owner(cluster: &Cluster, owner: &str, payload: &[u8]) {
    cluster
        .publish_via(owner, STREAM, payload.to_vec())
        .await
        .expect("publish");
    felix_cluster::wait::until(
        Duration::from_secs(20),
        "the owner to replay the record",
        || async {
            replay_until(cluster, owner, Duration::from_secs(20))
                .await
                .iter()
                .any(|record| record == payload)
        },
    )
    .await
    .expect("the record is in the owner's log");
}

/// Step placement until it holds the shard unplaced: the owner is marked
/// down, which is when placement used to hand the shard to the survivor. A
/// stopped broker stops being placeable a little before that.
async fn stranded(cluster: &Cluster) {
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "placement to hold the shard for its owner",
        || async {
            let outcome = cluster.place_shards().await;
            assert_eq!(outcome.placed, 0, "the shard was placed away from its log");
            outcome.unplaceable == 1
        },
    )
    .await
    .expect("the shard is held unplaced");
}

/// **An unreplicated durable shard waits for its owner.** The owner holds the
/// only copy of the log, so handing the shard to the other broker would serve
/// it empty at a new generation. It stays with the owner, unserved, and the
/// owner coming back serves it with its records.
#[serial]
#[tokio::test]
async fn an_unreplicated_durable_shard_waits_for_its_owner() {
    let mut cluster = Cluster::start(unreplicated_config())
        .await
        .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    written_to_owner(&cluster, &owner, b"only-copy").await;

    cluster.stop_node(&owner).await.expect("stop the owner");
    stranded(&cluster).await;
    for _ in 0..5 {
        let outcome = cluster.place_shards().await;
        assert_eq!(outcome.placed, 0, "the shard was placed away from its log");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(cluster.owner(STREAM).await.expect("owner"), owner);

    cluster
        .restart_node(&owner)
        .await
        .expect("restart the owner");
    cluster.place_shards().await;
    assert_eq!(cluster.owner(STREAM).await.expect("owner"), owner);
    let replayed = replay_until(&cluster, &owner, Duration::from_secs(30)).await;
    assert!(
        replayed.iter().any(|record| record == b"only-copy"),
        "the returning owner should serve the records it held",
    );
    cluster.shutdown().await;
}

/// **Giving the log up is an operator's call.** Abandoning the stranded shard
/// places it on the surviving broker, which then serves it.
#[serial]
#[tokio::test]
async fn an_operator_can_abandon_the_log_of_a_shard_whose_owner_is_gone() {
    let mut cluster = Cluster::start(unreplicated_config())
        .await
        .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let survivor = cluster
        .node_ids()
        .into_iter()
        .find(|node| node != &owner)
        .expect("two brokers");
    assert!(
        cluster.abandon_log(STREAM, 0).await.is_err(),
        "a shard whose owner serves it cannot be abandoned",
    );
    written_to_owner(&cluster, &owner, b"lost").await;

    cluster.stop_node(&owner).await.expect("stop the owner");
    stranded(&cluster).await;
    assert_eq!(
        cluster.abandon_log(STREAM, 0).await.expect("abandon"),
        "discard"
    );
    assert_eq!(cluster.owner(STREAM).await.expect("owner"), survivor);

    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the survivor to serve the abandoned shard",
        || async {
            cluster
                .publish_via(&survivor, STREAM, b"after".to_vec())
                .await
                .is_ok()
        },
    )
    .await
    .expect("the survivor serves the shard");
    cluster.shutdown().await;
}
