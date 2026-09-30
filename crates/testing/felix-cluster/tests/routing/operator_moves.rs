//! An operator moving shards by hand: start a move, cancel one before and
//! after its fence, and pause placement's own.
//!
//! Run with `cargo test -p felix-cluster --test routing operator_moves::`.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, Fault, StreamSpec};
use serial_test::serial;
use tokio::sync::Mutex;

const STREAM: &str = "orders";

async fn two_brokers() -> (Cluster, String, String) {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let other = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &owner)
        .expect("two brokers");
    (cluster, owner, other)
}

async fn publish_before(cluster: &Cluster, via: &str, count: usize) -> Vec<Vec<u8>> {
    let mut sent = Vec::new();
    for i in 0..count {
        let payload = format!("before-{i}").into_bytes();
        cluster
            .publish_keyed_via_settled(via, STREAM, b"k", payload.clone(), Duration::from_secs(30))
            .await
            .expect("publish before the move");
        sent.push(payload);
    }
    sent
}

/// The records of `owed` that `node` has, in the order its log holds them,
/// read from the start. Anything else in the log (the harness's own probe)
/// is left out.
///
/// The replay is retried until `node` takes it: the control plane names a
/// new owner before that broker hears of it through its watch and opens the
/// shard, and until then it refuses the subscribe as another's.
async fn stored_on(cluster: &Cluster, node: &str, owed: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let (_client, mut subscription) =
        felix_cluster::wait::until_some(Duration::from_secs(30), "the owner to serve", || async {
            cluster.replay_on(node, STREAM).await.ok()
        })
        .await
        .expect("replay");
    let mut got = Vec::new();
    while !owed.iter().all(|payload| got.contains(payload)) {
        match tokio::time::timeout(Duration::from_secs(10), subscription.next_event()).await {
            Ok(Ok(Some(event))) => got.push(event.payload.to_vec()),
            _ => break,
        }
    }
    got.retain(|payload| owed.contains(payload));
    got
}

/// **An operator's move runs to the end.** Started over the API, it is listed
/// as the operator's, and placement carries it through the fence and the
/// cut-over; the new owner has every record.
#[serial]
#[tokio::test]
async fn an_operator_moves_a_shard_and_the_move_completes() {
    let (cluster, owner, other) = two_brokers().await;
    let sent = publish_before(&cluster, &owner, 20).await;

    let step = cluster
        .start_move(STREAM, 0, &other)
        .await
        .expect("start the move");
    assert_eq!(step, "stage");
    let moves = cluster.shard_moves().await.expect("list moves");
    assert_eq!(moves["items"][0]["reason"], "operator", "{moves}");
    assert_eq!(moves["items"][0]["destination"], other.as_str(), "{moves}");

    felix_cluster::wait::until(Duration::from_secs(60), "the move to cut over", || async {
        cluster.place_shards().await;
        cluster.owner(STREAM).await.is_ok_and(|now| now == other)
    })
    .await
    .expect("move");
    assert_eq!(
        cluster.shard_moves().await.expect("list moves")["items"],
        serde_json::json!([])
    );
    assert_eq!(stored_on(&cluster, &other, &sent).await, sent);
    cluster.shutdown().await;
}

/// **Cancelled before the fence, nothing moved.** The destination is dropped
/// and the owner goes on serving, with nothing for placement to resume.
#[serial]
#[tokio::test]
async fn cancelling_a_staged_move_leaves_the_shard_where_it_was() {
    let (cluster, owner, other) = two_brokers().await;
    let sent = publish_before(&cluster, &owner, 10).await;

    assert_eq!(
        cluster.start_move(STREAM, 0, &other).await.expect("start"),
        "stage"
    );
    assert_eq!(
        cluster.cancel_move(STREAM, 0).await.expect("cancel"),
        "cancel"
    );
    assert_eq!(
        cluster.shard_moves().await.expect("list moves")["items"],
        serde_json::json!([])
    );
    let assignment = cluster
        .shard_assignments()
        .await
        .expect("assignments")
        .into_values()
        .find(|a| a.leader == owner)
        .expect("still led by its owner");
    assert!(
        !assignment.replicas.contains(&other),
        "the destination is still a replica: {assignment:?}"
    );
    for _ in 0..3 {
        cluster.place_shards().await;
    }
    assert_eq!(cluster.owner(STREAM).await.expect("owner"), owner);

    cluster
        .publish_keyed_via_settled(
            &owner,
            STREAM,
            b"k",
            b"after".to_vec(),
            Duration::from_secs(30),
        )
        .await
        .expect("the owner still serves");
    let mut owed = sent;
    owed.push(b"after".to_vec());
    assert_eq!(stored_on(&cluster, &owner, &owed).await, owed);
    cluster.shutdown().await;
}

/// **Cancelled after the fence, the old leader takes the shard back and
/// nothing is lost.** A publisher writes through the other broker and a
/// subscriber reads from the start on the owner, while the move is fenced
/// (the owner has stopped serving, publishes are held, the subscriber was
/// told to follow the shard to the destination) and then cancelled. The
/// owner serves again at a new generation; the subscriber ends up back on it
/// and sees every offset once, in order, with every acknowledged record from
/// before, during and after.
#[serial]
#[tokio::test]
async fn cancelling_a_fenced_move_loses_no_acknowledged_write() {
    let (cluster, owner, other) = two_brokers().await;
    let addr = |node: &str| cluster.node(node).expect("node").client_addr;
    let before = publish_before(&cluster, &owner, 20).await;

    let reader = Arc::new(
        felix_cluster::client::connect_cluster(
            &[addr(&owner)],
            &cluster.tenant_id,
            &cluster.client_token,
        )
        .await
        .expect("reader"),
    );
    let mut subscription = reader
        .subscribe_from(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            Some(felix_client::StartPosition::Offset(0)),
        )
        .await
        .expect("subscribe");

    let writer = felix_cluster::client::connect_cluster(
        &[addr(&other)],
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await
    .expect("writer");
    let acknowledged: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let publisher = {
        let (acknowledged, stop) = (Arc::clone(&acknowledged), Arc::clone(&stop));
        let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
        tokio::spawn(async move {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let payload = format!("during-{i}").into_bytes();
                i += 1;
                // Only an acknowledged record is owed.
                if writer
                    .publish(
                        &tenant,
                        &namespace,
                        STREAM,
                        payload.clone(),
                        felix_wire::AckMode::PerMessage,
                    )
                    .await
                    .is_ok()
                {
                    acknowledged.lock().await.push(payload);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        cluster.start_move(STREAM, 0, &other).await.expect("start"),
        "stage"
    );
    // Stepped by hand, so the move stops at the fence: the harness runs no
    // placement loop that would cut it over.
    felix_cluster::wait::until(Duration::from_secs(60), "the move to fence", || async {
        cluster.place_shards().await;
        cluster.shard_fenced(STREAM, 0).await.unwrap_or(false)
    })
    .await
    .expect("fence");
    let fenced_at = acknowledged.lock().await.len();
    // Long enough for the owner to stop serving and end its readers, and for
    // publishes to be held; short of the hold window, so none is refused for
    // it.
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        cluster.cancel_move(STREAM, 0).await.expect("cancel"),
        "retake"
    );
    assert_eq!(cluster.owner(STREAM).await.expect("owner"), owner);

    tokio::time::sleep(Duration::from_millis(1_000)).await;
    stop.store(true, Ordering::Relaxed);
    publisher.await.expect("publisher");
    for _ in 0..3 {
        cluster.place_shards().await;
    }
    assert_eq!(
        cluster.owner(STREAM).await.expect("owner"),
        owner,
        "the old leader keeps the shard"
    );

    let mut owed = before;
    let during = acknowledged.lock().await.clone();
    assert!(
        during.len() > fenced_at + 10,
        "nothing was acknowledged after the cancel ({} of {}), so it proves nothing",
        during.len() - fenced_at,
        during.len()
    );
    owed.extend(during);

    let mut offsets = Vec::new();
    let mut payloads = Vec::new();
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
    while !owed.iter().all(|payload| payloads.contains(payload)) {
        match tokio::time::timeout_at(deadline, subscription.next_event()).await {
            Ok(Ok(Some(event))) => {
                offsets.push(event.offset.expect("a durable stream carries offsets"));
                payloads.push(event.payload.to_vec());
            }
            Ok(Ok(None)) => panic!(
                "the subscription ended after {} records instead of following the shard back",
                payloads.len()
            ),
            Ok(Err(err)) => panic!("the subscription failed: {err:#}"),
            Err(_) => panic!(
                "timed out with {} of {} acknowledged records",
                owed.iter().filter(|p| payloads.contains(p)).count(),
                owed.len()
            ),
        }
    }
    let expected: Vec<u64> = (0..offsets.len() as u64).collect();
    assert_eq!(offsets, expected, "every offset once and in order");
    let mut unique = payloads.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), payloads.len(), "a record arrived twice");

    // And the owner's own log holds every one of them.
    let stored = stored_on(&cluster, &owner, &owed).await;
    let missing = owed.iter().filter(|p| !stored.contains(p)).count();
    assert_eq!(
        missing, 0,
        "acknowledged records missing from the owner's log"
    );
    cluster.shutdown().await;
}

/// **Paused, placement leaves a draining node's shard alone.** Resumed, it
/// moves it.
#[serial]
#[tokio::test]
async fn a_paused_placement_does_not_move_a_draining_nodes_shard() {
    let (cluster, owner, other) = two_brokers().await;
    cluster.pause_placement().await.expect("pause");
    assert_eq!(
        cluster.shard_moves().await.expect("list moves")["paused"],
        true
    );
    cluster.drain_node(&owner).await.expect("drain");
    for _ in 0..10 {
        let outcome = cluster.place_shards().await;
        assert_eq!(outcome.moved, 0, "a move started while paused: {outcome:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(cluster.owner(STREAM).await.expect("owner"), owner);
    assert_eq!(
        cluster.shard_moves().await.expect("list moves")["items"],
        serde_json::json!([])
    );

    cluster.resume_placement().await.expect("resume");
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "the drain to move the shard",
        || async {
            cluster.place_shards().await;
            cluster.owner(STREAM).await.is_ok_and(|now| now == other)
        },
    )
    .await
    .expect("move after resuming");
    cluster.shutdown().await;
}

/// **A move cut short by a kill leaves no broker that cannot rejoin (#878).**
///
/// The old leader writes records no follower gets, and the move copies them
/// to the destination. The destination takes over unable to reach any
/// follower and is killed, and so is the old leader, so a follower without
/// those records is promoted and writes over them. Both come back holding
/// them past their commit offset but below where their own last generation
/// began, so each halts and takes the new leader's rebuild. A leader used to
/// count a follower it had not heard from as holding everything below its own
/// generation, so the old leader's mark, and with it its commit offset,
/// covered records nobody else had; it then refused every rebuild for good.
#[serial]
#[tokio::test]
async fn a_move_cut_short_by_kills_leaves_no_replica_halted() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 4,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let before = cluster
        .shard_assignments()
        .await
        .expect("assignments")
        .into_values()
        .next()
        .expect("an assignment for the shard");
    let old = before.leader.clone();
    let destination = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != old && !before.replicas.contains(id))
        .expect("a fourth broker");
    let others: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != destination)
        .collect();
    let mut acknowledged = publish_before(&cluster, &old, 3).await;

    // Records only the old leader holds: nobody acknowledged them.
    let alone = Fault::Refuse {
        node: old.clone(),
        peers: before.replicas.clone(),
    };
    cluster.inject(&alone).await.expect("cut the followers off");
    // Two of them: the destination's mark must not cover the first one on
    // the strength of followers it never heard from.
    for i in 0..2 {
        let unacknowledged = tokio::time::timeout(
            Duration::from_secs(5),
            cluster.publish_via(&old, STREAM, format!("alone-{i}").into_bytes()),
        )
        .await;
        assert!(
            !matches!(unacknowledged, Ok(Ok(()))),
            "a write no follower holds was acknowledged",
        );
    }

    let unheard = Fault::Refuse {
        node: destination.clone(),
        peers: others,
    };
    cluster
        .inject(&unheard)
        .await
        .expect("keep the destination from its followers");
    cluster
        .start_move(STREAM, 0, &destination)
        .await
        .expect("start the move");
    felix_cluster::wait::until(Duration::from_secs(60), "the move to cut over", || async {
        cluster.place_shards().await;
        cluster
            .owner(STREAM)
            .await
            .is_ok_and(|now| now == destination)
    })
    .await
    .expect("cut over");
    let generation = cluster
        .shard_assignments()
        .await
        .expect("assignments")
        .into_values()
        .next()
        .expect("the assignment")
        .generation;
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the destination's first replica report",
        || async {
            cluster
                .replica_report(STREAM, 0)
                .await
                .ok()
                .flatten()
                .is_some_and(|report| report.generation == generation)
        },
    )
    .await
    .expect("report");
    // The pass that sent the report also writes any mark it published through
    // to the log's commit offset.
    tokio::time::sleep(Duration::from_millis(500)).await;
    cluster
        .kill_node(&destination)
        .expect("kill the destination");
    cluster.kill_node(&old).expect("kill the old leader");
    cluster.heal_all().await.expect("heal");

    felix_cluster::wait::until(
        Duration::from_secs(30),
        "a follower to be promoted",
        || async {
            cluster.place_shards().await;
            cluster
                .owner(STREAM)
                .await
                .is_ok_and(|owner| owner != destination && owner != old)
        },
    )
    .await
    .expect("promotion");
    cluster
        .restart_node(&old)
        .await
        .expect("restart the old leader, so the promoted one has a majority");
    let promoted = cluster.owner(STREAM).await.expect("owner");
    for i in 0..3 {
        let payload = format!("after-{i}").into_bytes();
        cluster
            .publish_keyed_via_settled(
                &promoted,
                STREAM,
                b"k",
                payload.clone(),
                Duration::from_secs(30),
            )
            .await
            .expect("publish on the promoted leader");
        acknowledged.push(payload);
    }

    cluster
        .restart_node(&destination)
        .await
        .expect("restart the destination");
    let rejoined = felix_cluster::wait::until(
        Duration::from_secs(60),
        "the destination and the old leader to rejoin",
        || async {
            cluster.place_shards().await;
            let halted = cluster
                .metric(&promoted, "felix_broker_replication_halted")
                .await
                .ok()
                .flatten()
                .unwrap_or(0.0);
            let level = cluster
                .replica_report(STREAM, 0)
                .await
                .ok()
                .flatten()
                .is_some_and(|report| {
                    report.caught_up.contains(&destination) && report.caught_up.contains(&old)
                });
            halted == 0.0
                && level
                && cluster
                    .moving_shards()
                    .await
                    .is_ok_and(|moving| moving.is_empty())
        },
    )
    .await;
    let report = cluster.replica_report(STREAM, 0).await.ok().flatten();
    assert!(
        rejoined.is_ok(),
        "{destination} and {old} did not both rejoin behind {promoted}; last report {report:?}",
    );
    assert_eq!(
        stored_on(&cluster, &promoted, &acknowledged).await,
        acknowledged
    );
    cluster.shutdown().await;
}
