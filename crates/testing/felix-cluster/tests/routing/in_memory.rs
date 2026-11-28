//! In-memory streams on a cluster whose durable shards run on Raft.
//!
//! An in-memory shard has no log for a group to run on, so it is served by
//! one owner the control plane places, with no offsets, acknowledged on
//! enqueue. These pin that behaviour so it survives the hybrid's deletion
//! (#3064, `docs/hybrid-retirement.md`, "In-memory shards").
//!
//! Run with `cargo test -p felix-cluster --test routing in_memory::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, Endpoint, Fault, StreamSpec};
use serial_test::serial;

const STREAM: &str = "ticks";
const SHARD: &str = "ticks/0";
const PLACED_WITHIN: Duration = Duration::from_secs(60);

fn config(proxy_links: bool) -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, 1).in_memory()],
        proxy_links,
        ..Default::default()
    }
}

/// The control plane's assignment of the stream's one shard: its owner and
/// generation.
async fn assignment(cluster: &Cluster) -> felix_cluster::Assignment {
    let assignments = cluster.shard_assignments().await.expect("assignments");
    assignments
        .into_iter()
        .find(|(key, _)| key.ends_with(SHARD))
        .map(|(_, assignment)| assignment)
        .expect("the in-memory shard is assigned")
}

/// The next event within ten seconds, or `None` if the subscription ended
/// or failed.
async fn next_payload(subscription: &mut felix_client::Subscription) -> Option<Vec<u8>> {
    match tokio::time::timeout(Duration::from_secs(10), subscription.next_event()).await {
        Ok(Ok(Some(event))) => {
            assert_eq!(event.offset, None, "an in-memory stream has no offsets");
            Some(event.payload.to_vec())
        }
        Ok(Ok(None) | Err(_)) => None,
        Err(_) => panic!("no event and no end within 10 s"),
    }
}

/// Publish through `node` until one is acknowledged: a shard between owners
/// answers "not ready", which a client retries.
async fn publish_eventually(cluster: &Cluster, node: &str, payload: &[u8]) {
    felix_cluster::wait::until(PLACED_WITHIN, "a publish to be acknowledged", || async {
        cluster
            .publish_via(node, STREAM, payload.to_vec())
            .await
            .is_ok()
    })
    .await
    .unwrap_or_else(|err| panic!("publish through {node}: {err:#}"));
}

/// **An in-memory stream on a Raft cluster is served by its placed owner,
/// through any broker, and forms no Raft group.**
#[serial]
#[tokio::test]
async fn an_in_memory_stream_is_served_by_its_owner_through_any_broker() {
    let cluster = Cluster::start(config(false)).await.expect("start cluster");
    let (owner, non_owner) = cluster.owner_and_non_owner(STREAM).await.expect("owner");
    let (_client, mut subscription) = cluster
        .subscribe_on(&owner, STREAM)
        .await
        .expect("subscribe on the owner");

    for node in [&owner, &non_owner] {
        let payload = format!("through-{node}").into_bytes();
        cluster
            .publish_via(node, STREAM, payload.clone())
            .await
            .expect("publish");
        assert_eq!(next_payload(&mut subscription).await, Some(payload));
    }

    for node in cluster.node_ids() {
        let led = cluster
            .metric(&node, "felix_broker_raft_shards_led")
            .await
            .expect("metrics")
            .expect("a Raft broker exports felix_broker_raft_shards_led");
        assert_eq!(
            led, 0.0,
            "{node} leads a Raft group for an in-memory stream"
        );
    }
    cluster.shutdown().await;
}

/// **When the owner dies the shard is placed again at once, at a new
/// generation, and the new owner starts empty.** A subscriber on the dead
/// owner is ended, not carried over: nothing could tell it about the new
/// generation's records otherwise.
#[serial]
#[tokio::test]
async fn an_in_memory_shard_whose_owner_dies_restarts_empty_at_a_new_generation() {
    let mut cluster = Cluster::start(config(false)).await.expect("start cluster");
    let before = assignment(&cluster).await;
    let owner = before.leader.clone();
    cluster
        .publish_via(&owner, STREAM, b"before".to_vec())
        .await
        .expect("publish before the kill");
    // Subscribes redirect to the owner, so a reader of this shard is on it.
    let (_client, mut subscription) = cluster
        .subscribe_on(&owner, STREAM)
        .await
        .expect("subscribe on the owner");

    cluster.kill_node(&owner).expect("kill the owner");
    let after =
        felix_cluster::wait::until_some(PLACED_WITHIN, "the shard to be placed again", || {
            let (cluster, owner) = (&cluster, &owner);
            async move {
                // The harness runs no reconciler on a timer: the test steps it.
                cluster.place_shards().await;
                let now = assignment(cluster).await;
                (now.leader != *owner).then_some(now)
            }
        })
        .await
        .expect("placed again");
    assert!(
        after.generation > before.generation,
        "re-placed at generation {} after {}",
        after.generation,
        before.generation,
    );
    // A dead broker sends no last frame, so the end is the closed
    // connection; what must not happen is a subscription that stays open.
    assert_ends(&mut subscription).await;

    publish_eventually(&cluster, &after.leader, b"after").await;
    let (_replay_client, mut replay) = cluster
        .replay_on(&after.leader, STREAM)
        .await
        .expect("replay on the new owner");
    cluster
        .publish_via(&after.leader, STREAM, b"probe".to_vec())
        .await
        .expect("publish to the new owner");
    loop {
        let payload = next_payload(&mut replay)
            .await
            .expect("the replay stays open");
        assert_ne!(
            payload,
            b"before".to_vec(),
            "the new owner holds a record published to the old one",
        );
        if payload == b"probe".to_vec() {
            break;
        }
    }
    cluster.shutdown().await;
}

/// Wait for `subscription` to end, failing if it is still open after 30 s.
async fn assert_ends(subscription: &mut felix_client::Subscription) {
    let ended = tokio::time::timeout(Duration::from_secs(30), async {
        while let Ok(Some(_)) = subscription.next_event().await {}
    })
    .await;
    assert!(
        ended.is_ok(),
        "the subscription stayed open across an owner change"
    );
}

/// **An owner cut off from the control plane stops acknowledging once its
/// lease lapses, and ends its readers saying so**, though it keeps running
/// and nothing else serves the shard from its view. Without this two brokers could both acknowledge writes for
/// one in-memory shard after it is placed again.
#[serial]
#[tokio::test]
async fn an_in_memory_owner_cut_off_from_the_control_plane_refuses_after_its_lease() {
    let cluster = Cluster::start(config(true)).await.expect("start cluster");
    let before = assignment(&cluster).await;
    let owner = before.leader.clone();
    cluster
        .publish_via(&owner, STREAM, b"before".to_vec())
        .await
        .expect("publish while whole");
    let (_client, mut subscription) = cluster
        .subscribe_on(&owner, STREAM)
        .await
        .expect("subscribe on the owner");

    for fault in Fault::partition(Endpoint::node(&owner), Endpoint::ControlPlane) {
        cluster.inject(&fault).await.expect("inject");
    }
    felix_cluster::wait::until(PLACED_WITHIN, "the owner to refuse a publish", || async {
        cluster
            .publish_via(&owner, STREAM, b"cut-off".to_vec())
            .await
            .is_err()
    })
    .await
    .expect("a cut-off owner kept acknowledging");
    assert!(
        cluster.node(&owner).expect("owner").is_running(),
        "the owner should still be running: only its control-plane link was cut",
    );
    // The live owner says why its reader ended: the shard has no owner it
    // can name, as of the generation it served. A cut-off broker knows no
    // newer one.
    assert_ends(&mut subscription).await;
    let moved = subscription
        .shard_moved()
        .expect("the reader is told the shard left its owner");
    assert_eq!(
        moved.resume_from, None,
        "an in-memory stream has no offsets"
    );
    assert!(
        moved.generation >= before.generation,
        "ended at generation {} below the served {}",
        moved.generation,
        before.generation,
    );

    cluster.heal_all().await.expect("heal");
    let now = assignment(&cluster).await;
    publish_eventually(&cluster, &now.leader, b"after-healing").await;
    cluster.shutdown().await;
}
