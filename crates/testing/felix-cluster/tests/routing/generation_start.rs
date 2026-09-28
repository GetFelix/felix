//! A leader that takes a shard without a promotion's fence (a move's
//! cut-over, or a cancelled move handing the shard back) writes a
//! generation-start record once the fleet has finalized `generation_start`,
//! and its quorum mark covers the records it inherited through that record
//! rather than on their own. A generation that already has records when the
//! fleet finalizes keeps serving without one.
//!
//! Run with `cargo test -p felix-cluster --test routing generation_start::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_controlplane_service::store::ControlPlaneStore;
use serial_test::serial;

const STREAM: &str = "orders";
const FEATURE: &str = "generation_start";

/// Three brokers and a `Quorum` stream on two of them, so a move has a
/// destination outside the replica set and the mark needs a follower.
async fn start(finalize: bool) -> Cluster {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 2)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    if finalize {
        finalize_on(&cluster).await;
    }
    cluster
}

/// Finalize `generation_start` and wait for every broker to turn it on.
async fn finalize_on(cluster: &Cluster) {
    cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store
        .finalize_fleet_feature(FEATURE)
        .await
        .expect("finalize generation_start");
    for id in cluster.node_ids() {
        felix_cluster::wait::until(Duration::from_secs(20), "the feature to turn on", || {
            let id = id.clone();
            async move {
                cluster
                    .metric(&id, "felix_broker_fleet_feature_enabled")
                    .await
                    .ok()
                    .flatten()
                    == Some(1.0)
            }
        })
        .await
        .expect("every broker enables generation_start");
    }
}

/// A broker that neither leads nor follows the stream.
async fn outsider(cluster: &Cluster) -> String {
    let assignment = cluster
        .shard_assignments()
        .await
        .expect("assignments")
        .into_values()
        .next()
        .expect("one shard");
    cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &assignment.leader && !assignment.replicas.contains(id))
        .expect("a broker outside the replica set")
}

/// The shard's committed offset on `node`, its quorum mark for a `Quorum`
/// stream, and the generation it was read at.
async fn committed_on(cluster: &Cluster, node: &str) -> Option<(u64, u64)> {
    let metrics = cluster.node(node).expect("node").metrics_addr;
    let body: serde_json::Value = reqwest::get(format!("http://{metrics}/backup/offsets"))
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    body["shards"].as_array()?.iter().find_map(|shard| {
        (shard["name"].as_str()? == STREAM && shard["shard"].as_u64()? == 0).then_some(())?;
        Some((
            shard["logs"]["records"].as_u64()?,
            shard["generation"].as_u64()?,
        ))
    })
}

/// The generation the control plane has the shard at.
async fn generation(cluster: &Cluster) -> u64 {
    cluster
        .shard_assignments()
        .await
        .expect("assignments")
        .into_values()
        .next()
        .expect("one shard")
        .generation
}

/// Publish on the owner and return the committed offset once it covers the
/// publishes: the log's length before the leadership changes.
async fn publish_before(cluster: &Cluster, owner: &str) -> u64 {
    for i in 0..10 {
        cluster
            .publish_keyed_via_settled(
                owner,
                STREAM,
                b"k",
                format!("before-{i}").into_bytes(),
                Duration::from_secs(30),
            )
            .await
            .expect("publish before the leadership changes");
    }
    committed_on(cluster, owner)
        .await
        .expect("the owner reports the shard")
        .0
}

/// What the new leader shows once it serves, with no client write since:
/// its committed offset, and then the offset and skip count of the next
/// record published through it.
///
/// The committed offset is read at the new generation once it passes
/// `past`. With no client write, only a start record of the new leader's own
/// can take it there.
async fn after_the_change(cluster: &Cluster, leader: &str, past: u64) -> (u64, u64, u64) {
    let generation = generation(cluster).await;
    let _ = felix_cluster::wait::until(
        Duration::from_secs(20),
        "the new leader's mark to settle",
        || async {
            committed_on(cluster, leader)
                .await
                .is_some_and(|(at, read_at)| read_at == generation && at > past)
        },
    )
    .await;
    let committed = committed_on(cluster, leader)
        .await
        .filter(|(_, read_at)| *read_at == generation)
        .map_or(0, |(at, _)| at);

    let (_client, mut subscription) = cluster.replay_on(leader, STREAM).await.expect("replay");
    cluster
        .publish_keyed_via_settled(
            leader,
            STREAM,
            b"k",
            b"after".to_vec(),
            Duration::from_secs(30),
        )
        .await
        .expect("publish after the leadership changes");
    loop {
        let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
            .await
            .expect("the record published after arrives")
            .expect("subscription")
            .expect("subscription open");
        if event.payload.as_ref() == b"after" {
            let offset = event.offset.expect("a durable stream carries offsets");
            return (committed, offset, event.skipped_before);
        }
    }
}

/// Move the shard to a broker outside its replica set and wait for the
/// cut-over; returns the destination.
async fn move_shard(cluster: &Cluster) -> String {
    let destination = outsider(cluster).await;
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
    .expect("move");
    destination
}

/// **A moved shard's new leader writes a generation-start record.** Once
/// `generation_start` is finalized, the destination appends the record at
/// the tail it inherited, and its mark reaches past the inherited records
/// with no client write, because a majority holds the record. The next
/// publish lands past it, and the subscriber is told the gap is start
/// records rather than a drop. FelixShardFigure8CutOver.cfg is the model.
#[serial]
#[tokio::test]
async fn a_moved_shard_writes_a_generation_start_record_and_counts_from_it() {
    let cluster = start(true).await;
    let owner = cluster.owner(STREAM).await.expect("owner");
    let inherited = publish_before(&cluster, &owner).await;
    let destination = move_shard(&cluster).await;

    let (committed, offset, skipped) = after_the_change(&cluster, &destination, inherited).await;
    // Every step of a move is a new generation, so the source writes records
    // at the stage and the fence too; the skip count explains all of them.
    assert!(skipped >= 1, "no generation-start record before {offset}");
    assert_eq!(
        offset - skipped,
        inherited,
        "the offsets past {inherited} should all be start records"
    );
    assert_eq!(
        committed, offset,
        "with no client write, the mark should cover every start record"
    );
    cluster.shutdown().await;
}

/// **A cancelled move hands the shard back behind a generation-start
/// record.** The old leader serves again at a new generation, which is a
/// leadership change like any other: it writes the record, and its mark
/// covers what it held before the move through it.
#[serial]
#[tokio::test]
async fn a_cancelled_move_hands_back_behind_a_generation_start_record() {
    let cluster = start(true).await;
    let owner = cluster.owner(STREAM).await.expect("owner");
    let inherited = publish_before(&cluster, &owner).await;

    let destination = outsider(&cluster).await;
    assert_eq!(
        cluster
            .start_move(STREAM, 0, &destination)
            .await
            .expect("start"),
        "stage"
    );
    felix_cluster::wait::until(Duration::from_secs(60), "the move to fence", || async {
        cluster.place_shards().await;
        cluster.shard_fenced(STREAM, 0).await.unwrap_or(false)
    })
    .await
    .expect("fence");
    assert_eq!(
        cluster.cancel_move(STREAM, 0).await.expect("cancel"),
        "retake"
    );
    assert_eq!(cluster.owner(STREAM).await.expect("owner"), owner);

    let (committed, offset, skipped) = after_the_change(&cluster, &owner, inherited).await;
    // Every step of a move is a new generation, so the source writes records
    // at the stage and the fence too; the skip count explains all of them.
    assert!(skipped >= 1, "no generation-start record before {offset}");
    assert_eq!(
        offset - skipped,
        inherited,
        "the offsets past {inherited} should all be start records"
    );
    assert_eq!(
        committed, offset,
        "with no client write, the mark should cover every start record"
    );
    cluster.shutdown().await;
}

/// **Until `generation_start` is finalized, a move writes no record.** A
/// fleet that may still hold a broker unable to read one keeps the old
/// behaviour: no record, so no offset taken, and the mark counts what the
/// destination inherited as before.
#[serial]
#[tokio::test]
async fn without_a_finalize_a_move_writes_no_generation_start_record() {
    let cluster = start(false).await;
    let owner = cluster.owner(STREAM).await.expect("owner");
    let inherited = publish_before(&cluster, &owner).await;
    let destination = move_shard(&cluster).await;

    let (_, offset, skipped) = after_the_change(&cluster, &destination, inherited - 1).await;
    assert_eq!(offset, inherited, "no record should take an offset");
    assert_eq!(skipped, 0);
    cluster.shutdown().await;
}

/// **A generation older than the finalize reopens without a record.** A live
/// fleet finalizes `generation_start` over a shard that already holds
/// records at its current generation. When the leader comes back at that
/// same generation, its fence opens the shard: every record past the
/// generation's start is its own, so there is nothing inherited to cover.
/// The shard serves, keeps what it acknowledged, and takes no offset for a
/// record.
#[serial]
#[tokio::test]
async fn a_generation_older_than_the_finalize_serves_again_after_a_restart() {
    let mut cluster = start(false).await;
    let owner = cluster.owner(STREAM).await.expect("owner");
    let held = publish_before(&cluster, &owner).await;
    let before = generation(&cluster).await;
    finalize_on(&cluster).await;

    // Placement is not stepped, so the leader comes back to the same
    // generation and fences it as a new term.
    cluster.stop_node(&owner).await.expect("stop the leader");
    cluster
        .restart_node(&owner)
        .await
        .expect("restart the leader");
    assert_eq!(cluster.owner(STREAM).await.expect("owner"), owner);
    assert_eq!(generation(&cluster).await, before);

    cluster
        .publish_keyed_via_settled(
            &owner,
            STREAM,
            b"k",
            b"after".to_vec(),
            Duration::from_secs(30),
        )
        .await
        .expect("the restarted leader serves the shard");
    let (_client, mut subscription) = cluster.replay_on(&owner, STREAM).await.expect("replay");
    let mut payloads = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
            .await
            .expect("the replay reaches the record published after")
            .expect("subscription")
            .expect("subscription open");
        let payload = String::from_utf8_lossy(event.payload.as_ref()).into_owned();
        if payload == "after" {
            assert_eq!(event.offset, Some(held), "no record should take an offset");
            assert_eq!(event.skipped_before, 0);
            break;
        }
        payloads.push(payload);
    }
    let expected: Vec<String> = (0..10).map(|i| format!("before-{i}")).collect();
    assert_eq!(
        payloads, expected,
        "the acknowledged records should survive"
    );
    cluster.shutdown().await;
}
