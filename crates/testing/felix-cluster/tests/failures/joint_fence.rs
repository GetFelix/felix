//! With joint fences on (`ClusterConfig::joint_fence`), a set change
//! that swaps two members does not serve until a majority of the set it
//! leaves has taken its fence, and a leader that cannot learn the set it
//! leaves is named again a generation up rather than left waiting.
//!
//! Run with `cargo test -p felix-cluster --test failures joint_fence::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_controlplane_service::store::ControlPlaneStore;
use serde_json::json;
use serial_test::serial;

const STREAM: &str = "orders";

async fn start() -> Cluster {
    Cluster::start(ClusterConfig {
        nodes: 5,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        broker_env: vec![(
            "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
            "2000".to_string(),
        )],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        joint_fence: true,
        ..Default::default()
    })
    .await
    .expect("start cluster with joint_fence")
}

async fn assignment(cluster: &Cluster) -> felix_cluster::Assignment {
    let key = format!("{}/{}/{STREAM}/0", cluster.tenant_id, cluster.namespace);
    cluster
        .shard_assignments()
        .await
        .expect("assignments")
        .remove(&key)
        .expect("the stream is placed")
}

/// Publish through `leader` until a report at `generation` names every one of
/// `followers` level with it.
async fn level_at(cluster: &Cluster, leader: &str, followers: &[String], generation: u64) {
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "a level report at the generation",
        || async {
            if cluster
                .publish_via(leader, STREAM, b"level".to_vec())
                .await
                .is_err()
            {
                return false;
            }
            cluster
                .replica_report(STREAM, 0)
                .await
                .ok()
                .flatten()
                .is_some_and(|report| {
                    report.generation == generation
                        && followers.iter().all(|id| {
                            report.caught_up.contains(id)
                                && report.offsets.get(id).copied() == report.leader_offset
                        })
                })
        },
    )
    .await
    .expect("the set holds the leader's writes");
}

/// **A two-member swap waits for a majority of the set it leaves.** Five
/// brokers, RF 3: the leader keeps the shard while both followers are
/// replaced at once by the two spares, as `replicas_after_cut_over` can do,
/// with both old followers down. The new set alone is a majority with the
/// leader, but the old set is not, so the shard does not serve. Once one old
/// follower is back it does, and the new followers hold its writes.
#[serial]
#[tokio::test]
async fn a_two_member_swap_waits_for_the_old_majority() {
    let mut cluster = start().await;
    // A set is kept from generation 1 on (0 is no set), and a set never kept
    // is one nobody can stand on. Write the same set again so the leader
    // keeps it before it changes.
    let placed = assignment(&cluster).await;
    let store = &cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store;
    let same = store
        .list_shard_assignments()
        .await
        .expect("assignments")
        .into_iter()
        .find(|assignment| assignment.key.stream == STREAM)
        .expect("the stream's assignment");
    store
        .put_shard_assignment(same)
        .await
        .expect("rewrite the set");
    let before = assignment(&cluster).await;
    assert!(before.generation > placed.generation, "{before:?}");
    let leader = before.leader.clone();
    assert_eq!(before.replicas.len(), 2, "RF 3: {before:?}");
    let spares: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader && !before.replicas.contains(id))
        .collect();
    assert_eq!(spares.len(), 2, "two spares: {spares:?}");
    level_at(&cluster, &leader, &before.replicas, before.generation).await;

    for follower in &before.replicas {
        cluster.kill_node(follower).expect("kill an old follower");
    }
    let store = &cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store;
    let mut swapped = store
        .list_shard_assignments()
        .await
        .expect("assignments")
        .into_iter()
        .find(|assignment| assignment.key.stream == STREAM)
        .expect("the stream's assignment");
    swapped.replicas = spares.clone();
    store
        .put_shard_assignment(swapped)
        .await
        .expect("swap both followers");
    let after = assignment(&cluster).await;
    assert!(after.generation > before.generation, "{after:?}");
    assert_eq!(after.leader, leader);

    // Long enough for the leader to see the new set and fence it several
    // times over.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline {
        assert!(
            cluster
                .publish_via(&leader, STREAM, b"too-early".to_vec())
                .await
                .is_err(),
            "the leader served the new set with no majority of the old one"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    cluster
        .restart_node(&before.replicas[0])
        .await
        .expect("restart one old follower");
    level_at(&cluster, &leader, &spares, after.generation).await;
    cluster.shutdown().await;
}

/// **A stream and a quorum cache created after `joint_fence` is finalized
/// open and take their first write.** A new shard's first open is fenced, and
/// under joint fences that needs to know nothing came before it, which a
/// broker reads from an incarnation above 0 (#1862).
#[serial]
#[tokio::test]
async fn a_stream_and_a_cache_created_under_joint_fence_open() {
    let cluster = start().await;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("http client");
    let base = format!(
        "{}/v1/tenants/{}/namespaces/{}",
        cluster.control_plane_url(),
        cluster.tenant_id,
        cluster.namespace
    );
    let created = [
        (
            "streams",
            json!({
                "tenant_id": cluster.tenant_id,
                "namespace": cluster.namespace,
                "stream": "created-later",
                "kind": "Stream",
                "shards": 1,
                "replication_factor": 3,
                "retention": { "max_age_seconds": null, "max_size_bytes": null },
                "consistency": "Quorum",
                "delivery": "AtLeastOnce",
                "durable": true,
            }),
        ),
        (
            "caches",
            json!({
                "cache": "cached-later",
                "display_name": "cached-later",
                "shards": 1,
                "replication_factor": 3,
                "consistency": "Quorum",
            }),
        ),
    ];
    for (kind, body) in created {
        let response = http
            .post(format!("{base}/{kind}"))
            .bearer_auth(cluster.admin_token())
            .json(&body)
            .send()
            .await
            .expect("create");
        let status = response.status();
        let answer: serde_json::Value = response.json().await.expect("create answer");
        assert!(status.is_success(), "create {kind}: {status} {answer}");
        assert!(
            answer["incarnation"].as_u64().unwrap_or(0) > 0,
            "{kind} created without an incarnation: {answer}"
        );
    }

    felix_cluster::wait::until(
        Duration::from_secs(60),
        "the new stream takes its first publish",
        || async {
            cluster.place_shards().await;
            cluster
                .publish_via_any("created-later", b"first".to_vec())
                .await
                .is_ok()
        },
    )
    .await
    .expect("a stream created under joint_fence opens");
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "the new cache takes its first put",
        || async {
            cluster.place_shards().await;
            let Ok(owners) = cluster.cache_shard_owners("cached-later").await else {
                return false;
            };
            let Some(owner) = owners.get(&0) else {
                return false;
            };
            cluster
                .cache_put_via(owner, "cached-later", "k", b"v")
                .await
                .is_ok()
        },
    )
    .await
    .expect("a quorum cache created under joint_fence opens");
    cluster.shutdown().await;
}

/// Publish to `stream` through any broker until one is acknowledged, placing
/// shards on each try so a refusal the leader reported is answered.
async fn serves(cluster: &Cluster, stream: &str, what: &str) {
    felix_cluster::wait::until(Duration::from_secs(90), what, || async {
        cluster.place_shards().await;
        cluster
            .publish_via_any(stream, b"served".to_vec())
            .await
            .is_ok()
    })
    .await
    .unwrap_or_else(|err| panic!("{what}: {err}"));
}

/// **After `joint_fence` is finalized, a rolling restart leaves every shard
/// serving** (#1872). Each leader opened its generation before the feature
/// was on, so its log keeps that set without the one it left, and after a
/// restart its watch learns the shard from a snapshot, with nothing before
/// it. Its fence cannot know the set being left, so it reports the
/// generation refused, and placement names it again one up, whose set before
/// it does know.
#[serial]
#[tokio::test]
async fn a_rolling_restart_after_joint_fence_is_finalized_serves_every_shard() {
    const STREAMS: [&str; 3] = ["restart-a", "restart-b", "restart-c"];
    let mut broker_env = felix_cluster::joint_fence_broker_env();
    broker_env.push((
        "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
        "2000".to_string(),
    ));
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: STREAMS
            .iter()
            .map(|name| StreamSpec::quorum(*name, 1, 3))
            .collect(),
        broker_env,
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster reporting joint_fence");
    // Every leader keeps its set while the feature is still off.
    for stream in STREAMS {
        serves(&cluster, stream, "a publish before joint_fence").await;
    }
    cluster
        .finalize_fleet_features(&["majority_ack", "joint_fence"], Duration::from_secs(20))
        .await
        .expect("finalize joint_fence");
    for stream in STREAMS {
        serves(&cluster, stream, "a publish under joint_fence").await;
    }

    for node in cluster.node_ids() {
        cluster.kill_node(&node).expect("kill");
        cluster.restart_node(&node).await.expect("restart");
        for stream in STREAMS {
            serves(
                &cluster,
                stream,
                &format!("{stream} serves after {node} restarted"),
            )
            .await;
        }
    }
    cluster.shutdown().await;
}

/// **A shard whose leader learns it only from a snapshot serves** (#1872).
/// The leader is down when the shard is first assigned to it, so its watch
/// takes the assignment from the snapshot it starts with and never sees that
/// nothing came before it. Placement names it again one generation up on its
/// refusal, and it opens there, still the leader.
#[serial]
#[tokio::test]
async fn a_shard_learned_only_from_a_snapshot_serves() {
    const LATER: &str = "learned-from-snapshot";
    let mut cluster = start().await;
    let ids = cluster.node_ids();
    let leader = ids[0].clone();
    let replicas = vec![ids[1].clone(), ids[2].clone()];
    cluster.kill_node(&leader).expect("kill the leader-to-be");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("http client");
    let response = http
        .post(format!(
            "{}/v1/tenants/{}/namespaces/{}/streams",
            cluster.control_plane_url(),
            cluster.tenant_id,
            cluster.namespace
        ))
        .bearer_auth(cluster.admin_token())
        .json(&json!({
            "tenant_id": cluster.tenant_id,
            "namespace": cluster.namespace,
            "stream": LATER,
            "kind": "Stream",
            "shards": 1,
            "replication_factor": 3,
            "retention": { "max_age_seconds": null, "max_size_bytes": null },
            "consistency": "Quorum",
            "delivery": "AtLeastOnce",
            "durable": true,
        }))
        .send()
        .await
        .expect("create");
    let status = response.status();
    assert!(status.is_success(), "create: {status}");

    // The shard's first assignment, naming the broker that is down.
    let store = &cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store;
    let assignments = store.list_shard_assignments().await.expect("assignments");
    assert!(
        !assignments.iter().any(|a| a.key.stream == LATER),
        "placed before the test assigned it: {assignments:?}"
    );
    let mut key = assignments
        .into_iter()
        .find(|a| a.key.stream == STREAM)
        .expect("the startup stream's assignment")
        .key;
    key.stream = LATER.to_string();
    let first = store
        .put_shard_assignment(felix_controlplane_service::model::ShardAssignment {
            key,
            leader: leader.clone(),
            replicas: replicas.clone(),
            generation: 0,
            state: felix_controlplane_service::model::ShardState::Assigning,
            // The store stamps the new stream's own.
            incarnation: 0,
            successor: None,
            joining: None,
            move_started_at_millis: None,
            move_reason: None,
            fence_storage_ids: Default::default(),
            member_storage_ids: Default::default(),
            learners: Default::default(),
            pending_set: None,
        })
        .await
        .expect("assign the new shard");

    cluster.restart_node(&leader).await.expect("restart");
    serves(&cluster, LATER, "the shard learned from a snapshot serves").await;

    let now = cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store
        .list_shard_assignments()
        .await
        .expect("assignments")
        .into_iter()
        .find(|a| a.key.stream == LATER)
        .expect("still assigned");
    assert_eq!(now.leader, leader, "placed elsewhere: {now:?}");
    assert!(now.generation > first.generation, "{now:?} after {first:?}");
    cluster.shutdown().await;
}
