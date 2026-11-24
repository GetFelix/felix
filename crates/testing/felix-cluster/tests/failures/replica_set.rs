//! With replica set checks on (`FELIX_INTERNAL_ELECTION`), a replica answers a
//! fence only at the newest set it knows. A broker that left the set and
//! still counts the old one cannot gather a majority with it.
//!
//! On Raft a member removed from its group is not told and stays open, still
//! counting the old voters. It must not be elected on them.
//!
//! Run with `cargo test -p felix-cluster --test failures replica_set::`.
use std::sync::Arc;
use std::time::{Duration, Instant};

use felix_cluster::{Cluster, ClusterConfig, Fault, StreamSpec};
use felix_controlplane_service::model::StreamKey;
use felix_replication::peer::config::PeerTlsConfig;
use felix_replication::peer::tls::PeerTls;
use felix_replication::peer::{PeerPool, PeerTransportConfig};
use felix_wire::internal::{
    ErrorCode, Fence, FenceElection, InternalMessage, ReplicaLog, ReplicaSetView, SetMember,
    ShardRef,
};
use serial_test::serial;

const STREAM: &str = "orders";

async fn start() -> Cluster {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 4,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        broker_env: vec![
            ("FELIX_INTERNAL_ELECTION".to_string(), "true".to_string()),
            (
                "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
                "2000".to_string(),
            ),
        ],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    cluster
        .finalize_fleet_features(
            &["generation_start", "majority_ack"],
            Duration::from_secs(20),
        )
        .await
        .expect("every broker enables generation_start and majority_ack");
    cluster
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
/// `followers` level with it. Each follower stored a batch stamped with that
/// generation, so each keeps the set it names.
async fn level_at(cluster: &Cluster, leader: &str, followers: &[String], generation: u64) {
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "a level report at the new generation",
        || async {
            if cluster
                .publish_via(leader, STREAM, b"on-the-new-set".to_vec())
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
    .expect("the new set holds the leader's writes");
}

/// A peer pool that speaks as `node_id`, with the certificate the harness
/// issued it, so every broker takes it for that broker.
fn pool_as(cluster: &Cluster, node_id: &str) -> Arc<PeerPool> {
    let root = cluster
        .node(node_id)
        .expect("node")
        .data_dir
        .parent()
        .expect("data root")
        .join("pki");
    let path = |name: String| root.join(name).to_string_lossy().into_owned();
    let tls = PeerTls::load(&PeerTlsConfig {
        cert_path: path(format!("{node_id}.pem")),
        key_path: path(format!("{node_id}.key.pem")),
        ca_path: path("ca.pem".to_string()),
    })
    .expect("load the departed broker's certificate");
    PeerPool::new_with_tls(
        node_id.to_string(),
        PeerTransportConfig {
            election: true,
            ..PeerTransportConfig::default()
        },
        tokio_util::sync::CancellationToken::new(),
        Some(Arc::new(tls)),
    )
    .expect("peer pool")
}

/// The group's voters as its leader last reported them, sorted.
async fn raft_voters(cluster: &Cluster) -> Vec<String> {
    cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store
        .list_shard_assignments()
        .await
        .expect("assignments")
        .into_iter()
        .find(|assignment| assignment.key.stream == STREAM)
        .and_then(|assignment| assignment.raft_voters)
        .map(|voters| voters.members.into_keys().collect())
        .unwrap_or_default()
}

/// Both directions between `node` and each of `peers` refused.
fn cut_off(node: &str, peers: &[String]) -> Vec<Fault> {
    std::iter::once(Fault::Refuse {
        node: node.to_string(),
        peers: peers.to_vec(),
    })
    .chain(peers.iter().map(|peer| Fault::Refuse {
        node: peer.clone(),
        peers: vec![node.to_string()],
    }))
    .collect()
}

/// The `record-` payloads `node` replays from the start, once it holds
/// `want` of them.
async fn replay(cluster: &Cluster, node: &str, want: usize) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mut payloads = Vec::new();
        if let Ok((_client, mut subscription)) = cluster.replay_on(node, STREAM).await {
            while let Ok(Ok(Some(event))) =
                tokio::time::timeout(Duration::from_secs(2), subscription.next_event()).await
            {
                let payload = String::from_utf8_lossy(&event.payload).into_owned();
                if payload.starts_with("record-") {
                    payloads.push(payload);
                }
                if payloads.len() == want {
                    return payloads;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "{node} never served every acknowledged record: {payloads:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The Raft form. Four brokers, RF 3. A follower is cut off from every other
/// broker, drained and replaced by the spare, so it never hears that it was
/// removed and its member still counts the old voters. Once it can reach
/// the others again the leader dies: by its own list, the departed member
/// and the follower that stayed are a majority. The departed member is never
/// elected, the new set elects among itself, and it holds every record.
async fn raft_a_removed_member_is_not_elected_on_the_old_voters() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 4,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let leader = cluster
        .serving_leader(STREAM, Duration::from_secs(30))
        .await
        .expect("a serving leader");
    let mut acked = Vec::new();
    for i in 0..5 {
        let payload = format!("record-{i}");
        cluster
            .publish_via(&leader, STREAM, payload.clone().into_bytes())
            .await
            .expect("publish");
        acked.push(payload);
    }
    let leader = cluster
        .wait_for_every_member_level(STREAM, Duration::from_secs(30))
        .await
        .expect("every member level");
    let placed = assignment(&cluster).await;
    let mut old_set: Vec<String> = placed.replicas.clone();
    old_set.push(placed.leader.clone());
    old_set.sort();
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "the old voters reported",
        || async { raft_voters(&cluster).await == old_set },
    )
    .await
    .expect("the group reports its voters");
    let departed = old_set
        .iter()
        .find(|id| **id != leader)
        .expect("a follower")
        .clone();
    let kept = old_set
        .iter()
        .find(|id| **id != leader && **id != departed)
        .expect("another follower")
        .clone();
    let joiner = cluster
        .node_ids()
        .into_iter()
        .find(|id| !old_set.contains(id))
        .expect("a fourth broker");

    let others: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != departed)
        .collect();
    let cut = cut_off(&departed, &others);
    for fault in &cut {
        cluster.inject(fault).await.expect("cut the follower off");
    }
    cluster.drain_node(&departed).await.expect("drain");
    let mut new_set = vec![leader.clone(), kept.clone(), joiner.clone()];
    new_set.sort();
    felix_cluster::wait::until(Duration::from_secs(120), "the new voters", || async {
        cluster.place_shards().await;
        raft_voters(&cluster).await == new_set
    })
    .await
    .expect("the joiner replaces the drained follower");
    for i in 5..10 {
        let payload = format!("record-{i}");
        cluster
            .publish_via(&leader, STREAM, payload.clone().into_bytes())
            .await
            .expect("publish on the new set");
        acked.push(payload);
    }

    // The hazard: the departed member is open and still counts itself, the
    // follower that stayed and the old leader as the voters.
    let stale = cluster
        .raft_member(&departed, STREAM)
        .await
        .expect("the departed broker keeps its member open");
    for node in [&leader, &kept, &departed] {
        assert!(
            stale.voters.contains(node),
            "{departed} does not count {node}: {stale:?}"
        );
    }
    assert!(
        !stale.voters.contains(&joiner),
        "{departed} heard of its removal: {stale:?}"
    );

    for fault in &cut {
        cluster.heal(fault).await.expect("heal");
    }
    cluster.kill_node(&leader).expect("kill the leader");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let role = cluster
            .raft_member(&departed, STREAM)
            .await
            .map(|raft| raft.role);
        assert_ne!(
            role.as_deref(),
            Some("leader"),
            "{departed} was elected on the voters it left"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let serving = cluster
        .promoted_leader(STREAM, &leader, Duration::from_secs(60))
        .await
        .expect("the new set elects");
    assert!(
        serving == kept || serving == joiner,
        "{serving} leads, outside the new set"
    );
    cluster
        .publish_via(&serving, STREAM, b"after".to_vec())
        .await
        .expect("the shard still serves");
    assert_eq!(replay(&cluster, &serving, acked.len()).await, acked);
    cluster.shutdown().await;
}

/// **A replica that left the set cannot win a fence.** Four brokers, RF 3. A
/// follower is drained and replaced by the spare, the new set acknowledges
/// writes, and the departed broker is killed. Speaking as it, with its own
/// certificate, a candidate fence at a newer generation carrying the set it
/// last knew is refused `StaleSet` by every live broker, so it has no
/// majority. The shard keeps serving.
#[serial]
#[tokio::test]
async fn a_replica_that_left_the_set_cannot_win_a_fence() {
    if felix_cluster::suite_runs_on_raft() {
        raft_a_removed_member_is_not_elected_on_the_old_voters().await;
        return;
    }
    let mut cluster = start().await;
    let before = assignment(&cluster).await;
    let leader = before.leader.clone();
    assert_eq!(before.replicas.len(), 2, "RF 3: {before:?}");
    let (departed, kept) = (before.replicas[0].clone(), before.replicas[1].clone());
    let joiner = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader && !before.replicas.contains(id))
        .expect("a fourth broker");
    level_at(&cluster, &leader, &before.replicas, before.generation).await;

    cluster.drain_node(&departed).await.expect("drain");
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "the joiner to replace the drained follower",
        || async {
            cluster.place_shards().await;
            let now = assignment(&cluster).await;
            now.leader == leader
                && now.replicas.contains(&joiner)
                && !now.replicas.contains(&departed)
        },
    )
    .await
    .expect("placement seats the spare in the drained follower's place");
    let after = assignment(&cluster).await;
    assert!(
        after.generation > before.generation,
        "a new set at a new generation: {before:?} then {after:?}"
    );
    level_at(
        &cluster,
        &leader,
        &[kept.clone(), joiner.clone()],
        after.generation,
    )
    .await;
    cluster
        .kill_node(&departed)
        .expect("kill the departed follower");

    let incarnation = cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store
        .get_stream(&StreamKey {
            tenant_id: cluster.tenant_id.clone(),
            namespace: cluster.namespace.clone(),
            stream: STREAM.to_string(),
        })
        .await
        .expect("stream")
        .incarnation;
    let old_set = ReplicaSetView {
        generation: before.generation,
        members: std::iter::once(&leader)
            .chain(&before.replicas)
            .map(|node| SetMember {
                node_id: node.clone(),
                storage_id: None,
                learner: false,
            })
            .collect(),
    };
    let pool = pool_as(&cluster, &departed);
    for target in [&leader, &kept, &joiner] {
        let fence = InternalMessage::Fence(Fence {
            correlation_id: 0,
            shard: ShardRef {
                tenant_id: cluster.tenant_id.clone(),
                namespace: cluster.namespace.clone(),
                stream: STREAM.to_string(),
                shard: 0,
                generation: after.generation + 1,
                incarnation: (incarnation != 0).then_some(incarnation),
            },
            log: ReplicaLog::Stream,
            election: Some(FenceElection {
                candidate: true,
                set: old_set.clone(),
                leaving: None,
                earlier: Vec::new(),
            }),
        });
        let addr = cluster.node(target).expect("node").internal_addr;
        let answer = pool
            .request(target, addr, fence)
            .await
            .unwrap_or_else(|err| panic!("{target} answers the fence: {err}"));
        match answer {
            InternalMessage::ReplicateError(refusal) if refusal.code == ErrorCode::StaleSet => {
                assert!(
                    refusal.expected_offset >= after.generation,
                    "{target} names set generation {}, older than {}",
                    refusal.expected_offset,
                    after.generation
                );
            }
            other => panic!(
                "{target} answered {departed}'s fence for the set of generation {} with \
                 {other:?}, not StaleSet",
                before.generation
            ),
        }
    }
    pool.shutdown().await;

    cluster
        .publish_via(&leader, STREAM, b"after".to_vec())
        .await
        .expect("the shard still serves");
    cluster.shutdown().await;
}
