//! Once the fleet finalizes `majority_ack`, a `Quorum` stream acknowledges a
//! write when a majority of its replicas has answered that it holds it at the
//! leader's generation. The control plane's report and the leader's lease are
//! off the write's path, so a leader cut off from the control plane goes on
//! acknowledging what its followers hold, and nothing else.
//!
//! Run with `cargo test -p felix-cluster --test failures majority_ack::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, Endpoint, Fault, StreamSpec};
use felix_controlplane_service::store::ControlPlaneStore;
use serial_test::serial;

const STREAM: &str = "orders";
const LEASE_HELD: &str = "felix_broker_lease_held";

/// Three brokers, a `Quorum` stream on all three, and both features the
/// follower acks need finalized.
async fn start(quorum_timeout: Duration) -> Cluster {
    start_nodes(3, quorum_timeout).await
}

/// `nodes` brokers and an RF 3 `Quorum` stream, both features finalized.
async fn start_nodes(nodes: usize, quorum_timeout: Duration) -> Cluster {
    let cluster = Cluster::start(ClusterConfig {
        nodes,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        proxy_links: true,
        broker_env: vec![(
            "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
            quorum_timeout.as_millis().to_string(),
        )],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let store = &cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store;
    for feature in ["generation_start", "majority_ack"] {
        store
            .finalize_fleet_feature(feature)
            .await
            .unwrap_or_else(|err| panic!("finalize {feature}: {err}"));
    }
    for id in cluster.node_ids() {
        felix_cluster::wait::until(Duration::from_secs(20), "both features to turn on", || {
            let id = id.clone();
            let cluster = &cluster;
            async move {
                cluster
                    .metric(&id, "felix_broker_fleet_feature_enabled")
                    .await
                    .ok()
                    .flatten()
                    == Some(2.0)
            }
        })
        .await
        .expect("every broker enables generation_start and majority_ack");
    }
    cluster
}

/// Cut every broker off from the control plane, and wait until `leader` no
/// longer holds its lease.
async fn partition_control_plane(cluster: &Cluster, leader: &str) {
    for id in cluster.node_ids() {
        for fault in Fault::partition(Endpoint::node(&id), Endpoint::ControlPlane) {
            cluster.inject(&fault).await.expect("partition");
        }
    }
    felix_cluster::wait::until(Duration::from_secs(20), "the lease to lapse", || async {
        cluster.metric(leader, LEASE_HELD).await.ok().flatten() == Some(0.0)
    })
    .await
    .expect("the leader's lease lapses with the control plane gone");
}

/// Everything `node` replays. Reads still need the lease, so a replay is
/// retried until the node has renewed it.
async fn held_by(cluster: &Cluster, node: &str) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let (_client, mut replay) = loop {
        match cluster.replay_on(node, STREAM).await {
            Ok(opened) => break opened,
            Err(err) if tokio::time::Instant::now() > deadline => {
                panic!("replay on {node}: {err:#}")
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    };
    let mut held = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(2), replay.next_event()).await
    {
        held.push(String::from_utf8_lossy(&event.payload).to_string());
    }
    held
}

/// **With the control plane unreachable, a leader keeps acknowledging what a
/// majority of its replicas holds.** Its lease has lapsed and no report can
/// land, and neither is in the condition any more. Once the control plane is
/// back, whoever leads holds every write acknowledged meanwhile.
#[serial]
#[tokio::test]
async fn acknowledgements_continue_while_the_control_plane_is_partitioned() {
    let cluster = start(Duration::from_secs(10)).await;
    let leader = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&leader, STREAM, b"before".to_vec())
        .await
        .expect("publish with the control plane reachable");

    partition_control_plane(&cluster, &leader).await;

    let mut acknowledged = vec!["before".to_string()];
    for i in 0..5 {
        let payload = format!("partitioned-{i}");
        cluster
            .publish_via(&leader, STREAM, payload.clone().into_bytes())
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "{payload} is on a majority at the leader's generation, so it is \
                     acknowledged whatever the lease and the report say: {err:#}"
                )
            });
        acknowledged.push(payload);
    }
    assert_eq!(
        cluster.metric(&leader, LEASE_HELD).await.ok().flatten(),
        Some(0.0),
        "the lease has to be lapsed while the writes are acknowledged",
    );

    cluster.heal_all().await.expect("heal");
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "a leader to take writes again",
        || async {
            cluster
                .publish_via_any(STREAM, b"after".to_vec())
                .await
                .is_ok()
        },
    )
    .await
    .expect("the shard serves once the control plane is back");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let held = held_by(&cluster, &owner).await;
    for record in &acknowledged {
        assert!(
            held.contains(record),
            "{owner} lost {record}, which was acknowledged: it holds {held:?}"
        );
    }
    cluster.shutdown().await;
}

/// **No majority, no acknowledgement**, with the lease out of the picture:
/// the control plane is gone, so the lease is lapsed either way, and what
/// holds the write back is the followers not having it.
#[serial]
#[tokio::test]
async fn a_write_no_majority_holds_is_not_acknowledged() {
    let cluster = start(Duration::from_secs(3)).await;
    let leader = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&leader, STREAM, b"before".to_vec())
        .await
        .expect("publish while whole");
    partition_control_plane(&cluster, &leader).await;

    let followers: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    let refuse = Fault::Refuse {
        node: leader.clone(),
        peers: followers,
    };
    cluster
        .inject(&refuse)
        .await
        .expect("cut the followers off");
    let refused = cluster
        .publish_via(&leader, STREAM, b"alone".to_vec())
        .await
        .expect_err("a write only the leader holds was acknowledged");
    let why = format!("{refused:#}");
    assert!(
        !why.contains("lease"),
        "the write was refused for the lease, which proves nothing here: {why}"
    );

    cluster.heal(&refuse).await.expect("heal the peers");
    cluster
        .publish_via(&leader, STREAM, b"together".to_vec())
        .await
        .expect("acknowledged again once a follower answers");
    cluster.shutdown().await;
}

/// **A leader cut off from the control plane keeps writing past its lease,
/// and loses nothing it acknowledged.** The control plane promotes a follower
/// from a report that predates those writes; the new leader fences a majority
/// and takes the furthest log before it serves, so every write acknowledged
/// on a majority is in it. From the fence on, the old leader's followers
/// refuse it and nothing more is acknowledged there.
#[serial]
#[tokio::test]
async fn a_leader_cut_off_from_the_control_plane_loses_nothing_it_acknowledged() {
    let cluster = start(Duration::from_secs(2)).await;
    let old = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&old, STREAM, b"before".to_vec())
        .await
        .expect("publish while whole");
    for fault in Fault::partition(Endpoint::node(&old), Endpoint::ControlPlane) {
        cluster.inject(&fault).await.expect("partition");
    }

    // The harness's control plane places only when asked, so nothing is
    // promoted until the loop below starts: this write is acknowledged with
    // the old leader's own lease lapsed and nobody else leading.
    felix_cluster::wait::until(Duration::from_secs(10), "the lease to lapse", || async {
        cluster.metric(&old, LEASE_HELD).await.ok().flatten() == Some(0.0)
    })
    .await
    .expect("the old leader's lease lapses");
    cluster
        .publish_via(&old, STREAM, b"past-the-lease".to_vec())
        .await
        .expect("a majority holds it, so it is acknowledged without the lease");

    // Then writing and promotion at once. The control plane promotes only
    // from a report fresher than about two expiries, and the old leader can
    // send none from here, so placement is stepped straight away.
    let promoted = std::sync::atomic::AtomicBool::new(false);
    let writes = async {
        let mut acknowledged = vec!["before".to_string(), "past-the-lease".to_string()];
        let mut after_promotion = 0;
        for i in 0.. {
            if after_promotion >= 10 {
                break;
            }
            if promoted.load(std::sync::atomic::Ordering::SeqCst) {
                after_promotion += 1;
            }
            let payload = format!("cut-off-{i}");
            if cluster
                .publish_via(&old, STREAM, payload.clone().into_bytes())
                .await
                .is_ok()
            {
                acknowledged.push(payload);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        acknowledged
    };
    let promotion = async {
        let promoted_in =
            felix_cluster::wait::until(Duration::from_secs(30), "a new leader", || async {
                cluster.place_shards().await;
                cluster.owner(STREAM).await.is_ok_and(|owner| owner != old)
            })
            .await;
        promoted.store(true, std::sync::atomic::Ordering::SeqCst);
        promoted_in
    };
    let (acknowledged, promoted_in) = tokio::join!(writes, promotion);
    promoted_in.expect("the control plane promotes a follower");

    cluster.heal_all().await.expect("heal");
    let new = cluster.owner(STREAM).await.expect("owner");
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the new leader to serve",
        || async {
            cluster
                .publish_via(&new, STREAM, b"after".to_vec())
                .await
                .is_ok()
        },
    )
    .await
    .expect("the new leader serves");
    let held = held_by(&cluster, &new).await;
    for record in &acknowledged {
        assert!(
            held.contains(record),
            "{new} lost {record}, which {old} acknowledged: it holds {held:?}"
        );
    }
    cluster.shutdown().await;
}

/// The shard's current assignment: its leader and followers.
async fn assignment(cluster: &Cluster) -> felix_cluster::Assignment {
    let key = format!("{}/{}/{STREAM}/0", cluster.tenant_id, cluster.namespace);
    cluster
        .shard_assignments()
        .await
        .expect("assignments")
        .remove(&key)
        .expect("the stream is placed")
}

/// Publish through `leader` and wait for the report that names every one of
/// `followers` level with it, so promotion can pick any of them.
async fn level_report(
    cluster: &Cluster,
    leader: &str,
    followers: &[String],
) -> felix_controlplane_service::model::ReplicaReport {
    let before = cluster
        .replica_report(STREAM, 0)
        .await
        .ok()
        .flatten()
        .and_then(|report| report.leader_offset);
    cluster
        .publish_via(leader, STREAM, b"before".to_vec())
        .await
        .expect("publish while whole");
    let level = std::sync::Mutex::new(None);
    felix_cluster::wait::until(Duration::from_secs(10), "a level report", || {
        let level = &level;
        async move {
            let Some(report) = cluster.replica_report(STREAM, 0).await.ok().flatten() else {
                return false;
            };
            let tail = report.leader_offset;
            let ok = tail > before
                && followers.iter().all(|id| {
                    report.caught_up.contains(id) && report.offsets.get(id).copied() == tail
                });
            if ok {
                *level.lock().expect("unpoisoned") = Some(report);
            }
            ok
        }
    })
    .await
    .expect("every follower reports level");
    level
        .into_inner()
        .expect("unpoisoned")
        .expect("the report the wait saw")
}

/// Store `report` again, stamped now, as the old leader's level report
/// arriving late would be. The harness judges a report stale about a second
/// after the leader is marked down, where a deployment has several; the
/// content is exactly what the old leader last said, so this only widens
/// that window, it does not invent a position.
async fn restamp(
    cluster: &Cluster,
    mut report: felix_controlplane_service::model::ReplicaReport,
    leader: &str,
) {
    let store = &cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store;
    report.reported_at_millis = store.now_millis().await.expect("store clock");
    let written = store
        .record_replica_report(report, leader)
        .await
        .expect("record the report");
    assert!(
        matches!(
            written,
            felix_controlplane_service::store::ReportWrite::Stored
        ),
        "a newer report landed after the level one: {written:?}"
    );
}

/// Step placement until someone other than `old` leads, and return who.
async fn promote_away_from(cluster: &Cluster, old: &str) -> String {
    let promoted = felix_cluster::wait::until(Duration::from_secs(10), "a new leader", || async {
        cluster.place_shards().await;
        cluster.owner(STREAM).await.is_ok_and(|owner| owner != old)
    })
    .await;
    if let Err(err) = promoted {
        let plan = cluster.plan_if_down(old).await.map(|plan| {
            plan.shards
                .into_iter()
                .filter(|shard| shard.key.stream == STREAM)
                .map(|shard| shard.decision)
                .collect::<Vec<_>>()
        });
        panic!("the control plane promotes a follower: {err:#}; placement decides {plan:?}");
    }
    cluster.owner(STREAM).await.expect("owner")
}

/// Cut `node` off from the control plane: no heartbeat, no report.
async fn cut_from_control_plane(cluster: &Cluster, node: &str) {
    for fault in Fault::partition(Endpoint::node(node), Endpoint::ControlPlane) {
        cluster.inject(&fault).await.expect("partition");
    }
}

/// Assert `node` holds every record in `acknowledged`.
async fn assert_holds(cluster: &Cluster, node: &str, acknowledged: &[String]) {
    let held = held_by(cluster, node).await;
    for record in acknowledged {
        assert!(
            held.contains(record),
            "{node} lost {record}, which was acknowledged: it holds {held:?}"
        );
    }
}

/// **A failover that brings in a spare broker loses nothing a majority of the
/// old replica set acknowledged.** Four brokers, RF 3: the old leader
/// acknowledges records held by itself and `other` alone while `promoted`
/// lags, and `promoted` is the replica placement names, from a report taken
/// before the lag. Were the spare swapped in for the old leader, `promoted`
/// and the spare would be a majority of the new set that never saw those
/// records. `promoted` is cut off from `other` while it fences, so only the
/// replica set decides whether it waits for `other`.
#[serial]
#[tokio::test]
async fn a_failover_onto_a_spare_broker_keeps_what_the_old_set_acknowledged() {
    use felix_controlplane_service::cluster::placement::Decision;

    let mut cluster = start_nodes(4, Duration::from_secs(2)).await;
    let assignment = assignment(&cluster).await;
    let old = assignment.leader.clone();
    assert_eq!(assignment.replicas.len(), 2, "RF 3: {assignment:?}");
    let report = level_report(&cluster, &old, &assignment.replicas).await;

    let plan = cluster.plan_if_down(&old).await.expect("dry-run placement");
    let promoted = plan
        .shards
        .iter()
        .find_map(|shard| match &shard.decision {
            Decision::Place(leader, _) if shard.key.stream == STREAM => Some(leader.clone()),
            _ => None,
        })
        .expect("placement promotes a follower once the leader is gone");
    let other = assignment
        .replicas
        .iter()
        .find(|id| **id != promoted)
        .expect("the other follower")
        .clone();

    // From here the old leader's report is frozen, and `promoted` gets
    // nothing more from it: what follows is acknowledged on {old, other}.
    cut_from_control_plane(&cluster, &old).await;
    cluster
        .inject(&Fault::Refuse {
            node: old.clone(),
            peers: vec![promoted.clone()],
        })
        .await
        .expect("cut the promoted replica off");
    let mut acknowledged = vec!["before".to_string()];
    for i in 0..5 {
        let payload = format!("on-a-majority-{i}");
        cluster
            .publish_via(&old, STREAM, payload.clone().into_bytes())
            .await
            .unwrap_or_else(|err| panic!("{payload} is held by {old} and {other}: {err:#}"));
        acknowledged.push(payload);
    }
    cluster.kill_node(&old).expect("kill the old leader");
    let fence_cut = Fault::Refuse {
        node: promoted.clone(),
        peers: vec![other.clone()],
    };
    cluster
        .inject(&fence_cut)
        .await
        .expect("cut promoted -> other");

    restamp(&cluster, report, &old).await;
    let new = promote_away_from(&cluster, &old).await;
    assert_eq!(new, promoted, "the dry run named the promoted replica");

    // Long enough for the fence to settle on whatever majority it can reach.
    tokio::time::sleep(Duration::from_secs(2)).await;
    cluster.heal(&fence_cut).await.expect("heal");
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the new leader to serve",
        || async {
            cluster
                .publish_via(&new, STREAM, b"after".to_vec())
                .await
                .is_ok()
        },
    )
    .await
    .expect("the new leader serves");
    assert_holds(&cluster, &new, &acknowledged).await;
    cluster.shutdown().await;
}

/// **A promotion waits for a majority of the old replica set.** Three
/// brokers, RF 3: the old leader acknowledges a record held by itself and one
/// follower, then both die. The last follower is promoted from a report that
/// named it level, and there is nobody live to put beside it. It must not
/// serve until one of the two holders is back, and then it serves with the
/// record.
#[serial]
#[tokio::test]
async fn a_promotion_waits_while_a_majority_of_the_old_set_is_down() {
    let mut cluster = start(Duration::from_secs(2)).await;
    let assignment = assignment(&cluster).await;
    let old = assignment.leader.clone();
    let report = level_report(&cluster, &old, &assignment.replicas).await;
    let (holder, lagging) = (
        assignment.replicas[0].clone(),
        assignment.replicas[1].clone(),
    );

    // The holder goes quiet first, so the control plane never sees it live
    // with the old leader down: its backstop pass would promote it.
    cut_from_control_plane(&cluster, &holder).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    cut_from_control_plane(&cluster, &old).await;
    cluster
        .inject(&Fault::Refuse {
            node: old.clone(),
            peers: vec![lagging.clone()],
        })
        .await
        .expect("cut the lagging follower off");
    cluster
        .publish_via(&old, STREAM, b"on-two-of-three".to_vec())
        .await
        .expect("held by the old leader and one follower");
    let acknowledged = vec!["before".to_string(), "on-two-of-three".to_string()];
    cluster.kill_node(&old).expect("kill the old leader");
    cluster.kill_node(&holder).expect("kill the other holder");
    felix_cluster::wait::until(Duration::from_secs(20), "both holders down", || async {
        cluster
            .placeable_nodes()
            .await
            .is_ok_and(|live| !live.contains(&old) && !live.contains(&holder))
    })
    .await
    .expect("both holders are marked down");

    restamp(&cluster, report, &old).await;
    let new = promote_away_from(&cluster, &old).await;
    assert_eq!(
        new,
        lagging,
        "the only live follower is promoted: old {old}, holder {holder}, now {:?}, live {:?}",
        cluster.shard_assignments().await,
        cluster.placeable_nodes().await
    );

    // Without a majority of {old, holder, lagging} it cannot fence, so it
    // takes nothing. Serving here would be serving without the record.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    while tokio::time::Instant::now() < deadline {
        cluster
            .publish_via(&new, STREAM, b"too-early".to_vec())
            .await
            .expect_err("the new leader served with a majority of its set down");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    for fault in Fault::partition(Endpoint::node(&holder), Endpoint::ControlPlane) {
        cluster.heal(&fault).await.expect("heal the holder's link");
    }
    cluster
        .restart_node(&holder)
        .await
        .expect("bring a holder back");
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the new leader to serve",
        || async {
            cluster
                .publish_via(&new, STREAM, b"after".to_vec())
                .await
                .is_ok()
        },
    )
    .await
    .expect("the new leader serves once it can fence a majority");
    assert_holds(&cluster, &new, &acknowledged).await;
    cluster.shutdown().await;
}

/// **A partition with the control plane on the minority of the replica set
/// does not give the shard two leaders.** Four brokers, RF 3, split
/// {old, holder} | {promoted, spare, control plane}. The old leader goes on
/// acknowledging on {old, holder}, since nothing on that path asks the
/// control plane. The control plane promotes `promoted`, which must not open
/// with the spare as its majority. Once healed, whoever leads holds every
/// write acknowledged on either side.
#[serial]
#[tokio::test]
async fn a_partitioned_minority_with_the_control_plane_does_not_open() {
    let cluster = start_nodes(4, Duration::from_secs(2)).await;
    let assignment = assignment(&cluster).await;
    let old = assignment.leader.clone();
    let report = level_report(&cluster, &old, &assignment.replicas).await;
    // The follower to promote is whichever the report would pick once the
    // holder is down too: the one left on the control plane's side.
    let (holder, promoted) = (
        assignment.replicas[0].clone(),
        assignment.replicas[1].clone(),
    );
    let spare = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != old && !assignment.replicas.contains(id))
        .expect("a fourth broker");

    let minority = [old.clone(), holder.clone()];
    let majority = [promoted.clone(), spare.clone()];
    let mut cuts = Vec::new();
    for (side, others) in [(&minority, &majority), (&majority, &minority)] {
        for node in side {
            cuts.push(Fault::Refuse {
                node: node.clone(),
                peers: others.to_vec(),
            });
        }
    }
    for node in &minority {
        cut_from_control_plane(&cluster, node).await;
    }
    for cut in &cuts {
        cluster.inject(cut).await.expect("partition the brokers");
    }

    // Writes on both sides, for as long as the promotion takes and a while
    // after. Only a side holding a majority of the old set may acknowledge.
    let promoted_yet = std::sync::atomic::AtomicBool::new(false);
    let writes = async {
        let mut acknowledged = vec!["before".to_string()];
        let mut after = 0;
        for i in 0.. {
            if after >= 20 {
                break;
            }
            if promoted_yet.load(std::sync::atomic::Ordering::SeqCst) {
                after += 1;
            }
            for node in [&old, &promoted] {
                let payload = format!("{node}-{i}");
                if cluster
                    .publish_via(node, STREAM, payload.clone().into_bytes())
                    .await
                    .is_ok()
                {
                    acknowledged.push(payload);
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        acknowledged
    };
    let promotion = async {
        // The minority's heartbeats stop with the partition; wait for both
        // to be marked down, then give placement the level report.
        felix_cluster::wait::until(Duration::from_secs(20), "the minority down", || async {
            cluster
                .placeable_nodes()
                .await
                .is_ok_and(|live| !live.contains(&old) && !live.contains(&holder))
        })
        .await
        .expect("the minority is marked down");
        restamp(&cluster, report, &old).await;
        let new = promote_away_from(&cluster, &old).await;
        promoted_yet.store(true, std::sync::atomic::Ordering::SeqCst);
        new
    };
    let (acknowledged, new) = tokio::join!(writes, promotion);
    assert_eq!(new, promoted, "the follower on the control plane's side");
    assert!(
        !acknowledged
            .iter()
            .any(|record| record.starts_with(&format!("{promoted}-"))),
        "{promoted} acknowledged writes with {old} still acknowledging on {holder}: {acknowledged:?}"
    );

    // The minority registers again once it reaches the control plane, and
    // the new leader can then reach it to fence.
    cluster.heal_all().await.expect("heal");
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the new leader to serve",
        || async {
            cluster
                .publish_via(&new, STREAM, b"after".to_vec())
                .await
                .is_ok()
        },
    )
    .await
    .expect("the new leader serves once it reaches the old set");
    assert_holds(&cluster, &new, &acknowledged).await;
    cluster.shutdown().await;
}

/// **Replacing a follower does not shrink the set below the majority that
/// acknowledged a write.** Four brokers, RF 3: the leader acknowledges records
/// on itself and `departing` while `lagging` is cut off, then `departing` is
/// drained and the fourth broker joins in its place, also cut off from the
/// leader. Seating the newcomer before it holds those records leaves them on
/// the leader alone among the new set, and once the leader dies, `lagging`
/// and the newcomer are a majority that never saw them.
#[serial]
#[tokio::test]
async fn seating_a_replacement_keeps_what_the_old_set_acknowledged() {
    let mut cluster = start_nodes(4, Duration::from_secs(2)).await;
    let before = assignment(&cluster).await;
    let leader = before.leader.clone();
    level_report(&cluster, &leader, &before.replicas).await;
    let (departing, lagging) = (before.replicas[0].clone(), before.replicas[1].clone());
    let joiner = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader && !before.replicas.contains(id))
        .expect("a fourth broker");

    // What follows is acknowledged on {leader, departing}.
    cluster
        .inject(&Fault::Refuse {
            node: leader.clone(),
            peers: vec![lagging.clone(), joiner.clone()],
        })
        .await
        .expect("cut the lagging follower and the joiner off");
    let mut acknowledged = vec!["before".to_string()];
    for i in 0..5 {
        let payload = format!("on-the-old-set-{i}");
        cluster
            .publish_via(&leader, STREAM, payload.clone().into_bytes())
            .await
            .unwrap_or_else(|err| panic!("{payload} is held by {leader} and {departing}: {err:#}"));
        acknowledged.push(payload);
    }

    cluster.drain_node(&departing).await.expect("drain");
    felix_cluster::wait::until(Duration::from_secs(10), "the joiner to join", || async {
        cluster.place_shards().await;
        assignment(&cluster).await.replicas.contains(&joiner)
    })
    .await
    .expect("placement starts replacing the drained follower");

    // Step placement for a while: a seat here drops `departing` while the
    // joiner holds nothing the leader wrote before it joined.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        cluster.place_shards().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let now = assignment(&cluster).await;
    let generation = now.generation;
    felix_cluster::wait::until(
        Duration::from_secs(10),
        "a report at this generation",
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
    .expect("the leader reports at the current generation");
    let report = cluster
        .replica_report(STREAM, 0)
        .await
        .expect("report")
        .expect("a report");

    cluster.kill_node(&leader).expect("kill the leader");
    felix_cluster::wait::until(Duration::from_secs(20), "the leader down", || async {
        cluster
            .placeable_nodes()
            .await
            .is_ok_and(|live| !live.contains(&leader))
    })
    .await
    .expect("the leader is marked down");
    restamp(&cluster, report, &leader).await;
    let new = promote_away_from(&cluster, &leader).await;
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the new leader to serve",
        || async {
            cluster
                .publish_via(&new, STREAM, b"after".to_vec())
                .await
                .is_ok()
        },
    )
    .await
    .unwrap_or_else(|err| panic!("{new} serves ({now:?} before the leader died): {err:#}"));
    assert_holds(&cluster, &new, &acknowledged).await;
    cluster.shutdown().await;
}

/// **A lost follower is replaced, and the shard gets back to three copies,
/// even when the first broker it is copied to dies mid-copy.** Five brokers,
/// RF 3. A follower is killed; once it has been gone past the restore delay,
/// placement copies the shard to a spare. That spare is killed before its
/// copy is seated, so placement drops it and copies to the other spare. The
/// set ends at three live brokers, and the new copy holds every record
/// acknowledged before the loss.
#[serial]
#[tokio::test]
async fn a_lost_follower_is_restored_even_when_its_first_replacement_dies() {
    use felix_controlplane_service::cluster::placement::MovePolicy;
    use felix_controlplane_service::model::{MoveReason, ShardKey, ShardKind};

    let mut cluster = start_nodes(5, Duration::from_secs(2)).await;
    let before = assignment(&cluster).await;
    let leader = before.leader.clone();
    level_report(&cluster, &leader, &before.replicas).await;
    let (lost, kept) = (before.replicas[0].clone(), before.replicas[1].clone());

    let mut acknowledged = vec!["before".to_string()];
    for i in 0..20 {
        let payload = format!("acknowledged-{i}");
        cluster
            .publish_via(&leader, STREAM, payload.clone().into_bytes())
            .await
            .unwrap_or_else(|err| panic!("{payload}: {err:#}"));
        acknowledged.push(payload);
    }

    cluster.kill_node(&lost).expect("kill a follower");
    felix_cluster::wait::until(Duration::from_secs(20), "the follower down", || async {
        cluster
            .placeable_nodes()
            .await
            .is_ok_and(|live| !live.contains(&lost))
    })
    .await
    .expect("the follower is marked down");

    let control_plane = cluster.control_plane.as_ref().expect("control plane");
    let store = &control_plane.store;
    let key = ShardKey {
        tenant_id: cluster.tenant_id.clone(),
        namespace: cluster.namespace.clone(),
        stream: STREAM.to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    };
    let restoring = MovePolicy {
        restore_after_millis: Some(1_000),
        ..MovePolicy::default()
    };

    // One pass at a time, so nothing seats the first copy before it dies.
    let first = felix_cluster::wait::until(Duration::from_secs(20), "a restore", || async {
        control_plane.place_shards_with(restoring.clone()).await;
        store
            .get_shard_assignment(&key)
            .await
            .is_ok_and(|now| now.move_reason == Some(MoveReason::Restore))
    })
    .await;
    first.expect("placement starts restoring the lost follower");
    let started = store.get_shard_assignment(&key).await.expect("assignment");
    let first = started.joining.clone().expect("a copy joining");
    assert!(
        !before.replicas.contains(&first) && first != leader,
        "{first} is not a spare"
    );
    // Wait for the leader to be shipping to it, then kill it mid-copy.
    felix_cluster::wait::until(Duration::from_secs(20), "the copy to start", || async {
        cluster
            .replica_report(STREAM, 0)
            .await
            .ok()
            .flatten()
            .is_some_and(|report| report.generation == started.generation)
    })
    .await
    .expect("the leader reports at the restore's generation");
    cluster
        .kill_node(&first)
        .expect("kill the first destination");

    let control_plane = cluster.control_plane.as_ref().expect("control plane");
    let store = &control_plane.store;
    let restored = felix_cluster::wait::until(Duration::from_secs(60), "three live copies", || {
        let restoring = restoring.clone();
        let (lost, first, key) = (lost.clone(), first.clone(), key.clone());
        async move {
            control_plane.place_shards_with(restoring).await;
            store.get_shard_assignment(&key).await.is_ok_and(|now| {
                now.joining.is_none()
                    && now.replicas.len() == 2
                    && !now.nodes().any(|node| *node == lost || *node == first)
            })
        }
    })
    .await;
    let now = store.get_shard_assignment(&key).await.expect("assignment");
    restored.unwrap_or_else(|err| panic!("the shard gets back to three copies: {err:#}; {now:?}"));
    assert_eq!(now.leader, leader);
    assert!(now.replicas.contains(&kept));
    let replacement = now
        .replicas
        .iter()
        .find(|node| **node != kept)
        .expect("a new follower")
        .clone();

    // The new copy is complete: move leadership onto it and read it back.
    cluster
        .start_move(STREAM, 0, &replacement)
        .await
        .expect("move to the restored copy");
    felix_cluster::wait::until(Duration::from_secs(30), "the move to finish", || async {
        cluster.place_shards().await;
        cluster
            .owner(STREAM)
            .await
            .is_ok_and(|owner| owner == replacement)
    })
    .await
    .expect("the restored copy takes over");
    assert_holds(&cluster, &replacement, &acknowledged).await;
    cluster.shutdown().await;
}
