//! Raft groups that lost a majority of their voters, and the operator
//! re-form that serves them again. `docs/per-shard-raft.md`, "Re-forming a
//! group" and "The restore check".
//!
//! What every test here pins: a copy that came back from a relaxed power
//! cut, a wipe or a restore never votes under the id it had, so no group
//! elects on it, nothing is acknowledged while the group lacks a majority,
//! and the re-form keeps every acknowledged record at its offset and gives
//! out no offset twice.
use std::path::Path;
use std::time::{Duration, Instant};

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_controlplane_service::store::reform::{
    ReformReply, ReformRequest, ReformScope, ReformShardView,
};
use serial_test::serial;

use super::{
    ELECTED_WITHIN, STREAM, Sent, engine, leader_among, opted_in, publish_acked, publish_tried,
    raft_cluster, raft_cluster_with, stored_set, voters_become,
};

const RECORDS: usize = 20;
/// How long a group without a majority is watched for a leader or an ack.
/// Several election timeouts.
const CLOSED_FOR: Duration = Duration::from_secs(15);
const REFORMED_WITHIN: Duration = Duration::from_secs(180);

async fn registered_storage(cluster: &Cluster, node: &str) -> String {
    cluster
        .registered_node(node)
        .await
        .expect("node")
        .status
        .storage_id
        .unwrap_or_else(|| panic!("{node} registered without a storage identity"))
}

/// Publish [`RECORDS`] records through `node` before any fault, each
/// acknowledged once.
async fn publish_before(cluster: &Cluster, node: &str, label: &str, sent: &mut Sent) {
    for n in 0..RECORDS {
        let payload = format!("{label}-{n}").into_bytes();
        let offset = publish_acked(cluster, node, &payload).await;
        sent.acked.push((offset, payload));
    }
}

/// For [`CLOSED_FOR`], none of `up` leads the shard and none acknowledges a
/// write. Each probe sent goes into `probes`, to be checked as never held: a
/// group with no majority has nowhere to commit it.
async fn closed_for(cluster: &Cluster, up: &[String], why: &str, probes: &mut Vec<Vec<u8>>) {
    felix_cluster::wait::until(
        ELECTED_WITHIN,
        "every running broker to run its member",
        || async {
            for node in up {
                if cluster.raft_member(node, STREAM).await.is_none() {
                    return false;
                }
            }
            true
        },
    )
    .await
    .expect("the hazard is set up");
    let deadline = Instant::now() + CLOSED_FOR;
    while Instant::now() < deadline {
        for node in up {
            let role = cluster
                .raft_member(node, STREAM)
                .await
                .map(|raft| raft.role);
            assert_ne!(role.as_deref(), Some("leader"), "{node} led {why}");
            let probe = format!("probe-{}", probes.len()).into_bytes();
            probes.push(probe.clone());
            let acked = cluster.publish_via(node, STREAM, probe).await.is_ok();
            assert!(!acked, "{node} acknowledged a write {why}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn reform(cluster: &Cluster, request: ReformRequest) -> ReformReply {
    cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store
        .reform(request)
        .await
        .expect("the re-form request is answered")
}

/// Shard 0 of [`STREAM`] in the plan `plan_id`, once `settled` says so.
async fn plan_shard(
    cluster: &Cluster,
    plan_id: &str,
    what: &str,
    settled: impl Fn(&str, &ReformShardView) -> bool,
) -> ReformShardView {
    let last = std::cell::RefCell::new(None);
    let found = felix_cluster::wait::until_some(REFORMED_WITHIN, what, || async {
        let request = ReformRequest::View {
            plan_id: plan_id.to_string(),
        };
        let ReformReply::Plan { plan: Some(plan) } = reform(cluster, request).await else {
            return None;
        };
        let shard = plan
            .shards
            .iter()
            .find(|shard| shard.key.stream == STREAM && shard.key.shard == 0)
            .cloned()?;
        *last.borrow_mut() = Some(format!("{} {shard:?}", plan.state));
        settled(&plan.state, &shard).then_some(shard)
    })
    .await;
    found.unwrap_or_else(|err| panic!("{what}: {err:#}; last seen {:?}", last.borrow()))
}

/// Run `plan` then `apply` for the whole cluster, as `felixctl raft reform`
/// does, wait until shard 0 has started its new epoch, and return its
/// voters. A plan marked as possibly losing acknowledged records is applied
/// with the loss accepted: the records the caller checks afterwards are what
/// decides.
///
/// Only the voters are checked after it: a stream learner cannot catch up
/// across a re-form's gap yet (#3005).
async fn plan_and_apply(cluster: &Cluster) -> Vec<String> {
    let ReformReply::Created { plan_id } = reform(
        cluster,
        ReformRequest::Create {
            scope: ReformScope::Cluster,
            restore: None,
        },
    )
    .await
    else {
        panic!("the plan was not made");
    };
    let planned = plan_shard(cluster, &plan_id, "the plan to settle", |state, _| {
        state == "done"
    })
    .await;
    assert_eq!(
        planned.state, "reform",
        "a group with every broker reachable and no leader is re-formed: {planned:?}"
    );
    let plan = planned.plan.as_ref().expect("a reform shard has a plan");
    eprintln!("re-form plan: {plan:?}");
    let accept_loss = if plan.needs_accept_loss {
        vec![planned.key.clone()]
    } else {
        Vec::new()
    };
    let applied = reform(
        cluster,
        ReformRequest::Apply {
            plan_id: plan_id.clone(),
            accept_loss,
        },
    )
    .await;
    assert!(
        matches!(applied, ReformReply::Applied { applying: 1 }),
        "apply starts the shard's stop round: {applied:?}"
    );
    let done = plan_shard(cluster, &plan_id, "the re-form to start", |_, shard| {
        shard.apply.as_ref().is_some_and(|apply| {
            !matches!(apply.state.as_str(), "stopping" | "writing" | "starting")
        })
    })
    .await;
    let apply = done.apply.as_ref().expect("applied");
    assert_eq!(
        apply.state, "started",
        "the re-form did not start: {done:?}"
    );
    plan.voters
        .iter()
        .map(|(broker, _)| broker.clone())
        .collect()
}

/// Everything `node` replays, with offsets, read until every acknowledged
/// offset in `sent` has arrived.
async fn replay_with_offsets(cluster: &Cluster, node: &str, sent: &Sent) -> Vec<(u64, Vec<u8>)> {
    let want = sent.acked.iter().map(|(offset, _)| *offset).max();
    let deadline = Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
    let mut held: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut next = 0u64;
    loop {
        if want.is_none_or(|want| next > want) {
            return held;
        }
        assert!(
            Instant::now() < deadline,
            "{node} replayed up to {next} of {want:?}: {held:?}"
        );
        let Ok(client) = cluster.client_on(node).await else {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        };
        let Ok(mut subscription) = client
            .subscribe_from(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                Some(felix_client::StartPosition::Offset(next)),
            )
            .await
        else {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        };
        while let Ok(Ok(Some(event))) =
            tokio::time::timeout(Duration::from_secs(10), subscription.next_event()).await
        {
            let offset = event.offset.expect("a durable stream delivers offsets");
            held.push((offset, event.payload.to_vec()));
            next = offset + 1;
            if want.is_some_and(|want| next > want) {
                break;
            }
        }
    }
}

/// Every acknowledged record is held once, at the offset it was
/// acknowledged at, and no probe is held at all.
async fn holds_every_ack(cluster: &Cluster, node: &str, sent: &Sent, probes: &[Vec<u8>]) {
    let held = replay_with_offsets(cluster, node, sent).await;
    let text = |payload: &[u8]| String::from_utf8_lossy(payload).into_owned();
    for (offset, payload) in &sent.acked {
        assert!(
            held.contains(&(*offset, payload.clone())),
            "{node} does not hold {:?} at {offset}, where it was acknowledged; holds {:?}",
            text(payload),
            held.iter().map(|(o, p)| (*o, text(p))).collect::<Vec<_>>(),
        );
    }
    let payloads: Vec<Vec<u8>> = held.into_iter().map(|(_, payload)| payload).collect();
    if let Err(problem) = sent.check(node, &payloads) {
        panic!("{problem}");
    }
    for probe in probes {
        assert!(
            !payloads.contains(probe),
            "{node} holds {:?}, sent while the group had no majority",
            text(probe)
        );
    }
}

/// Publish [`RECORDS`] more through the group's new leader. Each is
/// acknowledged above every offset acknowledged before the loss, so none is
/// given out twice.
async fn publish_after(cluster: &Cluster, among: &[String], sent: &mut Sent) {
    let before = sent
        .acked
        .iter()
        .map(|(offset, _)| *offset)
        .max()
        .expect("records before the loss");
    let leader = leader_among(cluster, among).await;
    for n in 0..RECORDS {
        let (offset, payload) = publish_tried(cluster, &leader, &format!("after-{n}"), sent).await;
        assert!(
            offset > before,
            "{:?} was acknowledged at {offset}, an offset given out before the loss \
             (up to {before})",
            String::from_utf8_lossy(&payload)
        );
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create the copy");
    for entry in std::fs::read_dir(from).expect("read the directory") {
        let entry = entry.expect("entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy a file");
        }
    }
}

/// **Two of three relaxed brokers lose power, and the group re-forms from
/// the third with every acknowledged record.** Under `periodic` fsync a
/// `Quorum` write is acknowledged once a majority has written it, so a
/// follower that loses power may lose its unsynced tail. It comes back under
/// a new copy id and must not vote: if the two followers counted as the
/// voters they were, they would elect with the leader down and could cut
/// what it acknowledged. The leader's process is killed, which keeps the
/// page cache, so its copy holds every acknowledgement. The group stays
/// closed until `plan` and `apply` re-form it, then every voter of the new
/// epoch serves every record at its offset and no offset is given out again.
#[tokio::test]
#[serial]
async fn a_group_whose_relaxed_followers_lose_power_re_forms_with_every_ack() {
    if !opted_in() {
        return;
    }
    if !cfg!(target_os = "linux") {
        eprintln!("skipped: a simulated power loss needs Linux");
        return;
    }
    let mut cluster = Cluster::start(ClusterConfig {
        power_loss: true,
        broker_env: vec![
            engine("raft"),
            (
                "FELIX_DURABLE_FSYNC_MODE".to_string(),
                "periodic".to_string(),
            ),
        ],
        ..raft_cluster()
    })
    .await
    .expect("start cluster");
    let nodes = cluster.node_ids();
    voters_become(&cluster, &nodes, ELECTED_WITHIN).await;
    let leader = leader_among(&cluster, &nodes).await;
    let followers: Vec<String> = nodes.iter().filter(|n| **n != leader).cloned().collect();
    let mut sent = Sent::default();
    publish_before(&cluster, &leader, "before", &mut sent).await;

    let identities: Vec<String> = {
        let mut ids = Vec::new();
        for node in &followers {
            ids.push(registered_storage(&cluster, node).await);
        }
        ids
    };
    let cut: Vec<&str> = followers.iter().map(String::as_str).collect();
    cluster
        .power_off_nodes(0x2250, &cut)
        .await
        .expect("cut the followers' power");
    cluster.kill_node(&leader).expect("kill the leader");
    for (node, before) in followers.iter().zip(&identities) {
        cluster
            .restart_node(node)
            .await
            .expect("restart a follower");
        assert_ne!(
            &registered_storage(&cluster, node).await,
            before,
            "{node} lost power on a relaxed run, so its storage identity rotates"
        );
    }
    let mut probes = Vec::new();
    closed_for(
        &cluster,
        &followers,
        "with the leader down and its two voters back from a relaxed power cut",
        &mut probes,
    )
    .await;

    cluster
        .restart_node(&leader)
        .await
        .expect("restart the leader");
    closed_for(
        &cluster,
        &nodes,
        "with one voter of three left and no re-form",
        &mut probes,
    )
    .await;

    let voters = plan_and_apply(&cluster).await;
    publish_after(&cluster, &voters, &mut sent).await;
    for node in &voters {
        holds_every_ack(&cluster, node, &sent, &probes).await;
    }
    cluster.shutdown().await;
}

/// **A data directory put back from a copy never votes.** `b`'s directory
/// is copied while it is stopped, and `b` then takes records the copy does
/// not hold while `c` is down. With the leader down too, `b` is put back
/// to the copy (an older log and an older vote under the same copy id) and
/// started beside `c`, which also lacks those records. If the restored copy
/// voted, `b` and `c` would be a majority and elect without the records.
/// Neither leads and nothing is acknowledged until the leader returns, and
/// then every record is served at its offset.
#[tokio::test]
#[serial]
async fn a_restored_data_directory_never_votes() {
    if !opted_in() {
        return;
    }
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    voters_become(&cluster, &nodes, ELECTED_WITHIN).await;
    let leader = leader_among(&cluster, &nodes).await;
    let others: Vec<String> = nodes.iter().filter(|n| **n != leader).cloned().collect();
    let (b, c) = (others[0].clone(), others[1].clone());
    let mut sent = Sent::default();
    publish_before(&cluster, &leader, "copied", &mut sent).await;

    let storage = cluster.node(&b).expect("b").storage_dir();
    let backup = storage.with_extension("backup");
    cluster.stop_node(&b).await.expect("stop b");
    copy_dir(&storage, &backup);
    cluster.restart_node(&b).await.expect("restart b");
    voters_become(&cluster, &nodes, ELECTED_WITHIN).await;

    cluster.stop_node(&c).await.expect("stop c");
    for n in 0..RECORDS {
        publish_tried(&cluster, &leader, &format!("after-the-copy-{n}"), &mut sent).await;
    }
    // Past one interval of the root's log marks, so the copy is behind them.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let restored_from = registered_storage(&cluster, &b).await;
    cluster.kill_node(&leader).expect("kill the leader");
    cluster.stop_node(&b).await.expect("stop b");
    std::fs::remove_dir_all(&storage).expect("remove b's storage");
    std::fs::rename(&backup, &storage).expect("put b's copy back");
    cluster.restart_node(&b).await.expect("restart b");
    cluster.restart_node(&c).await.expect("restart c");
    assert_ne!(
        registered_storage(&cluster, &b).await,
        restored_from,
        "b's restored directory is behind what it promised, so its storage identity rotates"
    );
    let mut probes = Vec::new();
    closed_for(
        &cluster,
        &[b.clone(), c.clone()],
        "with the leader down, b restored and c behind",
        &mut probes,
    )
    .await;

    cluster
        .restart_node(&leader)
        .await
        .expect("restart the leader");
    for node in [&leader, &c] {
        holds_every_ack(&cluster, node, &sent, &probes).await;
    }
    publish_after(&cluster, &[leader.clone(), c.clone()], &mut sent).await;
    holds_every_ack(&cluster, &leader, &sent, &probes).await;
    cluster.shutdown().await;
}

/// **A two-copy group whose only other copy was wiped re-forms from the
/// copy that is left.** The wiped copy comes back under a new copy id and
/// cannot vote, so the copy left is one voter of two and the group cannot
/// open. Nothing re-places it on its own. `plan` and `apply` re-form it from
/// that copy alone, which then serves every acknowledged record at its
/// offset and gives out no offset twice.
#[tokio::test]
#[serial]
async fn a_group_whose_only_other_copy_was_lost_re_forms_from_the_copy_left() {
    if !opted_in() {
        return;
    }
    let mut cluster = Cluster::start(raft_cluster_with(StreamSpec::quorum(STREAM, 1, 2)))
        .await
        .expect("start cluster");
    let (placed, replicas, _) = stored_set(&cluster).await;
    let mut members = replicas.clone();
    members.push(placed);
    voters_become(&cluster, &members, ELECTED_WITHIN).await;
    let leader = leader_among(&cluster, &members).await;
    let lost = members
        .iter()
        .find(|node| **node != leader)
        .expect("a second copy")
        .clone();
    let mut sent = Sent::default();
    publish_before(&cluster, &leader, "before", &mut sent).await;

    let before = registered_storage(&cluster, &lost).await;
    cluster.stop_node(&lost).await.expect("stop the other copy");
    let storage = cluster.node(&lost).expect("lost").storage_dir();
    std::fs::remove_dir_all(&storage).expect("wipe the other copy");
    // Restarted so it does not keep leading a term it can no longer commit
    // in; a group with a leader is left alone by the plan.
    cluster.stop_node(&leader).await.expect("stop the leader");
    cluster
        .restart_node(&leader)
        .await
        .expect("restart the leader");
    cluster
        .restart_node(&lost)
        .await
        .expect("restart the other copy");
    assert_ne!(registered_storage(&cluster, &lost).await, before);
    let mut probes = Vec::new();
    closed_for(
        &cluster,
        &members,
        "with its only other copy wiped",
        &mut probes,
    )
    .await;

    let voters = plan_and_apply(&cluster).await;
    assert_eq!(
        voters,
        std::slice::from_ref(&leader),
        "the copy left is the new epoch's only voter; the wiped one rejoins as a learner"
    );
    publish_after(&cluster, &voters, &mut sent).await;
    holds_every_ack(&cluster, &leader, &sent, &probes).await;
    cluster.shutdown().await;
}
