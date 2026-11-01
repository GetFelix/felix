//! Joint fences: a leader whose set changed needs a majority of the set it
//! leaves as well as of the new one. `JointFence` in
//! `docs/formal/FelixShard.tla`.

use felix_storage::disk_log::{Member, Membership, ReplicaSet};

use super::*;

/// What every replica here offers: the fence and set checks.
fn electing() -> PeerCapabilities {
    PeerCapabilities::from_bits(REQUIRED.bits()).union(PeerCapabilities::ELECTION)
}

fn addr_of(node_id: &str) -> Option<SocketAddr> {
    let port = 7100 + node_id.bytes().last().map_or(0, u16::from);
    Some(SocketAddr::from(([10, 0, 0, 2], port)))
}

/// broker-a leading at `PROMOTED` on {broker-a, broker-x, broker-z}.
fn swapped() -> Route {
    Route {
        leader: node(LEADER, 7001),
        replicas: vec![node("broker-x", 7004), node("broker-z", 7005)],
        generation: PROMOTED,
        draining: false,
        successor: None,
        incarnation: 0,
        fence_storage_ids: None,
    }
}

/// Every replica `ids` names, offering `ELECTION`, beside broker-b and
/// broker-c.
fn replicas_with(ids: &[&str]) -> Replicas {
    let mut replicas = Replicas::new();
    replicas.electing = true;
    for id in ids {
        replicas.replicas.insert(id.to_string(), Replica::new(id));
    }
    for replica in replicas.replicas.values_mut() {
        replica.capabilities = electing();
    }
    replicas
}

/// The leader's log keeps `members` as the set of generation 4, with the
/// ones in `learners` listed but not voting.
async fn kept_before(leader: &Broker, members: &[&str], learners: &[&str]) {
    kept_naming(leader, members, learners, &[]).await;
}

/// [`kept_before`], naming the copy of each member in `copies`.
async fn kept_naming(
    leader: &Broker,
    members: &[&str],
    learners: &[&str],
    copies: &[(&str, StorageId)],
) {
    let log = leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.accept_generation(4, Some("broker-x"))
        .await
        .expect("ballot");
    let current = Membership {
        generation: 4,
        members: members
            .iter()
            .map(|node| Member {
                node: node.to_string(),
                storage: copies
                    .iter()
                    .find(|(named, _)| named == node)
                    .map(|(_, copy)| *copy),
                learner: learners.contains(node),
            })
            .collect(),
    };
    log.accept_set(ReplicaSet {
        current,
        leaving: None,
    })
    .await
    .expect("keep the old set");
}

async fn fence(replicas: &Replicas, leader: &Arc<Broker>, joint: bool) -> Outcome {
    let addr_of = addr_of;
    let joint = joint.then(|| Joint::new(&addr_of));
    fence_shard(
        replicas,
        leader,
        LEADER,
        &key(),
        &swapped(),
        false,
        joint.as_ref(),
    )
    .await
}

/// **A cut-over that swaps two members waits for a majority of the set it
/// leaves.** {broker-x, broker-b, broker-c} hands the shard to broker-a, a
/// staged copy, on {broker-a, broker-x, broker-z}: two members out, two in.
/// broker-x and broker-z answer, a majority of the new set, but only one of
/// the old set's three voters does, so the shard stays closed: broker-b and
/// broker-c could still elect and acknowledge on the old set. Once broker-b
/// answers it opens, and broker-b keeps the new set with the old one beside
/// it.
#[tokio::test]
async fn a_cut_over_swapping_two_members_waits_for_the_old_majority() {
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    replicas.get_mut("broker-b").reachable = false;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_before(
        &leader,
        &["broker-x", "broker-b", "broker-c", LEADER],
        &[LEADER],
    )
    .await;

    let outcome = fence(&replicas, &leader, true).await;

    let Outcome::Pending { why, .. } = &outcome else {
        panic!("opened on a majority of the new set alone: {outcome:?}");
    };
    assert!(why.contains("1 of 3 voters of the set being left"), "{why}");
    assert!(
        replicas
            .sent(Kind::ElectionFence)
            .contains(&"broker-x".into())
    );

    replicas.get_mut("broker-b").reachable = true;
    let outcome = fence(&replicas, &leader, true).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    let kept = replicas
        .get("broker-b")
        .broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .replica_set()
        .expect("broker-b kept the set");
    assert_eq!(kept.current.generation, PROMOTED);
    assert_eq!(kept.leaving.map(|old| old.generation), Some(4));
    let own = leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .replica_set()
        .expect("the leader kept its set");
    assert_eq!(own.current.generation, PROMOTED);
    assert_eq!(own.leaving.map(|old| old.generation), Some(4));
}

/// **A joint fence never counts a learner.** The set being left is
/// {broker-x, broker-b} with this broker as a learner. Counting it, broker-x's
/// answer would make two of three; only the voters count, and one of two is
/// not a majority.
#[tokio::test]
async fn a_joint_fence_never_counts_a_learner() {
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    replicas.get_mut("broker-b").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_before(&leader, &["broker-x", "broker-b", LEADER], &[LEADER]).await;

    let outcome = fence(&replicas, &leader, true).await;

    let Outcome::Pending { why, .. } = &outcome else {
        panic!("a learner counted toward the old set's majority: {outcome:?}");
    };
    assert!(why.contains("1 of 2 voters of the set being left"), "{why}");
}

/// **Without `joint_fence` the fence is the one it always was**, set change
/// or not: the new set's majority opens it, and nothing it sends names the
/// set being left.
#[tokio::test]
async fn without_joint_fence_a_set_change_fences_the_new_set_alone() {
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    replicas.get_mut("broker-b").reachable = false;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_before(&leader, &["broker-x", "broker-b", "broker-c"], &[]).await;

    let outcome = fence(&replicas, &leader, false).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    let mut sent = replicas.sent(Kind::ElectionFence);
    sent.sort();
    assert_eq!(sent, vec!["broker-x".to_string(), "broker-z".to_string()]);
    let kept = leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .replica_set()
        .expect("kept");
    assert_eq!(kept.leaving, None);
}

/// **A member known not to check sets turns the joint fence off**: the
/// shard is fenced on its new set as it always was.
#[tokio::test]
async fn a_member_without_election_fences_the_new_set_alone() {
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    replicas.get_mut("broker-b").reachable = false;
    replicas.get_mut("broker-c").capabilities = REQUIRED;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_before(&leader, &["broker-x", "broker-b", "broker-c"], &[]).await;

    let outcome = fence(&replicas, &leader, true).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    assert!(
        replicas
            .sent(Kind::ElectionFence)
            .iter()
            .all(|node| node != "broker-c")
    );
}

/// **An answer counts toward the set being left only from the copy that set
/// names.** broker-b answers under another storage id, as a node id given to
/// a new, empty disk would: it holds nothing the old set acknowledged, so with
/// broker-c down the old set has no majority. From the copy it names, it does.
#[tokio::test]
async fn a_joint_fence_counts_the_old_set_only_from_the_copies_it_names() {
    for (copy, opens) in [
        (storage_of("broker-other"), false),
        (storage_of("broker-b"), true),
    ] {
        let mut replicas = replicas_with(&["broker-x", "broker-z"]);
        replicas.get_mut("broker-c").reachable = false;
        let (leader, _dir) = leader_holding(4, &["a"]).await;
        kept_naming(
            &leader,
            &["broker-x", "broker-b", "broker-c"],
            &[],
            &[("broker-b", copy)],
        )
        .await;

        let outcome = fence(&replicas, &leader, true).await;

        assert_eq!(
            matches!(outcome, Outcome::Fenced { .. }),
            opens,
            "{outcome:?}"
        );
    }
}
