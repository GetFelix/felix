//! Joint fences: a leader whose set changed needs a majority of the set it
//! leaves as well as of the new one. `JointFence` in
//! `docs/formal/FelixShard.tla`.

use felix_router::{PlacedSet, PriorSet};
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
        member_storage_ids: None,
        pending: None,
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
    fence_after(replicas, leader, joint, None).await
}

/// [`fence`], with what the watch saw right before the route.
async fn fence_after(
    replicas: &Replicas,
    leader: &Arc<Broker>,
    joint: bool,
    prior: Option<PriorSet>,
) -> Outcome {
    let addr_of = addr_of;
    let joint = joint.then(|| Joint::new(&addr_of, prior));
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
    kept_before(
        &leader,
        &["broker-x", "broker-b", "broker-c"],
        &["broker-c"],
    )
    .await;

    let outcome = fence(&replicas, &leader, true).await;

    let Outcome::Pending { why, .. } = &outcome else {
        panic!("a learner counted toward the old set's majority: {outcome:?}");
    };
    assert!(why.contains("1 of 2 voters of the set being left"), "{why}");
}

/// [`fence`] under `joint_fence` on a broker whose directory holds `own`.
async fn fence_holding(replicas: &Replicas, leader: &Arc<Broker>, own: StorageId) -> Outcome {
    let addr_of = addr_of;
    let joint = Joint::new(&addr_of, None).with_local_storage(Some(own));
    fence_shard(
        replicas,
        leader,
        LEADER,
        &key(),
        &swapped(),
        false,
        Some(&joint),
    )
    .await
}

/// **A wiped copy its kept set marks a learner does not open as leader.**
/// The control plane named it leader, but the set it holds names another
/// copy for it and it has not caught up (`~PromoteNonVoter` in
/// `docs/formal/FelixShard.tla`). It refuses before writing anything, and the
/// mark stays: opening would have kept the route's set over it and made it a
/// voter again. A staged destination, a learner that holds the copy its set
/// names, still leads.
#[tokio::test]
async fn a_learner_named_leader_refuses_and_keeps_its_mark() {
    let replicas = replicas_with(&["broker-x", "broker-z"]);
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    let old = StorageId::from_bytes([3; 16]);
    let own = StorageId::from_bytes([7; 16]);
    let members = ["broker-x", "broker-z", LEADER];
    kept_naming(&leader, &members, &[LEADER], &[(LEADER, old)]).await;
    let log = leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    let accepted = log.accepted_generation();

    let outcome = fence_holding(&replicas, &leader, own).await;

    let Outcome::Pending { why, .. } = &outcome else {
        panic!("a wiped learner opened as leader: {outcome:?}");
    };
    assert!(why.contains("learner"), "{why}");
    assert!(replicas.sent(Kind::ElectionFence).is_empty(), "fenced");
    assert_eq!(log.member_is_learner(LEADER), Some(true), "mark cleared");
    assert_eq!(log.set_generation(), 4, "wrote a set");
    assert_eq!(log.accepted_generation(), accepted, "took a ballot");

    // A staged destination holds the copy the set names.
    let (staged, _dir) = leader_holding(4, &["a"]).await;
    kept_naming(&staged, &members, &[LEADER], &[(LEADER, own)]).await;
    let outcome = fence_holding(&replicas, &staged, own).await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
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

/// **A member that does not check sets counts as not answering.** broker-c
/// lacks `ELECTION`, so it is not asked; with broker-b down, broker-x alone
/// is not a majority of the set being left, and the shard stays closed
/// rather than fall back to a fence on the new set.
#[tokio::test]
async fn a_member_without_election_counts_as_not_answering() {
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    replicas.get_mut("broker-b").reachable = false;
    replicas.get_mut("broker-c").capabilities = REQUIRED;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_before(&leader, &["broker-x", "broker-b", "broker-c"], &[]).await;

    let outcome = fence(&replicas, &leader, true).await;

    let Outcome::Pending { why, .. } = &outcome else {
        panic!("opened without a majority of the set being left: {outcome:?}");
    };
    assert!(why.contains("1 of 3 voters of the set being left"), "{why}");
    assert!(
        replicas
            .sent(Kind::ElectionFence)
            .iter()
            .all(|node| node != "broker-c")
    );
}

/// The control plane's set at generation 4, {broker-x, broker-b, broker-c},
/// as the watch saw it right before the route at `PROMOTED`.
fn prior_xbc() -> PriorSet {
    PriorSet {
        generation: PROMOTED,
        before: Some(Arc::new(PlacedSet {
            leader: "broker-x".into(),
            replicas: vec!["broker-b".into(), "broker-c".into()],
            successor: None,
            storage_ids: Default::default(),
            member_storage_ids: Default::default(),
        })),
    }
}

/// **The set being left is the control plane's, not whatever the leader's
/// log kept.** broker-a never got a batch stamped with the set it leaves, so
/// its log keeps none, but the watch saw the assignment at generation 4. With
/// broker-b and broker-c down the shard stays closed; once broker-b answers
/// it opens, keeping the old set beside its own.
#[tokio::test]
async fn a_leader_whose_log_kept_no_set_still_fences_the_old_majority() {
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    replicas.get_mut("broker-b").reachable = false;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;

    let outcome = fence_after(&replicas, &leader, true, Some(prior_xbc())).await;

    let Outcome::Pending { why, .. } = &outcome else {
        panic!("opened on a majority of the new set alone: {outcome:?}");
    };
    assert!(why.contains("1 of 3 voters of the set being left"), "{why}");

    replicas.get_mut("broker-b").reachable = true;
    let outcome = fence_after(&replicas, &leader, true, Some(prior_xbc())).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    let own = leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .replica_set()
        .expect("the leader kept its set");
    assert_eq!(own.leaving.map(|old| old.generation), Some(4));
}

/// **A set before that is not known keeps the shard closed.** Neither the
/// watch nor the leader's log knows the set at generation 4, so the fence
/// does not fall back to the new set alone. A first assignment, with nothing
/// before it, fences the new set.
#[tokio::test]
async fn an_unknown_set_before_keeps_the_fence_pending() {
    let replicas = replicas_with(&["broker-x", "broker-z"]);
    let (leader, _dir) = leader_holding(4, &["a"]).await;

    let outcome = fence_after(&replicas, &leader, true, None).await;

    let Outcome::Pending { why, .. } = &outcome else {
        panic!("fenced the new set alone: {outcome:?}");
    };
    assert!(why.contains("is not known here"), "{why}");

    let first = PriorSet {
        generation: PROMOTED,
        before: None,
    };
    let outcome = fence_after(&replicas, &leader, true, Some(first)).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
}

/// **A set kept at the route's generation without the one it leaves is not
/// taken as no change.** A ship pass that read `joint_fence` as off kept the
/// new set alone; the fence, reading it on, takes the set being left from the
/// control plane's record, and without that record stays pending.
#[tokio::test]
async fn a_set_kept_without_the_one_it_leaves_is_not_read_as_no_change() {
    for prior in [Some(prior_xbc()), None] {
        let mut replicas = replicas_with(&["broker-x", "broker-z"]);
        replicas.get_mut("broker-b").reachable = false;
        replicas.get_mut("broker-c").reachable = false;
        let (leader, _dir) = leader_holding(4, &["a"]).await;
        let log = leader
            .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("log");
        log.accept_generation(PROMOTED, Some(LEADER))
            .await
            .expect("ballot");
        log.accept_set(crate::replica_set::kept(&swapped(), None))
            .await
            .expect("keep the new set alone");

        let outcome = fence_after(&replicas, &leader, true, prior.clone()).await;

        let Outcome::Pending { why, .. } = &outcome else {
            panic!("read a set kept alone as no change: {outcome:?}");
        };
        let expected = if prior.is_some() {
            "1 of 3 voters of the set being left"
        } else {
            "is not known here"
        };
        assert!(why.contains(expected), "{why}");
    }
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

/// **A voter of the set being left answers a joint fence even once its
/// routing view no longer names it.** broker-b has already seen the new
/// route, which leaves it out; it still takes the fence, so the old set's
/// majority can be reached.
#[tokio::test]
async fn a_member_the_new_route_leaves_out_still_answers_a_joint_fence() {
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    replicas.get_mut("broker-c").reachable = false;
    let router = Arc::new(ShardRouter::new(
        "broker-b",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    router.publish(
        RoutingTable::build(
            [(
                key(),
                LEADER.to_string(),
                vec!["broker-x".to_string(), "broker-z".to_string()],
                PROMOTED,
            )],
            &nodes(),
        ),
        &nodes(),
    );
    // broker-b holds a promise, as a voter that took the leader's batches
    // does: under `joint_fence` one with none and no confirmed read is refused.
    kept_before(
        &replicas.get_mut("broker-b").broker,
        &["broker-x", "broker-b", "broker-c"],
        &[],
    )
    .await;
    // Only once its fleet has finalized `joint_fence`: before that a broker
    // no route names refuses as it always did.
    for finalized in [false, true] {
        let b = replicas.get_mut("broker-b");
        b.handler = ReplicaHandler::new(Arc::clone(&b.broker), Arc::clone(&router))
            .with_fleet(Arc::new(joint_fence_marks(finalized)));
        let (leader, _dir) = leader_holding(4, &["a"]).await;
        kept_before(&leader, &["broker-x", "broker-b", "broker-c"], &[]).await;

        let outcome = fence(&replicas, &leader, true).await;

        assert_eq!(
            matches!(outcome, Outcome::Fenced { .. }),
            finalized,
            "{outcome:?}"
        );
    }
}

/// Marks for a fleet that has finalized `joint_fence`, or has not.
fn joint_fence_marks(finalized: bool) -> crate::quorum::QuorumMarks {
    use felix_common::fleet::{FleetGate, JOINT_FENCE};
    let fleet = FleetGate::new([JOINT_FENCE.name()]);
    // As a registration answers, so the gate is read.
    fleet.restart(finalized.then_some(JOINT_FENCE.name()));
    crate::quorum::QuorumMarks::with_fleet(Arc::new(fleet))
}
