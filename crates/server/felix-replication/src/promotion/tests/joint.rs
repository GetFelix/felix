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
    kept_marked(leader, members, learners, copies, &[]).await;
}

/// [`kept_naming`], with the members in `wiped` marked wiped.
async fn kept_marked(
    leader: &Broker,
    members: &[&str],
    learners: &[&str],
    copies: &[(&str, StorageId)],
    wiped: &[&str],
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
                wiped: wiped.contains(node),
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

/// **A joint fence the set being left promised away is refused.** The new
/// set {broker-a, broker-x, broker-z} answers, a majority, but broker-b and
/// broker-c, two of the old set's three voters, promised the generation to
/// broker-d. Only broker-x is left of the old set, so no retry reaches its
/// majority and the shard says who holds the promise instead of staying
/// pending. Without the old set's count, the new set's answers alone opened.
#[tokio::test]
async fn a_joint_fence_the_set_being_left_promised_away_is_refused() {
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    replicas.ballots = true;
    for id in ["broker-b", "broker-c"] {
        replicas.get(id).holds(id, 4, 0, &["a"]).await;
        promised_to(&replicas, id, "broker-d").await;
    }
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_before(&leader, &["broker-x", "broker-b", "broker-c"], &[]).await;

    let outcome = fence(&replicas, &leader, true).await;

    let Outcome::Refused { promised_to, .. } = outcome else {
        panic!("opened on a majority of the new set alone: {outcome:?}");
    };
    assert_eq!(promised_to, "broker-d");
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

/// **A copy marked wiped does not lead, though its kept set names its own
/// storage id** as a staged destination's does: a replaced directory's copy
/// learns its first set under the new id. Nor does a copy with no set past
/// creation on a replaced directory (`~PromoteNonVoter` in
/// `docs/formal/FelixShard.tla`).
#[tokio::test]
async fn a_copy_marked_on_the_replaced_marker_does_not_lead() {
    let replicas = replicas_with(&["broker-x", "broker-z"]);
    let own = StorageId::from_bytes([7; 16]);
    let members = ["broker-x", "broker-z", LEADER];
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_marked(&leader, &members, &[LEADER], &[(LEADER, own)], &[LEADER]).await;
    let outcome = fence_holding(&replicas, &leader, own).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("a wiped learner opened as leader: {outcome:?}");
    };
    assert!(why.contains("learner"), "{why}");
    assert!(replicas.sent(Kind::ElectionFence).is_empty(), "fenced");

    let (setless, _dir) = leader_holding(4, &["a"]).await;
    let addr_of = addr_of;
    let joint = Joint::new(&addr_of, None)
        .with_local_storage(Some(own))
        .with_replaced_storage(true);
    let outcome = fence_shard(
        &replicas,
        &setless,
        LEADER,
        &key(),
        &swapped(),
        false,
        Some(&joint),
    )
    .await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("a copy with no set on a replaced directory led: {outcome:?}");
    };
    assert!(why.contains("learner"), "{why}");
    assert!(replicas.sent(Kind::ElectionFence).is_empty(), "fenced");
}

/// **A copy with no set on a replaced directory leads generation 1 only when
/// the route names its storage id**: then the shard was created after the
/// wipe. A route naming no ids may be a shard older than member ids, which
/// this copy may have held and lost.
#[tokio::test]
async fn a_setless_replaced_copy_leads_creation_only_under_its_own_id() {
    let replicas = replicas_with(&["broker-x", "broker-z"]);
    let own = StorageId::from_bytes([7; 16]);
    let addr_of = addr_of;
    let joint = Joint::new(&addr_of, None)
        .with_local_storage(Some(own))
        .with_replaced_storage(true);
    let created = |ids: Option<&[(&str, StorageId)]>| Route {
        generation: 1,
        member_storage_ids: ids.map(|ids| {
            Arc::new(
                ids.iter()
                    .map(|(node, id)| (node.to_string(), id.to_hex()))
                    .collect(),
            )
        }),
        ..swapped()
    };
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = broker_on(dir.path());
    let fence_at = |route: Route| {
        let (replicas, leader, joint) = (&replicas, &leader, &joint);
        async move { fence_shard(replicas, leader, LEADER, &key(), &route, false, Some(joint)).await }
    };

    let outcome = fence_at(created(None)).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("a copy with no set led an id-less creation: {outcome:?}");
    };
    assert!(why.contains("learner"), "{why}");
    assert!(replicas.sent(Kind::ElectionFence).is_empty(), "fenced");

    let ids = [
        (LEADER, own),
        ("broker-x", storage_of("broker-x")),
        ("broker-z", storage_of("broker-z")),
    ];
    let outcome = fence_at(created(Some(&ids))).await;
    assert!(
        !matches!(&outcome, Outcome::Pending { why, .. } if why.contains("learner")),
        "{outcome:?}"
    );
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

/// **A failover after a recorded-only adoption fences the recorded set and
/// the log's.** The control plane recorded an election at generation 4
/// without taking its set, so the watch saw {broker-x, broker-b, broker-c}
/// while broker-a's log keeps the elected leader's {broker-a, broker-y,
/// broker-w}. With broker-x down the two have disjoint majorities, and the
/// elected leader may have acknowledged on either, so the fence opens only
/// on a majority of each. A voter of the log's set alone is sent that set as
/// the one being left, or it would refuse the fence.
#[tokio::test]
async fn a_failover_fences_both_the_recorded_set_and_the_logs() {
    let mut replicas = replicas_with(&["broker-x", "broker-z", "broker-y", "broker-w"]);
    replicas.get_mut("broker-x").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_before(&leader, &[LEADER, "broker-y", "broker-w"], &[]).await;
    let reach = |replicas: &mut Replicas, up: &[&str]| {
        for id in ["broker-b", "broker-c", "broker-y", "broker-w"] {
            replicas.get_mut(id).reachable = up.contains(&id);
        }
    };

    reach(&mut replicas, &["broker-b", "broker-c"]);
    let outcome = fence_after(&replicas, &leader, true, Some(prior_xbc())).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("opened on a majority of the recorded set alone: {outcome:?}");
    };
    assert!(why.contains("2 of 3 and 1 of 3 voters"), "{why}");

    reach(&mut replicas, &["broker-y", "broker-w"]);
    let outcome = fence_after(&replicas, &leader, true, Some(prior_xbc())).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("opened on a majority of the log's set alone: {outcome:?}");
    };
    assert!(why.contains("0 of 3 and 3 of 3 voters"), "{why}");

    reach(&mut replicas, &["broker-b", "broker-c", "broker-y"]);
    let outcome = fence_after(&replicas, &leader, true, Some(prior_xbc())).await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    for (id, leaving) in [
        ("broker-b", ["broker-b", "broker-c", "broker-x"]),
        ("broker-y", [LEADER, "broker-w", "broker-y"]),
    ] {
        let kept = replicas
            .get(id)
            .broker
            .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("log")
            .replica_set()
            .expect("the replica kept the set");
        let mut kept: Vec<String> = kept
            .leaving
            .expect("the set being left rides the fence")
            .members
            .into_iter()
            .map(|member| member.node)
            .collect();
        kept.sort();
        assert_eq!(kept, leaving, "{id}");
    }
}

/// **Without a watch prior, a failover fences the set the log kept as
/// leaving.** broker-x led generation 4 on {broker-a, broker-x, broker-z} as a
/// change pending from {broker-x, broker-a, broker-b}, the control plane's
/// set, and broker-a's log keeps both. broker-x and broker-b acknowledged
/// records at generation 3 that broker-z does not hold yet. broker-a's watch
/// never saw generation 4, so its own set at 4 is all it has as the set
/// before: with broker-x and broker-b down, broker-a and broker-z alone must
/// not open the shard.
#[tokio::test]
async fn a_failover_without_a_prior_fences_the_set_the_log_kept_as_leaving() {
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    replicas.get_mut("broker-x").reachable = false;
    replicas.get_mut("broker-b").reachable = false;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    let log = leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.accept_generation(4, Some("broker-x"))
        .await
        .expect("ballot");
    let set = |generation: u64, members: &[&str]| Membership {
        generation,
        members: members
            .iter()
            .map(|node| Member {
                node: node.to_string(),
                storage: None,
                learner: false,
                wiped: false,
            })
            .collect(),
    };
    log.accept_set(ReplicaSet {
        current: set(4, &[LEADER, "broker-x", "broker-z"]),
        leaving: Some(set(3, &["broker-x", LEADER, "broker-b"])),
    })
    .await
    .expect("keep the pending set and the one it leaves");

    let outcome = fence_after(&replicas, &leader, true, None).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("opened without the set the log kept as leaving: {outcome:?}");
    };
    assert!(why.contains("2 of 3 and 1 of 3 voters"), "{why}");

    replicas.get_mut("broker-b").reachable = true;
    let outcome = fence_after(&replicas, &leader, true, None).await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
}

/// **A pending cut-over leaves the control plane's set, whatever the log
/// kept.** The control plane recorded an election at generation 4 without
/// taking it, so its set stayed {broker-a, broker-x, broker-b, broker-c}
/// while the elected leader shipped {broker-a, broker-x, broker-z}, which
/// broker-a's log keeps. Its watch never saw the step to the cut-over. The
/// route's pending change still names the set being left: with broker-b and
/// broker-c down the shard stays closed, and the fence carries the recorded
/// set.
#[tokio::test]
async fn a_pending_cut_over_leaves_the_recorded_set_whatever_the_log_kept() {
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    replicas.get_mut("broker-b").reachable = false;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_before(&leader, &[LEADER, "broker-x", "broker-z"], &[]).await;

    let outcome = fence_pending(&replicas, &leader).await;

    let Outcome::Pending { why, .. } = &outcome else {
        panic!("left the set the log kept, not the recorded one: {outcome:?}");
    };
    assert!(why.contains("2 of 4 voters of the set being left"), "{why}");

    replicas.get_mut("broker-b").reachable = true;
    let outcome = fence_pending(&replicas, &leader).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    let kept = replicas
        .get("broker-b")
        .broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .replica_set()
        .expect("broker-b kept the set");
    let leaving: Vec<String> = kept
        .leaving
        .expect("the set being left rides the fence")
        .members
        .into_iter()
        .map(|member| member.node)
        .collect();
    assert_eq!(leaving, [LEADER, "broker-b", "broker-c", "broker-x"]);
}

/// **With no source naming the recorded members, the route's copies are
/// kept.** The set being left still counts each member only from the copy
/// the control plane registered for it, not from any copy of that node.
#[test]
fn a_recorded_set_no_source_names_takes_the_routes_copies() {
    let copy = StorageId::from_bytes([7; 16]);
    let route = Route {
        pending: Some(Arc::new(felix_router::PendingChange {
            generation: PROMOTED,
            leaving: vec![LEADER.into(), "broker-b".into()],
        })),
        member_storage_ids: Some(Arc::new([("broker-b".to_string(), copy.to_hex())].into())),
        ..swapped()
    };

    let before = crate::promotion::set_before(None, &route, None)
        .expect("the recorded set")
        .expect("a set before");

    assert_eq!(before.generation, PROMOTED - 1);
    let named: Vec<_> = before
        .members
        .iter()
        .map(|member| (member.node.as_str(), member.storage))
        .collect();
    assert_eq!(named, [(LEADER, None), ("broker-b", Some(copy))]);
}

/// [`fence`] on [`swapped`] as a cut-over the control plane holds pending,
/// leaving {broker-a, broker-x, broker-b, broker-c}, with no step seen by the
/// watch.
async fn fence_pending(replicas: &Replicas, leader: &Arc<Broker>) -> Outcome {
    let route = Route {
        pending: Some(Arc::new(felix_router::PendingChange {
            generation: PROMOTED,
            leaving: vec![
                LEADER.into(),
                "broker-x".into(),
                "broker-b".into(),
                "broker-c".into(),
            ],
        })),
        ..swapped()
    };
    let addr_of = addr_of;
    let joint = Joint::new(&addr_of, None);
    fence_shard(
        replicas,
        leader,
        LEADER,
        &key(),
        &route,
        false,
        Some(&joint),
    )
    .await
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

/// **The same node list under another storage id is a set change.**
/// {broker-a, broker-x, broker-z} is kept with broker-x's old copy; the new
/// set names the copy broker-x answers as. With broker-z down, broker-x's
/// answer is a majority of the new set, but it is not the copy the old set
/// named, so the old set has only the leader and the fence stays closed. Once
/// broker-z answers it opens. Without the check the same node lists were no
/// change and broker-x alone opened the fence (#1737).
#[tokio::test]
async fn a_member_named_under_another_storage_id_is_a_set_change() {
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    replicas.get_mut("broker-z").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_naming(
        &leader,
        &[LEADER, "broker-x", "broker-z"],
        &[],
        &[("broker-x", storage_of("broker-x-before-the-wipe"))],
    )
    .await;
    let route = Route {
        member_storage_ids: Some(Arc::new(
            [("broker-x".to_string(), storage_of("broker-x").to_hex())].into(),
        )),
        ..swapped()
    };
    let addr_of = addr_of;
    let joint = Joint::new(&addr_of, None);

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key(),
        &route,
        false,
        Some(&joint),
    )
    .await;

    let Outcome::Pending { why, .. } = &outcome else {
        panic!("a wiped member's answer opened the old set: {outcome:?}");
    };
    assert!(why.contains("1 of 3 voters of the set being left"), "{why}");

    replicas.get_mut("broker-z").reachable = true;
    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key(),
        &route,
        false,
        Some(&joint),
    )
    .await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
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

/// **A candidate fences the set kept beside its own too.** A move's cut-over
/// leaves {broker-a, broker-b, broker-c} at 3 beside the same members at 2,
/// where broker-b was the staged successor and so a learner. broker-b's
/// answer is a majority of the new set, but of the old set's voters only
/// this broker has the fence, so the candidate does not open until broker-c
/// answers. The fence carries the set being left.
#[tokio::test]
async fn a_candidate_fences_the_set_a_move_left_jointly() {
    let set = |generation, learner: Option<&str>| Membership {
        generation,
        members: [LEADER, "broker-b", "broker-c"]
            .map(|node| Member {
                node: node.to_string(),
                storage: None,
                learner: learner == Some(node),
                wiped: false,
            })
            .to_vec(),
    };
    let candidacy = crate::election::Candidacy {
        set: crate::promotion::joint::view(&set(3, None)),
        leaving: Some(set(2, Some("broker-b"))),
    };
    let mut replicas = replicas_with(&[]);
    replicas.get_mut("broker-c").reachable = false;
    let addr_of = addr_of;
    let joint = Joint::new(&addr_of, None);
    let stand = async |replicas: &Replicas| {
        let (leader, dir) = leader_holding(4, &["a"]).await;
        let outcome = fence_shard_as(
            replicas,
            &leader,
            LEADER,
            &key(),
            &route(),
            false,
            Some(&joint),
            Some(&candidacy),
        )
        .await;
        (outcome, dir)
    };

    let (outcome, _dir) = stand(&replicas).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("opened without a majority of the set being left: {outcome:?}");
    };
    assert!(why.contains("1 of 2 voters of the set being left"), "{why}");

    replicas.get_mut("broker-c").reachable = true;
    let (outcome, _dir) = stand(&replicas).await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    let kept = replicas
        .get("broker-c")
        .broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .replica_set()
        .expect("broker-c kept the set");
    assert_eq!(kept.current.generation, 3);
    assert_eq!(kept.leaving.map(|old| old.generation), Some(2));
}

/// **The control plane reissuing an elected leader fences the set it was
/// elected on.** broker-a stood at generation 4 on {broker-a, broker-y,
/// broker-w}, the set its log keeps, while the control plane, unseen, named
/// generation 4 itself on {broker-a, broker-x, broker-b}. After that
/// collision the control plane names broker-a again at 5 on its own set, so
/// the route and the set before it are the same. A fence of that set alone
/// would leave broker-y and broker-w, a majority of the elected set, free to
/// elect at 5 beside it. With both down the shard stays closed; once broker-y
/// answers it opens.
#[tokio::test]
async fn a_reissued_elected_leader_fences_the_set_it_was_elected_on() {
    let mut replicas = replicas_with(&["broker-x", "broker-y", "broker-w"]);
    replicas.get_mut("broker-c").reachable = false;
    replicas.get_mut("broker-y").reachable = false;
    replicas.get_mut("broker-w").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_before(&leader, &[LEADER, "broker-y", "broker-w"], &[]).await;
    let reissued = Route {
        replicas: vec![node("broker-x", 7004), node("broker-b", 7002)],
        ..swapped()
    };
    let prior = PriorSet {
        generation: PROMOTED,
        before: Some(Arc::new(PlacedSet {
            leader: LEADER.into(),
            replicas: vec!["broker-x".into(), "broker-b".into()],
            successor: None,
            storage_ids: Default::default(),
            member_storage_ids: Default::default(),
        })),
    };
    let addr_of = addr_of;
    let joint = Joint::new(&addr_of, Some(prior));
    let fence = async |replicas: &Replicas| {
        fence_shard(
            replicas,
            &leader,
            LEADER,
            &key(),
            &reissued,
            false,
            Some(&joint),
        )
        .await
    };

    let outcome = fence(&replicas).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("opened without a majority of the elected set: {outcome:?}");
    };
    assert!(why.contains("1 of 3 voters"), "{why}");

    replicas.get_mut("broker-y").reachable = true;
    let outcome = fence(&replicas).await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
}

/// **A log set that differs from the set before only in who votes is fenced
/// beside it.** The log keeps broker-y and broker-w as learners where the
/// set before has them voting. broker-a, broker-y and broker-w are a
/// majority of the set before, of its names and of the new set, yet miss
/// every voter majority of the log's set, which broker-b and broker-c may
/// have acknowledged on.
#[test]
fn a_log_set_differing_only_in_learners_is_fenced_beside_the_one_before() {
    let set = |learners: &[&str]| Membership {
        generation: 4,
        members: [LEADER, "broker-b", "broker-c", "broker-y", "broker-w"]
            .map(|node| Member {
                node: node.to_string(),
                storage: None,
                learner: learners.contains(&node),
                wiped: false,
            })
            .to_vec(),
    };
    let stored = ReplicaSet {
        current: set(&["broker-y", "broker-w"]),
        leaving: None,
    };

    let besides =
        crate::promotion::joint::logged_besides(Some(&stored), &swapped(), &set(&[]), None);

    assert_eq!(besides, Some(stored.current));
}

/// **A log set naming another copy of a member is fenced beside the set
/// before, and each counts only the copy it names.** The watch saw
/// {broker-x, broker-b, broker-c} with broker-b's current copy, while
/// broker-a's log keeps the same names with an older copy of broker-b, which
/// may have acknowledged with broker-c. With broker-c down, broker-x and
/// broker-b's current copy are a majority of the set before but hold none of
/// the log set's records, so the fence stays closed until broker-c answers.
#[tokio::test]
async fn a_log_set_naming_another_copy_is_fenced_beside_the_one_before() {
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_naming(
        &leader,
        &["broker-x", "broker-b", "broker-c"],
        &[],
        &[("broker-b", storage_of("broker-old"))],
    )
    .await;
    let mut prior = prior_xbc();
    if let Some(before) = prior.before.as_mut() {
        Arc::make_mut(before).member_storage_ids =
            [("broker-b".to_string(), storage_of("broker-b").to_hex())].into();
    }

    replicas.get_mut("broker-c").reachable = false;
    let outcome = fence_after(&replicas, &leader, true, Some(prior.clone())).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("opened without a voter majority of the log's copies: {outcome:?}");
    };
    assert!(why.contains("2 of 3 and 1 of 3 voters"), "{why}");

    replicas.get_mut("broker-c").reachable = true;
    let outcome = fence_after(&replicas, &leader, true, Some(prior)).await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
}

/// **A promise away is not refused while a set is being left.** broker-b
/// promised the generation to broker-d and broker-c is down, so the old set
/// holds only broker-x's answer. A rename resets the copies the next joint
/// fence checks, so the fence waits instead of reporting a refusal.
#[tokio::test]
async fn a_joint_fence_with_one_promise_away_and_one_silent_stays_pending() {
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    replicas.ballots = true;
    replicas
        .get("broker-b")
        .holds("broker-b", 4, 0, &["a"])
        .await;
    promised_to(&replicas, "broker-b", "broker-d").await;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_before(&leader, &["broker-x", "broker-b", "broker-c"], &[]).await;

    let outcome = fence(&replicas, &leader, true).await;

    assert!(matches!(outcome, Outcome::Pending { .. }), "{outcome:?}");
}

/// broker-a's log keeps {broker-x, broker-b, broker-c} at generation 4 with
/// {broker-a, broker-y, broker-w}, the set its fence was leaving, beside it.
async fn kept_leaving_ayw(leader: &Broker) {
    let log = leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.accept_generation(4, Some("broker-x"))
        .await
        .expect("ballot");
    let set = |generation, members: [&str; 3]| Membership {
        generation,
        members: members
            .map(|node| Member {
                node: node.to_string(),
                storage: None,
                learner: false,
                wiped: false,
            })
            .to_vec(),
    };
    log.accept_set(ReplicaSet {
        current: set(4, ["broker-x", "broker-b", "broker-c"]),
        leaving: Some(set(3, [LEADER, "broker-y", "broker-w"])),
    })
    .await
    .expect("keep the fence's sets");
}

/// **A failover fences the set an unopened fence was still leaving.**
/// broker-a answered broker-x's joint fence at generation 4, so its log
/// keeps {broker-x, broker-b, broker-c} with {broker-a, broker-y, broker-w}
/// beside it, but holds nothing written at 4: that fence may never have
/// opened, and broker-y may hold a write acknowledged at 3 that no member of
/// the newer sets has. A majority of the new set and of the one at 4 is not
/// enough; the fence waits for broker-y, and sends it the set it votes in.
#[tokio::test]
async fn a_failover_fences_the_set_an_unopened_fence_was_leaving() {
    let mut replicas = replicas_with(&["broker-x", "broker-z", "broker-y", "broker-w"]);
    replicas.get_mut("broker-x").reachable = false;
    replicas.get_mut("broker-y").reachable = false;
    replicas.get_mut("broker-w").reachable = false;
    let (leader, _dir) = leader_holding(3, &["a"]).await;
    kept_leaving_ayw(&leader).await;

    let outcome = fence_after(&replicas, &leader, true, Some(prior_xbc())).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("opened without a majority of the set still being left: {outcome:?}");
    };
    assert!(why.contains("2 of 3 and 1 of 3 voters"), "{why}");

    replicas.get_mut("broker-y").reachable = true;
    let outcome = fence_after(&replicas, &leader, true, Some(prior_xbc())).await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    let kept = replicas
        .get("broker-y")
        .broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .replica_set()
        .expect("broker-y kept the fence's set");
    assert_eq!(kept.leaving.map(|old| old.generation), Some(3));
}

/// **A log holding a record of its set's generation drops the set that
/// fence left.** The same log as above, but holding a record written at 4,
/// which its leader wrote only once its fence opened on a majority of both
/// sets. The failover needs no answer from the set left at 3.
#[tokio::test]
async fn a_failover_after_an_opened_fence_does_not_fence_what_it_left() {
    let mut replicas = replicas_with(&["broker-x", "broker-z", "broker-y", "broker-w"]);
    replicas.get_mut("broker-x").reachable = false;
    replicas.get_mut("broker-y").reachable = false;
    replicas.get_mut("broker-w").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_leaving_ayw(&leader).await;

    let outcome = fence_after(&replicas, &leader, true, Some(prior_xbc())).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
}

/// **A generation recorded with nothing written at it is no evidence it
/// opened.** A leader records its term before it writes the start record, so
/// an epoch at 4 starting at the tail says nothing about the fence. The
/// failover still waits for the set left at 3.
#[tokio::test]
async fn a_generation_recorded_without_records_still_fences_what_it_left() {
    let mut replicas = replicas_with(&["broker-x", "broker-z", "broker-y", "broker-w"]);
    replicas.get_mut("broker-x").reachable = false;
    replicas.get_mut("broker-y").reachable = false;
    replicas.get_mut("broker-w").reachable = false;
    let (leader, _dir) = leader_holding(3, &["a"]).await;
    leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .record_generation(4, 1)
        .expect("term start");
    kept_leaving_ayw(&leader).await;

    let outcome = fence_after(&replicas, &leader, true, Some(prior_xbc())).await;

    assert!(matches!(outcome, Outcome::Pending { .. }), "{outcome:?}");
}
