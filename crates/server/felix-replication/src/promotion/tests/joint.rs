//! Joint fences: a leader whose set changed needs a majority of the set it
//! leaves as well as of the new one. `JointFence` in
//! `docs/formal/FelixShard.tla`.

use felix_router::{PlacedSet, PriorSet};
use felix_storage::disk_log::{Member, Membership, ReplicaSet};

use super::*;

/// What every replica here offers: the fence and set checks, keeping every
/// set a fence names.
fn electing() -> PeerCapabilities {
    PeerCapabilities::from_bits(REQUIRED.bits())
        .union(PeerCapabilities::ELECTION)
        .union(PeerCapabilities::KEPT_SETS)
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
    kept_under(leader, "broker-x", members, learners, copies, wiped).await;
}

/// [`kept_marked`], with the leader's own ballot at 4 naming `holder`.
async fn kept_under(
    leader: &Broker,
    holder: &str,
    members: &[&str],
    learners: &[&str],
    copies: &[(&str, StorageId)],
    wiped: &[&str],
) {
    let log = leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.accept_generation(4, Some(holder))
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
        earlier: Vec::new(),
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

/// **A joint fence the set being left promised away is refused when an
/// answer the route pins rule out makes it short** (#2046). broker-b promised
/// the generation to broker-d and broker-c answered from another copy than
/// the route pins. Placement keeps the pins across renames until a fenced
/// open, so broker-c counts no more one generation up, and only broker-x is
/// left of the set being left. Without pins on the route a rename may count
/// broker-c's answer, so the single promise waits.
#[tokio::test]
async fn a_joint_fence_short_on_a_promise_and_a_pinned_out_copy_is_refused() {
    for pinned in [true, false] {
        let mut replicas = replicas_with(&["broker-x", "broker-z"]);
        replicas.ballots = true;
        for id in ["broker-b", "broker-c"] {
            replicas.get(id).holds(id, 4, 0, &["a"]).await;
        }
        promised_to(&replicas, "broker-b", "broker-d").await;
        replicas.get_mut("broker-c").storage = Some(StorageId::from_bytes([0xee; 16]));
        let (leader, _dir) = leader_holding(4, &["a"]).await;
        kept_naming(
            &leader,
            &["broker-x", "broker-b", "broker-c"],
            &[],
            &[("broker-c", storage_of("broker-c"))],
        )
        .await;
        let route = Route {
            fence_storage_ids: pinned.then(|| {
                Arc::new(
                    ["broker-x", "broker-z", "broker-b", "broker-c"]
                        .into_iter()
                        .map(|id| (id.to_string(), storage_of(id).to_hex()))
                        .collect(),
                )
            }),
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

        match (&outcome, pinned) {
            (Outcome::Refused { promised_to, .. }, true) => assert_eq!(promised_to, "broker-d"),
            (Outcome::Pending { .. }, false) => {}
            _ => panic!("pinned {pinned}: {outcome:?}"),
        }
    }
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
        earlier: Vec::new(),
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

/// **A leader with no kept set and no first-assignment mark waits.** The
/// leader at generation 4 shipped `a`; this broker holds it but no set file,
/// and its watch did not see the step to generation 5. The set at 4 may
/// still be acknowledging behind a partition, so the fence neither fences
/// the new set alone nor asks for a rename, which would leave the set at 4
/// unfenced: it stays `Pending` (#1872). A first assignment, which the watch
/// or a snapshot marks, has nothing before it and fences the new set.
#[tokio::test]
async fn an_unknown_set_before_keeps_the_fence_pending() {
    let replicas = replicas_with(&["broker-x", "broker-z"]);
    let (leader, _dir) = leader_holding(4, &["a"]).await;

    let outcome = fence_after(&replicas, &leader, true, None).await;

    let Outcome::Pending { why, .. } = &outcome else {
        panic!("fenced the new set alone, or asked for a rename on no evidence: {outcome:?}");
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

        // Without the record this is a leader restarted at a generation it
        // kept before `joint_fence` was on: only a later generation can tell
        // it the set before (#1872).
        let why = match (&outcome, prior.is_some()) {
            (Outcome::Pending { why, .. }, true) | (Outcome::SetUnknown { why, .. }, false) => why,
            _ => panic!("read a set kept alone as no change: {outcome:?}"),
        };
        let expected = if prior.is_some() {
            "1 of 3 voters of the set being left"
        } else {
            "is not known here"
        };
        assert!(why.contains(expected), "{why}");
    }
}

/// **A set unknown is reported with the newest ballot a replica holds past
/// the generation** (#2046). A standing broker-x gave up left broker-z's ballot
/// at a generation past this one; named only one up, this broker would find
/// that generation promised away and wait. With no ballot past it, none is
/// reported.
#[tokio::test]
async fn a_set_unknown_carries_the_newest_ballot_past_the_generation() {
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

    let outcome = fence_after(&replicas, &leader, true, None).await;
    let Outcome::SetUnknown { ballot, .. } = outcome else {
        panic!("{outcome:?}");
    };
    assert_eq!(ballot, 0, "no replica holds a ballot past the generation");

    replicas
        .get("broker-z")
        .broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .accept_generation(PROMOTED + 2, Some("broker-x"))
        .await
        .expect("the withdrawn standing's ballot");

    let outcome = fence_after(&replicas, &leader, true, None).await;
    let Outcome::SetUnknown { ballot, .. } = outcome else {
        panic!("{outcome:?}");
    };
    assert_eq!(ballot, PROMOTED + 2);
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
            None,
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
        earlier: Vec::new(),
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

/// **Only a set the broker could not have learned asks for a new
/// generation** (#1872). A log keeping this generation's own set, with this
/// broker's own ballot there (checked in `sets`, not here), opened it as
/// leader, so a rename leaves a set already fenced. A log
/// keeping an older set belongs to a leader that never opened the generation
/// in between, and a log keeping none may be a failover's new leader whose
/// set before still acknowledges: a rename would leave that set unfenced, so
/// both only wait.
#[test]
fn only_a_set_this_broker_could_not_know_is_unknown() {
    use crate::promotion::joint::NoSetBefore;
    let route = swapped();
    let kept_at = |generation: u64| ReplicaSet {
        current: Membership {
            generation,
            ..crate::replica_set::kept(&route, None).current
        },
        leaving: None,
        earlier: Vec::new(),
    };

    assert_eq!(
        crate::promotion::set_before(None, &route, None),
        Err(NoSetBefore::Unseen(PROMOTED))
    );
    assert_eq!(
        crate::promotion::set_before(Some(&kept_at(PROMOTED)), &route, None),
        Err(NoSetBefore::Unknown(PROMOTED))
    );
    assert_eq!(
        crate::promotion::set_before(Some(&kept_at(PROMOTED - 2)), &route, None),
        Err(NoSetBefore::Skipped(PROMOTED))
    );
    // No log keeps a set at generation 0, and nothing comes before it.
    let first = Route {
        generation: 0,
        ..route
    };
    assert_eq!(
        crate::promotion::set_before(None, &first, None),
        Err(NoSetBefore::Unknown(0))
    );
}

/// **A rename past a ballot learns the set before from the watch** (#2024).
/// The log keeps the set from two generations back: the control plane
/// adopted this leader there, a standing it never saw left a ballot one up,
/// and placement named it again past that ballot. The control plane names the
/// generation the rename replaced, and the watch takes that assignment's set
/// as the prior, so the fence leaves it instead of waiting.
#[test]
fn a_rename_past_a_ballot_takes_the_set_before_from_the_watch() {
    use crate::promotion::joint::NoSetBefore;
    let route = swapped();
    let kept = ReplicaSet {
        current: Membership {
            generation: PROMOTED - 2,
            ..crate::replica_set::kept(&route, None).current
        },
        leaving: None,
        earlier: Vec::new(),
    };
    assert_eq!(
        crate::promotion::set_before(Some(&kept), &route, None),
        Err(NoSetBefore::Skipped(PROMOTED))
    );
    let prior = prior_xbc();
    assert_eq!(prior.generation, PROMOTED);
    let before = crate::promotion::set_before(Some(&kept), &route, Some(&prior))
        .expect("the watch names the set before")
        .expect("not a first assignment");
    assert_eq!(before.generation, PROMOTED - 1);
    let named: Vec<&str> = before.members.iter().map(|m| m.node.as_str()).collect();
    assert_eq!(named.len(), 3, "{named:?}");
    for node in ["broker-x", "broker-b", "broker-c"] {
        assert!(named.contains(&node), "{node} missing from {named:?}");
    }
}

/// **A leader restarted at generation 0 asks for a rename** (#1872). A log
/// never keeps a set at generation 0, so a shard never moved since its first
/// assignment, restarted after `joint_fence` is finalized, would otherwise
/// wait forever. Nothing precedes generation 0, so it asks for generation 1,
/// whose set before its watch knows. A ballot naming another node still
/// waits, as at any generation.
#[tokio::test]
async fn a_leader_at_generation_zero_renames_only_on_its_own_ballot() {
    let route = Route {
        generation: 0,
        ..swapped()
    };
    let addr_of = addr_of;
    for (ballot, renames) in [
        (Some(LEADER), true),
        (Some("broker-x"), false),
        (None, false),
    ] {
        let replicas = replicas_with(&["broker-x", "broker-z"]);
        let (leader, _dir) = leader_holding(0, &["a"]).await;
        let log = leader
            .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("log");
        if let Some(ballot) = ballot {
            log.accept_generation(0, Some(ballot))
                .await
                .expect("ballot");
        }
        log.accept_set(crate::replica_set::kept(&route, None))
            .await
            .expect("a set at generation 0 is taken as current");
        assert!(log.replica_set().is_none(), "kept a set at generation 0");

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

        match (&outcome, renames) {
            (Outcome::SetUnknown { .. }, true) | (Outcome::Pending { .. }, false) => {}
            _ => panic!("ballot {ballot:?}: {outcome:?}"),
        }
    }
}

/// **An adopted election's leader that restarts asks for a rename** (#1966).
/// Elected at a generation the control plane later adopted, it keeps the set
/// it stood on from an older generation, and after a restart its watch has no
/// prior: the set before is skipped. Its own ballot and start record at the
/// generation, and a kept set naming exactly the route's on the same copies,
/// show the kept set is the one before, so it reports the set unknown and is
/// named one up. Without the start record the ballot may be a withdrawn
/// standing, and another node's ballot proves nothing. A copy either side
/// leaves unnamed or names differently may be another copy, and a change
/// pending on the route leaves a set the kept one is not. So does a kept set
/// with a learner, a wiped copy or another set kept earlier. All of those wait.
#[tokio::test]
async fn an_adopted_leader_restarted_on_an_older_kept_set_renames_only_on_evidence() {
    let named = |nodes: &[&str]| -> Option<Arc<std::collections::BTreeMap<String, String>>> {
        Some(Arc::new(
            nodes
                .iter()
                .map(|node| (node.to_string(), storage_of(node).to_hex()))
                .collect(),
        ))
    };
    let all = [LEADER, "broker-x", "broker-z"];
    let route = Route {
        member_storage_ids: named(&all),
        ..swapped()
    };
    let older_x = Some(Arc::new(
        [
            (LEADER.to_string(), storage_of(LEADER).to_hex()),
            ("broker-x".to_string(), storage_of("broker-old").to_hex()),
            ("broker-z".to_string(), storage_of("broker-z").to_hex()),
        ]
        .into(),
    ));
    let pending = Route {
        pending: Some(Arc::new(felix_router::PendingChange {
            generation: PROMOTED - 4,
            leaving: vec![LEADER.into(), "broker-b".into(), "broker-c".into()],
        })),
        ..route.clone()
    };
    let unnamed = Route {
        member_storage_ids: None,
        ..route.clone()
    };
    let (own, x) = (Some(LEADER), Some("broker-x"));
    let (same, other) = (named(&all), older_x);
    // (case, ballot, start record, route, kept set's copies, copies of the
    // set kept as leaving, renames, what else the kept set says)
    for (case, ballot, opened, route, kept_copies, left_copies, renames, alter) in [
        ("evidence", own, true, &route, &same, None, true, ""),
        (
            "leaving the same copies",
            own,
            true,
            &route,
            &same,
            Some(&same),
            true,
            "",
        ),
        (
            "leaving other copies",
            own,
            true,
            &route,
            &same,
            Some(&other),
            false,
            "",
        ),
        (
            "no start record",
            own,
            false,
            &route,
            &same,
            None,
            false,
            "",
        ),
        ("another's ballot", x, true, &route, &same, None, false, ""),
        ("no ballot", None, true, &route, &same, None, false, ""),
        (
            "another copy kept",
            own,
            true,
            &route,
            &other,
            None,
            false,
            "",
        ),
        (
            "route names no copies",
            own,
            true,
            &unnamed,
            &same,
            None,
            false,
            "",
        ),
        (
            "kept names no copies",
            own,
            true,
            &route,
            &None,
            None,
            false,
            "",
        ),
        (
            "a change pending",
            own,
            true,
            &pending,
            &same,
            None,
            false,
            "",
        ),
        (
            "another set kept earlier",
            own,
            true,
            &route,
            &same,
            None,
            false,
            "earlier",
        ),
        (
            "a learner kept",
            own,
            true,
            &route,
            &same,
            None,
            false,
            "learner",
        ),
        (
            "a wiped copy kept",
            own,
            true,
            &route,
            &same,
            None,
            false,
            "wiped",
        ),
    ] {
        let replicas = replicas_with(&["broker-x", "broker-z"]);
        let (leader, _dir) = leader_holding(PROMOTED - 4, &["a"]).await;
        let log = leader
            .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("log");
        let stood = Route {
            generation: PROMOTED - 4,
            member_storage_ids: kept_copies.clone(),
            pending: None,
            ..swapped()
        };
        let mut kept = crate::replica_set::kept(&stood, None);
        kept.leaving = left_copies.map(|copies| {
            let left = Route {
                generation: PROMOTED - 5,
                member_storage_ids: copies.clone(),
                ..stood.clone()
            };
            crate::replica_set::kept(&left, None).current
        });
        match alter {
            "learner" => kept.current.members[1].learner = true,
            "wiped" => kept.current.members[1].wiped = true,
            "earlier" => {
                kept.earlier = vec![self::named(PROMOTED - 5, [LEADER, "broker-y", "broker-w"])]
            }
            _ => {}
        }
        if kept.earlier.is_empty() {
            log.accept_set(kept).await
        } else {
            log.accept_set_keeping_earlier(kept).await
        }
        .expect("the set the election stood on");
        if let Some(ballot) = ballot {
            log.accept_generation(PROMOTED, Some(ballot))
                .await
                .expect("ballot");
        }
        if opened {
            log.record_generation(PROMOTED, log.commit_offset())
                .expect("start record");
        }
        assert_eq!(
            log.replica_set().map(|set| set.current.generation),
            Some(PROMOTED - 4)
        );

        let joint = Joint::new(&addr_of, None);
        let outcome = fence_shard(
            &replicas,
            &leader,
            LEADER,
            &key(),
            route,
            false,
            Some(&joint),
        )
        .await;

        match (&outcome, renames) {
            (Outcome::SetUnknown { .. }, true) | (Outcome::Pending { .. }, false) => {}
            _ => panic!("{case}: {outcome:?}"),
        }
    }
}

/// **The rename past an adopted election fences the control plane's set and
/// the kept one** (#1966). After the restart the control plane names broker-a
/// one generation up. Its watch saw the adopted set at the generation before,
/// whose broker-x is the copy the control plane registered, while the log
/// still keeps the set the election stood on, naming an older copy of
/// broker-x. With broker-z down only the control plane's set has a majority,
/// so the shard stays closed until broker-z answers.
#[tokio::test]
async fn the_rename_after_an_adopted_election_fences_both_sets() {
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    let (leader, _dir) = leader_holding(PROMOTED - 4, &["a"]).await;
    let log = leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    let mut stood = crate::replica_set::kept(
        &Route {
            generation: PROMOTED - 4,
            ..swapped()
        },
        None,
    );
    for member in &mut stood.current.members {
        if member.node == "broker-x" {
            member.storage = Some(storage_of("broker-old"));
        }
    }
    log.accept_set(stood)
        .await
        .expect("the set the election stood on");
    log.accept_generation(PROMOTED - 1, Some(LEADER))
        .await
        .expect("ballot");
    log.record_generation(PROMOTED - 1, log.commit_offset())
        .expect("start record");
    let copies: std::collections::BTreeMap<String, String> = [LEADER, "broker-x", "broker-z"]
        .iter()
        .map(|node| (node.to_string(), storage_of(node).to_hex()))
        .collect();
    let adopted = PriorSet {
        generation: PROMOTED,
        before: Some(Arc::new(PlacedSet {
            leader: LEADER.into(),
            replicas: vec!["broker-x".into(), "broker-z".into()],
            successor: None,
            storage_ids: Default::default(),
            member_storage_ids: copies.clone(),
        })),
    };

    let renamed = Route {
        member_storage_ids: Some(Arc::new(copies)),
        ..swapped()
    };
    let joint = Joint::new(&addr_of, Some(adopted));

    replicas.get_mut("broker-z").reachable = false;
    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key(),
        &renamed,
        false,
        Some(&joint),
    )
    .await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("opened without a majority of the kept set: {outcome:?}");
    };
    assert!(why.contains("2 of 3 and 1 of 3 voters"), "{why}");

    replicas.get_mut("broker-z").reachable = true;
    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key(),
        &renamed,
        false,
        Some(&joint),
    )
    .await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
}

/// **A set kept from another node's request is not one this broker opened**
/// (#1872). Its log keeps the route's own generation's set, but the ballot
/// there names broker-x, as a vote in x's election leaves it. Nothing proves
/// the set before was fenced from here, so the fence waits instead of asking
/// for a rename that would leave that set unfenced.
#[tokio::test]
async fn a_set_kept_under_another_ballot_keeps_the_fence_pending() {
    let replicas = replicas_with(&["broker-x", "broker-z"]);
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    let log = leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.accept_generation(PROMOTED, Some("broker-x"))
        .await
        .expect("ballot");
    log.accept_set(crate::replica_set::kept(&swapped(), None))
        .await
        .expect("keep the set the request carried");

    let outcome = fence_after(&replicas, &leader, true, None).await;

    let Outcome::Pending { why, .. } = &outcome else {
        panic!("renamed past a generation this broker never opened: {outcome:?}");
    };
    assert!(why.contains("is not known here"), "{why}");
}

/// **Named above another node's ballot, the leader fences** (#1951). The
/// copy is as [`a_set_kept_under_another_ballot_keeps_the_fence_pending`]
/// leaves it: it answered broker-x's fence at its generation, and broker-x
/// never opened. At that generation the fence waits for good; one above it,
/// where placement names a leader that reports the ballot, it goes through.
#[tokio::test]
async fn a_leader_named_above_another_ballot_fences() {
    let replicas = replicas_with(&["broker-x", "broker-z"]);
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    let log = leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.accept_generation(PROMOTED, Some("broker-x"))
        .await
        .expect("ballot");
    log.accept_set(crate::replica_set::kept(&swapped(), None))
        .await
        .expect("keep the set the request carried");

    let route = Route {
        generation: PROMOTED + 1,
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
        earlier: Vec::new(),
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

fn named(generation: u64, members: [&str; 3]) -> Membership {
    Membership {
        generation,
        members: members
            .map(|node| Member {
                node: node.to_string(),
                storage: None,
                learner: false,
                wiped: false,
            })
            .to_vec(),
    }
}

/// broker-a answered two joint fences that never opened: at 3, leaving
/// {broker-a, broker-u, broker-v} for {broker-a, broker-y, broker-w}, then
/// at 4, leaving that for {broker-x, broker-b, broker-c}. It holds nothing
/// written at 3 or 4.
async fn kept_two_unopened(leader: &Broker) {
    let log = leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.accept_generation(4, Some("broker-x"))
        .await
        .expect("ballot");
    for (current, leaving) in [
        (
            named(3, [LEADER, "broker-y", "broker-w"]),
            named(2, [LEADER, "broker-u", "broker-v"]),
        ),
        (
            named(4, ["broker-x", "broker-b", "broker-c"]),
            named(3, [LEADER, "broker-y", "broker-w"]),
        ),
    ] {
        log.accept_set_keeping_earlier(ReplicaSet {
            current,
            leaving: Some(leaving),
            earlier: Vec::new(),
        })
        .await
        .expect("keep the fence's sets");
    }
}

/// **A failover fences every set since the last generation that opened,
/// not only the newest one left (#1881).** The fence at 4 overwrote the set
/// kept as leaving, but the log carried the set left at 3: broker-u may hold
/// a write acknowledged at 2 that no later set's majority has. The fence
/// waits for a majority of it too, and every member keeps all the sets, so a
/// failover from its log fences them as well.
#[tokio::test]
async fn a_failover_fences_the_sets_of_two_unopened_fences() {
    let mut replicas = replicas_with(&[
        "broker-x", "broker-z", "broker-y", "broker-w", "broker-u", "broker-v",
    ]);
    for down in ["broker-x", "broker-w", "broker-u", "broker-v"] {
        replicas.get_mut(down).reachable = false;
    }
    // Members keep earlier sets only once `joint_fence` is finalized, and
    // then answer only while they hold a promise.
    for member in ["broker-z", "broker-u"] {
        let replica = replicas.get_mut(member);
        replica.handler = ReplicaHandler::new(Arc::clone(&replica.broker), router_for(member, 3))
            .with_fleet(Arc::new(joint_fence_marks(true)));
        replica
            .broker
            .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("log")
            .accept_generation(1, Some(LEADER))
            .await
            .expect("ballot");
    }
    let (leader, _dir) = leader_holding(1, &["a"]).await;
    kept_two_unopened(&leader).await;

    let outcome = fence_after(&replicas, &leader, true, Some(prior_xbc())).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("opened without a majority of the oldest unopened set: {outcome:?}");
    };
    assert!(why.contains("2 of 3 and 2 of 3 and 1 of 3 voters"), "{why}");

    replicas.get_mut("broker-u").reachable = true;
    let outcome = fence_after(&replicas, &leader, true, Some(prior_xbc())).await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    for member in ["broker-z", "broker-u"] {
        let kept = replicas
            .get(member)
            .broker
            .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("log")
            .replica_set()
            .expect("kept the fence's sets");
        let mut generations: Vec<u64> = kept.leaving.iter().map(|old| old.generation).collect();
        generations.extend(kept.earlier.iter().map(|old| old.generation));
        generations.sort_unstable();
        assert_eq!(generations, vec![2, 3, 4], "{member}");
    }
}

/// **A member that would keep one set being left is not counted, even by a
/// fence with no earlier set to carry.** broker-c offers `ELECTION` but not
/// `KEPT_SETS`: it would overwrite the sets its own log kept, so a failover
/// from it could skip one. With broker-b down, broker-x alone is not a
/// majority of the set being left.
#[tokio::test]
async fn a_member_without_kept_sets_counts_as_not_answering() {
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    replicas.get_mut("broker-b").reachable = false;
    replicas.get_mut("broker-c").capabilities =
        PeerCapabilities::from_bits(REQUIRED.bits()).union(PeerCapabilities::ELECTION);
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_before(&leader, &["broker-x", "broker-b", "broker-c"], &[]).await;

    let outcome = fence(&replicas, &leader, true).await;

    let Outcome::Pending { why, .. } = &outcome else {
        panic!("counted a member that keeps one set: {outcome:?}");
    };
    assert!(why.contains("1 of 3 voters of the set being left"), "{why}");
    assert!(!replicas.sent_to("broker-c"), "asked broker-c");
}

/// broker-a's joint fence at 5, after serving 4 on {broker-a, broker-b,
/// broker-c}, `continuing` that generation or failing over to 5. Both
/// followers restarted after an unclean crash, so each answers from a new
/// storage id, while the old set and the fence ids name the ones before.
/// Each still holds its ballot naming broker-a at 4, and broker-b a record
/// past the leader's. With `kept`, the crash kept every ballot, so their
/// answers attest it; without, they answer as a restored copy can, with
/// nothing to show it never promised 5 elsewhere.
async fn fence_followers_from_new_copies(
    continuing: bool,
    kept: bool,
) -> (Outcome, Arc<Broker>, TempDir) {
    fence_followers_promised_at(continuing, kept, 4).await
}

/// [`fence_followers_from_new_copies`], with each follower's ballot naming
/// broker-a at `promised`.
async fn fence_followers_promised_at(
    continuing: bool,
    kept: bool,
    promised: u64,
) -> (Outcome, Arc<Broker>, TempDir) {
    let mut replicas = replicas_with(&[]);
    replicas.ballots = true;
    replicas
        .get("broker-b")
        .holds("broker-b", 4, 0, &["a", "b"])
        .await;
    replicas
        .get("broker-c")
        .holds("broker-c", 4, 0, &["a"])
        .await;
    let after_the_crash = |id: &str| storage_of(&format!("{id}-after-the-crash"));
    for id in ["broker-b", "broker-c"] {
        replicas
            .get(id)
            .broker
            .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("log")
            .accept_generation(promised, Some(LEADER))
            .await
            .expect("ballot");
        let replica = replicas.get_mut(id);
        replica.storage = Some(after_the_crash(id));
        if kept {
            replica.handler = ReplicaHandler::new(Arc::clone(&replica.broker), router_for(id, 3))
                .with_unclean_start(Arc::new(
                    crate::unclean::UncleanStart::new(id).with_kept_promises(true),
                ));
        }
    }
    let (leader, dir) = leader_holding(4, &["a"]).await;
    // It served 4 itself, so its own ballot names it there.
    kept_under(
        &leader,
        LEADER,
        &[LEADER, "broker-b", "broker-c"],
        &[],
        &[
            ("broker-b", storage_of("broker-b")),
            ("broker-c", storage_of("broker-c")),
        ],
        &[],
    )
    .await;
    let named = |id_of: &dyn Fn(&str) -> StorageId| {
        Some(Arc::new(
            ["broker-b", "broker-c"]
                .map(|id| (id.to_string(), id_of(id).to_hex()))
                .into(),
        ))
    };
    let route = Route {
        fence_storage_ids: named(&storage_of),
        member_storage_ids: named(&after_the_crash),
        ..route()
    };
    let addr_of = addr_of;
    let joint = Joint::new(&addr_of, None).with_continuing(continuing);

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
    (outcome, leader, dir)
}

/// **A continuing leader's joint fence counts followers answering from new
/// copies that kept their ballots.** broker-a holds every record
/// acknowledged before generation 5, and each follower attests its kept
/// ballot named broker-a at 4, so the fence needs only their promises, and
/// it takes no follower's tail (#2047).
#[tokio::test]
async fn a_continuing_joint_fence_counts_followers_answering_from_new_copies() {
    let (outcome, leader, _dir) = fence_followers_from_new_copies(true, true).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: None
        }
    );
    assert_eq!(held(&leader).await, vec!["a"]);
}

/// **The same answers do not open a failover's joint fence.** A leader that
/// did not serve the generation before may lack what those copies lost, so
/// the old set has only its own answer and the fence waits.
#[tokio::test]
async fn a_failover_joint_fence_does_not_count_followers_answering_from_new_copies() {
    let (outcome, _leader, _dir) = fence_followers_from_new_copies(false, true).await;

    assert!(
        matches!(&outcome, Outcome::Pending { why, .. } if why.contains("another copy")),
        "{outcome:?}"
    );
}

/// **A continuing leader does not count a new copy that attests nothing.**
/// A copy restored from a snapshot passes every refusal: its ballot names
/// broker-a at 4, but it may have promised 5 to another leader before the
/// restore rolled that ballot back. Its promise refuses older leaders and
/// counts for nothing here, so the fence waits (#2047).
#[tokio::test]
async fn a_continuing_joint_fence_does_not_count_new_copies_that_attest_no_kept_ballot() {
    let (outcome, _leader, _dir) = fence_followers_from_new_copies(true, false).await;

    assert!(
        matches!(&outcome, Outcome::Pending { why, .. } if why.contains("another copy")),
        "{outcome:?}"
    );
}

/// **A retried continuing fence still counts copies that kept their
/// ballots.** The first round reached both followers, so each ballot already
/// names broker-a at 5, but their answers were lost. Each round fences
/// afresh, so the retry must be attested again: a kept ballot naming the
/// leader at the generation itself promised it to no one else (#2047).
#[tokio::test]
async fn a_retried_continuing_fence_counts_copies_whose_ballot_names_it_already() {
    let (outcome, _leader, _dir) = fence_followers_promised_at(true, true, PROMOTED).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: None
        }
    );
}

/// broker-a's joint fence at `PROMOTED` on {broker-a, broker-x, broker-z},
/// leaving {broker-x, broker-b, broker-c}, counted by attestation. broker-x
/// led that set and crashed uncleanly, so its copy is doubted; every other
/// copy holds what it acknowledged. The ones in `down` do not answer.
async fn fence_readmitting(down: &[&str]) -> Outcome {
    use super::unified::{DOUBTED, TRUSTED, fence_unified, unified};
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    for id in ["broker-x", "broker-z", "broker-b", "broker-c"] {
        let replica = replicas.get_mut(id);
        replica.attests = Some(if id == "broker-x" { DOUBTED } else { TRUSTED });
        replica.reachable = !down.contains(&id);
    }
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_before(&leader, &["broker-x", "broker-b", "broker-c"], &[]).await;
    let addr_of = addr_of;
    let joint = Joint::new(&addr_of, None);
    fence_unified(&replicas, &leader, &swapped(), Some(&joint), &unified(true)).await
}

/// **A joint fence needs covering copies to meet every majority of each
/// set.** With broker-c down, the set being left has two promises but only
/// broker-b covers, one of three; with broker-z down, the new set has only
/// this broker. Each waits. With everyone answering, both sets are covered
/// and the fence opens, though broker-x is doubted in both.
#[tokio::test]
async fn a_joint_fence_waits_for_coverage_in_every_set() {
    let outcome = fence_readmitting(&["broker-c"]).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("opened with the set being left uncovered: {outcome:?}");
    };
    assert!(why.contains("1 of 3 and 2 of 3 attest"), "{why}");

    let outcome = fence_readmitting(&["broker-z"]).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("opened with the new set uncovered: {outcome:?}");
    };
    assert!(why.contains("2 of 3 and 1 of 3 attest"), "{why}");

    let outcome = fence_readmitting(&[]).await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
}

/// **A move off a 2-voter set needs one covering copy of it.** broker-a
/// leaves {broker-a, broker-b} for {broker-a, broker-x, broker-z}, and
/// broker-b is doubted: its promise and this broker's make the old majority,
/// and this broker's trusted copy meets every majority of two. Doubted here
/// too, nothing covers the set being left and the fence waits.
#[tokio::test]
async fn a_move_off_two_voters_opens_on_one_trusted_copy() {
    use super::unified::{DOUBTED, TRUSTED, doubting_own, fence_unified, unified};
    for own_doubted in [false, true] {
        let mut replicas = replicas_with(&["broker-x", "broker-z"]);
        for id in ["broker-x", "broker-z"] {
            replicas.get_mut(id).attests = Some(TRUSTED);
        }
        replicas.get_mut("broker-b").attests = Some(DOUBTED);
        replicas.get_mut("broker-c").reachable = false;
        let (leader, _dir) = leader_holding(4, &["a"]).await;
        kept_before(&leader, &[LEADER, "broker-b"], &[]).await;
        let trust_dir = tempfile::tempdir().expect("tempdir");
        let marks = if own_doubted {
            doubting_own(trust_dir.path())
        } else {
            unified(true)
        };
        let addr_of = addr_of;
        let joint = Joint::new(&addr_of, None);

        let outcome = fence_unified(&replicas, &leader, &swapped(), Some(&joint), &marks).await;

        match (&outcome, own_doubted) {
            (Outcome::Fenced { .. }, false) => {}
            (Outcome::Pending { why, .. }, true) => {
                assert!(why.contains("0 of 2 and 2 of 3 attest"), "{why}");
            }
            _ => panic!("own doubted {own_doubted}: {outcome:?}"),
        }
    }
}

/// broker-a's fence at `PROMOTED` after serving 4 on {broker-a, broker-b,
/// broker-c}, counted by attestation. Both followers are doubted, as after
/// a crash that may have cost them acknowledged records. broker-a's own
/// ballot names `holder` at `at`.
async fn fence_past_doubted(continuing: bool, holder: &str, at: u64) -> Outcome {
    use super::unified::{DOUBTED, fence_unified, unified};
    let mut replicas = replicas_with(&[]);
    for id in ["broker-b", "broker-c"] {
        replicas.get_mut(id).attests = Some(DOUBTED);
    }
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    let log = leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.accept_generation(at, Some(holder))
        .await
        .expect("ballot");
    log.accept_set(ReplicaSet {
        current: named(4, [LEADER, "broker-b", "broker-c"]),
        leaving: None,
        earlier: Vec::new(),
    })
    .await
    .expect("keep the set");
    let addr_of = addr_of;
    let joint = Joint::new(&addr_of, None).with_continuing(continuing);
    fence_unified(&replicas, &leader, &route(), Some(&joint), &unified(true)).await
}

/// **A continuing leader opens past two doubted copies.** broker-a served
/// generation 4 itself and its ballot still names it there, so it holds
/// everything acknowledged before 5 and needs only promises. A failover
/// with the same answers has one covering copy of three and waits, and so
/// does a continuing leader whose ballot another leader took in between.
#[tokio::test]
async fn a_continuing_leader_opens_past_two_doubted_copies() {
    assert_eq!(
        fence_past_doubted(true, LEADER, 4).await,
        Outcome::Fenced {
            caught_up_from: None
        }
    );
    for (continuing, holder) in [(false, LEADER), (true, "broker-x")] {
        let outcome = fence_past_doubted(continuing, holder, 4).await;
        assert!(
            matches!(&outcome, Outcome::Pending { why, .. } if why.contains("1 of 3 copies attest")),
            "continuing {continuing}, ballot for {holder}: {outcome:?}"
        );
    }
}

/// **The sets a failover fences are the chain of unopened starts its log
/// kept.** The log keeps {broker-x, broker-b, broker-c} at 4, which left
/// {broker-a, broker-y, broker-w} at 3, which left {broker-a, broker-u,
/// broker-v} at 2, and holds no record written at 3 or 4. The fence at 5
/// leaves all three. Once the log holds a record of 4, that fence opened,
/// and only the set at 4 is left.
#[tokio::test]
async fn the_sets_a_failover_fences_are_the_unopened_chain() {
    for opened in [false, true] {
        let replicas = replicas_with(&[
            "broker-x", "broker-z", "broker-y", "broker-w", "broker-u", "broker-v",
        ]);
        let (leader, _dir) = leader_holding(if opened { 4 } else { 1 }, &["a"]).await;
        kept_two_unopened(&leader).await;
        let log = leader
            .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("log");
        let route = swapped();
        let set = crate::replica_set::view(&route, None);
        let addr_of = addr_of;
        let joint = Joint::new(&addr_of, Some(prior_xbc()));

        let fenced = crate::promotion::joint::sets(&replicas, &log, LEADER, &route, &set, &joint)
            .await
            .expect("sets");

        let sets = fenced.joint.expect("a joint fence");
        let outside = "broker-q";
        let mut required: Vec<u64> = std::iter::once(sets.leaving_for(outside))
            .cloned()
            .chain(sets.earlier_for(outside))
            .map(|left| left.generation)
            .collect();
        required.sort_unstable();
        let kept = log.replica_set().expect("kept");
        let mut chain: Vec<u64> = std::iter::once(&kept.current)
            .chain(&kept.leaving)
            .chain(&kept.earlier)
            .map(|set| set.generation)
            .collect();
        chain.sort_unstable();
        assert_eq!(chain, vec![2, 3, 4]);
        let expected = if opened { vec![4] } else { chain };
        assert_eq!(required, expected, "opened {opened}");
    }
}

/// **A retried continuing fence, whose own ballot already names this broker
/// at the generation it fences, still skips coverage.** The first round took
/// its own ballot at 5 before the answers were lost.
#[tokio::test]
async fn a_continuing_leader_balloted_at_its_generation_skips_coverage() {
    assert_eq!(
        fence_past_doubted(true, LEADER, PROMOTED).await,
        Outcome::Fenced {
            caught_up_from: None
        }
    );
}

/// broker-a continuing generation 4 into a joint fence at `PROMOTED`, on
/// {broker-a, broker-x, broker-z}, leaving {broker-x, broker-b, broker-c},
/// counted by attestation. Every follower is doubted, and the ones in `down`
/// do not answer.
async fn fence_continuing_across(down: &[&str]) -> Outcome {
    use super::unified::{DOUBTED, fence_unified, unified};
    let mut replicas = replicas_with(&["broker-x", "broker-z"]);
    for id in ["broker-x", "broker-z", "broker-b", "broker-c"] {
        let replica = replicas.get_mut(id);
        replica.attests = Some(DOUBTED);
        replica.reachable = !down.contains(&id);
    }
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_under(
        &leader,
        LEADER,
        &["broker-x", "broker-b", "broker-c"],
        &[],
        &[],
        &[],
    )
    .await;
    let addr_of = addr_of;
    let joint = Joint::new(&addr_of, None).with_continuing(true);
    fence_unified(&replicas, &leader, &swapped(), Some(&joint), &unified(true)).await
}

/// **A continuing joint fence skips coverage but not exclusion.** With every
/// follower doubted it opens on promises alone. With broker-b and broker-c
/// down, the set being left has one promise of three and the fence waits.
#[tokio::test]
async fn a_continuing_joint_fence_skips_coverage_but_not_exclusion() {
    let outcome = fence_continuing_across(&[]).await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");

    let outcome = fence_continuing_across(&["broker-b", "broker-c"]).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("opened without a majority of the set being left: {outcome:?}");
    };
    assert!(why.contains("1 of 3 voters of the set being left"), "{why}");
}
