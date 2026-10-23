//! What an operator can do to moves: pause placement's own, and start or
//! cancel one.
use super::*;
use crate::model::MoveReason;
use crate::store::ControlPlaneStore;

const NOW: u64 = 10_000_000_000;

/// Reports as of `NOW` at generation 3, the one `pinned` uses.
#[derive(Default)]
struct Positions {
    level: BTreeSet<String>,
    drained: bool,
}

impl Positions {
    fn level(nodes: &[&str]) -> Self {
        Self {
            level: nodes.iter().map(|n| n.to_string()).collect(),
            drained: false,
        }
    }
}

impl CaughtUp for Positions {
    fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
        self.level.contains(node_id)
    }

    fn is_drained(&self, _key: &ShardKey, generation: u64) -> bool {
        self.drained && generation == 3
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }

    fn as_of_millis(&self) -> Option<u64> {
        Some(NOW)
    }
}

fn shard(stream: &str, leader: &str, replicas: &[&str]) -> ShardAssignment {
    let mut assignment = pinned(stream, 0, leader);
    assignment.replicas = replicas.iter().map(|r| r.to_string()).collect();
    assignment
}

fn staged(stream: &str, leader: &str, successor: &str) -> ShardAssignment {
    let mut assignment = shard(stream, leader, &[successor]);
    assignment.successor = Some(successor.to_string());
    assignment.move_started_at_millis = Some(NOW - 1_000);
    assignment.move_reason = Some(MoveReason::Balance);
    assignment
}

fn fenced(stream: &str, leader: &str, successor: &str) -> ShardAssignment {
    ShardAssignment {
        state: ShardState::Draining,
        ..staged(stream, leader, successor)
    }
}

fn paused() -> MovePolicy {
    MovePolicy {
        max_concurrent: 10,
        paused: true,
        ..MovePolicy::default()
    }
}

fn decision_for<'a>(plan: &'a Plan, stream: &str) -> &'a Decision {
    &plan
        .shards
        .iter()
        .find(|p| p.key.stream == stream)
        .expect("planned")
        .decision
}

fn catalog<'a>(
    streams: &'a [Stream],
    nodes: &'a [Node],
    existing: &'a [ShardAssignment],
    caught_up: &'a dyn CaughtUp,
    policy: MovePolicy,
) -> Catalog<'a> {
    Catalog {
        streams,
        caches: &[],
        nodes,
        existing,
        caught_up,
        policy,
    }
}

fn key(stream: &str) -> ShardKey {
    pinned(stream, 0, "any").key
}

/// Paused, placement starts nothing of its own: not a drain's move, not a
/// rebalancing move, not a follower replacement. A move already staged goes
/// on to its fence, and a new shard is still placed.
#[test]
fn a_paused_placement_starts_no_move_and_finishes_the_ones_in_flight() {
    let streams = vec![
        stream("drained", 1),
        stream("heavy-1", 1),
        stream("heavy-2", 1),
        stream("heavy-3", 1),
        replicated_stream("follower", 1, 2),
        stream("staged", 1),
        stream("new", 1),
    ];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Draining, None),
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![
        shard("drained", "broker-a", &[]),
        shard("heavy-1", "broker-b", &[]),
        shard("heavy-2", "broker-b", &[]),
        shard("heavy-3", "broker-b", &[]),
        shard("follower", "broker-b", &["broker-a"]),
        staged("staged", "broker-b", "broker-c"),
    ];
    let caught_up = Positions::level(&["broker-c"]);

    let plan = plan_with(&streams, &[], &nodes, &existing, &caught_up, paused());
    assert_eq!(
        decision_for(&plan, "drained"),
        &Decision::Waiting(Blocked::Paused)
    );
    assert_eq!(
        decision_for(&plan, "follower"),
        &Decision::Waiting(Blocked::Paused)
    );
    for heavy in ["heavy-1", "heavy-2", "heavy-3"] {
        assert!(
            matches!(
                decision_for(&plan, heavy),
                Decision::Waiting(Blocked::Paused) | Decision::Kept
            ),
            "{heavy}: {:?}",
            decision_for(&plan, heavy),
        );
    }
    assert!(
        matches!(
            decision_for(&plan, "staged"),
            Decision::Move(MoveStep::Fence, _)
        ),
        "{:?}",
        decision_for(&plan, "staged"),
    );
    assert!(matches!(decision_for(&plan, "new"), Decision::Place(..)));

    // Unpaused, the same read starts them.
    let running = MovePolicy {
        paused: false,
        ..paused()
    };
    let plan = plan_with(&streams, &[], &nodes, &existing, &caught_up, running);
    assert!(matches!(
        decision_for(&plan, "drained"),
        Decision::Move(MoveStep::Stage { .. }, _)
    ));
    assert!(matches!(
        decision_for(&plan, "follower"),
        Decision::Move(MoveStep::Reseat { .. }, _)
    ));
}

/// Every move says why it started, and the step that ends it clears that.
#[test]
fn a_move_says_why_it_started_until_it_ends() {
    let streams = vec![stream("drained", 1), replicated_stream("follower", 1, 2)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Draining, None),
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![
        shard("drained", "broker-a", &[]),
        shard("follower", "broker-b", &["broker-a"]),
    ];
    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &existing,
        &Positions::default(),
        MovePolicy {
            max_concurrent: 10,
            ..MovePolicy::default()
        },
    );
    let reason = |stream: &str| match decision_for(&plan, stream) {
        Decision::Move(_, assignment) => assignment.move_reason,
        other => panic!("{stream}: {other:?}"),
    };
    assert_eq!(reason("drained"), Some(MoveReason::Drain));
    assert_eq!(reason("follower"), Some(MoveReason::Replace));

    // Over its share: three shards on one of three nodes.
    let streams = vec![stream("s1", 1), stream("s2", 1), stream("s3", 1)];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let existing = vec![
        shard("s1", "broker-a", &[]),
        shard("s2", "broker-a", &[]),
        shard("s3", "broker-a", &[]),
    ];
    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &existing,
        &Positions::default(),
        MovePolicy::default(),
    );
    let (_, _, started) = plan.moves().next().expect("a rebalancing move");
    assert_eq!(started.move_reason, Some(MoveReason::Balance));

    // Fenced and drained: the cut-over ends it.
    let nodes = live(&["broker-a", "broker-b"]);
    let existing = vec![fenced("s1", "broker-a", "broker-b")];
    let caught_up = Positions {
        drained: true,
        ..Positions::level(&["broker-b"])
    };
    let plan = plan_with(
        &[stream("s1", 1)],
        &[],
        &nodes,
        &existing,
        &caught_up,
        MovePolicy::default(),
    );
    match decision_for(&plan, "s1") {
        Decision::Move(MoveStep::CutOver { .. }, assignment) => {
            assert_eq!(assignment.move_reason, None);
        }
        other => panic!("{other:?}"),
    }
}

/// An operator's move stages the destination and says who asked, with the
/// generation it was decided from.
#[test]
fn an_operator_move_stages_the_destination() {
    let streams = vec![stream("s1", 1)];
    let nodes = live(&["broker-a", "broker-b"]);
    let existing = vec![shard("s1", "broker-a", &[])];
    let caught_up = Positions::default();
    let decided = start_move(
        &catalog(
            &streams,
            &nodes,
            &existing,
            &caught_up,
            MovePolicy::default(),
        ),
        &key("s1"),
        "broker-b",
    )
    .expect("started");
    assert_eq!(
        decided.step,
        MoveStep::Stage {
            successor: "broker-b".to_string()
        }
    );
    assert_eq!(decided.expected_generation, 3);
    assert_eq!(decided.assignment.leader, "broker-a");
    assert_eq!(decided.assignment.successor.as_deref(), Some("broker-b"));
    assert_eq!(decided.assignment.replicas, vec!["broker-b".to_string()]);
    assert_eq!(decided.assignment.move_reason, Some(MoveReason::Operator));
    assert_eq!(decided.assignment.move_started_at_millis, Some(NOW));
}

/// A destination that already holds the copy is fenced at once.
#[test]
fn an_operator_move_to_a_caught_up_follower_fences_at_once() {
    let streams = vec![replicated_stream("s1", 1, 2)];
    let nodes = live(&["broker-a", "broker-b"]);
    let existing = vec![shard("s1", "broker-a", &["broker-b"])];
    let caught_up = Positions::level(&["broker-b"]);
    let decided = start_move(
        &catalog(
            &streams,
            &nodes,
            &existing,
            &caught_up,
            MovePolicy::default(),
        ),
        &key("s1"),
        "broker-b",
    )
    .expect("started");
    assert_eq!(decided.step, MoveStep::Fence);
    assert_eq!(decided.assignment.state, ShardState::Draining);
    assert_eq!(decided.assignment.replicas, vec!["broker-b".to_string()]);
}

/// Pausing does not stop an operator; the move limits do.
#[test]
fn an_operator_move_is_held_to_the_limits_but_not_to_a_pause() {
    let streams = vec![stream("s1", 1), stream("s2", 1)];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let existing = vec![shard("s1", "broker-a", &[]), shard("s2", "broker-c", &[])];
    let caught_up = Positions::default();
    let one = MovePolicy {
        max_concurrent: 1,
        paused: true,
        ..MovePolicy::default()
    };
    start_move(
        &catalog(&streams, &nodes, &existing, &caught_up, one.clone()),
        &key("s1"),
        "broker-b",
    )
    .expect("a pause does not stop an operator");

    let busy = vec![staged("s1", "broker-a", "broker-b"), existing[1].clone()];
    assert_eq!(
        start_move(
            &catalog(&streams, &nodes, &busy, &caught_up, one),
            &key("s2"),
            "broker-a",
        ),
        Err(Refused::Blocked(Blocked::MoveLimit)),
    );
    let per_node = MovePolicy {
        max_concurrent: 10,
        max_per_node: Some(1),
        ..MovePolicy::default()
    };
    assert_eq!(
        start_move(
            &catalog(&streams, &nodes, &busy, &caught_up, per_node),
            &key("s2"),
            "broker-b",
        ),
        Err(Refused::Blocked(Blocked::NodeMoveLimit {
            node: "broker-b".to_string()
        })),
    );
}

/// Refused where placement itself would not move the shard.
#[test]
fn an_operator_move_is_refused_where_placement_would_not_make_it() {
    let streams = vec![stream("s1", 1), stream("s2", 1), stream("moving", 1)];
    let mut nodes = live(&["broker-a", "broker-b"]);
    nodes.push(node("broker-d", NodeLifecycle::Draining, None));
    nodes.push(node("broker-down", NodeLifecycle::Down, None));
    nodes.push(node("broker-full", NodeLifecycle::Live, Some(1)));
    let existing = vec![
        shard("s1", "broker-a", &[]),
        shard("s2", "broker-down", &[]),
        staged("moving", "broker-a", "broker-b"),
        shard("elsewhere", "broker-full", &[]),
    ];
    let caught_up = Positions::default();
    let catalog = catalog(
        &streams,
        &nodes,
        &existing,
        &caught_up,
        MovePolicy {
            max_concurrent: 10,
            ..MovePolicy::default()
        },
    );
    let refused = |stream: &str, to: &str| start_move(&catalog, &key(stream), to).unwrap_err();

    assert_eq!(refused("nope", "broker-b"), Refused::UnknownShard);
    assert_eq!(
        refused("s1", "broker-z"),
        Refused::UnknownNode("broker-z".to_string())
    );
    assert!(matches!(refused("s1", "broker-d"), Refused::NotLive { .. }));
    assert!(matches!(
        refused("s1", "broker-down"),
        Refused::NotLive { .. }
    ));
    assert_eq!(
        refused("s1", "broker-a"),
        Refused::AlreadyLeads("broker-a".to_string())
    );
    assert_eq!(
        refused("s1", "broker-full"),
        Refused::AtCapacity("broker-full".to_string())
    );
    assert_eq!(
        refused("moving", "broker-a"),
        Refused::AlreadyLeads("broker-a".to_string())
    );
    assert_eq!(refused("moving", "broker-full"), Refused::Moving);
    assert_eq!(
        refused("s2", "broker-b"),
        Refused::LeaderUnavailable("broker-down".to_string())
    );
}

/// Cancelled before the fence, a move drops the destination it added, keeps
/// one the stream had anyway, and keeps its start so the shard queues
/// behind others for its next move.
#[test]
fn cancelling_a_staged_move_drops_the_destination_it_added() {
    let streams = vec![stream("added", 1), replicated_stream("kept", 1, 2)];
    let nodes = live(&["broker-a", "broker-b"]);
    let existing = vec![
        staged("added", "broker-a", "broker-b"),
        staged("kept", "broker-a", "broker-b"),
    ];
    let caught_up = Positions::default();
    let catalog = catalog(
        &streams,
        &nodes,
        &existing,
        &caught_up,
        MovePolicy::default(),
    );

    let added = cancel_move(&catalog, &key("added")).expect("cancel");
    assert_eq!(
        added.step,
        MoveStep::Cancel {
            successor: "broker-b".to_string()
        }
    );
    assert_eq!(added.expected_generation, 3);
    assert_eq!(added.assignment.leader, "broker-a");
    assert!(added.assignment.replicas.is_empty());
    assert_eq!(added.assignment.successor, None);
    assert_eq!(added.assignment.move_reason, None);
    assert_eq!(added.assignment.move_started_at_millis, Some(NOW - 1_000));

    let kept = cancel_move(&catalog, &key("kept")).expect("cancel");
    assert_eq!(kept.assignment.replicas, vec!["broker-b".to_string()]);
    assert_eq!(kept.assignment.successor, None);
}

/// Cancelled after the fence, the leader that stopped takes the shard back
/// at a new generation, without the destination the move added.
#[test]
fn cancelling_a_fenced_move_hands_the_shard_back_to_its_leader() {
    let streams = vec![stream("s1", 1)];
    let nodes = live(&["broker-a", "broker-b"]);
    let existing = vec![fenced("s1", "broker-a", "broker-b")];
    let caught_up = Positions {
        drained: true,
        ..Positions::level(&["broker-b"])
    };
    let decided = cancel_move(
        &catalog(
            &streams,
            &nodes,
            &existing,
            &caught_up,
            MovePolicy::default(),
        ),
        &key("s1"),
    )
    .expect("cancel");
    assert_eq!(
        decided.step,
        MoveStep::Retake {
            successor: Some("broker-b".to_string())
        }
    );
    assert_eq!(decided.expected_generation, 3);
    let back = decided.assignment;
    assert_eq!(back.leader, "broker-a");
    assert_eq!(back.state, ShardState::Assigning);
    assert!(back.replicas.is_empty());
    assert_eq!(back.successor, None);
    assert_eq!(back.move_reason, None);
    assert_eq!(back.move_started_at_millis, Some(NOW - 1_000));
}

/// A fenced move whose leader is gone is failover's to finish; a cancel
/// cannot hand the shard to a node that is down.
#[test]
fn a_fenced_move_whose_leader_is_down_is_not_taken_back() {
    let streams = vec![stream("s1", 1)];
    let mut nodes = live(&["broker-b"]);
    nodes.push(node("broker-a", NodeLifecycle::Down, None));
    let existing = vec![fenced("s1", "broker-a", "broker-b")];
    assert_eq!(
        cancel_move(
            &catalog(
                &streams,
                &nodes,
                &existing,
                &Positions::default(),
                MovePolicy::default()
            ),
            &key("s1"),
        ),
        Err(Refused::LeaderUnavailable("broker-a".to_string())),
    );
}

/// A replacement in progress is cancelled by dropping the follower being
/// copied in; the one it was replacing stays.
#[test]
fn cancelling_a_replacement_drops_the_follower_being_copied_in() {
    let streams = vec![replicated_stream("s1", 1, 2)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Draining, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let mut replacing = shard("s1", "broker-a", &["broker-b", "broker-c"]);
    replacing.joining = Some("broker-c".to_string());
    replacing.move_reason = Some(MoveReason::Replace);
    let existing = vec![replacing];
    let decided = cancel_move(
        &catalog(
            &streams,
            &nodes,
            &existing,
            &Positions::default(),
            MovePolicy::default(),
        ),
        &key("s1"),
    )
    .expect("cancel");
    assert_eq!(
        decided.step,
        MoveStep::Cancel {
            successor: "broker-c".to_string()
        }
    );
    assert_eq!(decided.assignment.replicas, vec!["broker-b".to_string()]);
    assert_eq!(decided.assignment.joining, None);
    assert_eq!(decided.assignment.move_reason, None);
}

/// Nothing to cancel on a shard that is not moving, which is also what a
/// move that has cut over looks like.
#[test]
fn nothing_to_cancel_on_a_shard_that_is_not_moving() {
    let streams = vec![stream("s1", 1)];
    let nodes = live(&["broker-a", "broker-b"]);
    let existing = vec![shard("s1", "broker-b", &[])];
    assert_eq!(
        cancel_move(
            &catalog(
                &streams,
                &nodes,
                &existing,
                &Positions::default(),
                MovePolicy::default()
            ),
            &key("s1"),
        ),
        Err(Refused::NotMoving),
    );
}

/// A cancel decided while the move is fenced, arriving after another
/// instance cut it over, writes nothing: handing the shard back then would
/// give it to a leader missing every write the new one acknowledged. Decided
/// again, there is nothing to cancel.
#[tokio::test]
async fn a_cancel_decided_before_a_cut_over_is_not_written_after_it() {
    use super::reconciler::{cluster, report, shard_zero};
    let store = cluster(&["broker-x", "broker-y"]).await;
    store
        .delete_stream(&crate::model::StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
        })
        .await
        .expect("drop the three-shard stream");
    store
        .create_stream(stream("orders", 1))
        .await
        .expect("stream");
    let mut move_ = fenced("orders", "broker-x", "broker-y");
    move_.state = ShardState::Active;
    let staged = store.put_shard_assignment(move_).await.expect("staged");
    let draining = store
        .put_shard_assignment(ShardAssignment {
            state: ShardState::Draining,
            ..staged
        })
        .await
        .expect("fenced");

    let liveness = Default::default();
    let read = PlacementRead::load(&store, &liveness).await.expect("read");
    let stale = cancel_move(&read.catalog(MovePolicy::default()), &shard_zero())
        .expect("a fenced move can be cancelled");

    report(&store, draining.generation, &["broker-y"], true).await;
    let cut = reconcile_once(&store, &liveness, MovePolicy::default()).await;
    assert_eq!(cut.moved, 1);
    assert_eq!(
        store
            .get_shard_assignment(&shard_zero())
            .await
            .expect("get")
            .leader,
        "broker-y"
    );

    // At the current token, so it is the generation that stops it.
    let stale = super::super::operator::FencedStep {
        step: stale,
        fence: store.placement_token().await.expect("token"),
    };
    let written = super::super::operator::write_operator_step(&store, &stale)
        .await
        .expect("write");
    assert!(
        matches!(written, super::super::operator::OperatorWrite::Stale),
        "the stale cancel landed: {written:?}"
    );
    assert_eq!(
        store
            .get_shard_assignment(&shard_zero())
            .await
            .expect("get")
            .leader,
        "broker-y"
    );

    let again = run_operator(
        &store,
        &liveness,
        MovePolicy::default(),
        &PlacementWakes::default(),
        |catalog| cancel_move(catalog, &shard_zero()),
    )
    .await;
    assert!(
        matches!(again, Err(OperatorError::Refused(Refused::NotMoving))),
        "{again:?}"
    );
}

/// Placement reads the pause from the store on every pass.
#[tokio::test]
async fn a_pause_in_the_store_holds_every_instance_placement() {
    use super::reconciler::{cluster, shard_zero};
    let store = cluster(&["broker-x", "broker-y"]).await;
    let liveness = Default::default();
    reconcile_once(&store, &liveness, MovePolicy::default()).await;
    let before = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    store
        .patch_node(
            &before.leader,
            crate::model::NodePatchRequest {
                lifecycle: Some(NodeLifecycle::Draining),
                ..Default::default()
            },
        )
        .await
        .expect("drain");

    store.set_moves_paused(true).await.expect("pause");
    let held = reconcile_once(&store, &liveness, MovePolicy::default()).await;
    assert_eq!(held.moved, 0, "{held:?}");
    assert!(held.waiting > 0, "{held:?}");

    store.set_moves_paused(false).await.expect("resume");
    let moving = reconcile_once(&store, &liveness, MovePolicy::default()).await;
    assert!(moving.moved > 0, "{moving:?}");
}

/// One shard of `orders` on three brokers, Quorum with three copies, placed
/// and reported level.
async fn quorum_shard() -> (crate::store::memory::InMemoryStore, ShardAssignment) {
    use super::reconciler::{cluster, shard_zero};
    let store = cluster(&["broker-x", "broker-y", "broker-z"]).await;
    store
        .delete_stream(&crate::model::StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
        })
        .await
        .expect("drop the three-shard stream");
    store
        .create_stream(Stream {
            consistency: ConsistencyLevel::Quorum,
            ..replicated_stream("orders", 1, 3)
        })
        .await
        .expect("stream");
    reconcile_once(&store, &Default::default(), unsettled()).await;
    let placed = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("placed");
    report_level(&store, &placed).await;
    (store, placed)
}

fn unsettled() -> MovePolicy {
    MovePolicy {
        settle_millis: None,
        ..MovePolicy::default()
    }
}

/// Report every follower of `assignment` level, and drained while fenced.
async fn report_level(store: &crate::store::memory::InMemoryStore, assignment: &ShardAssignment) {
    let followers: Vec<&str> = assignment
        .replicas
        .iter()
        .map(String::as_str)
        .filter(|replica| *replica != assignment.leader)
        .collect();
    super::reconciler::report(
        store,
        assignment.generation,
        &followers,
        assignment.state == ShardState::Draining,
    )
    .await;
}

/// Run passes until the shard stops moving, and return it.
async fn settle(store: &crate::store::memory::InMemoryStore) -> ShardAssignment {
    use super::reconciler::shard_zero;
    for _ in 0..4 {
        let current = store
            .get_shard_assignment(&shard_zero())
            .await
            .expect("get");
        report_level(store, &current).await;
        reconcile_once(store, &Default::default(), unsettled()).await;
    }
    store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get")
}

/// A pause stops placement starting moves, never finishing one: a fenced
/// leader has stopped taking writes, so a move held there stalls the shard.
/// Placement's own move, fenced before the pause, cuts over under it.
#[tokio::test]
async fn a_move_fenced_before_a_pause_cuts_over_under_it() {
    use super::reconciler::shard_zero;
    let (store, placed) = quorum_shard().await;
    let destination = placed.replicas[0].clone();
    let fenced = store
        .put_shard_assignment(ShardAssignment {
            state: ShardState::Draining,
            successor: Some(destination.clone()),
            move_started_at_millis: Some(1),
            move_reason: Some(MoveReason::Balance),
            ..placed.clone()
        })
        .await
        .expect("fenced");
    store.set_moves_paused(true).await.expect("pause");

    let after = settle(&store).await;
    assert_eq!(after.leader, destination, "{after:?}");
    assert_eq!(after.state, ShardState::Assigning);
    assert_eq!(after.successor, None);
    assert!(after.generation > fenced.generation);
    assert_eq!(
        store
            .get_shard_assignment(&shard_zero())
            .await
            .expect("get"),
        after
    );
}

/// An operator's move started under a pause runs to its cut-over without a
/// resume.
#[tokio::test]
async fn an_operator_move_started_under_a_pause_runs_to_its_cut_over() {
    use super::reconciler::shard_zero;
    let (store, placed) = quorum_shard().await;
    let destination = placed.replicas[0].clone();
    store.set_moves_paused(true).await.expect("pause");
    let started = run_operator(
        &store,
        &Default::default(),
        unsettled(),
        &PlacementWakes::default(),
        |catalog| start_move(catalog, &shard_zero(), &destination),
    )
    .await
    .expect("a pause does not stop an operator");
    assert!(
        matches!(started.step, MoveStep::Fence | MoveStep::Stage { .. }),
        "{:?}",
        started.step
    );

    let after = settle(&store).await;
    assert_eq!(after.leader, destination, "{after:?}");
    assert_eq!(after.successor, None);
    assert_ne!(after.state, ShardState::Draining);
    assert!(store.moves_paused().await.expect("read"), "still paused");
}
