//! Planned moves: a shard whose leader is alive is handed off, never
//! reassigned. Each step is checked from the store state that precedes it,
//! which is how a pass on any instance resumes a move.
use super::*;

/// What the leaders last reported: who is caught up, at which
/// generation, and whether the leader has drained.
struct Reported {
    caught_up: BTreeSet<String>,
    generation: u64,
    drained: bool,
}

impl Reported {
    /// At the generation `assigned` uses.
    fn caught_up(nodes: &[&str]) -> Self {
        Self {
            caught_up: nodes.iter().map(|n| n.to_string()).collect(),
            generation: 3,
            drained: false,
        }
    }

    fn at(mut self, generation: u64) -> Self {
        self.generation = generation;
        self
    }

    fn drained_at(mut self, generation: u64) -> Self {
        self.generation = generation;
        self.drained = true;
        self
    }
}

impl CaughtUp for Reported {
    fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
        self.caught_up.contains(node_id)
    }

    fn is_drained(&self, _key: &ShardKey, generation: u64) -> bool {
        self.drained && self.generation == generation
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(self.generation)
    }
}

fn one_shard(leader: &str, replicas: &[&str]) -> ShardAssignment {
    assigned("orders", leader, replicas)
}

fn only_decision(plan: &Plan) -> &Decision {
    assert_eq!(plan.shards.len(), 1);
    &plan.shards[0].decision
}

fn draining_cluster() -> Vec<Node> {
    vec![
        node("broker-a", NodeLifecycle::Draining, None),
        node("broker-b", NodeLifecycle::Live, None),
    ]
}

/// Step one: the destination joins the replica set as the successor. The
/// old leader keeps leading -- nothing is reassigned to a node holding
/// none of the log.
#[test]
fn a_draining_leader_stages_its_successor_as_a_replica() {
    let streams = vec![stream("orders", 1)];
    let existing = vec![one_shard("broker-a", &[])];

    let plan = plan(
        &streams,
        &[],
        &draining_cluster(),
        &existing,
        &NothingCaughtUp,
    );

    match only_decision(&plan) {
        Decision::Move(MoveStep::Stage { successor }, next) => {
            assert_eq!(successor, "broker-b");
            assert_eq!(next.leader, "broker-a", "the leader does not change yet");
            assert_eq!(next.replicas, vec!["broker-b".to_string()]);
            assert_eq!(next.successor.as_deref(), Some("broker-b"));
            assert_eq!(next.state, ShardState::Active);
        }
        other => panic!("expected a stage, got {other:?}"),
    }
    assert_eq!(plan.to_place().count(), 0, "nothing is reassigned outright");
}

/// Step two waits: a staged successor that has not caught up is not fenced.
#[test]
fn a_successor_that_is_not_caught_up_is_waited_for() {
    let streams = vec![stream("orders", 1)];
    let mut staged = one_shard("broker-a", &["broker-b"]);
    staged.successor = Some("broker-b".to_string());

    let plan = plan(
        &streams,
        &[],
        &draining_cluster(),
        &[staged],
        &NothingCaughtUp,
    );

    assert_eq!(
        only_decision(&plan),
        &Decision::Waiting(Blocked::DestinationCatchingUp {
            successor: "broker-b".to_string()
        })
    );
}

/// Step two: a caught-up successor fences the leader. The assignment goes
/// `Draining` with everything else unchanged.
#[test]
fn a_caught_up_successor_fences_the_leader() {
    let streams = vec![stream("orders", 1)];
    let mut staged = one_shard("broker-a", &["broker-b"]);
    staged.successor = Some("broker-b".to_string());

    let plan = plan(
        &streams,
        &[],
        &draining_cluster(),
        &[staged],
        &Reported::caught_up(&["broker-b"]),
    );

    match only_decision(&plan) {
        Decision::Move(MoveStep::Fence, next) => {
            assert_eq!(next.leader, "broker-a");
            assert_eq!(next.state, ShardState::Draining);
            assert_eq!(next.successor.as_deref(), Some("broker-b"));
        }
        other => panic!("expected a fence, got {other:?}"),
    }
}

/// Step three waits: a fenced leader that has not reported drained is
/// still writing as far as the control plane knows.
#[test]
fn a_fenced_shard_waits_for_the_leader_to_report_drained() {
    let streams = vec![stream("orders", 1)];
    let mut fenced = one_shard("broker-a", &["broker-b"]);
    fenced.successor = Some("broker-b".to_string());
    fenced.state = ShardState::Draining;

    let plan = plan(
        &streams,
        &[],
        &draining_cluster(),
        std::slice::from_ref(&fenced),
        &Reported::caught_up(&["broker-b"]),
    );
    assert_eq!(
        only_decision(&plan),
        &Decision::Waiting(Blocked::LeaderStopping)
    );

    // A drained report from before the fence does not count.
    let plan = super::plan(
        &streams,
        &[],
        &draining_cluster(),
        &[fenced],
        &Reported::caught_up(&["broker-b"]).drained_at(2),
    );
    assert_eq!(
        only_decision(&plan),
        &Decision::Waiting(Blocked::LeaderStopping)
    );
}

/// A promotion's identities do not ride along into the move's later writes.
#[test]
fn a_cut_over_carries_no_fence_storage_ids() {
    let streams = vec![stream("orders", 1)];
    let mut fenced = one_shard("broker-a", &["broker-b"]);
    fenced.successor = Some("broker-b".to_string());
    fenced.state = ShardState::Draining;
    fenced
        .fence_storage_ids
        .insert("broker-b".to_string(), "b".repeat(32));

    let plan = plan(
        &streams,
        &[],
        &draining_cluster(),
        &[fenced.clone()],
        &Reported::caught_up(&["broker-b"]).drained_at(fenced.generation),
    );

    match only_decision(&plan) {
        Decision::Move(MoveStep::CutOver { .. }, next) => {
            assert!(next.fence_storage_ids.is_empty());
        }
        other => panic!("expected a cut-over, got {other:?}"),
    }
}

/// Step three: the drained report at the fenced generation cuts over. The
/// old leader is not kept as a follower -- it is draining.
#[test]
fn a_drained_report_cuts_over_to_the_successor() {
    let streams = vec![stream("orders", 1)];
    let mut fenced = one_shard("broker-a", &["broker-b"]);
    fenced.successor = Some("broker-b".to_string());
    fenced.state = ShardState::Draining;

    let plan = plan(
        &streams,
        &[],
        &draining_cluster(),
        &[fenced.clone()],
        &Reported::caught_up(&["broker-b"]).drained_at(fenced.generation),
    );

    match only_decision(&plan) {
        Decision::Move(MoveStep::CutOver { from, to }, next) => {
            assert_eq!((from.as_str(), to.as_str()), ("broker-a", "broker-b"));
            assert_eq!(next.leader, "broker-b");
            assert!(next.replicas.is_empty(), "replication factor one");
            assert_eq!(next.state, ShardState::Assigning);
            assert_eq!(next.successor, None);
        }
        other => panic!("expected a cut-over, got {other:?}"),
    }
}

/// The whole move, driven to the end, writes exactly three steps and then
/// nothing: staged, fenced, cut over.
#[test]
fn a_drain_converges_in_three_writes() {
    let streams = vec![stream("orders", 1)];
    let nodes = draining_cluster();
    let mut existing = vec![one_shard("broker-a", &[])];
    let mut steps = Vec::new();
    for _ in 0..6 {
        let reported = if existing[0].state == ShardState::Draining {
            Reported::caught_up(&["broker-b"]).drained_at(existing[0].generation)
        } else {
            Reported::caught_up(&["broker-b"]).at(existing[0].generation)
        };
        let plan = plan(&streams, &[], &nodes, &existing, &reported);
        match only_decision(&plan) {
            Decision::Move(step, next) => {
                steps.push(step.label());
                let mut next = next.clone();
                next.generation = existing[0].generation + 1;
                existing = vec![next];
            }
            Decision::Kept => break,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(steps, vec!["stage", "fence", "cut_over"]);
    assert_eq!(existing[0].leader, "broker-b");
}

/// A destination that already holds a copy is fenced straight away: the
/// stage step exists only to make the copy.
#[test]
fn a_caught_up_replica_needs_no_staging() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let existing = vec![one_shard("broker-a", &["broker-b"])];

    let plan = plan(
        &streams,
        &[],
        &draining_cluster(),
        &existing,
        &Reported::caught_up(&["broker-b"]),
    );

    match only_decision(&plan) {
        Decision::Move(MoveStep::Fence, next) => {
            assert_eq!(next.successor.as_deref(), Some("broker-b"));
            assert_eq!(next.state, ShardState::Draining);
        }
        other => panic!("expected a fence, got {other:?}"),
    }
}

/// A report from before the staging write says nothing about the
/// successor: the leader may have written since. The move waits for a
/// report at its own generation.
#[test]
fn a_report_from_an_older_generation_does_not_fence() {
    let streams = vec![stream("orders", 1)];
    let mut staged = one_shard("broker-a", &["broker-b"]);
    staged.successor = Some("broker-b".to_string());
    staged.generation = 4;

    let plan = plan(
        &streams,
        &[],
        &draining_cluster(),
        &[staged.clone()],
        &Reported::caught_up(&["broker-b"]).at(3),
    );
    assert_eq!(
        only_decision(&plan),
        &Decision::Waiting(Blocked::DestinationCatchingUp {
            successor: "broker-b".to_string()
        })
    );

    // Nor does it let a caught-up replica skip the staging.
    let existing = vec![one_shard("broker-a", &["broker-b"])];
    let plan = super::plan(
        &[replicated_stream("orders", 1, 2)],
        &[],
        &draining_cluster(),
        &existing,
        &Reported::caught_up(&["broker-b"]).at(2),
    );
    assert!(
        matches!(
            only_decision(&plan),
            Decision::Move(MoveStep::Stage { .. }, _)
        ),
        "got {:?}",
        only_decision(&plan)
    );
}

/// A staged destination that stops being live is dropped from the move,
/// not waited on.
#[test]
fn a_lost_destination_abandons_the_move() {
    let streams = vec![stream("orders", 1)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Down, None),
    ];
    let mut staged = one_shard("broker-a", &["broker-b"]);
    staged.successor = Some("broker-b".to_string());

    let plan = plan(&streams, &[], &nodes, &[staged], &NothingCaughtUp);

    match only_decision(&plan) {
        Decision::Move(MoveStep::Abandon { successor }, next) => {
            assert_eq!(successor, "broker-b");
            assert_eq!(next.leader, "broker-a");
            assert!(next.replicas.is_empty());
            assert_eq!(next.successor, None);
        }
        other => panic!("expected the move to be abandoned, got {other:?}"),
    }
}

/// A destination that dies between the fence and the cut-over does not
/// take the shard. Nothing else holds the log, so the old leader takes
/// it back at a new generation and the move is chosen again.
#[test]
fn a_destination_lost_after_the_fence_does_not_lead() {
    let streams = vec![stream("orders", 1)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Down, None),
    ];
    let mut fenced = one_shard("broker-a", &["broker-b"]);
    fenced.successor = Some("broker-b".to_string());
    fenced.state = ShardState::Draining;

    let plan = plan(
        &streams,
        &[],
        &nodes,
        &[fenced.clone()],
        &Reported::caught_up(&["broker-b"]).drained_at(fenced.generation),
    );

    match only_decision(&plan) {
        Decision::Move(MoveStep::CutOver { to, .. }, next) => {
            assert_eq!(to, "broker-a");
            assert_eq!(next.leader, "broker-a");
            assert_eq!(next.state, ShardState::Assigning);
            assert_eq!(next.successor, None);
        }
        other => panic!("expected the leader to take the shard back, got {other:?}"),
    }
}

/// The leader dying mid-move is a failover, and the successor -- a replica
/// like any other -- is promoted if it holds the log.
#[test]
fn a_leader_lost_mid_move_fails_over_to_the_successor() {
    let streams = vec![stream("orders", 1)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Down, None),
        node("broker-b", NodeLifecycle::Live, None),
    ];
    let mut fenced = one_shard("broker-a", &["broker-b"]);
    fenced.successor = Some("broker-b".to_string());
    fenced.state = ShardState::Draining;

    let plan = plan(
        &streams,
        &[],
        &nodes,
        &[fenced],
        &Reported::caught_up(&["broker-b"]),
    );
    assert_eq!(
        only_decision(&plan),
        &Decision::Place("broker-b".to_string(), Vec::new())
    );

    // And not if it does not: the shard stays unavailable rather than
    // served empty.
    let mut fenced = one_shard("broker-a", &["broker-b"]);
    fenced.successor = Some("broker-b".to_string());
    let plan = super::plan(&streams, &[], &nodes, &[fenced], &NothingCaughtUp);
    assert_eq!(
        only_decision(&plan),
        &Decision::Unplaceable(Unplaceable::NoCaughtUpReplica)
    );
}

/// The old leader stays as a follower after a rebalance, so the stream
/// keeps its copies without a fresh catch-up.
#[test]
fn a_cut_over_keeps_the_old_leader_as_a_follower_when_it_is_staying() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let nodes = live(&["broker-a", "broker-b"]);
    let mut fenced = one_shard("broker-a", &["broker-b"]);
    fenced.successor = Some("broker-b".to_string());
    fenced.state = ShardState::Draining;

    let plan = plan(
        &streams,
        &[],
        &nodes,
        &[fenced.clone()],
        &Reported::caught_up(&["broker-b"]).drained_at(fenced.generation),
    );

    match only_decision(&plan) {
        Decision::Move(MoveStep::CutOver { .. }, next) => {
            assert_eq!(next.leader, "broker-b");
            assert_eq!(next.replicas, vec!["broker-a".to_string()]);
        }
        other => panic!("expected a cut-over, got {other:?}"),
    }
}

/// Only as many moves as the policy allows are in flight at once; the
/// rest wait, visibly.
#[test]
fn moves_are_bounded_by_the_policy() {
    let streams = vec![stream("orders", 4)];
    let existing: Vec<ShardAssignment> = (0..4)
        .map(|shard| pinned("orders", shard, "broker-a"))
        .collect();

    let plan = plan_with(
        &streams,
        &[],
        &draining_cluster(),
        &existing,
        &NothingCaughtUp,
        MovePolicy {
            max_concurrent: 2,
            ..MovePolicy::default()
        },
    );
    assert_eq!(plan.moves().count(), 2);
    assert_eq!(
        plan.waiting()
            .filter(|(_, why)| **why == Blocked::MoveLimit)
            .count(),
        2
    );

    // A move already in flight holds its slot.
    let mut existing = existing;
    existing[0].successor = Some("broker-b".to_string());
    existing[0].replicas = vec!["broker-b".to_string()];
    let plan = plan_with(
        &streams,
        &[],
        &draining_cluster(),
        &existing,
        &NothingCaughtUp,
        MovePolicy {
            max_concurrent: 1,
            ..MovePolicy::default()
        },
    );
    assert_eq!(plan.moves().count(), 0);
    assert_eq!(plan.waiting().count(), 4);

    // Zero holds everything.
    let plan = plan_with(
        &streams,
        &[],
        &draining_cluster(),
        &existing[1..],
        &NothingCaughtUp,
        MovePolicy {
            max_concurrent: 0,
            ..MovePolicy::default()
        },
    );
    assert_eq!(plan.moves().count(), 0);
}

/// A fresh cluster is placed with leaders spread, so it never needs a move
/// to get there -- however the hash falls, and however many roles each node
/// holds. Replication factor equal to the node count is the case that used
/// to slip through: every node held a role for every shard, so the role
/// bound was met with every leader on one node.
#[test]
fn fresh_placement_needs_no_rebalance() {
    for (shards, nodes, rf) in [(4, 3, 3), (6, 2, 2), (5, 3, 1), (12, 4, 3), (7, 3, 2)] {
        let streams = vec![replicated_stream("orders", shards, rf)];
        let ids: Vec<String> = (0..nodes).map(|i| format!("broker-{i}")).collect();
        let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
        let nodes = live(&ids);
        let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);
        let placed: Vec<ShardAssignment> = plan
            .to_place()
            .map(|(key, leader, replicas)| assignment_for(key, leader, replicas.to_vec()))
            .collect();
        assert_eq!(placed.len() as u32, shards);

        let again = super::plan(&streams, &[], &nodes, &placed, &NothingCaughtUp);
        assert_eq!(
            again.moves().count(),
            0,
            "{shards} shards over {} nodes at rf {rf} were placed unevenly: {:?}",
            nodes.len(),
            placed.iter().map(|a| a.leader.as_str()).collect::<Vec<_>>()
        );
        assert_eq!(again.kept() as u32, shards);
    }
}

/// The motivating case: every shard landed on one broker while the other
/// was registering. Rebalancing moves shards from the node over its share
/// to the one under it, and stops when neither holds.
#[test]
fn an_overloaded_node_gives_shards_to_an_idle_one_until_balanced() {
    let streams = vec![stream("orders", 6)];
    let nodes = live(&["broker-a", "broker-b"]);
    let mut existing: Vec<ShardAssignment> = (0..6)
        .map(|shard| pinned("orders", shard, "broker-a"))
        .collect();

    let mut writes = 0;
    for _ in 0..40 {
        // One report per shard in the store; here one fixture answers for
        // all of them at the generation of whichever shard is moving.
        let moving = existing
            .iter()
            .find(|a| a.state == ShardState::Draining || a.successor.is_some());
        let reported = Reported {
            caught_up: ["broker-a", "broker-b"]
                .iter()
                .map(|n| n.to_string())
                .collect(),
            generation: moving.map_or(3, |a| a.generation),
            drained: moving.is_some_and(|a| a.state == ShardState::Draining),
        };
        let plan = plan_with(
            &streams,
            &[],
            &nodes,
            &existing,
            &reported,
            MovePolicy {
                max_concurrent: 1,
                ..MovePolicy::default()
            },
        );
        let mut wrote = false;
        for (key, _, next) in plan.moves() {
            let slot = existing.iter_mut().find(|a| &a.key == key).expect("known");
            let mut next = next.clone();
            next.generation = slot.generation + 1;
            *slot = next;
            wrote = true;
            writes += 1;
        }
        if !wrote && plan.waiting().count() == 0 {
            break;
        }
    }

    let on_a = existing.iter().filter(|a| a.leader == "broker-a").count();
    let on_b = existing.iter().filter(|a| a.leader == "broker-b").count();
    assert_eq!((on_a, on_b), (3, 3), "balanced");
    assert_eq!(
        writes, 9,
        "three moves of three writes each, and no churn after"
    );
    assert!(existing.iter().all(|a| a.successor.is_none()));
}

/// One shard over is not an imbalance worth a move: the share is a ceiling,
/// and five shards on two nodes is three and two.
#[test]
fn a_cluster_within_one_of_balanced_is_left_alone() {
    let streams = vec![stream("orders", 5)];
    let nodes = live(&["broker-a", "broker-b"]);
    let existing: Vec<ShardAssignment> = (0..5)
        .map(|shard| {
            pinned(
                "orders",
                shard,
                if shard < 3 { "broker-a" } else { "broker-b" },
            )
        })
        .collect();

    let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);
    assert_eq!(plan.kept(), 5);
    assert_eq!(plan.moves().count(), 0);
}

/// An ephemeral stream has no log to hand off, so a draining node's shard
/// of one is simply reassigned, as it always was.
#[test]
fn an_ephemeral_stream_is_reassigned_rather_than_moved() {
    let mut ephemeral = stream("orders", 1);
    ephemeral.durable = false;
    let existing = vec![one_shard("broker-a", &[])];

    let plan = plan(
        &[ephemeral],
        &[],
        &draining_cluster(),
        &existing,
        &NothingCaughtUp,
    );
    assert_eq!(
        only_decision(&plan),
        &Decision::Place("broker-b".to_string(), Vec::new())
    );
}

/// A follower on a draining node is replaced by one that is staying, so
/// the node ends up holding nothing and can leave.
#[test]
fn a_follower_on_a_draining_node_is_reseated() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Draining, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![one_shard("broker-a", &["broker-b"])];

    let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);
    match only_decision(&plan) {
        Decision::Move(MoveStep::Reseat { from, to }, next) => {
            assert_eq!((from.as_str(), to.as_str()), ("broker-b", "broker-c"));
            assert_eq!(next.leader, "broker-a");
            // Beside the follower it replaces, until it has caught up.
            assert_eq!(next.replicas, vec!["broker-b", "broker-c"]);
            assert_eq!(next.joining.as_deref(), Some("broker-c"));
        }
        other => panic!("expected a reseat, got {other:?}"),
    }

    // A follower that is merely down is left where it is.
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Down, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let plan = super::plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);
    assert_eq!(only_decision(&plan), &Decision::Kept);
}

/// A draining node with nowhere to send its shards waits, and says so.
#[test]
fn a_drain_with_no_destination_waits() {
    let streams = vec![stream("orders", 1)];
    let nodes = vec![node("broker-a", NodeLifecycle::Draining, None)];
    let existing = vec![one_shard("broker-a", &[])];

    let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);
    assert_eq!(
        only_decision(&plan),
        &Decision::Waiting(Blocked::NoDestination)
    );
}

/// Four shards on two brokers, with shard 0 fenced on its way from
/// `broker-a` to `broker-b` and every other shard led by `leaders[i - 1]`.
fn one_fenced_move(leaders: [&str; 3]) -> Vec<ShardAssignment> {
    let mut fenced = pinned("orders", 0, "broker-a");
    fenced.replicas = vec!["broker-b".to_string()];
    fenced.successor = Some("broker-b".to_string());
    fenced.state = ShardState::Draining;
    let mut existing = vec![fenced];
    for (shard, leader) in (1..).zip(leaders) {
        existing.push(pinned("orders", shard, leader));
    }
    existing
}

/// Two moves to one destination, the second cut over a pass before the
/// first. The fenced shard was already counted toward `broker-b`, so cutting
/// it over must not count it again: `broker-b` is at its share, and the
/// shard it just took stays put.
#[test]
fn a_cut_over_counts_its_leadership_once() {
    let streams = vec![stream("orders", 4)];
    let existing = one_fenced_move(["broker-b", "broker-a", "broker-a"]);

    let plan = plan_with(
        &streams,
        &[],
        &live(&["broker-a", "broker-b"]),
        &existing,
        &Reported::caught_up(&["broker-b"]).drained_at(3),
        MovePolicy {
            max_concurrent: 2,
            ..MovePolicy::default()
        },
    );

    let steps: Vec<_> = plan
        .moves()
        .map(|(key, step, _)| (key.shard, step.clone()))
        .collect();
    assert_eq!(
        steps,
        vec![(
            0,
            MoveStep::CutOver {
                from: "broker-a".to_string(),
                to: "broker-b".to_string(),
            }
        )],
        "only the fenced shard moves"
    );
}

/// A fenced shard whose successor never caught up goes back to its old
/// leader. The count it held for the successor moves with it, so the old
/// leader is seen over its share and a shard is staged off it again.
#[test]
fn a_take_back_returns_the_leadership_it_counted_for_the_successor() {
    let streams = vec![stream("orders", 4)];
    let existing = one_fenced_move(["broker-a", "broker-a", "broker-b"]);

    let plan = plan_with(
        &streams,
        &[],
        &live(&["broker-a", "broker-b"]),
        &existing,
        &Reported::caught_up(&[]).drained_at(3),
        MovePolicy {
            max_concurrent: 2,
            ..MovePolicy::default()
        },
    );

    let steps: Vec<_> = plan
        .moves()
        .map(|(key, step, _)| (key.shard, step.clone()))
        .collect();
    assert_eq!(
        steps[0],
        (
            0,
            MoveStep::CutOver {
                from: "broker-a".to_string(),
                to: "broker-a".to_string(),
            }
        ),
        "nothing else holds the log"
    );
    assert!(
        steps[1..].iter().any(|(_, step)| *step
            == MoveStep::Stage {
                successor: "broker-b".to_string()
            }),
        "broker-a leads three of four and should give one up: {steps:?}"
    );
}
