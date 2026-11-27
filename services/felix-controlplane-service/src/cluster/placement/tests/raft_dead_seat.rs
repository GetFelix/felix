//! A Raft seat whose broker died before its copy opened (#3310). No target
//! is stored for a set until every member reports a copy id, so the swap
//! that seated it never reaches the group. The seat goes to another live
//! broker, as long as the group's reported voters do not name it.
use super::restore::{MINUTE, Reports, gone};
use super::*;
use crate::model::{RaftFormed, RaftVoters};

fn raft() -> MovePolicy {
    MovePolicy {
        raft_streams: true,
        ..MovePolicy::default()
    }
}

/// `orders` shard 0 placed on `leader` and `followers` with no target
/// stored yet: only `opened` have reported a copy, and the group's leader
/// last reported `voters`.
fn swapped(leader: &str, followers: &[&str], opened: &[&str], voters: &[&str]) -> ShardAssignment {
    let copies = |ids: &[&str]| -> BTreeMap<String, u64> {
        ids.iter().map(|node| (node.to_string(), 7)).collect()
    };
    let mut assignment = pinned("orders", 0, leader);
    assignment.replicas = followers.iter().map(|node| node.to_string()).collect();
    assignment.state = ShardState::Active;
    assignment.raft_target = None;
    assignment.raft_target_counter = 2;
    assignment.raft_formed = Some(RaftFormed {
        incarnation: assignment.incarnation,
        epoch: 0,
    });
    assignment.raft_voters = Some(RaftVoters {
        incarnation: assignment.incarnation,
        epoch: 0,
        index: 1,
        members: copies(voters),
    });
    assignment.raft_copy_ids = copies(opened);
    assignment
}

/// Live brokers `up` and brokers `lost` gone past the replace delay.
fn cluster(up: &[&str], lost: &[&str]) -> Vec<Node> {
    let mut nodes = live(up);
    nodes.extend(lost.iter().map(|id| gone(id, 10 * MINUTE)));
    nodes
}

fn pass(existing: &ShardAssignment, nodes: &[Node], rf: u32, policy: MovePolicy) -> Plan {
    plan_with(
        &[replicated_stream("orders", 1, rf)],
        &[],
        nodes,
        std::slice::from_ref(existing),
        &Reports::default(),
        policy,
    )
}

/// The one swap `plan` makes, checked to be one the store takes.
fn the_swap(plan: &Plan, existing: &ShardAssignment) -> (String, String, ShardAssignment) {
    let moves: Vec<_> = plan.moves().collect();
    assert_eq!(moves.len(), 1, "{plan:?}");
    let (_, step, written) = moves[0];
    let MoveStep::RaftReplace { from, to } = step else {
        panic!("expected a swap, got {step:?}");
    };
    assert_eq!(written.leader, existing.leader, "the leader keeps its seat");
    assert_eq!(written.nodes().count(), existing.nodes().count());
    assert!(
        !existing.moves_formed_set(written, existing.incarnation),
        "the store would refuse {written:?}"
    );
    (from.clone(), to.clone(), written.clone())
}

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

/// **The issue's case.** RF 3 on five brokers: `broker-3` was lost and
/// `broker-4` seated in its place, then `broker-4` died before it opened a
/// copy. The voters are still the first three. `broker-4`'s seat goes to
/// `broker-5`, which the two live voters keep a majority of.
#[test]
fn a_seat_whose_broker_died_before_its_copy_opened_is_given_away() {
    let existing = swapped(
        "broker-2",
        &["broker-4", "broker-1"],
        &["broker-1", "broker-2"],
        &["broker-1", "broker-2", "broker-3"],
    );
    let nodes = cluster(
        &["broker-1", "broker-2", "broker-5"],
        &["broker-3", "broker-4"],
    );

    let plan = pass(&existing, &nodes, 3, raft());

    let (from, to, written) = the_swap(&plan, &existing);
    assert_eq!((from.as_str(), to.as_str()), ("broker-4", "broker-5"));
    assert_eq!(
        written.nodes().cloned().collect::<BTreeSet<_>>(),
        set(&["broker-1", "broker-2", "broker-5"])
    );
}

/// **Two replacements dying in a row.** Each dead seat is given away in
/// turn while the voters stay the first three.
#[test]
fn a_second_replacement_that_dies_is_given_away_too() {
    let voters = ["broker-1", "broker-2", "broker-3"];
    let opened = ["broker-1", "broker-2"];
    let first = swapped("broker-2", &["broker-4", "broker-1"], &opened, &voters);
    let nodes = cluster(
        &["broker-1", "broker-2", "broker-5"],
        &["broker-3", "broker-4"],
    );
    let (_, to, written) = the_swap(&pass(&first, &nodes, 3, raft()), &first);
    assert_eq!(to, "broker-5");

    // The store drops the copy ids of a changed set; the voters stay.
    let followers: Vec<&str> = written.replicas.iter().map(String::as_str).collect();
    let second = swapped(&written.leader, &followers, &opened, &voters);
    let nodes = cluster(
        &["broker-1", "broker-2", "broker-6"],
        &["broker-3", "broker-4", "broker-5"],
    );
    let (from, to, _) = the_swap(&pass(&second, &nodes, 3, raft()), &second);
    assert_eq!((from.as_str(), to.as_str()), ("broker-5", "broker-6"));
}

/// **No spare broker.** The seat waits, named, and goes to the first
/// broker that comes up.
#[test]
fn a_dead_seat_with_no_spare_waits_until_one_appears() {
    let existing = swapped(
        "broker-2",
        &["broker-4", "broker-1"],
        &["broker-1", "broker-2"],
        &["broker-1", "broker-2", "broker-3"],
    );
    let none = cluster(&["broker-1", "broker-2"], &["broker-3", "broker-4"]);
    let plan = pass(&existing, &none, 3, raft());
    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    assert!(
        matches!(
            plan.waiting().next(),
            Some((_, Blocked::NoReplacement { node, why: Leaving::Lost })) if node == "broker-4"
        ),
        "{plan:?}"
    );

    let one = cluster(
        &["broker-1", "broker-2", "broker-5"],
        &["broker-3", "broker-4"],
    );
    let (from, to, _) = the_swap(&pass(&existing, &one, 3, raft()), &existing);
    assert_eq!((from.as_str(), to.as_str()), ("broker-4", "broker-5"));
}

/// **A seat made by a top-up.** The set was short, a top-up added
/// `broker-4`, and it died before its copy opened.
#[test]
fn a_dead_seat_a_top_up_made_is_given_away() {
    let existing = swapped(
        "broker-1",
        &["broker-2", "broker-4"],
        &["broker-1", "broker-2"],
        &["broker-1", "broker-2"],
    );
    let nodes = cluster(&["broker-1", "broker-2", "broker-3"], &["broker-4"]);

    let (from, to, _) = the_swap(&pass(&existing, &nodes, 3, raft()), &existing);
    assert_eq!((from.as_str(), to.as_str()), ("broker-4", "broker-3"));
}

/// **RF 2.** The group runs on `broker-1` and the lost `broker-2`; the
/// seat placement gave `broker-4` is given to `broker-3`.
#[test]
fn a_dead_seat_at_rf_2_is_given_away() {
    let existing = swapped(
        "broker-1",
        &["broker-4"],
        &["broker-1"],
        &["broker-1", "broker-2"],
    );
    let nodes = cluster(&["broker-1", "broker-3"], &["broker-2", "broker-4"]);

    let (from, to, _) = the_swap(&pass(&existing, &nodes, 2, raft()), &existing);
    assert_eq!((from.as_str(), to.as_str()), ("broker-4", "broker-3"));
}

/// Paused, the dead seat waits like any other swap.
#[test]
fn a_paused_placement_leaves_a_dead_seat() {
    let existing = swapped(
        "broker-2",
        &["broker-4", "broker-1"],
        &["broker-1", "broker-2"],
        &["broker-1", "broker-2", "broker-3"],
    );
    let nodes = cluster(
        &["broker-1", "broker-2", "broker-5"],
        &["broker-3", "broker-4"],
    );
    let policy = MovePolicy {
        paused: true,
        ..raft()
    };

    let plan = pass(&existing, &nodes, 3, policy);

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    assert!(
        plan.waiting().any(|(_, why)| why == &Blocked::Paused),
        "{plan:?}"
    );
}

/// **A down broker the voters name keeps the settled-target rule.** It may
/// hold acknowledged records and the group needs its vote to drop it, so
/// with no target stored the swap waits as before. With the target stored
/// it is replaced the usual way.
#[test]
fn a_dead_seat_the_voters_name_waits_for_the_target() {
    let voters = ["broker-1", "broker-2", "broker-4"];
    let nodes = cluster(&["broker-1", "broker-2", "broker-5"], &["broker-4"]);
    let unsettled = swapped(
        "broker-2",
        &["broker-4", "broker-1"],
        &["broker-1", "broker-2"],
        &voters,
    );
    let plan = pass(&unsettled, &nodes, 3, raft());
    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    assert!(
        plan.waiting()
            .any(|(_, why)| why == &Blocked::TargetUnsettled),
        "{plan:?}"
    );

    let mut settled = swapped("broker-2", &["broker-4", "broker-1"], &voters, &voters);
    settled.raft_target = Some(crate::model::RaftTarget {
        counter: 3,
        members: settled.raft_copy_ids.clone(),
    });
    let (from, to, _) = the_swap(&pass(&settled, &nodes, 3, raft()), &settled);
    assert_eq!((from.as_str(), to.as_str()), ("broker-4", "broker-5"));
}

/// With no voter report at this incarnation and epoch there is no evidence
/// the dead broker is not voting, so its seat waits.
#[test]
fn a_dead_seat_without_a_voter_report_waits() {
    let mut existing = swapped(
        "broker-2",
        &["broker-4", "broker-1"],
        &["broker-1", "broker-2"],
        &["broker-1", "broker-2", "broker-3"],
    );
    existing.raft_voters = None;
    let nodes = cluster(
        &["broker-1", "broker-2", "broker-5"],
        &["broker-3", "broker-4"],
    );

    let plan = pass(&existing, &nodes, 3, raft());

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
}

/// **The dead broker comes back** before its seat is given away: it serves,
/// so it opens its copy and the target settles. Placement does not swap it.
#[test]
fn a_dead_seat_that_returns_keeps_its_seat() {
    let existing = swapped(
        "broker-2",
        &["broker-4", "broker-1"],
        &["broker-1", "broker-2"],
        &["broker-1", "broker-2", "broker-3"],
    );
    let nodes = cluster(
        &["broker-1", "broker-2", "broker-4", "broker-5"],
        &["broker-3"],
    );

    let plan = pass(&existing, &nodes, 3, raft());

    assert!(
        plan.moves()
            .all(|(_, step, _)| !matches!(step, MoveStep::RaftReplace { .. })),
        "{plan:?}"
    );
}
