//! A Raft voter that died after a set change, before it reported its copy
//! again (#3313). The change dropped the copy ids, and a target waits for
//! every member's, so placement reports the dead voter's copy from the
//! voters the group reported, and the swap that replaces it can follow.
use super::restore::{MINUTE, Reports, gone};
use super::*;
use crate::model::{RaftFormed, RaftTarget, RaftVoters};

const VOTER_COPY: u64 = 44;
const ROTATED_COPY: u64 = 45;

fn raft() -> MovePolicy {
    MovePolicy {
        raft_streams: true,
        ..MovePolicy::default()
    }
}

fn copies(ids: &[(&str, u64)]) -> BTreeMap<String, u64> {
    ids.iter()
        .map(|(node, copy)| (node.to_string(), *copy))
        .collect()
}

/// `orders` shard 0 on `leader` and `followers` with no target stored for
/// the set: `reported` have reported copies since it changed, and the
/// group's leader last reported `voters`.
fn changed(
    leader: &str,
    followers: &[&str],
    reported: &[(&str, u64)],
    voters: &[(&str, u64)],
) -> ShardAssignment {
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
    assignment.raft_copy_ids = copies(reported);
    assignment
}

/// The issue's record: `{1, 2, 4}` formed, `broker-1` drained and swapped
/// for `broker-5`, and `broker-4` died before it reported again.
fn after_drain() -> ShardAssignment {
    changed(
        "broker-2",
        &["broker-4", "broker-5"],
        &[("broker-2", 7), ("broker-5", 9)],
        &[("broker-1", 7), ("broker-2", 7), ("broker-4", VOTER_COPY)],
    )
}

/// Live brokers `up`, the draining `broker-1`, and brokers `lost` gone
/// past the replace delay.
fn cluster(up: &[&str], lost: &[&str]) -> Vec<Node> {
    let mut nodes = live(up);
    nodes.push(node("broker-1", NodeLifecycle::Draining, None));
    nodes.extend(lost.iter().map(|id| gone(id, 10 * MINUTE)));
    nodes
}

fn pass(existing: &ShardAssignment, nodes: &[Node], rf: u32) -> Plan {
    plan_with(
        &[replicated_stream("orders", 1, rf)],
        &[],
        nodes,
        std::slice::from_ref(existing),
        &Reports::default(),
        raft(),
    )
}

/// The one copy id `plan` reports, for `node`.
fn the_vouch(plan: &Plan, node: &str) -> u64 {
    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    assert_eq!(plan.vouched.len(), 1, "{plan:?}");
    let vouched = &plan.vouched[0];
    assert_eq!(vouched.node_id, node, "{plan:?}");
    vouched.copy_id
}

/// What the store does with a copy report: note it and settle the target,
/// holding one the voters keep no majority of.
fn report(existing: &ShardAssignment, node: &str, copy: u64) -> ShardAssignment {
    let mut stored = existing.clone();
    stored.raft_copy_ids.insert(node.to_string(), copy);
    stored.settle_raft_target(true).expect("counter");
    stored
}

/// The group's leader reports `voters`, as it does once it acted on the
/// target.
fn voting(existing: &ShardAssignment, voters: &[(&str, u64)]) -> ShardAssignment {
    let mut next = existing.clone();
    let held = next.raft_voters.as_mut().expect("voters");
    held.index += 1;
    held.members = copies(voters);
    next
}

fn target(existing: &ShardAssignment) -> Option<&BTreeMap<String, u64>> {
    existing
        .raft_target
        .as_ref()
        .map(|target: &RaftTarget| &target.members)
}

/// **The issue's case.** Placement reports `broker-4`'s copy from the
/// voters, the target for `{2, 4, 5}` is stored, and once the group runs
/// on it `broker-4` is swapped for `broker-6` by a step the store takes.
#[test]
fn a_voter_that_died_after_a_set_change_is_replaced() {
    let existing = after_drain();
    let nodes = cluster(&["broker-2", "broker-5", "broker-6"], &["broker-4"]);

    let copy = the_vouch(&pass(&existing, &nodes, 3), "broker-4");
    assert_eq!(copy, VOTER_COPY);
    let settled = report(&existing, "broker-4", copy);
    assert_eq!(
        target(&settled),
        Some(&copies(&[
            ("broker-2", 7),
            ("broker-4", VOTER_COPY),
            ("broker-5", 9)
        ]))
    );

    // The voters are still the old set: the swap waits for the group.
    let plan = pass(&settled, &nodes, 3);
    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    assert!(plan.vouched.is_empty(), "{plan:?}");
    assert!(
        plan.waiting().any(|(_, why)| why == &Blocked::Unapplied),
        "{plan:?}"
    );

    let applied = voting(
        &settled,
        &[("broker-2", 7), ("broker-4", VOTER_COPY), ("broker-5", 9)],
    );
    let plan = pass(&applied, &nodes, 3);
    let moves: Vec<_> = plan.moves().collect();
    assert_eq!(moves.len(), 1, "{plan:?}");
    let (_, step, written) = moves[0];
    assert_eq!(
        step,
        &MoveStep::RaftReplace {
            from: "broker-4".into(),
            to: "broker-6".into()
        }
    );
    assert!(!applied.moves_formed_set(written, applied.incarnation));
}

/// **It comes back with the same copy.** The stored target already names
/// that copy, so nothing is reported again and the member is not swapped.
#[test]
fn a_voter_that_returns_with_its_copy_keeps_the_target() {
    let existing = after_drain();
    let down = cluster(&["broker-2", "broker-5", "broker-6"], &["broker-4"]);
    let copy = the_vouch(&pass(&existing, &down, 3), "broker-4");
    let settled = report(&existing, "broker-4", copy);

    // Its own report names the copy the target already has.
    let back = report(&settled, "broker-4", VOTER_COPY);
    assert_eq!(back, settled);
    let up = cluster(&["broker-2", "broker-4", "broker-5", "broker-6"], &[]);
    let plan = pass(&back, &up, 3);
    assert!(plan.vouched.is_empty(), "{plan:?}");
    assert!(
        plan.moves().all(
            |(_, step, _)| !matches!(step, MoveStep::RaftReplace { from, .. } if from == "broker-4")
        ),
        "{plan:?}"
    );
}

/// **It comes back with a rotated copy.** Its own report replaces the copy
/// placement reported, and placement reports nothing for it again, down or
/// up. The stored target keeps the old copy, the one the group's voter
/// holds, until the voters keep a majority of the new copies (#2905).
#[test]
fn a_voter_that_returns_with_a_rotated_copy_is_named_by_its_own_report() {
    let existing = after_drain();
    let down = cluster(&["broker-2", "broker-5", "broker-6"], &["broker-4"]);
    let copy = the_vouch(&pass(&existing, &down, 3), "broker-4");
    let settled = report(&existing, "broker-4", copy);

    // Reported before its heartbeat is seen: still down.
    let rotated = report(&settled, "broker-4", ROTATED_COPY);
    assert_eq!(rotated.raft_copy_ids.get("broker-4"), Some(&ROTATED_COPY));
    assert_eq!(target(&rotated), target(&settled));
    assert!(pass(&rotated, &down, 3).vouched.is_empty());
    let up = cluster(&["broker-2", "broker-4", "broker-5", "broker-6"], &[]);
    assert!(pass(&rotated, &up, 3).vouched.is_empty());

    // Once the group runs on `{2, 4, 5}`, the next settle names the new copy.
    let mut applied = voting(
        &rotated,
        &[("broker-2", 7), ("broker-4", VOTER_COPY), ("broker-5", 9)],
    );
    applied.settle_raft_target(true).expect("counter");
    assert_eq!(
        target(&applied).and_then(|members| members.get("broker-4")),
        Some(&ROTATED_COPY)
    );
}

/// **Two members down.** No copy is reported for either: the joint change
/// needs a majority of the new set, which two down members of three deny,
/// so the target and the swap both wait.
#[test]
fn two_members_down_still_wait() {
    let existing = after_drain();
    let nodes = cluster(&["broker-2", "broker-6"], &["broker-4", "broker-5"]);
    let plan = pass(&existing, &nodes, 3);
    assert!(plan.vouched.is_empty(), "{plan:?}");
    assert_eq!(plan.moves().count(), 0, "{plan:?}");

    // Or the other live voter down.
    let nodes = cluster(&["broker-5", "broker-6"], &["broker-2", "broker-4"]);
    let plan = pass(&existing, &nodes, 3);
    assert!(plan.vouched.is_empty(), "{plan:?}");
    assert!(
        plan.moves()
            .all(|(_, step, _)| !matches!(step, MoveStep::RaftReplace { .. })),
        "{plan:?}"
    );
}

/// **RF 2.** `{1, 4}` formed, `broker-1` drained and swapped for
/// `broker-5`, `broker-4` died. The target for `{4, 5}` is stored from the
/// voters, but the group of two needs `broker-4`'s vote for any change, so
/// the swap waits rather than leave the group's voters behind.
#[test]
fn a_voter_that_died_at_rf_2_settles_the_target_and_waits() {
    let existing = changed(
        "broker-5",
        &["broker-4"],
        &[("broker-5", 9)],
        &[("broker-1", 7), ("broker-4", VOTER_COPY)],
    );
    let nodes = cluster(&["broker-5", "broker-6"], &["broker-4"]);

    let copy = the_vouch(&pass(&existing, &nodes, 2), "broker-4");
    let settled = report(&existing, "broker-4", copy);
    assert_eq!(
        target(&settled),
        Some(&copies(&[("broker-4", VOTER_COPY), ("broker-5", 9)]))
    );

    let plan = pass(&settled, &nodes, 2);
    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    assert!(
        plan.waiting().any(|(_, why)| why == &Blocked::Unapplied),
        "{plan:?}"
    );
}

/// A down member the voters do not name gets nothing reported: that is a
/// seat that never opened, given away instead (#3310).
#[test]
fn a_dead_member_the_voters_do_not_name_is_not_reported_for() {
    let existing = changed(
        "broker-2",
        &["broker-4", "broker-5"],
        &[("broker-2", 7), ("broker-5", 9)],
        &[("broker-1", 7), ("broker-2", 7), ("broker-3", 7)],
    );
    let nodes = cluster(&["broker-2", "broker-5", "broker-6"], &["broker-4"]);
    assert!(pass(&existing, &nodes, 3).vouched.is_empty());
}
