//! The majority a `Quorum` acknowledgement rests on.

use super::*;
use crate::quorum::held_by_both;

/// A majority always includes the leader, so a set of one is satisfied by
/// the leader alone — which is why `replication_factor: 1` costs nothing.
#[test]
fn a_majority_counts_the_leader() {
    assert_eq!(majority_of(0), 1, "a set of one is the leader alone");
    assert_eq!(majority_of(1), 2, "a set of two needs both");
    assert_eq!(majority_of(2), 2, "a set of three needs two");
    assert_eq!(majority_of(3), 3, "a set of four needs three");
    assert_eq!(majority_of(4), 3, "a set of five needs three");
}

/// **A majority is more than half.** Half of an even set is not a majority:
/// two disjoint halves could each acknowledge a different record, and both
/// would believe they had a quorum.
#[test]
fn half_of_an_even_set_is_not_a_majority() {
    for replicas in [1usize, 3, 5] {
        let set = replicas + 1;
        assert!(
            majority_of(replicas) * 2 > set,
            "a set of {set} accepted {} as a majority",
            majority_of(replicas),
        );
    }
}

/// With no followers the leader's own tail is the quorum point, so a
/// `Quorum` stream on an unreplicated shard behaves exactly like `Leader`
/// rather than never acknowledging.
#[test]
fn the_leader_alone_is_a_quorum_of_one() {
    assert_eq!(quorum_offset(10, &[]), 10);
}

/// Two of three: the quorum point is where the faster follower has got to,
/// not the slower one.
#[test]
fn a_set_of_three_advances_with_its_faster_follower() {
    let followers = vec![answered(8), answered(3)];

    assert_eq!(quorum_offset(10, &followers), 8);
}

/// All of two: the quorum point is the slower of the pair, because a set of
/// two needs both.
#[test]
fn a_set_of_two_waits_for_its_only_follower() {
    assert_eq!(quorum_offset(10, &[answered(4)]), 4);
}

/// A follower reporting past the leader's tail does not drag the quorum
/// point beyond what the leader actually holds.
#[test]
fn a_follower_ahead_of_the_leader_does_not_overstate_the_quorum() {
    assert_eq!(quorum_offset(10, &[answered(50), answered(9)]), 10);
}

/// **A halted follower counts for nothing.** It has stopped rather than
/// fallen behind, and letting its last position count toward a majority
/// makes an acknowledgement mean less than it says.
#[test]
fn a_halted_follower_does_not_count_toward_the_majority() {
    let mut halted = answered(10);
    halted.halted = Some(Halt::Diverged);
    let followers = vec![halted, answered(3)];

    assert_eq!(
        quorum_offset(10, &followers),
        3,
        "a diverged follower was counted toward the quorum",
    );
}

/// Every follower halted leaves the leader alone, which is not a majority
/// of three — so nothing new reaches the quorum point.
#[test]
fn a_shard_with_every_follower_halted_stops_acknowledging() {
    let mut a = answered(10);
    a.halted = Some(Halt::Diverged);
    let mut b = answered(10);
    b.halted = Some(Halt::Fenced);

    assert_eq!(
        quorum_offset(10, &[a, b]),
        0,
        "a quorum was claimed with only the leader in a set of three",
    );
}

/// A follower that has stored nothing holds offset zero, so a set of three
/// with one empty follower still has a quorum through the other.
#[test]
fn an_empty_follower_holds_nothing_but_still_counts_as_a_member() {
    assert_eq!(quorum_offset(10, &[answered(0), answered(7)]), 7);
}

/// **A follower counts toward the majority only as far as it has answered**,
/// not from where the leader guessed it stands (#878). Counted from the guess,
/// a new leader that reached none of its followers published a mark over the
/// records it inherited, raised its own commit offset to it, and once
/// deposed refused to drop those records, so it never rejoined.
#[test]
fn a_follower_not_yet_heard_from_counts_for_nothing() {
    assert_eq!(quorum_offset(10, &[cursor(9), cursor(9)]), 0);
    assert_eq!(quorum_offset(10, &[cursor(9), answered(10)]), 10);
}

fn node_cursor(node: &str, confirmed: u64) -> FollowerCursor {
    let mut cursor = answered(confirmed);
    cursor.node_id = node.to_string();
    cursor
}

fn set(nodes: &[&str]) -> Vec<String> {
    nodes.iter().map(|node| node.to_string()).collect()
}

/// **A pending change holds an acknowledgement to the least of both
/// majorities.** The new set's majority is at 9; the set being left
/// {a, b, c} has the leader and `b` at 4, so 4 is what both hold.
#[test]
fn a_pending_change_holds_the_least_of_both_majorities() {
    let followers = [node_cursor("b", 4), node_cursor("d", 9)];

    let held = held_by_both(9, 10, true, "a", &followers, &set(&["a", "b", "c"]), None);

    assert_eq!(held, 4);
}

/// **A majority of the set being left alone is not enough.** The new set's
/// majority holds nothing, so nothing is acknowledged however far the set
/// being left has got.
#[test]
fn an_old_majority_alone_acknowledges_nothing() {
    let followers = [node_cursor("b", 10), node_cursor("c", 10)];

    let held = held_by_both(0, 10, true, "a", &followers, &set(&["a", "b", "c"]), None);

    assert_eq!(held, 0);
}

/// **A member the leader has no cursor for holds nothing**, and a halted one
/// holds nothing either, so a set it cannot reach keeps the mark back.
#[test]
fn a_member_with_no_cursor_or_a_halted_one_holds_nothing() {
    let mut halted = node_cursor("c", 10);
    halted.halted = Some(crate::Halt::Fenced);
    let followers = [halted];

    let held = held_by_both(10, 10, true, "a", &followers, &set(&["a", "b", "c"]), None);

    assert_eq!(held, 0);
}

/// **An unknown set being left acknowledges nothing**, and a leader that no
/// longer counts its own tail does not count it in the set being left.
#[test]
fn an_unknown_set_being_left_or_a_stopped_leader_acknowledges_nothing() {
    let followers = [node_cursor("b", 10)];

    assert_eq!(held_by_both(10, 10, true, "a", &followers, &[], None), 0);
    assert_eq!(
        held_by_both(10, 10, false, "a", &followers, &set(&["a", "b", "c"]), None),
        0
    );
}

/// **A learner the set being left names is counted both ways**: a majority of
/// the set with it and without it must hold the record.
#[test]
fn a_learner_in_the_set_being_left_is_counted_both_ways() {
    let followers = [node_cursor("b", 10), node_cursor("c", 0)];

    // {a, b, c} has a majority at 10 (a, b); {a, b} without the learner `c`
    // needs both and also has it.
    assert_eq!(
        held_by_both(
            10,
            10,
            true,
            "a",
            &followers,
            &set(&["a", "b", "c"]),
            Some("c")
        ),
        10
    );
    // {a, c, d} with `d` the learner: without it {a, c} needs both.
    let followers = [node_cursor("c", 3), node_cursor("d", 10)];
    assert_eq!(
        held_by_both(
            10,
            10,
            true,
            "a",
            &followers,
            &set(&["a", "c", "d"]),
            Some("d")
        ),
        3
    );
}
