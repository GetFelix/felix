//! The majority a `Quorum` acknowledgement rests on.

use super::*;

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
    let followers = vec![cursor(8), cursor(3)];

    assert_eq!(quorum_offset(10, &followers), 8);
}

/// All of two: the quorum point is the slower of the pair, because a set of
/// two needs both.
#[test]
fn a_set_of_two_waits_for_its_only_follower() {
    assert_eq!(quorum_offset(10, &[cursor(4)]), 4);
}

/// A follower reporting past the leader's tail does not drag the quorum
/// point beyond what the leader actually holds.
#[test]
fn a_follower_ahead_of_the_leader_does_not_overstate_the_quorum() {
    assert_eq!(quorum_offset(10, &[cursor(50), cursor(9)]), 10);
}

/// **A halted follower counts for nothing.** It has stopped rather than
/// fallen behind, and letting its last position count toward a majority
/// makes an acknowledgement mean less than it says.
#[test]
fn a_halted_follower_does_not_count_toward_the_majority() {
    let mut halted = cursor(10);
    halted.halted = Some(Halt::Diverged);
    let followers = vec![halted, cursor(3)];

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
    let mut a = cursor(10);
    a.halted = Some(Halt::Diverged);
    let mut b = cursor(10);
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
    assert_eq!(quorum_offset(10, &[cursor(0), cursor(7)]), 7);
}
