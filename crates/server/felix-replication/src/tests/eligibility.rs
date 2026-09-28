//! Which followers the leader reports as fit to lead.

use super::*;

#[test]
fn a_follower_level_with_the_leader_is_caught_up() {
    assert_eq!(caught_up(10, &[cursor(10)]), vec!["broker-b".to_string()]);
}

/// **Behind is not caught up.** The bound is zero, so a follower missing
/// even one record cannot lead: promoting it would lose that record, and a
/// bound above zero is a decision about how much loss is acceptable that
/// nobody has made.
#[test]
fn a_follower_missing_even_one_record_is_not_caught_up() {
    assert!(caught_up(10, &[cursor(9)]).is_empty());
}

/// **A halted follower never qualifies**, however close it was. It has
/// stopped rather than fallen behind, so its position is not moving toward
/// the leader's and never will without intervention.
#[test]
fn a_halted_follower_is_never_reported_as_caught_up() {
    for halt in [Halt::Diverged, Halt::Fenced, Halt::NeedsBootstrap] {
        let mut stopped = cursor(10);
        stopped.halted = Some(halt);

        assert!(
            caught_up(10, &[stopped]).is_empty(),
            "{halt:?} was reported as fit to lead",
        );
    }
}

/// A follower reporting past the leader's tail is caught up rather than
/// rejected: it happens while an answer is in flight.
#[test]
fn a_follower_ahead_of_the_tail_is_caught_up() {
    assert_eq!(caught_up(10, &[cursor(11)]), vec!["broker-b".to_string()]);
}

#[test]
fn only_the_caught_up_followers_are_named() {
    let mut behind = cursor(3);
    behind.node_id = "broker-c".to_string();

    assert_eq!(
        caught_up(10, &[cursor(10), behind]),
        vec!["broker-b".to_string()]
    );
}

/// An empty replica set reports nobody, rather than the leader itself.
#[test]
fn a_shard_with_no_followers_reports_nobody() {
    assert!(caught_up(10, &[]).is_empty());
}
