//! The lag figure an operator reads to size the `Leader` loss window.

use super::*;

#[test]
fn lag_is_measured_against_the_slowest_follower() {
    let mut behind = cursor(10);
    behind.node_id = "broker-c".to_string();
    let followers = vec![cursor(90), behind];

    assert_eq!(lag_records(100, &followers), Some(90));
}

#[test]
fn a_caught_up_follower_reports_no_lag() {
    assert_eq!(lag_records(100, &[cursor(100)]), Some(0));
}

/// A follower ahead of this leader's tail is not negative lag. It happens
/// while an answer is in flight, and wrapping would report an enormous one.
#[test]
fn a_follower_ahead_of_the_tail_does_not_wrap() {
    assert_eq!(lag_records(100, &[cursor(105)]), Some(0));
}

/// **A halted follower is not lagging, it has stopped.** Folding the two
/// together hides a stopped follower behind a number that merely looks
/// large, and the two need different responses.
#[test]
fn a_halted_follower_is_left_out_of_the_lag() {
    let mut halted = cursor(0);
    halted.halted = Some(Halt::Diverged);

    assert_eq!(lag_records(100, &[cursor(95), halted]), Some(5));
}

#[test]
fn no_followers_means_no_lag_to_report() {
    assert_eq!(lag_records(100, &[]), None);
}
