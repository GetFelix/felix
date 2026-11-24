use std::collections::BTreeSet;

use felix_storage::raft_log::LogPoint;

use super::{Allowance, BUDGET, Granting, Held};
use crate::raft::types::MemberId;

fn member(name: &str) -> MemberId {
    MemberId::new(name, 1)
}

fn voters(names: &[&str]) -> Vec<BTreeSet<MemberId>> {
    vec![names.iter().map(|name| member(name)).collect()]
}

fn at(term: u64, index: u64) -> Option<LogPoint> {
    Some(LogPoint { term, index })
}

fn held(term: u64, end: u64) -> Held {
    Held {
        leader: member("a"),
        allowance: Allowance { term, seq: 1, end },
    }
}

#[test]
fn a_voter_refuses_a_candidate_short_of_the_allowance() {
    let held = held(3, 10);
    assert!(held.refuses(&member("b"), at(3, 8)));
    assert!(held.refuses(&member("b"), at(2, 50)));
    assert!(held.refuses(&member("b"), None));
    assert!(!held.refuses(&member("b"), at(3, 9)));
    assert!(!held.refuses(&member("b"), at(4, 0)));
}

#[test]
fn the_leader_that_named_it_is_let_through_under_its_own_member_id() {
    let held = held(3, 10);
    assert!(!held.refuses(&member("a"), at(3, 2)));
    // A rotated copy of the same broker is another member.
    assert!(held.refuses(&MemberId::new("a", 2), at(3, 2)));
}

#[test]
fn an_allowance_of_nothing_refuses_nobody() {
    assert!(!held(3, 0).refuses(&member("b"), None));
}

#[test]
fn a_follower_keeps_the_newest_allowance_even_when_it_is_lower() {
    let had = held(3, 10);
    let newer = Allowance {
        term: 3,
        seq: 2,
        end: 4,
    };
    assert!(Held::replaced_by(Some(&had), &newer));
    let stale = Allowance {
        term: 3,
        seq: 1,
        end: 40,
    };
    assert!(!Held::replaced_by(Some(&had), &stale));
    let older_term = Allowance {
        term: 2,
        seq: 9,
        end: 40,
    };
    assert!(!Held::replaced_by(Some(&had), &older_term));
    assert!(Held::replaced_by(None, &stale));
}

#[test]
fn a_writing_leader_asks_its_budget_and_an_idle_one_its_log_end() {
    let mut granting = Granting::default();
    assert_eq!(granting.name(3, 10, true).end, 10 + BUDGET);
    assert_eq!(granting.name(3, 12, false).end, 12);
}

#[test]
fn nothing_is_granted_until_a_majority_echoes() {
    let set = voters(&["a", "b", "c"]);
    let mut granting = Granting::default();
    let named = granting.name(3, 10, true);
    assert_eq!(granting.granted(3, &set, &member("a")), 0);
    granting.echoed(&member("b"), named);
    assert_eq!(granting.granted(3, &set, &member("a")), named.end);
    // Not in another term.
    assert_eq!(granting.granted(4, &set, &member("a")), 0);
}

#[test]
fn an_echo_of_another_term_counts_for_nothing() {
    let set = voters(&["a", "b", "c"]);
    let mut granting = Granting::default();
    let named = granting.name(3, 10, true);
    granting.echoed(&member("b"), Allowance { term: 2, ..named });
    assert_eq!(granting.granted(3, &set, &member("a")), 0);
}

#[test]
fn an_echo_counts_for_the_lowest_allowance_named_after_it() {
    // b echoed the large allowance, then may have taken the shrink that
    // followed and not said so yet. It counts for the shrink.
    let set = voters(&["a", "b", "c"]);
    let mut granting = Granting::default();
    let large = granting.name(3, 10, true);
    granting.echoed(&member("b"), large);
    granting.name(3, 12, false);
    granting.name(3, 13, true);
    assert_eq!(granting.granted(3, &set, &member("a")), 12);
}

#[test]
fn a_joint_membership_needs_a_majority_of_both_sets() {
    let set = vec![
        voters(&["a", "b", "c"]).remove(0),
        voters(&["a", "d", "e"]).remove(0),
    ];
    let mut granting = Granting::default();
    let named = granting.name(3, 10, true);
    granting.echoed(&member("b"), named);
    assert_eq!(granting.granted(3, &set, &member("a")), 0);
    granting.echoed(&member("d"), named);
    assert_eq!(granting.granted(3, &set, &member("a")), named.end);
}

#[test]
fn an_echo_older_than_every_kept_allowance_counts_for_nothing() {
    let set = voters(&["a", "b", "c"]);
    let mut granting = Granting::default();
    let first = granting.name(3, 1, true);
    granting.echoed(&member("b"), first);
    for end in 2..200 {
        granting.name(3, end, true);
    }
    assert_eq!(granting.granted(3, &set, &member("a")), 0);
}
