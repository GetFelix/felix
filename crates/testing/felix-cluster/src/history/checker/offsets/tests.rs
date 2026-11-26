//! Hand-built histories for rule 22: each way of holding one offset as two
//! records is reported exactly once, and a record seen many times, or an
//! offset nobody saw, is not.

use std::collections::BTreeMap;

use super::super::super::model::{
    Action, AppendOutcome, Consistency, Element, History, ListSpec, Op, Resume, Subscription,
};
use super::super::{Rule, Violation, check as check_all};
use super::check;

/// List `q` (`Quorum`, its final read not taken) and list `l` (`Leader`),
/// both based at offset 10. Values 1 to 3 are appended to `q` and
/// acknowledged at 10, 11 and 13 (12 is a blank entry); values 4 and 5 to
/// `l`, acknowledged at 10 and 11.
fn history() -> History {
    let lists = BTreeMap::from([
        (
            "q".to_string(),
            ListSpec {
                consistency: Consistency::Quorum,
                base: 10,
            },
        ),
        (
            "l".to_string(),
            ListSpec {
                consistency: Consistency::Leader,
                base: 10,
            },
        ),
    ]);
    let ops = [
        ("q", 1, 10),
        ("q", 2, 11),
        ("q", 3, 13),
        ("l", 4, 10),
        ("l", 5, 11),
    ]
    .iter()
    .map(|&(list, value, offset)| Op {
        process: 0,
        invoke: value * 10,
        complete: value * 10 + 1,
        action: Action::Append {
            list: list.to_string(),
            value,
            outcome: AppendOutcome::Ok {
                offset: Some(offset),
            },
        },
    })
    .collect();
    History {
        lists,
        ops,
        final_reads: BTreeMap::from([("l".to_string(), elements(&[(10, 4), (11, 5)]))]),
        unread_lists: ["q".to_string()].into(),
        ..History::default()
    }
}

fn elements(pairs: &[(u64, u64)]) -> Vec<Element> {
    pairs
        .iter()
        .map(|&(offset, value)| Element {
            offset,
            value,
            skipped_before: u64::from(offset == 13),
        })
        .collect()
}

fn read(list: &str, seen: &[(u64, u64)]) -> Op {
    Op {
        process: 1,
        invoke: 100,
        complete: 101,
        action: Action::Read {
            list: list.to_string(),
            observed: elements(seen),
        },
    }
}

fn subscriber(subscriber: usize, list: &str, delivered: &[(u64, u64)]) -> Subscription {
    let delivered = elements(delivered);
    Subscription {
        subscriber,
        list: list.to_string(),
        start: 10,
        next: delivered.last().map_or(10, |e| e.offset + 1),
        delivered,
        resumes: Vec::new(),
    }
}

fn violations(history: &History) -> Vec<Violation> {
    let mut out = Vec::new();
    check(history, &mut out);
    out
}

#[test]
fn two_subscribers_holding_two_values_at_one_offset_is_one_violation() {
    let mut history = history();
    history.final_reads.remove("l");
    history.subscriptions = vec![
        subscriber(0, "l", &[(10, 4), (11, 5)]),
        subscriber(1, "l", &[(10, 4), (11, 6)]),
    ];
    let found = violations(&history);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].rule, Rule::OffsetReuse);
    assert_eq!(found[0].list, "l");
    assert!(
        found[0]
            .explanation
            .starts_with("offset 11 holds 5 in event 1 delivered to subscriber 0")
            && found[0]
                .explanation
                .ends_with("6 in event 1 delivered to subscriber 1"),
        "{}",
        found[0].explanation
    );
    // Through the whole checker too, which says nothing else about a
    // `Leader` list's subscribers.
    assert!(check_all(&history).has(Rule::OffsetReuse));
}

#[test]
fn an_acknowledged_offset_read_back_as_another_record_is_a_violation() {
    let mut history = history();
    // Value 2 was acknowledged at 11; a follower later serves 3 there.
    history.ops.push(read("q", &[(10, 1), (11, 3)]));
    let found = violations(&history);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].ops, vec![1, 5], "the append and the read");
    assert!(
        found[0]
            .explanation
            .starts_with("offset 11 holds 2 in the acknowledgement of op #1"),
        "{}",
        found[0].explanation
    );
}

#[test]
fn a_leader_acknowledgement_lost_to_a_failover_is_not_an_observation() {
    let mut history = history();
    // Value 5's leader acknowledgement was lost and 6 took offset 11; every
    // reader saw only 6.
    history
        .final_reads
        .insert("l".to_string(), elements(&[(10, 4), (11, 6)]));
    history.ops.push(read("l", &[(11, 6)]));
    history.subscriptions = vec![subscriber(0, "l", &[(10, 4), (11, 6)])];
    assert_eq!(violations(&history), Vec::new());

    // But two readers of a `Leader` list must still agree.
    history.ops.push(read("l", &[(10, 4), (11, 5)]));
    let found = violations(&history);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].list, "l");
}

#[test]
fn one_record_seen_many_times_is_not_a_violation() {
    let mut history = history();
    history.ops.push(read("q", &[(10, 1), (11, 2)]));
    history.ops.push(read("q", &[(11, 2), (13, 3)]));
    history.ops.push(read("l", &[(10, 4), (11, 5)]));
    let mut redelivered = subscriber(0, "q", &[(10, 1), (11, 2), (11, 2), (13, 3)]);
    redelivered.resumes.push(Resume {
        at: 50,
        from: 11,
        first: 2,
        reason: "the subscription failed".to_string(),
    });
    history.subscriptions = vec![
        redelivered,
        subscriber(1, "q", &[(13, 3)]),
        subscriber(2, "l", &[(10, 4), (11, 5)]),
    ];
    assert_eq!(violations(&history), Vec::new());
}

#[test]
fn an_offset_nobody_saw_is_not_a_violation() {
    let mut history = history();
    // Offset 12 is a term's blank entry: no read, delivery or
    // acknowledgement ever names it, and a reader skips it.
    history.ops.push(read("q", &[(11, 2), (13, 3)]));
    history.subscriptions = vec![subscriber(0, "q", &[(10, 1), (13, 3)])];
    // Records below the base belong to the harness, whatever they hold.
    history.ops.push(read("q", &[(9, 77), (10, 1)]));
    history.ops.push(read("q", &[(9, 78)]));
    assert_eq!(violations(&history), Vec::new());
}

#[test]
fn a_quorum_list_with_a_final_read_is_left_to_rules_3_and_10() {
    let mut history = history();
    history.unread_lists.clear();
    history
        .final_reads
        .insert("q".to_string(), elements(&[(10, 1), (11, 2), (13, 3)]));
    history.subscriptions = vec![subscriber(0, "q", &[(10, 1), (11, 9)])];
    assert_eq!(violations(&history), Vec::new());
    let report = check_all(&history);
    assert!(report.has(Rule::LostDelivery), "{report}");
    assert!(!report.has(Rule::OffsetReuse), "{report}");
}

#[test]
fn each_reused_offset_is_reported_once() {
    let mut history = history();
    for _ in 0..3 {
        history.ops.push(read("q", &[(10, 9), (11, 8)]));
    }
    let found = violations(&history);
    let offsets: Vec<&str> = found
        .iter()
        .map(|v| v.explanation.split(' ').nth(1).expect("an offset"))
        .collect();
    assert_eq!(offsets, vec!["10", "11"], "{found:?}");
}
