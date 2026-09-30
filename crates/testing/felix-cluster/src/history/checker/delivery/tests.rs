//! Hand-built subscriptions: a clean one passes, a visible drop is counted
//! and passes, and each way of breaking rules 9 to 11 is caught under its own
//! rule.

use std::collections::BTreeMap;

use super::super::super::model::{
    Action, AppendOutcome, Consistency, Element, History, ListSpec, Op, Resume, Subscription,
};
use super::super::{Rule, check};
use super::dropped;

/// List `a`, based at offset 10, holding values 0 to 3 at offsets 10, 11, 13
/// and 14 (12 is a generation-start record), each appended and acknowledged. `delivered` is `(offset, value, skipped_before)`.
fn history(delivered: &[(u64, u64, u64)], resumes: Vec<Resume>) -> History {
    let log = [(10, 0, 0), (11, 1, 0), (13, 2, 1), (14, 3, 0)];
    let mut lists = BTreeMap::new();
    lists.insert(
        "a".to_string(),
        ListSpec {
            consistency: Consistency::Quorum,
            base: 10,
        },
    );
    let mut final_reads = BTreeMap::new();
    final_reads.insert("a".to_string(), elements(&log));
    let ops = log
        .iter()
        .map(|&(_, value, _)| Op {
            process: 0,
            invoke: value * 10,
            complete: value * 10 + 1,
            action: Action::Append {
                list: "a".to_string(),
                value,
                outcome: AppendOutcome::Ok { offset: None },
            },
        })
        .collect();
    let delivered = elements(delivered);
    let next = delivered.last().map_or(10, |e| e.offset + 1);
    History {
        lists,
        ops,
        final_reads,
        subscriptions: vec![Subscription {
            subscriber: 0,
            list: "a".to_string(),
            start: 10,
            delivered,
            resumes,
            next,
        }],
        ..History::default()
    }
}

fn elements(triples: &[(u64, u64, u64)]) -> Vec<Element> {
    triples
        .iter()
        .map(|&(offset, value, skipped_before)| Element {
            offset,
            value,
            skipped_before,
        })
        .collect()
}

fn resume(from: u64, first: usize) -> Resume {
    Resume {
        at: 1_000_000,
        from,
        first,
        reason: "the subscription failed".to_string(),
    }
}

fn assert_only(history: &History, rule: Rule) {
    let report = check(history);
    assert!(report.has(rule), "expected a {rule} violation:\n{report}");
    assert!(
        report.violations.iter().all(|v| v.rule == rule),
        "expected only {rule}:\n{report}"
    );
}

#[test]
fn a_whole_delivery_across_a_resume_is_valid() {
    let h = history(
        &[(10, 0, 0), (11, 1, 0), (13, 2, 1), (14, 3, 0)],
        vec![resume(13, 2)],
    );
    let report = check(&h);
    assert!(report.is_valid(), "{report}");
    assert_eq!(dropped(&h.subscriptions[0], 10, &h.final_reads["a"]), 0);
}

/// Offset 11 was dropped, and the jump to 13 shows it; offset 12 holds no
/// value, so the jump over it is no drop.
#[test]
fn a_visible_drop_is_counted_not_reported() {
    let h = history(&[(10, 0, 0), (13, 2, 0), (14, 3, 0)], vec![]);
    let report = check(&h);
    assert!(report.is_valid(), "{report}");
    assert_eq!(dropped(&h.subscriptions[0], 10, &h.final_reads["a"]), 1);
}

#[test]
fn a_delivered_record_the_final_log_lacks_is_lost() {
    // A different value at 11, and a record past the final log's end.
    let h = history(
        &[(10, 0, 0), (11, 9, 0), (13, 2, 1), (14, 3, 0), (15, 4, 0)],
        vec![],
    );
    let report = check(&h);
    assert_eq!(
        report
            .violations
            .iter()
            .filter(|v| v.rule == Rule::LostDelivery)
            .count(),
        2,
        "{report}"
    );
    assert_only(&h, Rule::LostDelivery);
}

#[test]
fn offsets_delivered_out_of_order_break_delivery_order() {
    let h = history(&[(10, 0, 0), (13, 2, 1), (11, 1, 0), (14, 3, 0)], vec![]);
    assert_only(&h, Rule::DeliveryOrder);
}

#[test]
fn a_resume_that_redelivers_breaks_delivery_order() {
    let h = history(
        &[(10, 0, 0), (11, 1, 0), (11, 1, 0), (13, 2, 1), (14, 3, 0)],
        vec![resume(11, 2)],
    );
    let report = check(&h);
    assert_eq!(report.violations.len(), 1, "{report}");
    assert!(
        report.violations[0]
            .explanation
            .contains("resumed from offset 11"),
        "{report}"
    );
    assert_only(&h, Rule::DeliveryOrder);
}

/// Offset 11 holds a value, yet the event at 13 said 11 and 12 hold none.
#[test]
fn a_skip_over_a_record_is_a_missing_delivery() {
    let h = history(&[(10, 0, 0), (13, 2, 2), (14, 3, 0)], vec![]);
    assert_only(&h, Rule::MissingDelivery);
}

#[test]
fn a_subscriber_that_stops_short_is_a_missing_delivery() {
    let h = history(&[(10, 0, 0), (11, 1, 0)], vec![]);
    assert_only(&h, Rule::MissingDelivery);
    let nothing = history(&[], vec![]);
    assert_only(&nothing, Rule::MissingDelivery);
}
