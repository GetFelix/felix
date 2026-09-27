//! Hand-built histories, one per rule, and valid ones that must pass.
//!
//! These are what stop the checker from quietly accepting everything: each
//! violating history has to be reported under its own rule, and each valid
//! one, however concurrent or uncertain, has to pass.

use std::collections::BTreeMap;

use super::super::model::{
    Action, AppendOutcome, Consistency, Element, FOREIGN_PAYLOAD, History, ListSpec, Op,
};
use super::super::rng::Rng;
use super::{Rule, check};

const OK: AppendOutcome = AppendOutcome::Ok { offset: None };
const FAIL: AppendOutcome = AppendOutcome::Fail;
const INFO: AppendOutcome = AppendOutcome::Info;

fn append(
    process: usize,
    invoke: u64,
    complete: u64,
    list: &str,
    value: u64,
    outcome: AppendOutcome,
) -> Op {
    Op {
        process,
        invoke,
        complete,
        action: Action::Append {
            list: list.to_string(),
            value,
            outcome,
        },
    }
}

fn read(process: usize, invoke: u64, complete: u64, list: &str, seen: &[(u64, u64)]) -> Op {
    Op {
        process,
        invoke,
        complete,
        action: Action::Read {
            list: list.to_string(),
            observed: elements(seen),
        },
    }
}

fn elements(pairs: &[(u64, u64)]) -> Vec<Element> {
    pairs
        .iter()
        .map(|&(offset, value)| Element { offset, value })
        .collect()
}

/// One `Quorum` list `a`, based at offset 0, with this final read.
fn history(ops: Vec<Op>, final_a: &[(u64, u64)]) -> History {
    let mut lists = BTreeMap::new();
    lists.insert(
        "a".to_string(),
        ListSpec {
            consistency: Consistency::Quorum,
            base: 0,
        },
    );
    let mut final_reads = BTreeMap::new();
    final_reads.insert("a".to_string(), elements(final_a));
    History {
        lists,
        ops,
        final_reads,
        faults: Vec::new(),
    }
}

fn assert_valid(history: &History) {
    let report = check(history);
    assert!(report.is_valid(), "a valid history was rejected:\n{report}");
}

fn assert_only(history: &History, rule: Rule) {
    let report = check(history);
    assert!(report.has(rule), "expected a {rule} violation:\n{report}");
    let others: Vec<_> = report
        .violations
        .iter()
        .filter(|v| v.rule != rule)
        .collect();
    assert!(others.is_empty(), "expected only {rule}:\n{report}");
}

// --- valid histories ---------------------------------------------------------

/// Overlapping appends may land in either order; an unknown append may land
/// or not; a failed one is absent; reads see prefixes, possibly with holes
/// where a subscriber dropped a record.
#[test]
fn a_concurrent_history_with_unknown_outcomes_is_valid() {
    let ops = vec![
        append(0, 0, 10, "a", 0, OK),
        // Concurrent with the first, and landed before it.
        append(1, 5, 15, "a", 1, OK),
        // Unknown, landed.
        append(2, 12, 30, "a", 2, INFO),
        // Unknown, did not land.
        append(3, 13, 31, "a", 3, INFO),
        // Refused, and absent.
        append(4, 14, 16, "a", 4, FAIL),
        read(5, 2, 3, "a", &[]),
        read(5, 16, 17, "a", &[(0, 1), (1, 0)]),
        // A hole where a subscriber dropped offset 1.
        read(5, 32, 33, "a", &[(0, 1), (2, 2)]),
        // Saw the unknown append before its client gave up on it.
        read(4, 20, 21, "a", &[(0, 1), (1, 0), (2, 2)]),
        // Acknowledged at the offset it landed at.
        append(0, 40, 41, "a", 5, AppendOutcome::Ok { offset: Some(3) }),
    ];
    assert_valid(&history(ops, &[(0, 1), (1, 0), (2, 2), (3, 5)]));
}

/// Records below the base are not the workload's and are not checked.
#[test]
fn records_below_the_base_are_ignored() {
    let mut h = history(vec![append(0, 0, 1, "a", 0, OK)], &[(7, 0)]);
    h.lists.get_mut("a").unwrap().base = 7;
    h.ops
        .push(read(1, 2, 3, "a", &[(5, FOREIGN_PAYLOAD), (7, 0)]));
    assert_valid(&h);
}

/// An empty run is valid, and so is a list nobody wrote.
#[test]
fn an_empty_history_is_valid() {
    assert_valid(&history(Vec::new(), &[]));
}

/// Histories from a correct log, however they interleave, always pass.
///
/// Each append takes effect at a random instant inside its own interval (an
/// unknown one possibly after its client gave up), offsets follow those
/// instants, and each read sees exactly what took effect before its own.
/// That is a linearizable log, so any report against it is a checker bug.
#[test]
fn random_linearizable_histories_are_valid() {
    for seed in 0..200 {
        assert_valid(&linearizable(seed, 60));
    }
}

/// Under `Leader`, an acknowledged append may be lost to a failover.
#[test]
fn a_leader_list_may_lose_an_acknowledged_append() {
    let mut h = history(vec![append(0, 0, 1, "a", 0, OK)], &[]);
    h.lists.get_mut("a").unwrap().consistency = Consistency::Leader;
    assert_valid(&h);
}

// --- rule 1: lost writes -----------------------------------------------------

#[test]
fn an_acknowledged_append_missing_from_the_final_read_is_lost() {
    let h = history(
        vec![append(0, 0, 1, "a", 0, OK), append(1, 2, 3, "a", 1, OK)],
        &[(0, 0)],
    );
    assert_only(&h, Rule::LostWrite);
    let report = check(&h);
    assert_eq!(report.violations[0].ops, vec![1]);
}

/// Seen by a read and then gone: lost, and the read was not a prefix of the
/// final log either.
#[test]
fn an_acknowledged_append_that_was_read_and_then_vanished_is_lost() {
    let h = history(
        vec![
            append(0, 0, 1, "a", 0, OK),
            append(0, 2, 3, "a", 1, OK),
            read(1, 4, 5, "a", &[(0, 0), (1, 1)]),
        ],
        &[(0, 0)],
    );
    let report = check(&h);
    assert!(report.has(Rule::LostWrite), "{report}");
    assert!(report.has(Rule::NotAPrefix), "{report}");
    assert!(
        format!("{report}").contains("op #2 had read it at offset 1"),
        "{report}"
    );
}

// --- rule 2: duplicates ------------------------------------------------------

#[test]
fn a_value_twice_in_the_final_read_is_a_duplicate() {
    let h = history(vec![append(0, 0, 1, "a", 0, OK)], &[(0, 0), (1, 0)]);
    assert_only(&h, Rule::Duplicate);
}

#[test]
fn a_value_at_two_offsets_across_reads_is_a_duplicate() {
    let h = history(
        vec![
            append(0, 0, 1, "a", 0, INFO),
            append(0, 2, 3, "a", 1, OK),
            read(1, 4, 5, "a", &[(0, 1), (1, 0)]),
        ],
        &[(0, 0), (1, 1)],
    );
    let report = check(&h);
    assert!(report.has(Rule::Duplicate), "{report}");
}

// --- rule 3: prefixes and offsets ----------------------------------------------

#[test]
fn a_read_that_disagrees_with_the_final_log_is_not_a_prefix() {
    let h = history(
        vec![
            append(0, 0, 1, "a", 0, OK),
            append(1, 0, 1, "a", 1, OK),
            read(2, 5, 6, "a", &[(0, 1)]),
        ],
        &[(0, 0), (1, 1)],
    );
    let report = check(&h);
    assert!(report.has(Rule::NotAPrefix), "{report}");
    assert!(format!("{report}").contains("offset 0 holds 0"), "{report}");
}

#[test]
fn a_read_whose_offsets_go_backwards_is_not_a_prefix() {
    let h = history(
        vec![
            append(0, 0, 1, "a", 0, OK),
            append(0, 2, 3, "a", 1, OK),
            read(2, 5, 6, "a", &[(1, 1), (0, 0)]),
        ],
        &[(0, 0), (1, 1)],
    );
    assert_only(&h, Rule::NotAPrefix);
}

#[test]
fn a_read_past_the_end_of_the_final_log_is_not_a_prefix() {
    let h = history(
        vec![
            append(0, 0, 1, "a", 0, OK),
            append(0, 2, 3, "a", 1, INFO),
            read(2, 5, 6, "a", &[(0, 0), (1, 1)]),
        ],
        &[(0, 0)],
    );
    assert_only(&h, Rule::NotAPrefix);
}

#[test]
fn an_append_acknowledged_at_one_offset_and_found_at_another_is_not_a_prefix() {
    let h = history(
        vec![
            append(0, 0, 1, "a", 0, OK),
            append(0, 2, 3, "a", 1, AppendOutcome::Ok { offset: Some(0) }),
        ],
        &[(0, 0), (1, 1)],
    );
    assert_only(&h, Rule::NotAPrefix);
}

// --- rule 4: real-time order ---------------------------------------------------

#[test]
fn an_append_acknowledged_first_must_sit_first() {
    let h = history(
        vec![append(0, 0, 10, "a", 0, OK), append(1, 20, 30, "a", 1, OK)],
        &[(0, 1), (1, 0)],
    );
    assert_only(&h, Rule::RealTimeOrder);
    let report = check(&h);
    assert_eq!(report.violations[0].ops, vec![0, 1]);
}

/// B's outcome does not matter, only that it landed: it cannot have been
/// written before it was sent.
#[test]
fn an_unknown_append_that_landed_is_ordered_too() {
    let h = history(
        vec![
            append(0, 0, 10, "a", 0, OK),
            append(1, 20, 30, "a", 1, INFO),
        ],
        &[(0, 1), (1, 0)],
    );
    assert_only(&h, Rule::RealTimeOrder);
}

/// The furthest earlier append is what counts, not the latest to complete.
#[test]
fn real_time_order_compares_against_every_earlier_acknowledgement() {
    let h = history(
        vec![
            append(0, 0, 5, "a", 0, OK),
            append(1, 1, 8, "a", 1, OK),
            append(2, 20, 30, "a", 2, OK),
        ],
        // 2 began after both 0 and 1 were acknowledged, yet sits between them.
        &[(0, 1), (1, 2), (2, 0)],
    );
    let report = check(&h);
    assert!(report.has(Rule::RealTimeOrder), "{report}");
}

// --- rule 5: phantoms ------------------------------------------------------------

#[test]
fn a_value_no_append_wrote_is_a_phantom() {
    let h = history(vec![append(0, 0, 1, "a", 0, OK)], &[(0, 0), (1, 99)]);
    assert_only(&h, Rule::Phantom);
}

#[test]
fn a_payload_the_workload_did_not_write_is_a_phantom() {
    let h = history(vec![], &[(0, FOREIGN_PAYLOAD)]);
    assert_only(&h, Rule::Phantom);
    assert!(format!("{}", check(&h)).contains("a payload no append wrote"));
}

#[test]
fn a_value_read_before_its_append_began_is_a_phantom() {
    let h = history(
        vec![read(1, 0, 5, "a", &[(0, 0)]), append(0, 10, 11, "a", 0, OK)],
        &[(0, 0)],
    );
    assert_only(&h, Rule::Phantom);
}

#[test]
fn a_value_read_from_the_wrong_list_is_a_phantom() {
    let mut h = history(vec![append(0, 0, 1, "b", 0, INFO)], &[(0, 0)]);
    h.lists.insert(
        "b".to_string(),
        ListSpec {
            consistency: Consistency::Quorum,
            base: 0,
        },
    );
    h.final_reads.insert("b".to_string(), Vec::new());
    assert_only(&h, Rule::Phantom);
}

// --- rule 6: failed writes ---------------------------------------------------------

#[test]
fn a_definitely_failed_append_must_not_appear() {
    let h = history(vec![append(0, 0, 1, "a", 0, FAIL)], &[(0, 0)]);
    assert_only(&h, Rule::FailedWriteVisible);
}

#[test]
fn a_definitely_failed_append_must_not_appear_in_any_read() {
    let h = history(
        vec![append(0, 0, 1, "a", 0, FAIL), read(1, 2, 3, "a", &[(0, 0)])],
        &[(0, 0)],
    );
    let report = check(&h);
    assert!(report.has(Rule::FailedWriteVisible), "{report}");
}

// --- the harness's own mistakes ------------------------------------------------------

#[test]
fn a_final_read_with_a_hole_is_reported_as_incomplete() {
    let h = history(
        vec![append(0, 0, 1, "a", 0, OK), append(0, 2, 3, "a", 1, OK)],
        &[(0, 0), (2, 1)],
    );
    assert_only(&h, Rule::IncompleteFinalRead);
}

#[test]
fn a_final_read_that_starts_past_the_base_is_incomplete() {
    let h = history(vec![append(0, 0, 1, "a", 0, OK)], &[(1, 0)]);
    assert_only(&h, Rule::IncompleteFinalRead);
}

#[test]
fn the_same_value_appended_twice_is_a_malformed_history() {
    let h = history(
        vec![append(0, 0, 1, "a", 0, OK), append(1, 0, 1, "a", 0, OK)],
        &[(0, 0)],
    );
    assert_only(&h, Rule::MalformedHistory);
}

#[test]
fn a_list_without_a_final_read_is_a_malformed_history() {
    let mut h = history(vec![append(0, 0, 1, "a", 0, OK)], &[]);
    h.final_reads.clear();
    assert_only(&h, Rule::MalformedHistory);
}

// --- the checker against a correct log, then broken ------------------------------------

/// Breaking a correct history in each way the rules describe is caught.
#[test]
fn breaking_a_valid_history_is_caught() {
    for seed in 0..50 {
        let valid = linearizable(seed, 40);
        let acknowledged: Vec<u64> = valid
            .ops
            .iter()
            .filter_map(|op| match op.action {
                Action::Append {
                    value, outcome: OK, ..
                } => Some(value),
                _ => None,
            })
            .collect();
        let Some(&victim) = acknowledged.first() else {
            continue;
        };

        // Lose it: drop it from the final read and renumber the rest, so the
        // final read stays whole and only the loss is wrong.
        let mut lost = valid.clone();
        let final_a = lost.final_reads.get_mut("a").unwrap();
        final_a.retain(|e| e.value != victim);
        for (offset, element) in final_a.iter_mut().enumerate() {
            element.offset = offset as u64;
        }
        assert!(
            check(&lost).has(Rule::LostWrite),
            "seed {seed}: loss not caught"
        );

        // Duplicate it at the end.
        let mut duplicated = valid.clone();
        let final_a = duplicated.final_reads.get_mut("a").unwrap();
        let next = final_a.len() as u64;
        final_a.push(Element {
            offset: next,
            value: victim,
        });
        assert!(
            check(&duplicated).has(Rule::Duplicate),
            "seed {seed}: duplicate not caught"
        );

        // Mark it failed.
        let mut failed = valid.clone();
        for op in &mut failed.ops {
            if let Action::Append { value, outcome, .. } = &mut op.action
                && *value == victim
            {
                *outcome = FAIL;
            }
        }
        assert!(
            check(&failed).has(Rule::FailedWriteVisible),
            "seed {seed}: failed write not caught",
        );
    }
}

/// A history of `appends` appends and some reads against a correct,
/// linearizable log. See `random_linearizable_histories_are_valid`.
fn linearizable(seed: u64, appends: u64) -> History {
    let mut rng = Rng::new(seed);
    // (effect instant, value) for every append that takes effect.
    let mut effects: Vec<(u64, u64)> = Vec::new();
    let mut ops = Vec::new();
    for value in 0..appends {
        let invoke = rng.below(1_000);
        let complete = invoke + 1 + rng.below(100);
        let effect = invoke + rng.below(complete - invoke);
        let outcome = match rng.below(10) {
            0 => FAIL,
            1 | 2 => INFO,
            _ => OK,
        };
        let lands = match outcome {
            AppendOutcome::Fail => false,
            AppendOutcome::Info => rng.percent(50),
            AppendOutcome::Ok { .. } => true,
        };
        if lands {
            // An unknown append may also land after its client gave up.
            let effect = if outcome == INFO {
                effect + rng.below(200)
            } else {
                effect
            };
            effects.push((effect, value));
        }
        ops.push(append(
            rng.below(8) as usize,
            invoke,
            complete,
            "a",
            value,
            outcome,
        ));
    }
    // Ties broken by value so the order is total and the same on every run.
    effects.sort();
    let log: Vec<(u64, u64)> = effects
        .iter()
        .enumerate()
        .map(|(offset, &(_, value))| (offset as u64, value))
        .collect();

    for _ in 0..appends / 3 {
        let invoke = rng.below(1_300);
        let complete = invoke + 1 + rng.below(50);
        let instant = invoke + rng.below(complete - invoke);
        let visible = effects
            .iter()
            .filter(|(effect, _)| *effect < instant)
            .count();
        let mut seen: Vec<(u64, u64)> = log[..visible].to_vec();
        // A subscriber may drop records; that is a hole, not a violation.
        if rng.percent(20) && !seen.is_empty() {
            seen.remove(rng.below(seen.len() as u64) as usize);
        }
        ops.push(read(rng.below(8) as usize, invoke, complete, "a", &seen));
    }
    history(ops, &log)
}
