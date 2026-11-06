//! Known-good concurrent histories pass, one hand-built history per anomaly
//! class fails, and a history too tangled to decide returns `Unknown`.

use std::time::{Duration, Instant};

use super::super::rng::Rng;
use super::{Budget, Linearizability, check_all};
use crate::history::register::{RegisterAction, RegisterOp};

fn op(process: usize, invoke: u64, complete: u64, action: RegisterAction) -> RegisterOp {
    RegisterOp {
        process,
        invoke,
        complete,
        key: "k".to_string(),
        action,
    }
}

fn put(process: usize, invoke: u64, complete: u64, value: u64, acknowledged: bool) -> RegisterOp {
    let action = RegisterAction::Put {
        value,
        acknowledged,
    };
    op(process, invoke, complete, action)
}

fn get(process: usize, invoke: u64, complete: u64, value: Option<u64>) -> RegisterOp {
    op(process, invoke, complete, RegisterAction::Get { value })
}

fn cas(
    process: usize,
    (invoke, complete): (u64, u64),
    expect: Option<u64>,
    value: u64,
    applied: Option<bool>,
) -> RegisterOp {
    let action = RegisterAction::PutIf {
        expect,
        value,
        applied,
    };
    op(process, invoke, complete, action)
}

fn delete(process: usize, invoke: u64, complete: u64, removed: Option<Option<u64>>) -> RegisterOp {
    op(
        process,
        invoke,
        complete,
        RegisterAction::Delete { removed },
    )
}

fn verdict(ops: &[RegisterOp]) -> Linearizability {
    check_all(ops, Budget::default()).remove("k").unwrap()
}

fn assert_violation(ops: &[RegisterOp]) -> String {
    match verdict(ops) {
        Linearizability::Violation(witness) => witness.text,
        other => panic!("expected a violation, got {other:?}"),
    }
}

#[test]
fn concurrent_puts_may_be_read_in_either_order() {
    let ops = [
        put(0, 0, 10, 1, true),
        put(1, 5, 15, 2, true),
        get(2, 20, 25, Some(1)),
        get(3, 20, 25, Some(2)),
    ];
    // Both reads are after both puts, so they see one final value, not two.
    assert_violation(&ops);
    let ops = [
        put(0, 0, 10, 1, true),
        put(1, 5, 15, 2, true),
        get(2, 6, 25, Some(1)),
        get(3, 6, 25, Some(2)),
    ];
    assert_eq!(verdict(&ops), Linearizability::Ok);
}

#[test]
fn an_unknown_put_may_never_have_happened_or_land_late() {
    let ops = [put(0, 0, 10, 1, false), get(1, 20, 30, None)];
    assert_eq!(verdict(&ops), Linearizability::Ok);
    let ops = [put(0, 0, 10, 1, false), get(1, 500, 600, Some(1))];
    assert_eq!(verdict(&ops), Linearizability::Ok);
}

#[test]
fn a_stale_read_is_caught() {
    let text = assert_violation(&[
        put(0, 0, 10, 1, true),
        put(0, 20, 30, 2, true),
        get(1, 40, 50, Some(1)),
    ]);
    assert!(text.contains("get k -> 1"), "{text}");
    assert!(text.contains("longest order"), "{text}");
}

#[test]
fn a_read_of_a_value_nobody_wrote_is_caught() {
    assert_violation(&[put(0, 0, 10, 1, true), get(1, 20, 30, Some(9))]);
}

#[test]
fn a_lost_acknowledged_put_is_caught() {
    assert_violation(&[put(0, 0, 10, 1, true), get(1, 20, 30, None)]);
}

#[test]
fn reads_that_go_back_across_clients_are_caught() {
    // Put 2 is unknown, so either answer is fine alone, but client 1 saw 2
    // finish before client 2 began and still read the older value.
    let ops = [
        put(0, 0, 10, 1, true),
        put(0, 20, 1_000, 2, false),
        get(1, 30, 40, Some(2)),
        get(2, 50, 60, Some(1)),
    ];
    assert_violation(&ops);
}

#[test]
fn two_conditional_puts_on_an_absent_key_cannot_both_apply() {
    let ops = [
        cas(0, (0, 10), None, 1, Some(true)),
        cas(1, (20, 30), None, 2, Some(true)),
    ];
    assert_violation(&ops);
    let ops = [
        cas(0, (0, 10), None, 1, Some(true)),
        cas(1, (5, 30), None, 2, Some(false)),
        get(2, 40, 50, Some(1)),
    ];
    assert_eq!(verdict(&ops), Linearizability::Ok);
}

#[test]
fn a_conditional_put_that_applied_against_the_wrong_value_is_caught() {
    let ops = [
        put(0, 0, 10, 1, true),
        put(0, 20, 30, 2, true),
        cas(1, (40, 50), Some(1), 3, Some(true)),
    ];
    assert_violation(&ops);
}

#[test]
fn a_refused_conditional_put_needs_a_different_value() {
    let ops = [
        put(0, 0, 10, 1, true),
        cas(1, (20, 30), Some(1), 2, Some(false)),
    ];
    assert_violation(&ops);
}

#[test]
fn deletes_clear_the_key_and_must_report_what_they_removed() {
    let ops = [
        put(0, 0, 10, 1, true),
        delete(0, 20, 30, Some(Some(1))),
        get(1, 40, 50, None),
        delete(1, 60, 70, Some(None)),
    ];
    assert_eq!(verdict(&ops), Linearizability::Ok);
    assert_violation(&[
        put(0, 0, 10, 1, true),
        delete(0, 20, 30, Some(Some(1))),
        get(1, 40, 50, Some(1)),
    ]);
    assert_violation(&[put(0, 0, 10, 1, true), delete(0, 20, 30, Some(None))]);
}

/// Puts, gets, deletes and conditional puts against a correct register. Each
/// operation takes effect at one instant: inside its span, or for an unknown
/// one maybe later or never. Operations then run in effect order and report
/// what they find.
fn linearizable(seed: u64) -> Vec<RegisterOp> {
    let mut rng = Rng::new(seed);
    struct Plan {
        invoke: u64,
        complete: u64,
        effect: u64,
        happens: bool,
        known: bool,
        kind: u64,
        value: u64,
        process: usize,
    }
    let mut plans: Vec<Plan> = (0..40)
        .map(|value| {
            let invoke = rng.below(1_000);
            let complete = invoke + 1 + rng.below(120);
            let known = !rng.percent(20);
            let (effect, happens) = if known {
                (invoke + rng.below(complete - invoke), true)
            } else {
                (invoke + rng.below(complete - invoke + 200), rng.percent(50))
            };
            Plan {
                invoke,
                complete,
                effect,
                happens,
                known,
                kind: rng.below(10),
                value,
                process: rng.below(8) as usize,
            }
        })
        .collect();
    plans.sort_by_key(|plan| plan.effect);
    let mut state: Option<u64> = None;
    let mut written: Vec<u64> = Vec::new();
    let mut ops = Vec::new();
    for plan in plans {
        let Plan {
            invoke,
            complete,
            value,
            process,
            ..
        } = plan;
        let span = (invoke, complete);
        match plan.kind {
            0..=3 => {
                if plan.happens {
                    state = Some(value);
                    written.push(value);
                }
                ops.push(put(process, invoke, complete, value, plan.known));
            }
            4..=6 => {
                if plan.known {
                    ops.push(get(process, invoke, complete, state));
                }
            }
            7 => {
                let removed = plan.known.then_some(state);
                if plan.happens {
                    state = None;
                }
                ops.push(delete(process, invoke, complete, removed));
            }
            _ => {
                let expect = if rng.percent(50) {
                    state
                } else {
                    written.last().copied()
                };
                let hit = expect == state;
                if plan.happens && hit {
                    state = Some(value);
                    written.push(value);
                }
                let applied = plan.known.then_some(hit);
                ops.push(cas(process, span, expect, value, applied));
            }
        }
    }
    ops
}

#[test]
fn random_linearizable_histories_are_valid() {
    for seed in 0..500 {
        let ops = linearizable(seed);
        let found = verdict(&ops);
        assert_eq!(found, Linearizability::Ok, "seed {seed}: {ops:#?}");
    }
}

#[test]
fn a_bent_random_history_is_caught() {
    // Make the last acknowledged get lie by reading a value nobody wrote.
    let mut caught = 0;
    for seed in 0..200 {
        let mut ops = linearizable(seed);
        let Some(last) = ops
            .iter_mut()
            .rev()
            .find(|op| matches!(op.action, RegisterAction::Get { .. }))
        else {
            continue;
        };
        last.action = RegisterAction::Get { value: Some(1_000) };
        assert!(
            matches!(verdict(&ops), Linearizability::Violation(_)),
            "seed {seed}"
        );
        caught += 1;
    }
    assert!(caught > 150);
}

#[test]
fn a_tangled_history_returns_unknown_quickly() {
    // Forty puts that all overlap can be ordered in about 2^40 ways, and the
    // read at the end wants a value that is only written after it finished.
    let mut ops: Vec<RegisterOp> = (0..40)
        .map(|value| put(value as usize, 0, 1_000, value, true))
        .collect();
    ops.push(get(99, 2_000, 2_010, Some(500)));
    ops.push(put(98, 3_000, 3_010, 500, true));
    let budget = Budget {
        steps: 200_000,
        states: 100_000,
    };
    let started = Instant::now();
    let found = check_all(&ops, budget).remove("k").unwrap();
    assert!(
        matches!(found, Linearizability::Unknown { .. }),
        "{found:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(10));
}
