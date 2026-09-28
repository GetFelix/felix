//! Register histories: valid ones, however concurrent, must pass, and a
//! planted stale or phantom get must be caught under its own rule.

use super::super::checker::{Rule, Violation};
use super::super::model::FOREIGN_PAYLOAD;
use super::super::rng::Rng;
use super::{RegisterAction, RegisterOp, check};

fn put(invoke: u64, complete: u64, value: u64, acknowledged: bool) -> RegisterOp {
    RegisterOp {
        process: 0,
        invoke,
        complete,
        key: "k".to_string(),
        action: RegisterAction::Put {
            value,
            acknowledged,
        },
    }
}

fn get(invoke: u64, complete: u64, value: Option<u64>) -> RegisterOp {
    RegisterOp {
        process: 1,
        invoke,
        complete,
        key: "k".to_string(),
        action: RegisterAction::Get { value },
    }
}

fn run(ops: &[RegisterOp]) -> Vec<Violation> {
    let mut out = Vec::new();
    check(ops, &mut out);
    out
}

fn assert_only(ops: &[RegisterOp], rule: Rule) {
    let found = run(ops);
    assert!(!found.is_empty(), "expected a {rule} violation");
    assert!(
        found.iter().all(|v| v.rule == rule),
        "expected only {rule}: {found:?}"
    );
}

#[test]
fn concurrent_puts_may_be_read_in_either_order() {
    let ops = [
        put(0, 10, 1, true),
        put(5, 15, 2, true),
        get(20, 25, Some(1)),
        get(20, 25, Some(2)),
        // An unknown put may land at any time after it began.
        put(30, 40, 3, false),
        get(100, 110, Some(3)),
        get(105, 120, Some(3)),
    ];
    assert!(run(&ops).is_empty(), "{:?}", run(&ops));
}

#[test]
fn a_miss_before_any_put_took_effect_is_fine() {
    let ops = [get(0, 5, None), put(3, 10, 1, true), get(4, 12, None)];
    assert!(run(&ops).is_empty(), "{:?}", run(&ops));
}

#[test]
fn a_get_of_an_overwritten_value_is_stale() {
    let ops = [
        put(0, 10, 1, true),
        put(20, 30, 2, true),
        get(40, 50, Some(1)),
    ];
    assert_only(&ops, Rule::StaleRead);
}

#[test]
fn a_get_older_than_an_earlier_get_is_stale() {
    // Put 2's acknowledgement is lost, but a get saw it before the last get
    // began, so the last get cannot go back to 1.
    let ops = [
        put(0, 10, 1, true),
        put(20, 1_000, 2, false),
        get(30, 40, Some(2)),
        get(50, 60, Some(1)),
    ];
    assert_only(&ops, Rule::StaleRead);
}

#[test]
fn a_miss_after_an_acknowledged_put_is_stale() {
    let ops = [put(0, 10, 1, true), get(20, 30, None)];
    assert_only(&ops, Rule::StaleRead);
}

#[test]
fn a_value_nobody_put_is_a_phantom() {
    assert_only(&[get(0, 10, Some(7))], Rule::Phantom);
    assert_only(&[get(0, 10, Some(FOREIGN_PAYLOAD))], Rule::Phantom);
    assert_only(&[get(0, 10, Some(1)), put(20, 30, 1, true)], Rule::Phantom);
}

#[test]
fn random_linearizable_register_histories_are_valid() {
    for seed in 0..200 {
        let ops = linearizable(seed);
        let found = run(&ops);
        assert!(found.is_empty(), "seed {seed}: {found:?}");
    }
}

/// Serving a value one put behind the newest, as a deposed leader would,
/// is caught in a random history.
#[test]
fn a_planted_stale_get_is_caught() {
    let mut caught = 0;
    for seed in 0..200 {
        let mut ops = linearizable(seed);
        // The last acknowledged put that an acknowledged put strictly
        // follows, and a get after both that returns the older value.
        let acked: Vec<&RegisterOp> = ops
            .iter()
            .filter(|op| {
                matches!(
                    op.action,
                    RegisterAction::Put {
                        acknowledged: true,
                        ..
                    }
                )
            })
            .collect();
        let pair = acked.iter().find_map(|older| {
            acked
                .iter()
                .find(|newer| newer.invoke > older.complete)
                .map(|newer| (*older, *newer))
        });
        let Some((older, newer)) = pair else {
            continue;
        };
        let RegisterAction::Put { value, .. } = older.action else {
            unreachable!()
        };
        let at = newer.complete + 1;
        ops.push(get(at, at + 5, Some(value)));
        let found = run(&ops);
        assert!(
            found.iter().any(|v| v.rule == Rule::StaleRead),
            "seed {seed}: stale get not caught"
        );
        caught += 1;
    }
    assert!(caught > 100, "only {caught} seeds had a pair to plant on");
}

/// Puts and gets against a correct register: each put takes effect at an
/// instant inside its span (an unknown one maybe later, or never), and each
/// get returns whatever took effect last before its own instant.
fn linearizable(seed: u64) -> Vec<RegisterOp> {
    let mut rng = Rng::new(seed);
    let mut effects: Vec<(u64, u64)> = Vec::new();
    let mut ops = Vec::new();
    for value in 0..30 {
        let invoke = rng.below(1_000);
        let complete = invoke + 1 + rng.below(100);
        let acknowledged = !rng.percent(20);
        let effect = invoke + rng.below(complete - invoke);
        if acknowledged {
            effects.push((effect, value));
        } else if rng.percent(50) {
            effects.push((effect + rng.below(200), value));
        }
        ops.push(put(invoke, complete, value, acknowledged));
    }
    effects.sort_unstable();
    for _ in 0..30 {
        let invoke = rng.below(1_300);
        let complete = invoke + 1 + rng.below(100);
        let at = invoke + rng.below(complete - invoke);
        let value = effects
            .iter()
            .take_while(|(effect, _)| *effect <= at)
            .last()
            .map(|&(_, value)| value);
        ops.push(get(invoke, complete, value));
    }
    ops
}
