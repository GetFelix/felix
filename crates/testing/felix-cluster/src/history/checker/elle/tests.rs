//! Dependency cycles: a random correct list must never show one, and each
//! cycle class has a hand-built history that does.

use std::collections::BTreeMap;

use super::super::super::model::{
    Action, AppendOutcome, Consistency, Element, History, ListSpec, Op, Resume, Subscription,
};
use super::super::super::rng::Rng;
use super::super::{Rule, check};
use super::{Edge, ListGraph, check_list_append};

const OK: AppendOutcome = AppendOutcome::Ok { offset: None };
const INFO: AppendOutcome = AppendOutcome::Info;
const FAIL: AppendOutcome = AppendOutcome::Fail;

fn append(process: usize, invoke: u64, complete: u64, value: u64, outcome: AppendOutcome) -> Op {
    Op {
        process,
        invoke,
        complete,
        action: Action::Append {
            list: "a".to_string(),
            value,
            outcome,
        },
    }
}

fn read(process: usize, invoke: u64, complete: u64, seen: &[(u64, u64)]) -> Op {
    Op {
        process,
        invoke,
        complete,
        action: Action::Read {
            list: "a".to_string(),
            observed: elements(seen),
        },
    }
}

fn elements(pairs: &[(u64, u64)]) -> Vec<Element> {
    pairs
        .iter()
        .map(|&(offset, value)| Element {
            offset,
            value,
            skipped_before: 0,
        })
        .collect()
}

/// One `Quorum` list `a` at base 0 with exact tails, so short reads count.
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
        exact_tails: true,
        ..History::default()
    }
}

fn rules(history: &History) -> Vec<Rule> {
    let mut found: Vec<Rule> = check(history).violations.iter().map(|v| v.rule).collect();
    found.dedup();
    found
}

#[test]
fn random_correct_histories_have_no_cycle() {
    for seed in 0..300 {
        let h = random_history(seed, 50);
        let report = check(&h);
        assert!(report.is_valid(), "seed {seed}:\n{report}");
    }
}

/// A value at two offsets with another between puts append order in a loop.
/// The full check names it a duplicate and does not add the same finding as
/// a cycle.
#[test]
fn a_contradictory_order_is_g0() {
    let ops = vec![append(0, 0, 10, 0, OK), append(1, 0, 10, 1, OK)];
    let h = history(ops, &[(0, 0), (1, 1), (2, 0)]);
    let found = check_list_append(&h);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].rule, Rule::G0);
    assert_eq!(found[0].ops.len(), 2);
    assert_eq!(rules(&h), vec![Rule::Duplicate]);
}

/// A read saw an append, then an append that began after that read finished
/// sits before it: the read depends on the append, which depends on the read.
#[test]
fn a_read_that_saw_the_future_is_g1c_realtime() {
    let ops = vec![
        // Unknown outcome, so rule 4 has nothing to say.
        append(0, 0, 100, 1, INFO),
        read(1, 0, 5, &[(1, 1)]),
        append(2, 10, 20, 0, OK),
    ];
    let h = history(ops, &[(0, 0), (1, 1)]);
    assert_eq!(rules(&h), vec![Rule::G1cRealtime]);
    let found = check(&h).violations;
    assert_eq!(found[0].ops.len(), 3, "{found:?}");
}

/// A read began after an append was acknowledged and missed it.
#[test]
fn a_stale_read_is_g_single_realtime() {
    let ops = vec![
        append(0, 0, 10, 0, OK),
        append(0, 20, 30, 1, OK),
        read(1, 40, 50, &[(0, 0)]),
    ];
    let h = history(ops, &[(0, 0), (1, 1)]);
    assert_eq!(rules(&h), vec![Rule::GSingleRealtime]);
    let text = check(&h).to_string();
    assert!(text.contains("-rw->") && text.contains("-rt->"), "{text}");

    // A recorder whose reads may drop their newest records cannot say this.
    let mut lenient = h.clone();
    lenient.exact_tails = false;
    assert!(check(&lenient).is_valid());
}

/// Cycles with two anti-dependencies are named G2. With one total order a
/// shorter cycle with one always exists, so the search finds G-single first;
/// the classifier is checked on the longer cycle directly.
#[test]
fn two_anti_dependencies_are_g2() {
    let ops = vec![
        append(0, 0, 10, 0, OK),
        append(0, 20, 30, 1, OK),
        append(0, 40, 50, 2, OK),
        read(1, 60, 70, &[(0, 0)]),
        read(2, 60, 70, &[(0, 0), (1, 1)]),
    ];
    let h = history(ops, &[(0, 0), (1, 1), (2, 2)]);
    let graph = ListGraph::build(&h, "a", 0);
    let node = |op: usize| graph.ops.iter().position(|&o| o == op).unwrap();
    let cycle = [
        (node(3), Edge::Rw),
        (node(1), Edge::Wr),
        (node(4), Edge::Rw),
        (node(2), Edge::Rt),
    ];
    assert_eq!(graph.report(&cycle).rule, Rule::G2Realtime);
    let cycle = [
        (node(3), Edge::Rw),
        (node(1), Edge::Wr),
        (node(4), Edge::Rw),
    ];
    assert_eq!(graph.report(&cycle).rule, Rule::G2);
    // The graph itself reports the shorter stale-read cycle.
    assert_eq!(rules(&h), vec![Rule::GSingleRealtime]);
}

#[test]
fn single_edge_kinds_are_named_g1c_and_g_single() {
    let h = history(
        vec![append(0, 0, 10, 0, OK), read(1, 0, 5, &[(0, 0)])],
        &[(0, 0)],
    );
    let graph = ListGraph::build(&h, "a", 0);
    let (a, r) = (0, 1);
    let cycle = [(a, Edge::Wr), (r, Edge::Ww)];
    assert_eq!(graph.report(&cycle).rule, Rule::G1c);
    let cycle = [(a, Edge::Wr), (r, Edge::Rw)];
    assert_eq!(graph.report(&cycle).rule, Rule::GSingle);
}

// Anomalies other rules already name stay under that name alone.

#[test]
fn a_lost_acknowledged_append_is_not_also_a_cycle() {
    let ops = vec![
        append(0, 0, 10, 0, OK),
        append(0, 20, 30, 1, OK),
        read(1, 40, 50, &[(0, 0)]),
    ];
    assert_eq!(rules(&history(ops, &[(0, 0)])), vec![Rule::LostWrite]);
}

#[test]
fn incompatible_orders_name_both_reads() {
    let ops = vec![
        append(0, 0, 10, 0, INFO),
        append(1, 0, 10, 1, INFO),
        read(2, 20, 30, &[(0, 0)]),
        read(3, 20, 30, &[(0, 1)]),
    ];
    let report = check(&history(ops, &[(0, 0), (1, 1)]));
    let v = report
        .violations
        .iter()
        .find(|v| v.rule == Rule::NotAPrefix)
        .unwrap();
    assert!(v.ops.contains(&3), "{report}");
    assert!(!rules(&history(vec![], &[])).contains(&Rule::G0));
}

#[test]
fn a_duplicate_is_not_also_a_cycle() {
    let ops = vec![append(0, 0, 10, 0, OK)];
    assert_eq!(
        rules(&history(ops, &[(0, 0), (1, 0)])),
        vec![Rule::Duplicate]
    );
}

#[test]
fn a_phantom_is_not_also_a_cycle() {
    let ops = vec![read(0, 0, 10, &[(0, 7)])];
    assert_eq!(rules(&history(ops, &[(0, 7)])), vec![Rule::Phantom]);
}

#[test]
fn a_read_of_a_failed_append_is_g1a_only() {
    let ops = vec![append(0, 0, 10, 0, FAIL), read(1, 20, 30, &[(0, 0)])];
    assert_eq!(
        rules(&history(ops, &[(0, 0)])),
        vec![Rule::FailedWriteVisible]
    );
}

#[test]
fn an_unknown_append_may_be_absent_and_may_land_late() {
    let ops = vec![
        append(0, 0, 10, 0, INFO),
        append(1, 0, 10, 1, INFO),
        read(2, 20, 30, &[]),
        // Landed after its client gave up, after the first read.
        read(2, 40, 50, &[(0, 1)]),
    ];
    assert!(check(&history(ops, &[(0, 1)])).is_valid());
}

/// A correct serial list under overlapping appends and reads, with unknown
/// appends, dropped records inside reads, reads that start mid-list, and one
/// subscriber that resumes.
fn random_history(seed: u64, appends: u64) -> History {
    let mut rng = Rng::new(seed);
    let mut effects: Vec<(u64, u64)> = Vec::new();
    let mut spans = Vec::new();
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
        let effect = if outcome == INFO {
            effect + rng.below(200)
        } else {
            effect
        };
        if lands {
            effects.push((effect, value));
        }
        spans.push((invoke, complete, value, outcome));
    }
    effects.sort();
    let log: Vec<(u64, u64)> = effects
        .iter()
        .enumerate()
        .map(|(offset, &(_, value))| (offset as u64, value))
        .collect();
    let offset_of: BTreeMap<u64, u64> = log.iter().map(|&(o, v)| (v, o)).collect();

    let mut ops = Vec::new();
    for (invoke, complete, value, outcome) in spans {
        // Half of the acknowledgements say where the record landed.
        let outcome = match outcome {
            AppendOutcome::Ok { .. } if rng.percent(50) => AppendOutcome::Ok {
                offset: offset_of.get(&value).copied(),
            },
            other => other,
        };
        ops.push(append(
            rng.below(8) as usize,
            invoke,
            complete,
            value,
            outcome,
        ));
    }
    for _ in 0..appends {
        let invoke = rng.below(1_300);
        let complete = invoke + 1 + rng.below(50);
        let instant = invoke + rng.below(complete - invoke);
        let visible = effects.iter().filter(|(e, _)| *e < instant).count();
        let from = if rng.percent(40) {
            0
        } else {
            rng.below(visible as u64 + 1)
        };
        let mut seen: Vec<(u64, u64)> = log[..visible]
            .iter()
            .copied()
            .filter(|&(offset, _)| offset >= from)
            .collect();
        // Holes inside a read are dropped records; the last one stays.
        let last = seen.pop();
        seen.retain(|_| !rng.percent(15));
        seen.extend(last);
        ops.push(read(rng.below(8) as usize, invoke, complete, &seen));
    }
    let mut h = history(ops, &log);
    h.subscriptions = vec![subscription(&mut rng, &log)];
    h
}

/// One subscriber that got the whole log bar a few dropped records, over two
/// sessions.
fn subscription(rng: &mut Rng, log: &[(u64, u64)]) -> Subscription {
    let mut delivered = Vec::new();
    for (i, &(offset, value)) in log.iter().enumerate() {
        if i + 1 < log.len() && rng.percent(10) {
            continue;
        }
        delivered.push(Element {
            offset,
            value,
            skipped_before: 0,
        });
    }
    let resumes = match delivered.len() {
        0..=2 => Vec::new(),
        n => {
            let first = 1 + rng.below(n as u64 - 1) as usize;
            vec![Resume {
                at: 0,
                from: delivered[first].offset,
                first,
                reason: "stream closed".to_string(),
            }]
        }
    };
    Subscription {
        subscriber: 0,
        list: "a".to_string(),
        start: 0,
        next: log.last().map_or(0, |&(o, _)| o + 1),
        delivered,
        resumes,
    }
}
