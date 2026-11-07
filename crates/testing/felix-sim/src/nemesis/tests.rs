use super::*;

const WINDOW: Duration = Duration::from_secs(90);

#[test]
fn the_plan_is_a_function_of_the_seed() {
    assert_eq!(
        Plan::generate(7, 5, WINDOW, false),
        Plan::generate(7, 5, WINDOW, false)
    );
    assert_ne!(
        Plan::generate(7, 5, WINDOW, false),
        Plan::generate(8, 5, WINDOW, false)
    );
}

#[test]
fn every_fault_starts_inside_the_window_and_names_real_brokers() {
    for seed in 0..50 {
        for brokers in 3..=5 {
            let plan = Plan::generate(seed, brokers, WINDOW, false);
            assert!(!plan.steps.is_empty());
            let mut last = Duration::ZERO;
            for step in &plan.steps {
                assert!(step.at < WINDOW && step.at >= last, "{step:?}");
                last = step.at;
                match &step.fault {
                    Fault::Crash { node, .. }
                    | Fault::Freeze { node, .. }
                    | Fault::Isolate { node, .. } => assert!(*node < brokers),
                    Fault::OneWay { from, to, .. } => {
                        assert!(*from < brokers && *to < brokers && from != to);
                    }
                    Fault::Split { minority, .. } => {
                        assert!(!minority.is_empty() && minority.len() <= (brokers - 1) / 2);
                        assert!(minority.iter().all(|n| *n < brokers));
                    }
                    Fault::ControlPlane { .. } | Fault::PausedThenCrash { .. } => {}
                    other => panic!("a placement fault without placement faults on: {other}"),
                }
            }
        }
    }
}

#[test]
fn plans_cover_every_kind_of_fault() {
    let mut seen = std::collections::BTreeSet::new();
    for seed in 0..40 {
        for step in Plan::generate(seed, 4, WINDOW, false).steps {
            seen.insert(match step.fault {
                Fault::Crash { .. } => "crash",
                Fault::Freeze { .. } => "freeze",
                Fault::Isolate { .. } => "isolate",
                Fault::Split { .. } => "split",
                Fault::OneWay { .. } => "one-way",
                Fault::ControlPlane {
                    fault: CpFault::Slow(_),
                    ..
                } => "cp-slow",
                Fault::ControlPlane {
                    fault: CpFault::Paused,
                    ..
                } => "cp-paused",
                Fault::ControlPlane {
                    fault: CpFault::Down,
                    ..
                } => "cp-down",
                Fault::ControlPlane { .. } => "cp-other",
                Fault::PausedThenCrash { .. } => "paused-then-crash",
                _ => "placement",
            });
        }
    }
    assert_eq!(seen.len(), 9, "{seen:?}");
    assert!(!seen.contains("cp-other"));
}

/// How the crashes of `plan` stop their brokers, in order.
fn stops(plan: &Plan) -> Vec<Stop> {
    plan.steps
        .iter()
        .filter_map(|step| match step.fault {
            Fault::Crash { stop, .. } | Fault::PausedThenCrash { stop, .. } => Some(stop),
            _ => None,
        })
        .collect()
}

#[test]
fn plans_hold_both_kills_and_clean_crashes() {
    let (mut kills, mut clean) = (0, 0);
    for seed in 0..40 {
        for stop in stops(&Plan::generate(seed, 4, WINDOW, false)) {
            match stop {
                Stop::Kill => kills += 1,
                Stop::Clean => clean += 1,
                Stop::PowerLoss => panic!("a power cut in a run without power loss"),
            }
        }
    }
    assert!(kills > 0 && clean > 0, "kills {kills}, clean {clean}");
}

#[test]
fn power_loss_cuts_the_power_where_a_kill_would_be_and_moves_nothing_else() {
    let mut cuts = 0;
    for seed in 0..40 {
        let plain = Plan::generate(seed, 4, WINDOW, false);
        let power = Plan::generate(seed, 4, WINDOW, true);
        assert_eq!(plain.steps.len(), power.steps.len(), "seed {seed}");
        for (a, b) in plain.steps.iter().zip(&power.steps) {
            assert_eq!(a.at, b.at, "seed {seed}");
            match (&a.fault, &b.fault) {
                (
                    Fault::Crash { stop: x, .. } | Fault::PausedThenCrash { stop: x, .. },
                    Fault::Crash { stop: y, .. } | Fault::PausedThenCrash { stop: y, .. },
                ) => {
                    let expected = if *x == Stop::Kill {
                        Stop::PowerLoss
                    } else {
                        *x
                    };
                    assert_eq!(*y, expected, "seed {seed}");
                    cuts += usize::from(*y == Stop::PowerLoss);
                }
                (x, y) => assert_eq!(x, y, "seed {seed}"),
            }
        }
        assert!(!stops(&power).contains(&Stop::Kill), "seed {seed}");
    }
    assert!(cuts > 0);
}

#[test]
fn a_power_loss_reads_as_one() {
    let fault = Fault::Crash {
        node: 1,
        down_for: Duration::from_secs(4),
        stop: Stop::PowerLoss,
    };
    assert_eq!(fault.to_string(), "power-cut b2 for 4s");
    assert!(fault.can_unseat(&[1].into()));
}
