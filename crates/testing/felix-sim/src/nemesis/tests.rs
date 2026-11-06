use super::*;

const WINDOW: Duration = Duration::from_secs(90);

#[test]
fn the_plan_is_a_function_of_the_seed() {
    assert_eq!(Plan::generate(7, 5, WINDOW), Plan::generate(7, 5, WINDOW));
    assert_ne!(Plan::generate(7, 5, WINDOW), Plan::generate(8, 5, WINDOW));
}

#[test]
fn every_fault_starts_inside_the_window_and_names_real_brokers() {
    for seed in 0..50 {
        for brokers in 3..=5 {
            let plan = Plan::generate(seed, brokers, WINDOW);
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
        for step in Plan::generate(seed, 4, WINDOW).steps {
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

#[test]
fn plans_hold_both_kills_and_clean_crashes() {
    let (mut kills, mut clean) = (0, 0);
    for seed in 0..40 {
        for step in Plan::generate(seed, 4, WINDOW).steps {
            if let Fault::Crash { kill, .. } | Fault::PausedThenCrash { kill, .. } = step.fault {
                if kill {
                    kills += 1;
                } else {
                    clean += 1;
                }
            }
        }
    }
    assert!(kills > 0 && clean > 0, "kills {kills}, clean {clean}");
}
