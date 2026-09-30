use super::*;

fn view() -> ClusterView {
    let nodes: Vec<String> = (0..3).map(|i| format!("broker-{i}")).collect();
    let shard = |kind, name: &str, leader: &str| ShardView {
        kind,
        name: name.to_string(),
        shard: 0,
        leader: leader.to_string(),
    };
    ClusterView {
        leaders: vec![nodes[0].clone(), nodes[1].clone()],
        shards: vec![
            shard("stream", "history-0", &nodes[0]),
            shard("stream", "history-1", &nodes[0]),
            shard("cache", "history-cache", &nodes[1]),
        ],
        nodes,
    }
}

/// Every fault `all_faults` can pick, over enough seeds to see each kind.
fn sample_all_faults() -> Vec<Fault> {
    let mut nemesis = RandomNemesis::all_faults();
    let view = view();
    let mut faults = Vec::new();
    for seed in 0..50 {
        let mut rng = Rng::new(seed);
        for _ in 0..20 {
            faults.extend(nemesis.next_fault(&mut rng, &view));
        }
    }
    faults
}

/// A broker's lease clock is boottime, which never runs back; the harness
/// refuses such a step, and the nemesis must never ask for one.
#[test]
fn all_faults_never_steps_a_broker_clock_back() {
    for fault in sample_all_faults() {
        for harness in fault.harness_faults() {
            if let HarnessFault::Clock {
                process: Endpoint::Node(node),
                fault: ClockFault::StepMillis(by),
            } = harness
            {
                assert!(by >= 0, "{fault} steps {node}'s clock back by {by}ms");
            }
        }
    }
}

#[test]
fn all_faults_covers_every_family() {
    let families: std::collections::BTreeSet<_> =
        sample_all_faults().iter().map(Fault::family).collect();
    assert_eq!(
        families.into_iter().collect::<Vec<_>>(),
        vec![
            FaultFamily::Process,
            FaultFamily::Link,
            FaultFamily::Clock,
            FaultFamily::Disk,
            FaultFamily::Assignment,
        ],
    );
}

/// A move names a shard of the workload, its current leader, and another
/// broker to take it; when the target leads shards, it is one of those.
#[test]
fn a_move_goes_from_the_leader_to_another_broker() {
    let view = view();
    let mut nemesis = RandomNemesis::new(vec![FaultKind::MoveShard]);
    let mut seen = std::collections::BTreeSet::new();
    for seed in 0..200 {
        let mut rng = Rng::new(seed);
        let Some(Fault::MoveShard {
            kind,
            name,
            shard,
            from,
            to,
        }) = nemesis.next_fault(&mut rng, &view)
        else {
            panic!("seed {seed}: not a move");
        };
        let target = view
            .shards
            .iter()
            .find(|s| s.kind == kind && s.name == name && s.shard == shard)
            .unwrap_or_else(|| panic!("{kind} {name}/{shard} is not the workload's"));
        assert_eq!(from, target.leader);
        assert_ne!(to, from);
        assert!(view.nodes.contains(&to));
        seen.insert(name);
    }
    assert_eq!(
        seen.len(),
        view.shards.len(),
        "every shard gets moved: {seen:?}"
    );
}

#[test]
fn no_move_without_a_shard_to_move() {
    let mut view = view();
    view.shards.clear();
    let mut nemesis = RandomNemesis::new(vec![FaultKind::MoveShard]);
    assert_eq!(nemesis.next_fault(&mut Rng::new(1), &view), None);
}

/// The per-PR campaign runs `process_faults` on a cluster with no proxies
/// and the default fsync mode; only `all_faults` asks for more.
#[test]
fn only_the_faults_that_need_it_ask_for_proxies_or_on_commit_flushes() {
    let process = RandomNemesis::process_faults();
    let assignment = RandomNemesis::new(vec![FaultKind::MoveShard, FaultKind::Drain]);
    assert!(!assignment.needs_proxy_links());
    assert!(!assignment.needs_fsync_on_commit());
    assert!(!process.needs_proxy_links());
    assert!(!process.needs_fsync_on_commit());

    let all = RandomNemesis::all_faults();
    assert!(all.needs_proxy_links());
    assert!(all.needs_fsync_on_commit());
}

/// Adding kinds must not change what a seed picks from `process_faults`:
/// kind, target, and nothing else is drawn for those.
#[test]
fn process_faults_draw_three_numbers_per_fault() {
    let view = view();
    let mut nemesis = RandomNemesis::process_faults();
    let mut rng = Rng::new(7);
    let fault = nemesis.next_fault(&mut rng, &view).expect("a fault");
    let mut expected = Rng::new(7);
    let kind = *expected.pick(&[FaultKind::Kill, FaultKind::Pause, FaultKind::Partition]);
    let targets = if expected.percent(75) {
        &view.leaders
    } else {
        &view.nodes
    };
    let node = expected.pick(targets).clone();
    let wanted = match kind {
        FaultKind::Kill => Fault::Kill { node },
        FaultKind::Pause => Fault::Pause { node },
        _ => Fault::Partition { node },
    };
    assert_eq!(fault, wanted);
    assert_eq!(rng.next_u64(), expected.next_u64());
}

fn four_brokers() -> ClusterView {
    let mut view = view();
    view.nodes.push("broker-3".to_string());
    view
}

/// Every fault `adversarial` picks on four brokers, over enough seeds to see
/// each kind many times.
fn sample_adversarial() -> Vec<Fault> {
    let mut nemesis = RandomNemesis::adversarial();
    let view = four_brokers();
    let mut faults = Vec::new();
    for seed in 0..100 {
        let mut rng = Rng::new(seed);
        for _ in 0..20 {
            faults.extend(nemesis.next_fault(&mut rng, &view));
        }
    }
    faults
}

#[test]
fn adversarial_picks_every_kind_it_lists() {
    let picked: std::collections::BTreeSet<String> = sample_adversarial()
        .iter()
        .map(|fault| format!("{:?}", fault.kind()))
        .collect();
    let listed: std::collections::BTreeSet<String> = RandomNemesis::adversarial()
        .kinds
        .iter()
        .map(|kind| format!("{kind:?}"))
        .collect();
    assert_eq!(picked, listed);
}

/// Two faults in effect together are aimed at different brokers; otherwise
/// an overlap is one fault with extra steps.
#[test]
fn overlapping_faults_are_aimed_at_different_brokers() {
    let mut several = 0;
    for fault in sample_adversarial() {
        let Fault::Several { faults, .. } = &fault else {
            continue;
        };
        several += 1;
        assert_eq!(faults.len(), 2, "{fault}");
        let (first, second) = (faults[0].targets(), faults[1].targets());
        assert!(
            first.iter().all(|node| !second.contains(node)),
            "{fault} aims both faults at one broker"
        );
    }
    assert!(several > 100, "only {several} overlapping faults");
}

/// Isolating a follower forces no failover, so an isolation goes to a leader.
#[test]
fn an_isolation_is_aimed_at_a_leader() {
    let view = four_brokers();
    for fault in sample_adversarial() {
        if let Fault::Isolate { node } = &fault {
            assert!(view.leaders.contains(node), "{fault}");
        }
    }
}

#[test]
fn an_interrupted_move_kills_one_end_of_it() {
    let mut victims = std::collections::BTreeSet::new();
    for fault in sample_adversarial() {
        if let Fault::InterruptedMove {
            from, to, victim, ..
        } = &fault
        {
            assert_ne!(from, to);
            assert!(victim == from || victim == to, "{fault}");
            victims.insert(victim == from);
        }
    }
    assert_eq!(
        victims.len(),
        2,
        "both the source and the destination get killed"
    );
}

/// A drain healed before the kill beside it would wait for moves that need
/// the killed broker back.
#[test]
fn assignment_faults_are_healed_last() {
    let faults = vec![
        Fault::Drain {
            node: "broker-0".to_string(),
        },
        Fault::Kill {
            node: "broker-1".to_string(),
        },
        Fault::Pause {
            node: "broker-2".to_string(),
        },
    ];
    let order: Vec<String> = compound::heal_order(&faults)
        .iter()
        .map(|fault| fault.to_string())
        .collect();
    assert_eq!(order, ["pause broker-2", "kill broker-1", "drain broker-0"]);
}

#[test]
fn adversarial_asks_for_proxies_and_on_commit_flushes() {
    let adversarial = RandomNemesis::adversarial();
    assert!(adversarial.needs_proxy_links());
    assert!(adversarial.needs_fsync_on_commit());
}

/// A power loss keeps only what was flushed, so it needs acknowledgements
/// that wait for the flush, and brokers running under the power-loss model.
#[test]
fn a_power_loss_needs_on_commit_flushes_and_the_model() {
    assert!(FaultKind::PowerLoss.needs_fsync_on_commit());
    assert!(FaultKind::PowerLoss.needs_power_loss());
    assert!(!FaultKind::PowerLoss.needs_proxy_links());
    assert!(!FaultKind::ControlPlaneCrash.needs_power_loss());
    assert!(RandomNemesis::adversarial().needs_power_loss());
    assert!(!RandomNemesis::all_faults().needs_power_loss());
    assert!(!RandomNemesis::process_faults().needs_power_loss());
}

/// A control plane crash lands during each kind of work it can interrupt,
/// and a kill it lands during is of a leader, so a failover is pending.
#[test]
fn a_control_plane_crash_interrupts_a_move_a_drain_or_a_failover() {
    let view = four_brokers();
    let mut seen = std::collections::BTreeSet::new();
    for fault in sample_adversarial() {
        let Fault::ControlPlaneCrash { during } = &fault else {
            continue;
        };
        if let Fault::Kill { node } = during.as_ref() {
            assert!(view.leaders.contains(node), "{fault}");
        }
        assert_eq!(fault.targets(), during.targets());
        seen.insert(format!("{:?}", during.kind()));
    }
    let expected: std::collections::BTreeSet<String> = compound::IN_FLIGHT_KINDS
        .iter()
        .map(|kind| format!("{kind:?}"))
        .collect();
    assert_eq!(seen, expected);
}

#[test]
fn the_new_compound_faults_say_what_they_do() {
    assert_eq!(
        Fault::PowerLoss { seed: 7 }.to_string(),
        "cut the power to every broker (image seed 7)"
    );
    let crash = Fault::ControlPlaneCrash {
        during: Box::new(Fault::Drain {
            node: "broker-1".to_string(),
        }),
    };
    assert_eq!(
        crash.to_string(),
        "crash the control plane during: drain broker-1"
    );
    assert_eq!(crash.kind(), FaultKind::ControlPlaneCrash);
    assert_eq!(crash.family(), FaultFamily::Compound);
}
