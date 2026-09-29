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
