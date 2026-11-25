use super::*;

fn view() -> ClusterView {
    let nodes: Vec<String> = (0..3).map(|i| format!("broker-{i}")).collect();
    ClusterView {
        leaders: vec![nodes[2].clone()],
        shards: vec![super::super::ShardView {
            kind: "stream",
            name: "history-1".to_string(),
            shard: 0,
            leader: nodes[2].clone(),
        }],
        nodes,
    }
}

fn every_family() -> EveryFamily {
    EveryFamily::new(vec![
        FaultKind::DropOutbound,
        FaultKind::ClockRate,
        FaultKind::FsyncFailOnce,
        FaultKind::MoveShard,
        FaultKind::Kill,
        FaultKind::Drain,
    ])
}

/// #3120: the operator API refused the move, and the next pick was a kill,
/// so the run ended with no assignment fault healed. A refused kind is
/// picked again, and the nemesis is not satisfied until every family has
/// had a fault healed.
#[test]
fn a_refused_move_is_picked_again_until_its_family_heals() {
    let mut nemesis = every_family();
    let (mut rng, view) = (Rng::new(1592590337), view());
    for _ in 0..3 {
        let fault = nemesis.next_fault(&mut rng, &view).expect("a fault");
        nemesis.outcome(&fault, true);
    }
    let refused = nemesis.next_fault(&mut rng, &view).expect("a move");
    assert_eq!(refused.kind(), FaultKind::MoveShard);
    nemesis.outcome(&refused, false);
    assert!(!nemesis.satisfied());

    let again = nemesis.next_fault(&mut rng, &view).expect("a move");
    assert_eq!(again.kind(), FaultKind::MoveShard, "picked {again} instead");
    nemesis.outcome(&again, true);
    assert!(!nemesis.satisfied(), "no process fault has healed yet");

    let kill = nemesis.next_fault(&mut rng, &view).expect("a kill");
    assert_eq!(kill.kind(), FaultKind::Kill);
    nemesis.outcome(&kill, true);
    assert!(nemesis.satisfied());
    assert_eq!(nemesis.healed().len(), 5);
}

/// A kind that is never injected does not hold the schedule up for good.
#[test]
fn a_kind_refused_every_time_is_given_up_after_its_tries() {
    let mut nemesis = EveryFamily::new(vec![FaultKind::MoveShard, FaultKind::Kill]);
    let (mut rng, view) = (Rng::new(7), view());
    for _ in 0..TRIES_PER_KIND {
        let fault = nemesis.next_fault(&mut rng, &view).expect("a move");
        assert_eq!(fault.kind(), FaultKind::MoveShard);
        nemesis.outcome(&fault, false);
    }
    let next = nemesis.next_fault(&mut rng, &view).expect("a kill");
    assert_eq!(next.kind(), FaultKind::Kill);
    assert!(!nemesis.satisfied());
}
