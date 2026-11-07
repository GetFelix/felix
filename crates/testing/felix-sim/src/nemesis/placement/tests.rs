use super::*;

const WINDOW: Duration = Duration::from_secs(90);

fn kind(fault: &Fault) -> &'static str {
    match fault {
        Fault::Move { .. } => "move",
        Fault::Drain { .. } => "drain",
        Fault::Remove { .. } => "remove",
        Fault::Abandon { .. } => "abandon",
        Fault::KillBeforeOpen { .. } => "kill-before-open",
        Fault::SeatRace { .. } => "seat-race",
        _ => "other",
    }
}

#[test]
fn placement_steps_are_a_function_of_the_seed() {
    assert_eq!(placement_steps(7, 4, WINDOW), placement_steps(7, 4, WINDOW));
    assert_ne!(placement_steps(7, 4, WINDOW), placement_steps(8, 4, WINDOW));
}

#[test]
fn adding_placement_faults_keeps_every_step_the_seed_had() {
    for seed in 0..30 {
        let base = Plan::generate(seed, 4, WINDOW, false);
        let both = base.clone().with_placement(seed, 4, WINDOW);
        let kept: Vec<&Step> = both
            .steps
            .iter()
            .filter(|step| kind(&step.fault) == "other")
            .collect();
        assert_eq!(kept, base.steps.iter().collect::<Vec<_>>(), "seed {seed}");
        assert!(both.steps.len() > base.steps.len(), "seed {seed}");
        assert!(both.steps.windows(2).all(|w| w[0].at <= w[1].at));
    }
}

#[test]
fn placement_faults_start_inside_the_window_and_name_real_brokers() {
    for seed in 0..50 {
        for brokers in 3..=5 {
            for step in placement_steps(seed, brokers, WINDOW).steps {
                assert!(step.at < WINDOW, "{step:?}");
                match step.fault {
                    Fault::Move { to: node, .. }
                    | Fault::Drain { node, .. }
                    | Fault::Remove { node, .. }
                    | Fault::SeatRace { node, .. } => assert!(node < brokers),
                    Fault::Abandon { .. } | Fault::KillBeforeOpen { .. } => {}
                    other => panic!("not a placement fault: {other:?}"),
                }
            }
        }
    }
}

#[test]
fn every_placement_fault_is_drawn_and_seat_races_need_a_spare_broker() {
    let mut seen = std::collections::BTreeSet::new();
    for seed in 0..40 {
        for step in placement_steps(seed, 4, WINDOW).steps {
            seen.insert(kind(&step.fault));
        }
        let three = placement_steps(seed, 3, WINDOW);
        assert!(
            !three
                .steps
                .iter()
                .any(|step| matches!(step.fault, Fault::SeatRace { .. })),
            "seed {seed}: three brokers have no broker to take a seat"
        );
    }
    assert_eq!(seen.len(), 6, "{seen:?}");
    assert!(!seen.contains("other"));
}

#[test]
fn every_placement_fault_can_unseat_a_leader() {
    let none = std::collections::BTreeSet::new();
    for seed in 0..10 {
        for step in placement_steps(seed, 5, WINDOW).steps {
            assert!(step.fault.can_unseat(&none), "{}", step.fault);
        }
    }
}

fn four_brokers() -> crate::cluster::SimConfig {
    crate::cluster::SimConfig {
        brokers: 4,
        shards: 6,
        cache_shards: 2,
        clients: 2,
        cache_keys: 4,
        ..crate::cluster::SimConfig::for_seed(1)
    }
    .with_placement_faults()
}

#[tokio::test(start_paused = true)]
async fn draining_a_broker_replaces_the_seats_it_follows() {
    let cluster = crate::cluster::SimCluster::start(1, four_brokers())
        .await
        .unwrap();
    let nemesis = Nemesis::new(cluster.clone(), crate::client::Recorder::new(6));
    nemesis.hold_drain(1).await;
    let mut seen = None;
    for _ in 0..600 {
        if let Some(found) = nemesis.replacing("b2").await {
            seen = Some(found);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let Some((key, _, joining)) = seen else {
        let assignments = cluster
            .cp()
            .request(axum::http::Method::GET, "/v1/placement/plan", None)
            .await
            .unwrap()
            .text();
        panic!(
            "no seat of b2 was replaced within a minute: {:?}\n{assignments}",
            nemesis.recorder.history().faults
        );
    };
    assert_ne!(joining, "b2", "{}", describe(&key));
}

#[tokio::test(start_paused = true)]
async fn a_moved_shard_s_new_leader_is_stopped_before_it_opens() {
    let cluster = crate::cluster::SimCluster::start(1, four_brokers())
        .await
        .unwrap();
    let nemesis = Nemesis::new(cluster.clone(), crate::client::Recorder::new(6));
    let key = crate::cluster::stream_key(0);
    let views = cluster.cp().shard_views().await.unwrap();
    let leader = views[&describe(&key)].leader.clone();
    let destination = (0..4)
        .map(|index| cluster.node_id(index).to_string())
        .find(|id| *id != leader)
        .unwrap();
    let watch = tokio::spawn({
        let nemesis = nemesis.clone();
        let key = key.clone();
        async move {
            nemesis
                .kill_next_promoted(
                    Some(&key),
                    Duration::from_secs(60),
                    Duration::from_secs(5),
                    true,
                )
                .await
        }
    });
    let answer = cluster.cp().move_shard(&key, &destination).await.unwrap();
    assert!(answer.ok, "{answer:?}");
    assert!(
        watch.await.unwrap(),
        "{:?}",
        nemesis.recorder.history().faults
    );
    let faults = nemesis.recorder.history().faults;
    assert!(
        faults
            .iter()
            .any(|f| f.what.contains("leader of stream/0") && f.what.contains("before it opens")),
        "{faults:?}"
    );
}
