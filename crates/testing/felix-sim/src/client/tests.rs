use super::*;
use crate::cluster::SimConfig;

fn config() -> SimConfig {
    SimConfig {
        brokers: 3,
        shards: 4,
        cache_shards: 2,
        cache_keys: 3,
        ..SimConfig::for_seed(5)
    }
}

async fn client(cluster: &Arc<SimCluster>) -> (SimClient, Arc<Recorder>) {
    let recorder = Recorder::new(cluster.config.shards);
    (
        SimClient::new(0, cluster.clone(), recorder.clone(), Rng::new(9)),
        recorder,
    )
}

#[test]
fn payloads_round_trip_and_foreign_ones_are_marked() {
    assert_eq!(decode(&42u64.to_le_bytes()), 42);
    assert_eq!(decode(b"short"), FOREIGN_PAYLOAD);
}

#[tokio::test(start_paused = true)]
async fn an_append_is_acknowledged_and_read_back_at_its_offset() {
    let cluster = SimCluster::start(5, config()).await.unwrap();
    let (mut client, recorder) = client(&cluster).await;
    client.append(2).await;
    let history = recorder.history();
    let Action::Append { outcome, value, .. } = &history.ops[0].action else {
        panic!("{:?}", history.ops[0]);
    };
    let AppendOutcome::Ok {
        offset: Some(offset),
    } = outcome
    else {
        panic!("{outcome:?}");
    };
    let read = client.read_shard(2).await.unwrap();
    assert!(
        read.iter()
            .any(|e| e.offset == *offset && e.value == *value)
    );
}

#[tokio::test(start_paused = true)]
async fn a_cache_put_then_get_is_recorded_as_registers() {
    let cluster = SimCluster::start(5, config()).await.unwrap();
    let (mut client, recorder) = client(&cluster).await;
    client.cache_put().await;
    client.get_every_key().await;
    let history = recorder.history();
    assert!(matches!(
        history.registers[0].action,
        RegisterAction::Put {
            acknowledged: true,
            ..
        }
    ));
    let gets = history
        .registers
        .iter()
        .filter(|op| matches!(op.action, RegisterAction::Get { value: Some(_) }))
        .count();
    assert_eq!(gets, 1);
    assert_eq!(history.registers.len(), 1 + 3);
}

#[tokio::test(start_paused = true)]
async fn an_append_with_no_broker_to_take_it_fails_definitely() {
    let cluster = SimCluster::start(5, config()).await.unwrap();
    for index in 0..3 {
        cluster.crash(index).await;
    }
    let (mut client, recorder) = client(&cluster).await;
    client.append(0).await;
    let history = recorder.history();
    let Action::Append { outcome, .. } = &history.ops[0].action else {
        panic!();
    };
    assert_eq!(*outcome, AppendOutcome::Fail);
}

/// A leader cut off from its peers and the control plane, as a frozen one is,
/// is replaced and, until it hears, still serves reads up to its own committed
/// mark. That read misses what its
/// successor acknowledged, so it must not be recorded as the log's tail.
#[tokio::test(start_paused = true)]
async fn a_read_on_a_replaced_leader_that_has_not_heard_is_not_recorded() {
    let cluster = SimCluster::start(5, config()).await.unwrap();
    let (mut client, recorder) = client(&cluster).await;
    let shard = 2;
    let key = crate::cluster::stream_key(shard);
    let old = cluster.servers_of(&key).await[0];
    let old_generation = cluster.snapshot(old, &key).await.unwrap().generation;
    cluster.cut_controlplane(old, true);
    for other in (0..cluster.broker_count()).filter(|index| *index != old) {
        cluster
            .net()
            .partition(cluster.node_id(old), cluster.node_id(other));
    }

    let give_up = Instant::now() + Duration::from_secs(120);
    let new = 'found: loop {
        assert!(Instant::now() < give_up, "no broker took the shard over");
        for index in (0..cluster.broker_count()).filter(|index| *index != old) {
            if let Some(at) = cluster.snapshot(index, &key).await
                && at.serving
                && at.generation > old_generation
            {
                break 'found index;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let offset = loop {
        assert!(
            Instant::now() < give_up,
            "the new leader acknowledged nothing"
        );
        client.hints.insert(shard, new);
        client.append(shard).await;
        if let Some(Action::Append {
            outcome: AppendOutcome::Ok {
                offset: Some(offset),
            },
            ..
        }) = recorder.history().ops.last().map(|op| &op.action)
        {
            break *offset;
        }
    };

    let stale = cluster.handles(old).unwrap();
    assert!(
        check_readable(Some(&stale.ingress), &key).is_ok(),
        "the old leader already stopped serving reads, so this tests nothing"
    );
    client.hints.insert(shard, old);
    if let Some(read) = client.read_shard(shard).await {
        assert!(
            read.iter().any(|element| element.offset == offset),
            "a read missed offset {offset}, acknowledged before it began: {read:?}"
        );
    }
}
