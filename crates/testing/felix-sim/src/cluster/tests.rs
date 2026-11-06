use super::*;

fn small() -> SimConfig {
    SimConfig {
        brokers: 3,
        shards: 6,
        cache_shards: 2,
        clients: 2,
        cache_keys: 4,
        ..SimConfig::for_seed(1)
    }
}

fn log() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

#[tokio::test(start_paused = true)]
async fn starts_and_serves_every_shard_once() {
    log();
    let cluster = SimCluster::start(1, small()).await.unwrap();
    assert!(cluster.unserved().await.is_empty());
    assert_eq!(cluster.running().len(), 3);
    println!("started after {:?} virtual", cluster.elapsed());
}

/// A leader dials with the pool's offer, so a refusal reaches it with its
/// reason and a learner's ack as one. Otherwise the typed paths never run.
#[tokio::test(start_paused = true)]
async fn a_leader_offers_what_the_pool_offers() {
    log();
    let cluster = SimCluster::start(1, small()).await.unwrap();
    let want = felix_wire::internal::PeerCapabilities::LEARNER_ACKS
        .union(felix_wire::internal::PeerCapabilities::REFUSAL_REASONS);
    // A record is overwritten by the other direction's handshake, so ask
    // whether any broker was last told this by a dialling peer.
    let offered = (0..cluster.broker_count()).any(|from| {
        (0..cluster.broker_count()).any(|target| {
            cluster
                .net()
                .offered_to(cluster.node_id(target), cluster.node_id(from))
                .is_some_and(|offer| offer.contains(want))
        })
    });
    assert!(offered, "no broker dialled with the pool's offer");
}

/// A copy whose routing view missed its leader's first fence holds no
/// promise, and a shard first placed past generation 1 is not one it was
/// created on. Only the control plane's confirmed assignment backs the
/// leader then; without it the copy refuses every fence and batch, and a
/// later fence that needs it never opens (#2028).
#[tokio::test(start_paused = true)]
async fn a_copy_that_missed_the_first_fence_takes_its_leader_once_confirmed() {
    log();
    let cluster = SimCluster::start(
        1,
        SimConfig {
            shards: 2,
            cache_shards: 1,
            ..small()
        },
    )
    .await
    .unwrap();
    let late = 2;
    let shards = 4;
    cluster.cut_controlplane(late, true);
    cluster
        .cp()
        .create_durable_stream(TENANT, NAMESPACE, "late", shards, 3)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    cluster.cut_controlplane(late, false);
    tokio::time::sleep(Duration::from_secs(30)).await;

    let handles = cluster.handles(late).expect("the broker is running");
    let storage = handles.broker.durable_storage().expect("durable storage");
    let mut followed = 0;
    for shard in 0..shards {
        let key = ShardKey {
            stream: "late".to_string(),
            ..stream_key(shard)
        };
        let snapshot = cluster.snapshot(late, &key).await.expect("a snapshot");
        let (leader, generation) = snapshot.routed_leader.expect("a route");
        if leader == cluster.node_id(late) {
            continue;
        }
        followed += 1;
        let accepted = storage
            .opened_stream(TENANT, NAMESPACE, "late", shard)
            .map(|log| log.accepted_generation());
        assert_eq!(
            accepted,
            Some(generation),
            "late/{shard}: the copy on {} has not taken {leader} at generation {generation}",
            cluster.node_id(late),
        );
    }
    assert!(followed > 0, "the cut broker leads every shard");
}
