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
