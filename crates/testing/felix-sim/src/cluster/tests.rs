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
