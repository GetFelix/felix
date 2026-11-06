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

/// Restart `index` and wait for it to be running again.
async fn restarted(cluster: &Arc<SimCluster>, index: usize) {
    cluster.restart(index);
    for _ in 0..600 {
        if cluster.is_running(index) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("b{} did not come back", index + 1);
}

#[tokio::test(start_paused = true)]
async fn a_killed_broker_restarts_unclean_and_a_crashed_one_does_not() {
    log();
    let cluster = Arc::new(SimCluster::start(1, small()).await.unwrap());
    assert_eq!(cluster.started_unclean(0), Some(false));

    cluster.crash(0).await;
    restarted(&cluster, 0).await;
    assert_eq!(cluster.started_unclean(0), Some(false), "a clean stop");

    cluster.kill(0).await;
    restarted(&cluster, 0).await;
    assert_eq!(cluster.started_unclean(0), Some(true), "a kill");
}
