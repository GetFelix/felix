use felix_replication::liveness::Decision;

use super::*;
use crate::cluster::SimConfig;

fn small() -> SimConfig {
    SimConfig {
        brokers: 3,
        shards: 6,
        cache_shards: 2,
        clients: 2,
        cache_keys: 4,
        faults: false,
        store_liveness: true,
        ..SimConfig::for_seed(1)
    }
}

fn secs(secs: u64) -> Duration {
    Duration::from_secs(secs)
}

fn liveness(cluster: &SimCluster, index: usize) -> Arc<Liveness> {
    cluster
        .store_liveness(index)
        .expect("a running broker with store liveness on")
}

fn withdrew(cluster: &SimCluster, index: usize, peer: usize) -> bool {
    liveness(cluster, index)
        .shadow()
        .decisions
        .contains(&Decision::Withdraw {
            peer: cluster.node_id(peer).to_string(),
        })
}

/// Every broker sends and has every other broker's support.
fn all_supported(cluster: &SimCluster) -> bool {
    (0..cluster.broker_count()).all(|index| {
        let liveness = liveness(cluster, index);
        liveness.shadow().sending
            && (0..cluster.broker_count())
                .filter(|peer| *peer != index)
                .all(|peer| liveness.supports(cluster.node_id(peer)) == Some(true))
    })
}

/// The slow-sync fault. **A broker whose probe sync stalls goes silent while
/// its network works, its peers withdraw support, and the hybrid does
/// nothing about it**: every shard keeps its leader and its generation, and
/// stays served. When the disk answers again, so does the broker.
#[tokio::test(start_paused = true)]
async fn a_stalled_probe_silences_a_broker_and_the_hybrid_does_not_move() {
    let cluster = SimCluster::start(1, small()).await.unwrap();
    tokio::time::sleep(secs(5)).await;
    assert!(all_supported(&cluster));
    let before = format!("{:?}", cluster.cp().shard_views().await.unwrap());
    let stalled = 0;

    cluster.slow_probe_sync(stalled, secs(20));
    tokio::time::sleep(secs(4)).await;
    assert!(
        all_supported(&cluster),
        "under the stall limit a slow sync silences nobody"
    );

    tokio::time::sleep(secs(6)).await;
    assert!(!liveness(&cluster, stalled).shadow().sending);
    for peer in 1..cluster.broker_count() {
        assert!(withdrew(&cluster, peer, stalled), "b{}", peer + 1);
        assert_eq!(
            liveness(&cluster, stalled).supports(cluster.node_id(peer)),
            Some(true),
            "the stalled broker still hears b{}",
            peer + 1
        );
    }

    // Shadow mode: the withdrawal is on record and the hybrid has not acted.
    assert_eq!(
        format!("{:?}", cluster.cp().shard_views().await.unwrap()),
        before,
        "a leader or a generation changed"
    );
    assert!(cluster.unserved().await.is_empty());

    cluster.slow_probe_sync(stalled, Duration::ZERO);
    tokio::time::sleep(secs(25)).await;
    assert!(all_supported(&cluster));
    assert_eq!(
        format!("{:?}", cluster.cp().shard_views().await.unwrap()),
        before
    );
}

/// **One slow sync withdraws nothing.** A single 3 s sync is longer than the
/// silence after which a peer withdraws support and shorter than the stall
/// limit.
#[tokio::test(start_paused = true)]
async fn one_slow_probe_sync_withdraws_nothing() {
    let cluster = SimCluster::start(1, small()).await.unwrap();
    tokio::time::sleep(secs(5)).await;
    cluster.slow_probe_sync(0, secs(3));
    tokio::time::sleep(Duration::from_millis(600)).await;
    cluster.slow_probe_sync(0, Duration::ZERO);
    tokio::time::sleep(secs(10)).await;

    assert!(all_supported(&cluster));
    for index in 0..cluster.broker_count() {
        let decisions = liveness(&cluster, index).shadow().decisions;
        assert!(
            !decisions
                .iter()
                .any(|d| matches!(d, Decision::Withdraw { .. } | Decision::Silenced)),
            "b{}: {decisions:?}",
            index + 1
        );
    }
}

/// **The control plane is not on the path.** With every broker cut off from
/// it, a stalled disk is still noticed by the broker's peers.
#[tokio::test(start_paused = true)]
async fn a_stall_is_noticed_with_the_control_plane_cut_off() {
    let cluster = SimCluster::start(1, small()).await.unwrap();
    tokio::time::sleep(secs(5)).await;
    for index in 0..cluster.broker_count() {
        cluster.cut_controlplane(index, true);
    }
    cluster.slow_probe_sync(0, secs(60));
    tokio::time::sleep(secs(10)).await;
    for peer in 1..cluster.broker_count() {
        assert!(withdrew(&cluster, peer, 0), "b{}", peer + 1);
    }
}

/// **A restart shows a new epoch to peers.** The epoch is kept in the data
/// directory, so it rises across the restart.
#[tokio::test(start_paused = true)]
async fn a_restart_shows_a_new_epoch_to_peers() {
    let cluster = Arc::new(SimCluster::start(1, small()).await.unwrap());
    tokio::time::sleep(secs(5)).await;
    let restarted = cluster.node_id(0).to_string();
    let before = liveness(&cluster, 0).shadow().epoch;
    assert_eq!(liveness(&cluster, 1).epoch_of(&restarted), Some(before));

    cluster.crash(0).await;
    cluster.restart(0);
    for _ in 0..600 {
        if cluster.is_running(0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(secs(5)).await;

    assert_eq!(liveness(&cluster, 0).shadow().epoch, before + 1);
    for peer in 1..cluster.broker_count() {
        assert!(
            liveness(&cluster, peer)
                .shadow()
                .decisions
                .contains(&Decision::Restarted {
                    peer: restarted.clone(),
                    epoch: before + 1
                }),
            "b{}",
            peer + 1
        );
    }
}

/// **Off unless a run asks.** Without `store_liveness` no broker runs it, so
/// existing seeds send what they always sent.
#[tokio::test(start_paused = true)]
async fn store_liveness_is_off_by_default() {
    assert!(!SimConfig::for_seed(1).store_liveness);
    let config = SimConfig {
        store_liveness: false,
        ..small()
    };
    let cluster = SimCluster::start(1, config).await.unwrap();
    for index in 0..cluster.broker_count() {
        assert!(cluster.store_liveness(index).is_none());
    }
}
