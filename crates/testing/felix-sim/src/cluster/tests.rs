use super::*;

fn small() -> SimConfig {
    SimConfig {
        brokers: 3,
        shards: 6,
        cache_shards: 2,
        clients: 2,
        cache_keys: 4,
        fsync_mode: FsyncMode::None,
        power_loss: false,
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

/// The first stream shard with a leader and a follower, and which brokers.
async fn a_shard_and_its_two_brokers(cluster: &SimCluster) -> (String, usize, usize) {
    let views = cluster.cp().shard_views().await.unwrap();
    let (shard, view) = views
        .iter()
        .find(|(shard, view)| shard.starts_with("stream/") && view.members.len() == 2)
        .expect("a stream shard with two members");
    let follower = view.members.iter().find(|m| **m != view.leader).unwrap();
    let index = |id: &str| cluster.index_of(id).unwrap();
    (shard.clone(), index(follower), index(&view.leader))
}

#[tokio::test(start_paused = true)]
async fn a_clean_stop_beside_a_kill_holds_nothing() {
    log();
    let cluster = Arc::new(SimCluster::start(1, small()).await.unwrap());
    let (_, follower, leader) = a_shard_and_its_two_brokers(&cluster).await;
    cluster.crash(follower).await;
    cluster.kill(leader).await;
    assert!(cluster.held_shards().await.is_empty());
}

/// A follower killed, then the leader: the shard is held, a shard led by the
/// broker still up is not, and after both restart the shard stays unserved
/// and held: neither restarted copy is trusted.
#[tokio::test(start_paused = true)]
async fn a_held_shard_stays_unserved_after_both_copies_return() {
    log();
    let cluster = Arc::new(SimCluster::start(1, small()).await.unwrap());
    let before = cluster.cp().shard_views().await.unwrap();
    let (shard, follower, leader) = a_shard_and_its_two_brokers(&cluster).await;
    cluster.kill(follower).await;
    cluster.kill(leader).await;
    let held = cluster.held_shards().await;
    assert!(held.contains(&shard), "{held:?}");
    let killed = [cluster.node_id(follower), cluster.node_id(leader)];
    for other in &held {
        assert!(
            killed.contains(&before[other].leader.as_str()),
            "{other} was led by {}, which is still up",
            before[other].leader
        );
    }
    restarted(&cluster, follower).await;
    restarted(&cluster, leader).await;
    tokio::time::sleep(Duration::from_secs(120)).await;
    let unserved: Vec<_> = cluster
        .unserved()
        .await
        .into_iter()
        .map(|(key, _)| describe(&key))
        .collect();
    assert!(
        unserved.contains(&shard),
        "{shard} was served again: {unserved:?}"
    );
    let held = cluster.held_shards().await;
    assert!(held.contains(&shard), "{shard} is no longer held: {held:?}");
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

#[test]
fn power_loss_is_off_by_default_and_fsyncs_on_commit_when_on() {
    let config = SimConfig::for_seed(1);
    assert!(!config.power_loss);
    assert_eq!(config.fsync_mode, FsyncMode::None);
    let config = config.with_power_loss();
    assert!(config.power_loss);
    assert_eq!(config.fsync_mode, FsyncMode::OnCommit);
}

#[tokio::test(start_paused = true)]
async fn power_loss_needs_fsync_on_commit() {
    let config = SimConfig {
        power_loss: true,
        fsync_mode: FsyncMode::None,
        ..small()
    };
    assert!(SimCluster::start(1, config).await.is_err());
}

/// A cut inside an append leaves a torn tail; the restart repairs it, keeps
/// its storage id and starts clean, since every acknowledged record was
/// synced, so nothing is held.
#[tokio::test(start_paused = true)]
async fn a_power_cut_during_an_append_is_repaired_on_the_restart() {
    use crate::client::{Recorder, SimClient};
    log();
    let config = SimConfig {
        fsync_mode: FsyncMode::OnCommit,
        power_loss: true,
        ..small()
    };
    let cluster = SimCluster::start(1, config).await.unwrap();
    let recorder = Recorder::new(cluster.config.shards);
    let stop = CancellationToken::new();
    let client = SimClient::new(0, cluster.clone(), recorder, Rng::new(1));
    let client = tokio::spawn(client.run(stop.clone()));
    tokio::time::sleep(Duration::from_secs(2)).await;

    let before = cluster.torn_tails_repaired(0).len();
    assert!(cluster.power_loss(0).await, "no append to cut");
    assert!(cluster.is_down(0));
    let torn = cluster.power_losses()[0]
        .torn
        .clone()
        .expect("a torn segment");
    restarted(&cluster, 0).await;
    assert_eq!(cluster.started_unclean(0), Some(false));
    let repaired = || cluster.torn_tails_repaired(0)[before..].contains(&torn);
    for _ in 0..300 {
        if repaired() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(repaired(), "{} was not repaired", torn.display());
    stop.cancel();
    let _ = client.await;
    assert!(cluster.held_shards().await.is_empty());
    let losses = cluster.power_losses();
    assert_eq!(losses.len(), 1);
    assert!(losses[0].restarted_at.is_some());
}

#[tokio::test(start_paused = true)]
async fn a_copy_on_disk_but_not_open_after_a_kill_is_untrusted() {
    log();
    let cluster = Arc::new(SimCluster::start(1, small()).await.unwrap());
    cluster.kill(0).await;
    cluster.restart(0);
    while !cluster.is_running(0) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(cluster.started_unclean(0), Some(true));

    let broker = cluster.handles(0).expect("running").broker;
    let mut unopened = 0;
    for key in cluster.shard_keys() {
        let storage = broker.durable_storage().expect("durable storage");
        let on_disk = key.kind == ShardKind::Stream
            && storage.stream_on_disk(&key.tenant_id, &key.namespace, &key.stream, key.shard);
        let open = on_disk
            && storage
                .opened_stream(&key.tenant_id, &key.namespace, &key.stream, key.shard)
                .is_some();
        if on_disk && !open {
            unopened += 1;
            assert!(
                matches!(
                    cluster.nodes[0].copy_trust(&key).await,
                    Some(kills::Copy::Untrusted)
                ),
                "{} is on disk but not open, and a fence would find it untrusted",
                describe(&key),
            );
        }
    }
    assert!(unopened > 0, "no copy was left unopened by the restart");
}
