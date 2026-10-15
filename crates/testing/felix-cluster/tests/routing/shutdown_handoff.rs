//! A broker told to stop hands its shards to the others first, so a rolling
//! restart moves shards instead of failing them over.
//!
//! Run with `cargo test -p felix-cluster --test routing shutdown_handoff::`.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use felix_cluster::{Cluster, ClusterConfig, Endpoint, Fault, StreamSpec};
use serial_test::serial;
use tokio::sync::Mutex;

const STREAM: &str = "orders";

/// The broker's default `FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS` plus its default
/// drain deadline: an idle client connection holds the drain until the latter.
const EXIT_WITHIN: Duration = Duration::from_secs(30 + 25 + 10);

/// **SIGTERM under load moves the shard; nothing fails over.** A publisher
/// writes through another broker and a subscriber reads from the stopping
/// one. The shard is moved (placement writes move steps, never a new
/// placement), no publish is refused, every acknowledged record reaches the
/// subscriber once and in order, and the broker exits in bounded time.
#[serial]
#[tokio::test]
async fn a_stopping_broker_hands_its_shard_over_under_load() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::replicated(STREAM, 1, 2)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster
        .wait_for_replication(STREAM, Duration::from_secs(30))
        .await
        .expect("the owner ships to its follower");
    let other = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &owner)
        .expect("another broker");
    let addr = |cluster: &Cluster, node: &str| cluster.node(node).expect("node").client_addr;

    let mut owed: Vec<Vec<u8>> = Vec::new();
    for i in 0..20 {
        let payload = format!("before-{i}").into_bytes();
        cluster
            .publish_keyed_via_settled(
                &owner,
                STREAM,
                b"k",
                payload.clone(),
                Duration::from_secs(30),
            )
            .await
            .expect("publish before the stop");
        owed.push(payload);
    }

    let reader = Arc::new(
        felix_cluster::client::connect_cluster(
            &[addr(&cluster, &owner)],
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("reader"),
    );
    let mut subscription = reader
        .subscribe_from(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            Some(felix_client::StartPosition::Offset(0)),
        )
        .await
        .expect("subscribe");

    let writer = felix_cluster::client::connect_cluster(
        &[addr(&cluster, &other)],
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("writer");
    let acknowledged: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
    let refused: Arc<Mutex<Vec<String>>> = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let publisher = {
        let (acknowledged, refused, stop) = (
            Arc::clone(&acknowledged),
            Arc::clone(&refused),
            Arc::clone(&stop),
        );
        let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
        tokio::spawn(async move {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let payload = format!("during-{i}").into_bytes();
                i += 1;
                match writer
                    .publish(
                        &tenant,
                        &namespace,
                        STREAM,
                        payload.clone(),
                        felix_wire::AckMode::PerMessage,
                    )
                    .await
                {
                    Ok(_) => acknowledged.lock().await.push(payload),
                    Err(err) => {
                        refused.lock().await.push(format!("{err:#}"));
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    tokio::time::sleep(Duration::from_millis(300)).await;
    let signalled = Instant::now();
    cluster.terminate_node(&owner).expect("SIGTERM");

    // Placement stepped by hand, so every pass's outcome is seen: a failover
    // is a placement, a handoff is move steps.
    let (mut placed, mut moved) = (0usize, 0usize);
    let mut exited = None;
    let mut moved_at = None;
    while exited.is_none() || moved_at.is_none() {
        let outcome = cluster.place_shards().await;
        placed += outcome.placed + outcome.failed;
        moved += outcome.moved;
        if moved_at.is_none() && cluster.owner(STREAM).await.is_ok_and(|now| now != owner) {
            moved_at = Some(signalled.elapsed());
        }
        if exited.is_none() {
            exited = cluster
                .exit_status(&owner)
                .expect("node")
                .map(|status| (status, signalled.elapsed()));
        }
        assert!(
            signalled.elapsed() < EXIT_WITHIN,
            "after {:?}: exited {exited:?}, shard moved at {moved_at:?}",
            signalled.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (status, exited_after) = exited.expect("exited");
    let moved_at = moved_at.expect("moved");

    // Traffic after the stop too, so the destination is shown taking writes.
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    publisher.await.expect("publisher");
    let during = acknowledged.lock().await.clone();
    let refused = refused.lock().await.clone();

    assert!(
        placed == 0 && moved > 0 && refused.is_empty(),
        "the shard should be handed over, not failed over: {placed} placements, \
         {moved} move steps, {} publishes refused (of {}); moved {moved_at:?} \
         after SIGTERM, exited after {exited_after:?}; refusals: {refused:?}",
        refused.len(),
        during.len() + refused.len()
    );
    assert!(status.success(), "the broker should exit cleanly: {status}");
    assert!(
        moved_at < exited_after,
        "the shard should move before the broker exits ({moved_at:?} vs {exited_after:?})"
    );
    assert!(
        during.len() > 20,
        "too little was acknowledged to prove anything"
    );
    owed.extend(during);

    // Offsets less the generation-start records skipped so far, so a promotion
    // leaves no gap and any other jump is a drop.
    let mut offsets = Vec::new();
    let mut skipped = 0;
    let mut payloads = Vec::new();
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
    while !owed.iter().all(|payload| payloads.contains(payload)) {
        match tokio::time::timeout_at(deadline, subscription.next_event()).await {
            Ok(Ok(Some(event))) => {
                skipped += event.skipped_before;
                offsets.push(event.offset.expect("a durable stream carries offsets") - skipped);
                payloads.push(event.payload.to_vec());
            }
            Ok(Ok(None)) => panic!(
                "the subscription ended after {} records instead of following the shard",
                payloads.len()
            ),
            Ok(Err(err)) => panic!("the subscription failed: {err:#}"),
            Err(_) => panic!(
                "timed out with {} of {} acknowledged records",
                owed.iter().filter(|p| payloads.contains(p)).count(),
                owed.len()
            ),
        }
    }
    let expected: Vec<u64> = (0..offsets.len() as u64).collect();
    assert_eq!(offsets, expected, "every offset once and in order");
    let mut unique = payloads.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), payloads.len(), "a record arrived twice");
    cluster.shutdown().await;
}

/// **A reader told only that its shard moved finds it even when its one way
/// in is shutting down.** The reader's only seed is the owner. The owner is
/// cut off from the control plane, so its lease lapses and it ends the reader
/// without naming a new owner, which sends the client back to its entry
/// broker: the owner itself, now refusing for want of a lease. The shard fails
/// over, and then the owner is stopped with no handoff, so it answers
/// `draining` until it exits. The reader has to leave it for a broker that
/// knows where the shard went, and resume there with every acknowledged
/// record once and in order.
#[serial]
#[tokio::test]
async fn a_reader_follows_a_moved_shard_off_a_draining_entry_broker() {
    use felix_controlplane_service::cluster::placement::Decision;

    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::replicated(STREAM, 1, 2)],
        proxy_links: true,
        broker_env: vec![
            (
                "FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS".to_string(),
                "0".to_string(),
            ),
            (
                "FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS".to_string(),
                "3000".to_string(),
            ),
        ],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster
        .wait_for_replication(STREAM, Duration::from_secs(30))
        .await
        .expect("the owner ships to its follower");
    let other = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &owner)
        .expect("another broker");

    let mut owed: Vec<Vec<u8>> = Vec::new();
    for i in 0..20 {
        let payload = format!("before-{i}").into_bytes();
        cluster
            .publish_keyed_via_settled(
                &owner,
                STREAM,
                b"k",
                payload.clone(),
                Duration::from_secs(30),
            )
            .await
            .expect("publish before the fault");
        owed.push(payload);
    }
    // A failover keeps only what the owner last reported its follower holds,
    // so wait for a report that would promote it with everything above.
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "a report naming a follower that holds every record",
        || async {
            let level = cluster
                .metric(&owner, "felix_broker_replication_lag_records")
                .await
                .ok()
                .flatten()
                == Some(0.0);
            level
                && cluster.plan_if_down(&owner).await.is_ok_and(|plan| {
                    plan.shards.iter().any(|shard| {
                        shard.key.stream == STREAM && matches!(shard.decision, Decision::Place(..))
                    })
                })
        },
    )
    .await
    .expect("the follower caught up");

    let reader = Arc::new(
        felix_cluster::client::connect_cluster(
            &[cluster.node(&owner).expect("node").client_addr],
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("reader"),
    );
    let mut subscription = reader
        .subscribe_from(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            Some(felix_client::StartPosition::Offset(0)),
        )
        .await
        .expect("subscribe");

    // Offsets less the generation-start records skipped so far, so a promotion
    // leaves no gap and any other jump is a drop.
    let mut offsets = Vec::new();
    let mut skipped = 0;
    let mut payloads: Vec<Vec<u8>> = Vec::new();
    let mut read_until = async |owed: &[Vec<u8>], payloads: &mut Vec<Vec<u8>>| {
        let deadline =
            tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
        while !owed.iter().all(|payload| payloads.contains(payload)) {
            match tokio::time::timeout_at(deadline, subscription.next_event()).await {
                Ok(Ok(Some(event))) => {
                    skipped += event.skipped_before;
                    offsets.push(event.offset.expect("a durable stream carries offsets") - skipped);
                    payloads.push(event.payload.to_vec());
                }
                Ok(Ok(None)) => panic!(
                    "the subscription ended after {} records instead of following the shard",
                    payloads.len()
                ),
                Ok(Err(err)) => panic!("the subscription failed: {err:#}"),
                Err(_) => panic!(
                    "timed out with {} of {} acknowledged records",
                    owed.iter().filter(|p| payloads.contains(p)).count(),
                    owed.len()
                ),
            }
        }
    };
    read_until(&owed, &mut payloads).await;

    // The lapse ends the reader with no owner named: the owner cannot see
    // the assignment, so it does not know one.
    let log_path = cluster
        .node(&owner)
        .expect("node")
        .data_dir
        .join("broker.log");
    let ended = || {
        std::fs::read_to_string(&log_path)
            .unwrap_or_default()
            .matches("ended readers of a shard this broker no longer serves")
            .count()
    };
    let ended_before = ended();
    for fault in Fault::partition(Endpoint::node(&owner), Endpoint::ControlPlane) {
        cluster
            .inject(&fault)
            .await
            .expect("cut from the control plane");
    }
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the owner to end its readers when its lease lapses",
        || async { ended() > ended_before },
    )
    .await
    .expect("readers ended");
    felix_cluster::wait::until(Duration::from_secs(30), "a failover", || async {
        cluster.place_shards().await;
        cluster.owner(STREAM).await.is_ok_and(|now| now != owner)
    })
    .await
    .expect("the follower is promoted");

    // No handoff, so it answers `draining` at once and exits after the drain.
    cluster.terminate_node(&owner).expect("SIGTERM");

    for i in 0..20 {
        let payload = format!("after-{i}").into_bytes();
        cluster
            .publish_keyed_via_settled(
                &other,
                STREAM,
                b"k",
                payload.clone(),
                Duration::from_secs(30),
            )
            .await
            .expect("publish after the failover");
        owed.push(payload);
    }
    read_until(&owed, &mut payloads).await;

    let expected: Vec<u64> = (0..offsets.len() as u64).collect();
    assert_eq!(offsets, expected, "every offset once and in order");
    let mut unique = payloads.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), payloads.len(), "a record arrived twice");
    cluster.shutdown().await;
}

/// **A restarted broker takes shards again.** The drain its shutdown asked
/// for does not outlive the process: once it registers again it is placeable
/// and rebalancing gives it back a share.
#[serial]
#[tokio::test]
async fn a_restarted_broker_takes_shards_again() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::replicated(STREAM, 3, 2)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let leads = |owners: &std::collections::HashMap<u32, String>, node: &str| {
        owners
            .values()
            .filter(|owner| owner.as_str() == node)
            .count()
    };
    let owners = cluster.shard_owners_for(STREAM).await.expect("owners");
    let stopping = owners.values().next().expect("a leader").clone();

    cluster.terminate_node(&stopping).expect("SIGTERM");
    let signalled = Instant::now();
    loop {
        cluster.place_shards().await;
        if cluster.exit_status(&stopping).expect("node").is_some() {
            break;
        }
        assert!(signalled.elapsed() < EXIT_WITHIN, "{stopping} did not exit");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let owners = cluster.shard_owners_for(STREAM).await.expect("owners");
    assert_eq!(leads(&owners, &stopping), 0, "it handed everything over");

    cluster.restart_node(&stopping).await.expect("restart");
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "the restarted broker to lead a shard again",
        || async {
            cluster.place_shards().await;
            cluster
                .shard_owners_for(STREAM)
                .await
                .is_ok_and(|owners| leads(&owners, &stopping) > 0)
        },
    )
    .await
    .expect("rebalanced onto the restarted broker");
    cluster.shutdown().await;
}

/// **A lone broker does not wait for a handoff that cannot happen.** With
/// nobody to take its shard it stops as promptly as before.
#[serial]
#[tokio::test]
async fn a_single_broker_stops_without_waiting_for_a_handoff() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    cluster.terminate_node(&owner).expect("SIGTERM");
    let status = cluster
        .wait_for_exit(&owner, Duration::from_secs(10))
        .await
        .expect("a lone broker should not wait out the handoff timeout");
    assert!(status.success(), "clean exit: {status}");
    cluster.shutdown().await;
}

/// Brokers that start with `handoff_ms` as their handoff timeout.
fn config_with_handoff(nodes: usize, stream: StreamSpec, handoff_ms: u64) -> ClusterConfig {
    ClusterConfig {
        nodes,
        streams: vec![stream],
        broker_env: vec![
            (
                "FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS".to_string(),
                handoff_ms.to_string(),
            ),
            // A publisher holds its connection open, so the drain would wait
            // out its whole default deadline.
            (
                "FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS".to_string(),
                "3000".to_string(),
            ),
        ],
        ..Default::default()
    }
}

/// A publish the broker acknowledged: what, under which key, and when.
#[derive(Clone)]
struct Acked {
    payload: Vec<u8>,
    key: usize,
    at: Instant,
}

/// Keyed publishers sharing one `ClusterClient` until told to stop, keeping
/// what was acknowledged. A failed publish is never sent again.
struct Publisher {
    keys: usize,
    acknowledged: Arc<Mutex<Vec<Acked>>>,
    refused: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Publisher {
    /// Several in flight at once: one at a time, a debug-build `Quorum`
    /// publish is too slow to put much through a one-second window.
    const WORKERS: usize = 8;

    fn start(client: felix_client::ClusterClient, cluster: &Cluster, keys: usize) -> Self {
        let client = Arc::new(client);
        let acknowledged: Arc<Mutex<Vec<Acked>>> = Arc::default();
        let refused = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let tasks = (0..Self::WORKERS)
            .map(|worker| {
                let (client, acknowledged, refused, stop) = (
                    Arc::clone(&client),
                    Arc::clone(&acknowledged),
                    Arc::clone(&refused),
                    Arc::clone(&stop),
                );
                let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
                tokio::spawn(async move {
                    let mut i = 0usize;
                    while !stop.load(Ordering::Relaxed) {
                        let payload = format!("during-{worker}-{i}").into_bytes();
                        let key_index = (i * Self::WORKERS + worker) % keys;
                        let key = bytes::Bytes::from(format!("k{key_index}"));
                        i += 1;
                        match client
                            .publish_keyed(
                                &tenant,
                                &namespace,
                                STREAM,
                                payload.clone(),
                                key,
                                felix_wire::AckMode::PerMessage,
                            )
                            .await
                        {
                            Ok(_) => acknowledged.lock().await.push(Acked {
                                payload,
                                key: key_index,
                                at: Instant::now(),
                            }),
                            Err(_) => {
                                refused.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                })
            })
            .collect();
        Self {
            keys,
            acknowledged,
            refused,
            stop,
            tasks,
        }
    }

    /// Keep publishing until at least `total` publishes are acknowledged and,
    /// with `every_key_since`, every key has had one acknowledged since then,
    /// or `bound` runs out. Returns whether that was reached; the caller's
    /// assertions say what falling short means. Waits on progress rather than
    /// a fixed time: a slow machine acknowledges less in any given window.
    async fn wait_for(
        &self,
        total: usize,
        every_key_since: Option<Instant>,
        bound: Duration,
    ) -> bool {
        let deadline = Instant::now() + felix_cluster::wait::budget(bound);
        loop {
            {
                let acked = self.acknowledged.lock().await;
                let keys_covered = every_key_since.is_none_or(|since| {
                    let keys: std::collections::HashSet<usize> = acked
                        .iter()
                        .filter(|ack| ack.at >= since)
                        .map(|ack| ack.key)
                        .collect();
                    keys.len() == self.keys
                });
                if acked.len() >= total && keys_covered {
                    return true;
                }
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Stop, and return what was acknowledged and how many were refused.
    async fn finish(self) -> (Vec<Acked>, usize) {
        self.stop.store(true, Ordering::Relaxed);
        for task in self.tasks {
            task.await.expect("publisher");
        }
        let acknowledged = self.acknowledged.lock().await.clone();
        (acknowledged, self.refused.load(Ordering::Relaxed))
    }
}

/// A shard and the `(offset, payload)` records read back from it.
type ShardRecords = (u32, Vec<(u64, Vec<u8>)>);

/// Replay every shard from its current leader until each has been quiet for
/// a while, and return each shard's `(offset, payload)` in delivery order.
async fn read_every_shard(cluster: &Cluster, shards: u32) -> Vec<ShardRecords> {
    let owners = cluster.shard_owners_for(STREAM).await.expect("owners");
    let mut readers = Vec::new();
    for shard in 0..shards {
        let owner = owners.get(&shard).expect("every shard is led").clone();
        let (client, mut subscription) = felix_cluster::wait::until_some(
            Duration::from_secs(30),
            &format!("replay shard {shard} on {owner}"),
            || async { cluster.replay_shard(&owner, STREAM, shard).await.ok() },
        )
        .await
        .expect("replay");
        readers.push(tokio::spawn(async move {
            let _client = client;
            // As above: offsets less the generation-start records skipped.
            let mut records = Vec::new();
            let mut skipped = 0;
            loop {
                match tokio::time::timeout(Duration::from_secs(2), subscription.next_event()).await
                {
                    Ok(Ok(Some(event))) => {
                        skipped += event.skipped_before;
                        records.push((
                            event.offset.expect("a durable stream carries offsets") - skipped,
                            event.payload.to_vec(),
                        ));
                    }
                    Ok(Ok(None)) => break,
                    Ok(Err(err)) => panic!("replay of shard {shard} failed: {err:#}"),
                    Err(_) => break,
                }
            }
            (shard, records)
        }));
    }
    let mut out = Vec::new();
    for reader in readers {
        out.push(reader.await.expect("reader"));
    }
    out
}

/// Every acknowledged record is in the stream exactly once, no record is
/// there twice, and every shard's offsets run from 0 without a gap.
fn assert_exactly_once(owed: &[Vec<u8>], read: &[ShardRecords]) {
    let mut seen: std::collections::HashMap<&[u8], usize> = std::collections::HashMap::new();
    for (shard, records) in read {
        let offsets: Vec<u64> = records.iter().map(|(offset, _)| *offset).collect();
        let expected: Vec<u64> = (0..offsets.len() as u64).collect();
        assert_eq!(
            offsets, expected,
            "shard {shard}: every offset once and in order"
        );
        for (_, payload) in records {
            *seen.entry(payload.as_slice()).or_default() += 1;
        }
    }
    let twice: Vec<String> = seen
        .iter()
        // The harness's own start-up probes are published once per broker.
        .filter(|(payload, count)| **count > 1 && *payload != b"harness-probe")
        .map(|(payload, count)| format!("{} x{count}", String::from_utf8_lossy(payload)))
        .collect();
    assert!(twice.is_empty(), "stored more than once: {twice:?}");
    let missing: Vec<String> = owed
        .iter()
        .filter(|payload| !seen.contains_key(payload.as_slice()))
        .map(|payload| String::from_utf8_lossy(payload).into_owned())
        .collect();
    assert!(
        missing.is_empty(),
        "{} of {} acknowledged records are gone: {missing:?}",
        missing.len(),
        owed.len()
    );
}

/// **A handoff that times out loses no acknowledged record.** Sixteen
/// `Quorum` shards, three copies each, on four brokers; publishers ask for a
/// per-message ack, which for `Quorum` means a majority has the record even
/// with `FELIX_ACK_ON_COMMIT` off. The stopping broker gets 1.5 s to hand off
/// while placement runs one move at a time, so some shards move and the rest
/// are still led when it gives up; those fail over once it has exited. Every
/// acknowledged record is then read back from the new leaders exactly once,
/// with no gap in any shard's offsets.
///
/// `Leader` consistency makes no such promise: the ack goes out before a
/// follower has the record, so a failover may lose it.
#[serial]
#[tokio::test]
async fn a_handoff_that_times_out_loses_no_acknowledged_record() {
    const SHARDS: u32 = 16;
    let mut cluster = Cluster::start(config_with_handoff(
        4,
        StreamSpec::quorum(STREAM, SHARDS, 3),
        1_500,
    ))
    .await
    .expect("start cluster");
    let owners = cluster.shard_owners_for(STREAM).await.expect("owners");
    let stopping = cluster
        .node_ids()
        .into_iter()
        .max_by_key(|node| owners.values().filter(|owner| *owner == node).count())
        .expect("a broker");
    let led = owners.values().filter(|owner| **owner == stopping).count();
    assert!(
        led >= 3,
        "{stopping} should lead several shards, leads {led}"
    );
    let addrs = cluster.broker_addrs();

    let mut owed = Vec::new();
    for i in 0..64 {
        let payload = format!("before-{i}").into_bytes();
        let node = cluster.node_ids()[i % 4].clone();
        cluster
            .publish_keyed_via_settled(
                &node,
                STREAM,
                format!("k{i}").as_bytes(),
                payload.clone(),
                Duration::from_secs(30),
            )
            .await
            .expect("publish before the stop");
        owed.push(payload);
    }

    let writer =
        felix_cluster::client::connect_cluster(&addrs, &cluster.tenant_id, &cluster.client_token())
            .await
            .expect("writer");
    let publisher = Publisher::start(writer, &cluster, 64);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let log_path = cluster
        .node(&stopping)
        .expect("node")
        .data_dir
        .join("broker.log");
    let timed_out = || {
        std::fs::read_to_string(&log_path)
            .unwrap_or_default()
            .contains("shutdown handoff timed out")
    };
    let signalled = Instant::now();
    cluster.terminate_node(&stopping).expect("SIGTERM");

    // Moves until the handoff gives up. A move takes three or four passes, so
    // one at a time, a pass every 300 ms, leaves shards led when 1.5 s is up.
    let mut moved = 0usize;
    while !timed_out() {
        moved += cluster.place_shards_moving(1).await.moved;
        assert!(
            signalled.elapsed() < EXIT_WITHIN,
            "the handoff did not time out ({moved} move steps)"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    // No placement while it drains, or the moves in flight finish on the
    // still-serving broker and nothing fails over.
    let status = cluster
        .wait_for_exit(&stopping, EXIT_WITHIN)
        .await
        .expect("the broker exits once its drain is done");
    assert!(status.success(), "clean exit: {status}");
    let failed_over: Vec<u32> = cluster
        .shard_owners_for(STREAM)
        .await
        .expect("owners")
        .into_iter()
        .filter(|(_, owner)| *owner == stopping)
        .map(|(shard, _)| shard)
        .collect();
    let still_led = failed_over.len();
    assert!(
        moved > 0 && still_led > 0,
        "some shards should have moved and some still be led at exit: \
         {moved} move steps, {still_led} still led"
    );

    // The rest fail over.
    let mut placed = 0usize;
    let exited_at = Instant::now();
    loop {
        let outcome = cluster.place_shards_moving(1).await;
        placed += outcome.placed + outcome.failed;
        let owners = cluster.shard_owners_for(STREAM).await.unwrap_or_default();
        if owners.len() == SHARDS as usize && owners.values().all(|owner| owner != &stopping) {
            break;
        }
        assert!(
            exited_at.elapsed() < Duration::from_secs(60),
            "shards still on {stopping}: {owners:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(placed > 0, "the shards still led should have failed over");

    // Writes to the new leaders too: every key, so every shard that failed
    // over, has to take one before the publishers stop.
    let failed_over_at = Instant::now();
    publisher
        .wait_for(100, Some(failed_over_at), Duration::from_secs(60))
        .await;
    let (during, refused) = publisher.finish().await;
    assert!(
        during.len() > 100,
        "too little was acknowledged to prove anything ({} acked, {refused} refused)",
        during.len()
    );
    let after_failover: std::collections::HashSet<Vec<u8>> = during
        .iter()
        .filter(|ack| ack.at >= failed_over_at)
        .map(|ack| ack.payload.clone())
        .collect();
    owed.extend(during.into_iter().map(|ack| ack.payload));

    let read = read_every_shard(&cluster, SHARDS).await;
    assert_exactly_once(&owed, &read);
    for shard in &failed_over {
        let (_, records) = read.iter().find(|(s, _)| s == shard).expect("read");
        assert!(
            records
                .iter()
                .any(|(_, payload)| after_failover.contains(payload)),
            "shard {shard} failed over but took no acknowledged write afterwards \
             ({} acknowledged after failover in all)",
            after_failover.len()
        );
    }
    cluster.shutdown().await;
}

/// **A lone broker that stops keeps what it acknowledged.** There is nowhere
/// to hand off, so it stops at once, under a publisher asking for per-message
/// acks with `FELIX_ACK_ON_COMMIT` off (the ack goes out once the record is
/// queued, before it is written). The drain is what has to get those records
/// to disk: every acknowledged one is there when the broker comes back.
#[serial]
#[tokio::test]
async fn a_lone_broker_that_stops_keeps_what_it_acknowledged() {
    const SHARDS: u32 = 4;
    let mut cluster = Cluster::start(config_with_handoff(
        1,
        StreamSpec::new(STREAM, SHARDS),
        30_000,
    ))
    .await
    .expect("start cluster");
    let node = cluster.node_ids()[0].clone();
    let writer = felix_cluster::client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("writer");
    let publisher = Publisher::start(writer, &cluster, 16);
    publisher.wait_for(100, None, Duration::from_secs(30)).await;

    cluster.terminate_node(&node).expect("SIGTERM");
    let status = cluster
        .wait_for_exit(&node, Duration::from_secs(15))
        .await
        .expect("a lone broker should not wait out the handoff timeout");
    assert!(status.success(), "clean exit: {status}");
    let (acked, refused) = publisher.finish().await;
    let owed: Vec<Vec<u8>> = acked.into_iter().map(|ack| ack.payload).collect();
    assert!(
        owed.len() > 50,
        "too little was acknowledged to prove anything ({} acked, {refused} refused)",
        owed.len()
    );

    cluster.restart_node(&node).await.expect("restart");
    let read = read_every_shard(&cluster, SHARDS).await;
    assert_exactly_once(&owed, &read);
    cluster.shutdown().await;
}

/// **A stopping leader leaves a follower that can take over.** The shard's
/// last record reached only the leader: its followers were cut off when it
/// was written. The leader is stopped with no handoff, and the partition heals
/// while it drains. It has to ship that record before it stops replicating,
/// because the control plane promotes only a follower its leader last named
/// as holding everything, and a leader that stops shipping first leaves a last
/// report naming nobody: the shard then never fails over.
#[serial]
#[tokio::test]
async fn a_stopping_leader_ships_its_tail_before_it_stops_replicating() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        broker_env: vec![
            (
                "FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS".to_string(),
                "0".to_string(),
            ),
            (
                "FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS".to_string(),
                "10000".to_string(),
            ),
        ],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let leader = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&leader, STREAM, b"shipped".to_vec())
        .await
        .expect("publish while the cluster is whole");

    // Written on the leader, refused for want of a quorum.
    cluster.partition_node(&leader).expect("partition");
    assert!(
        cluster
            .publish_via(&leader, STREAM, b"leader-only".to_vec())
            .await
            .is_err(),
        "the partition did not take effect"
    );

    cluster.terminate_node(&leader).expect("SIGTERM");
    tokio::time::sleep(Duration::from_secs(1)).await;
    cluster.heal_partitions().expect("heal");
    let status = cluster
        .wait_for_exit(&leader, Duration::from_secs(20))
        .await
        .expect("the leader exits");
    assert!(status.success(), "clean exit: {status}");

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        cluster.place_shards().await;
        let owner = cluster.owner(STREAM).await.expect("owner");
        if owner != leader {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the shard never failed over from {leader}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cluster.shutdown().await;
}
