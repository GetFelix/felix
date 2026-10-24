//! A durable `Quorum` shard's quorum mark moves when a follower answers, not
//! when the replication tick next fires (#1479).
//!
//! Run with `cargo test -p felix-cluster --test failures quorum_mark_wake::`.
use std::sync::Arc;
use std::time::{Duration, Instant};

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_wire::AckMode;
use serial_test::serial;

const STREAM: &str = "orders";
const SHARDS: u32 = 4;
const CONNECTIONS: usize = 8;
const PUBLISHERS_PER_CONNECTION: usize = 8;
const PAYLOAD: usize = 256;
const LOAD: Duration = Duration::from_secs(25);
/// Every fleet feature but the opt-in `publisher_principal`, prerequisites first.
const FEATURES: [&str; 9] = [
    "generation_start",
    "majority_ack",
    "lease_free_reads",
    "fenced_caches",
    "jump_hash_routing",
    "atomic_commit",
    "group_cursor_dependencies",
    "stream_incarnations",
    "cache_incarnations",
];
/// `FELIX_CONTROLPLANE_SYNC_INTERVAL_MS`, which is the replication tick and
/// also the cadence of the broker's metadata and routing feeds. A wake the
/// mark misses costs about this long. Fine here: placement is driven by the
/// harness, the membership heartbeat runs on the interval the control plane
/// hands out, and the settle wait below covers routes and leaders.
const TICK_MS: u64 = 30_000;
/// Consecutive non-stall errors a publisher retries before giving up.
const RETRIES: u32 = 3;
/// Non-stall errors allowed in the whole run, each retried successfully.
const OTHER_ERRORS_ALLOWED: usize = 5;
/// What the broker says when a Quorum publish times out waiting on its mark.
const STALL_TEXT: &str = "did not reach a majority";
/// A healthy ack takes milliseconds; this leaves room for a slow runner.
const LIMIT: Duration = Duration::from_secs(10);

/// **A busy follower's late answer moves the quorum mark at once.** The tick is
/// thirty seconds, so a publish whose mark waits for it is acknowledged far
/// past the limit. Every publisher keeps a window of publishes open, so no new
/// append arrives to start a pass in place of the missed wake.
#[serial]
#[tokio::test]
async fn a_busy_followers_answer_moves_the_mark_without_the_tick() {
    let env = |key: &str, value: &str| (key.to_string(), value.to_string());
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, SHARDS, 3)],
        sync_interval_ms: TICK_MS,
        broker_env: vec![
            env("FELIX_DURABLE_FSYNC_MODE", "on_commit"),
            env("FELIX_ACK_ON_COMMIT", "1"),
        ],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");

    // What a new cluster in production starts with. The harness creates its
    // streams before the first broker registers, so it never reads as new and
    // finalizes nothing itself.
    cluster
        .finalize_fleet_features(&FEATURES, Duration::from_secs(90))
        .await
        .expect("every broker enables the fleet features");

    // Finalizing runs after the startup wait, so let the leaders and their
    // replica reports settle again before the clock starts.
    felix_cluster::wait::until(Duration::from_secs(60), "the cluster to settle", || async {
        let placed = cluster.place_shards().await;
        placed.moved == 0
            && placed.waiting == 0
            && cluster
                .unreported_shards()
                .await
                .is_ok_and(|s| s.is_empty())
    })
    .await
    .expect("every shard has a leader and reported replicas");

    let mut clients = Vec::new();
    for _ in 0..CONNECTIONS {
        clients.push(Arc::new(
            felix_cluster::client::connect_cluster(
                &cluster.broker_addrs(),
                &cluster.tenant_id,
                &cluster.client_token(),
            )
            .await
            .expect("connect"),
        ));
    }

    // One publish per shard on every connection, so routes are learned before
    // the clock starts.
    for client in &clients {
        for shard in 0..SHARDS {
            felix_cluster::wait::until(Duration::from_secs(60), "a warm-up publish", || async {
                client
                    .publish_on_shard(
                        &cluster.tenant_id,
                        &cluster.namespace,
                        STREAM,
                        shard,
                        vec![b'w'; PAYLOAD],
                        AckMode::PerMessage,
                    )
                    .await
                    .is_ok()
            })
            .await
            .expect("every shard acknowledges a publish");
        }
    }

    let stop = Instant::now() + LOAD;
    let mut tasks = Vec::new();
    for (c, client) in clients.iter().enumerate() {
        for p in 0..PUBLISHERS_PER_CONNECTION {
            let client = Arc::clone(client);
            let tenant = cluster.tenant_id.clone();
            let namespace = cluster.namespace.clone();
            let shard = ((c * PUBLISHERS_PER_CONNECTION + p) as u32) % SHARDS;
            tasks.push(tokio::spawn(async move {
                let mut run = Run::default();
                let mut failures = 0;
                while Instant::now() < stop {
                    let sent = Instant::now();
                    // Bounded so a stalled ack is reported rather than hanging.
                    let acked = tokio::time::timeout(
                        Duration::from_secs(60),
                        client.publish_on_shard(
                            &tenant,
                            &namespace,
                            STREAM,
                            shard,
                            vec![b'x'; PAYLOAD],
                            AckMode::PerMessage,
                        ),
                    )
                    .await;
                    let latency = sent.elapsed();
                    match acked {
                        Ok(Ok(_)) => {
                            failures = 0;
                            run.latencies.push(latency);
                        }
                        Ok(Err(err)) if format!("{err:#}").contains(STALL_TEXT) => {
                            run.stalls += 1;
                            run.latencies.push(latency);
                        }
                        Ok(Err(err)) => {
                            run.others.push(format!("{err:#}"));
                            failures += 1;
                            if failures > RETRIES {
                                run.gave_up = true;
                                break;
                            }
                        }
                        Err(_) => {
                            run.stalls += 1;
                            run.latencies.push(latency);
                        }
                    }
                }
                run
            }));
        }
    }
    let mut total = Run::default();
    for task in tasks {
        let run = task.await.expect("publisher");
        total.latencies.extend(run.latencies);
        total.stalls += run.stalls;
        total.others.extend(run.others);
        total.gave_up |= run.gave_up;
    }
    cluster.shutdown().await;

    assert!(!total.latencies.is_empty(), "no publish was acknowledged");
    total.latencies.sort();
    let max = *total.latencies.last().unwrap();
    let p99 = total.latencies[(total.latencies.len() * 99 / 100).min(total.latencies.len() - 1)];
    assert!(
        total.stalls == 0
            && max < LIMIT
            && !total.gave_up
            && total.others.len() <= OTHER_ERRORS_ALLOWED,
        "the quorum mark waited for the replication tick instead of a follower's answer (#1479): \
         max ack {max:?}, p99 {p99:?}, {} acks, {} stalled, {} other errors (gave up: {}): {:?}",
        total.latencies.len(),
        total.stalls,
        total.others.len(),
        total.gave_up,
        total.others.iter().take(3).collect::<Vec<_>>(),
    );
}

/// What one publisher saw.
#[derive(Default)]
struct Run {
    latencies: Vec<Duration>,
    /// Acks that never came: the broker's quorum timeout, or none in a minute.
    stalls: usize,
    /// Errors that are not a stall (a stale route, a reconnect), each retried.
    others: Vec<String>,
    gave_up: bool,
}
