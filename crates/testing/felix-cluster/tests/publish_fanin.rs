//! Does group commit still group once the QUIC transport is in the path?
//!
//! Two in-process tests bracket this from below and both come out clean:
//! `felix-storage`'s group commit coalesces at 7.25x under 16-way concurrency,
//! and `felix-broker`'s publish path at 10.97x. Yet an Azure session measured
//! the fan-in actually achieved in a running cluster
//! (`felix_storage_sync_batch_appends`) at **1.004-1.007** with 48 publishers.
//!
//! Everything below the transport is therefore exonerated, which leaves
//! `services/felix-broker-service`'s publish ingress. This test is the one that reaches it:
//! real brokers, real QUIC, one client connection per publisher — the same
//! shape `felix-loadgen` uses — and it reads the fan-in off the broker's own
//! metrics endpoint rather than inferring it from timings.
//!
//! Run with `cargo test -p felix-cluster --test publish_fanin -- --nocapture`.
//! `FELIX_CORE_SHARDS` and `FELIX_DURABLE_FSYNC_MODE` are inherited by the
//! spawned brokers, so the same test measures either configuration.

use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_wire::AckMode;
use serial_test::serial;

const STREAM: &str = "orders";
const PUBLISHERS: usize = 16;
const PER_PUBLISHER: usize = 64;
const PAYLOAD: usize = 4096;

fn config() -> ClusterConfig {
    ClusterConfig {
        // One broker: this is about one log's commit path, not about placement
        // or forwarding. Both of those are separate findings (#536).
        nodes: 1,
        // One shard, so every publish lands on the same log and the fan-in
        // question is asked of a single commit path.
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    }
}

/// Mean appends per device flush, read from the broker's metrics endpoint.
///
/// A Prometheus histogram exposes `_sum` and `_count`; their ratio is the fan-in
/// the broker actually achieved. `docs/storage-performance.md` budgets this at
/// **>= 8** and calls a value near 1 the signature of the flush lock having
/// stopped batching.
async fn fan_in(cluster: &Cluster, node: &str) -> Option<f64> {
    let sum = cluster
        .metric(node, "felix_storage_sync_batch_appends_sum")
        .await
        .ok()??;
    let count = cluster
        .metric(node, "felix_storage_sync_batch_appends_count")
        .await
        .ok()??;
    (count > 0.0).then(|| sum / count)
}

/// **Concurrent publishers over QUIC should share device flushes.**
///
/// This measured 1.000 before the publish path was split into a claim and a
/// completion (#535): one worker owned each shard and awaited every flush, so
/// nothing was ever concurrent at the sync point. It is the regression test for
/// that fix, and it fails immediately if the split is undone.
#[serial]
#[tokio::test]
async fn concurrent_publishers_over_quic_share_a_flush() {
    // The brokers inherit this. Without it the default is `Periodic`, which
    // acknowledges before the flush and so asks no question about group commit.
    unsafe { std::env::set_var("FELIX_DURABLE_FSYNC_MODE", "on_commit") };
    // The harness's default ingress queue is sized for correctness tests, not
    // for 16 publishers in flight, and sheds with "publish queue full" well
    // before the commit path is the constraint. Wait for capacity instead of
    // shedding, and give the queue room: the question here is what the flush
    // path does under concurrency, so admission must not be the thing measured.
    unsafe {
        std::env::set_var("FELIX_PUB_INGRESS_WAIT", "1");
        std::env::set_var("FELIX_PUBLISH_QUEUE_WAIT_MS", "30000");
        std::env::set_var("FELIX_BROKER_PUB_QUEUE_DEPTH", "4096");
        std::env::set_var("FELIX_BROKER_PUBLISH_INFLIGHT_BYTES", "1073741824");
        std::env::set_var("FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES", "268435456");
    }

    let cluster = Cluster::start(config()).await.expect("start cluster");
    let node = cluster.node_ids().first().expect("a node").clone();
    let core_shards = std::env::var("FELIX_CORE_SHARDS").unwrap_or_else(|_| "unset".into());

    // One connection per publisher, which is what felix-loadgen does: each
    // connection gets its own publish worker pool on the broker, so this is the
    // arrangement most likely to produce concurrency if any arrangement does.
    let mut clients = Vec::with_capacity(PUBLISHERS);
    for _ in 0..PUBLISHERS {
        clients.push(
            felix_cluster::client::connect_cluster(
                &cluster.broker_addrs(),
                &cluster.tenant_id,
                &cluster.client_token(),
            )
            .await
            .expect("connect"),
        );
    }

    let before = fan_in(&cluster, &node).await;

    let tenant = cluster.tenant_id.clone();
    let namespace = cluster.namespace.clone();
    let started = std::time::Instant::now();
    let mut tasks = Vec::with_capacity(PUBLISHERS);
    for client in clients {
        let tenant = tenant.clone();
        let namespace = namespace.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..PER_PUBLISHER {
                client
                    .publish(
                        &tenant,
                        &namespace,
                        STREAM,
                        vec![b'x'; PAYLOAD],
                        AckMode::PerMessage,
                    )
                    .await
                    .expect("publish");
            }
        }));
    }
    for task in tasks {
        task.await.expect("task");
    }
    let elapsed = started.elapsed();

    // The counters are updated on the flush path; give the last ones a moment.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after = fan_in(&cluster, &node).await;

    let total = PUBLISHERS * PER_PUBLISHER;
    let throughput = (total * PAYLOAD) as f64 / elapsed.as_secs_f64() / 1e6;
    eprintln!("FELIX_CORE_SHARDS={core_shards}");
    eprintln!(
        "  {PUBLISHERS} publishers x {PER_PUBLISHER} publishes = {total} in {elapsed:?} \
         ({throughput:.1} MB/s)"
    );
    eprintln!("  fan-in before: {before:?}, after: {after:?}");

    cluster.shutdown().await;

    let after = after.expect("the broker recorded no device flushes at all");
    eprintln!("  => group-commit fan-in {after:.3}");

    // Deliberately well below the `>= 8` that `docs/storage-performance.md`
    // budgets. That number is a property of the perf rig -- NVMe, ~300 us
    // flushes, publishes arriving fast enough to stack up behind one. A shared
    // CI runner is several times slower end to end, so fewer publishes are
    // waiting at any given flush and the honest fan-in there is lower: this
    // test measured 9.5 on a laptop and 4.3 on a GitHub runner, both healthy.
    //
    // What this guards is the structural claim, which does not vary: that
    // publishes reach the flush *together at all*. The regression it exists to
    // catch collapses to exactly 1.0, so anything clearly above 1 separates
    // "coalescing" from "serialised". Gating the production budget belongs on
    // the rig, not here (#535).
    assert!(
        after >= 2.0,
        "group-commit fan-in was {after:.3} with {PUBLISHERS} concurrent publishers over QUIC. \
         At 1.0 the publish ingress is serialising again -- one worker per shard awaiting each \
         device flush -- and group commit has nothing to coalesce (#535)."
    );
}
