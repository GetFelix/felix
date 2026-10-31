//! A head trim on a `Quorum` stream across a failover.
//!
//! Run with `cargo test -p felix-cluster --test failures head_trim::`. The
//! harness runs a prebuilt `target/<profile>/felix-broker`; build it first with
//! `cargo build -p felix-broker-service --bin felix-broker`.
use std::time::Duration;

use felix_client::{ConditionalTrim, CursorErrorReason, SubscribeCursorError};
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";
const HOLDER: &[u8] = b"holder-1";

/// What `node` reads from `from`, retried until it serves: a promoted broker
/// is named leader before it has opened the shard.
async fn read_until_served(
    cluster: &Cluster,
    node: &str,
    from: u64,
) -> anyhow::Result<Vec<(u64, String)>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let attempt = async {
            let connection = cluster.client_on(node).await?;
            connection
                .read(
                    &cluster.tenant_id,
                    &cluster.namespace,
                    STREAM,
                    0,
                    from,
                    None,
                    100,
                )
                .await
        }
        .await;
        match attempt {
            Ok(page) => {
                return Ok(page
                    .records
                    .into_iter()
                    .map(|record| {
                        (
                            record.offset,
                            String::from_utf8_lossy(&record.payload).into_owned(),
                        )
                    })
                    .collect());
            }
            Err(err) if err.is::<SubscribeCursorError>() => return Err(err),
            Err(err) if std::time::Instant::now() >= deadline => return Err(err),
            Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
}

/// The floor `node` refuses a read below, with the headers of the trim that
/// set it.
async fn floor_on(cluster: &Cluster, node: &str, below: u64) -> SubscribeCursorError {
    let refused = read_until_served(cluster, node, below)
        .await
        .expect_err("a read below the floor is refused");
    refused
        .downcast_ref::<SubscribeCursorError>()
        .cloned()
        .unwrap_or_else(|| panic!("not a cursor error: {refused:#}"))
}

/// One trim through a fresh cluster client that may trim, seeded with the
/// brokers still up.
async fn writer_trim(
    cluster: &Cluster,
    before: u64,
    expected_offset: u64,
    headers: Option<&[u8]>,
) -> ConditionalTrim {
    let writer = felix_cluster::client::connect_cluster(
        &cluster.live_broker_addrs(),
        &cluster.tenant_id,
        &cluster.trimmer_token(),
    )
    .await
    .expect("a cluster client that may trim");
    writer
        .trim_before(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            0,
            before,
            expected_offset,
            headers,
        )
        .await
        .expect("trim")
}

async fn replicated_to_all(cluster: &Cluster, leader: &str, shipped_before: f64) {
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the records to be shipped and acknowledged",
        || async {
            let shipped = cluster
                .metric(leader, "felix_broker_replication_shipped_total")
                .await
                .ok()
                .flatten()
                .unwrap_or(0.0);
            let lag = cluster
                .metric(leader, "felix_broker_replication_lag_records")
                .await
                .ok()
                .flatten();
            shipped > shipped_before && lag == Some(0.0)
        },
    )
    .await
    .expect("replication should reach the followers");
}

/// **A committed trim outlives its leader.** Ten records are published and the
/// first six trimmed. After the leader is killed, the promoted broker still
/// refuses a read below the floor, names the trim that set it, serves every
/// record above it, and answers a retry of the old trim as a stale tail
/// rather than writing a second one.
#[serial]
#[tokio::test]
async fn a_trim_holds_on_the_leader_promoted_after_it() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    cluster
        .finalize_fleet_features(&["head_trim"], Duration::from_secs(30))
        .await
        .expect("finalize head_trim");
    let leader = cluster.owner(STREAM).await.expect("owner");
    // The harness may have written a probe of its own, so offsets are
    // relative to where the first publish landed.
    let mut base = None;
    for i in 0..10 {
        let offset = cluster
            .publish_via_at(&leader, STREAM, format!("before-{i}").into_bytes())
            .await
            .expect("publish under quorum")
            .expect("a durable publish reports its offset");
        assert_eq!(offset, *base.get_or_insert(offset) + i, "offsets are dense");
    }
    let base: u64 = base.expect("published");
    let (floor_at, trim_at) = (base + 6, base + 10);

    // The trim record takes the offset after the last record.
    let trimmed = writer_trim(&cluster, floor_at, trim_at, Some(HOLDER)).await;
    assert_eq!(
        trimmed,
        ConditionalTrim::Trimmed {
            offset: Some(trim_at),
            oldest: floor_at
        }
    );
    let floor = floor_on(&cluster, &leader, base + 2).await;
    assert_eq!(floor.reason, CursorErrorReason::TooOld);
    assert_eq!(floor.available, floor_at);
    assert_eq!(floor.trim_headers.as_deref(), Some(HOLDER));

    // A trim whose answer is lost is sent again with the same expected
    // offset, and is refused with the tail one past the record it wrote.
    let again = writer_trim(&cluster, floor_at, trim_at, Some(HOLDER)).await;
    assert_eq!(again, ConditionalTrim::Refused { tail: trim_at + 1 });
    // At the real tail, a floor already in force writes nothing.
    let noop = writer_trim(&cluster, base + 4, trim_at + 1, None).await;
    assert_eq!(
        noop,
        ConditionalTrim::Trimmed {
            offset: None,
            oldest: floor_at
        }
    );

    let shipped_before = cluster
        .metric(&leader, "felix_broker_replication_shipped_total")
        .await
        .ok()
        .flatten()
        .unwrap_or(0.0);
    for i in 0..2 {
        let offset = cluster
            .publish_via_at(&leader, STREAM, format!("after-{i}").into_bytes())
            .await
            .expect("publish after the trim");
        assert_eq!(
            offset,
            Some(trim_at + 1 + i),
            "the trim record took an offset"
        );
    }
    replicated_to_all(&cluster, &leader, shipped_before).await;

    cluster.kill_node(&leader).expect("kill the leader");
    felix_cluster::wait::until(Duration::from_secs(30), "a new leader", || async {
        cluster.place_shards().await;
        cluster
            .owner(STREAM)
            .await
            .is_ok_and(|owner| owner != leader)
    })
    .await
    .expect("a replica should have been promoted");
    let promoted = cluster.owner(STREAM).await.expect("owner");

    let floor = floor_on(&cluster, &promoted, base + 2).await;
    assert_eq!(floor.available, floor_at, "the floor survives the failover");
    assert_eq!(floor.trim_headers.as_deref(), Some(HOLDER));
    let held = read_until_served(&cluster, &promoted, floor_at)
        .await
        .expect("read above the floor");
    let expected: Vec<(u64, String)> = (6..10)
        .map(|i| (base + i, format!("before-{i}")))
        .chain((0..2).map(|i| (trim_at + 1 + i, format!("after-{i}"))))
        .collect();
    assert_eq!(held, expected, "every record above the floor is intact");

    // The old trim is refused by the new leader, and a new one lands.
    let stale = writer_trim(&cluster, floor_at, trim_at, Some(HOLDER)).await;
    assert_eq!(stale, ConditionalTrim::Refused { tail: trim_at + 3 });
    let next = writer_trim(&cluster, floor_at + 2, trim_at + 3, Some(b"holder-2")).await;
    assert_eq!(
        next,
        ConditionalTrim::Trimmed {
            offset: Some(trim_at + 3),
            oldest: floor_at + 2
        }
    );
    cluster.shutdown().await;
}
