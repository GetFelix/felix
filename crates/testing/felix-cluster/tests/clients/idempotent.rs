//! Idempotent producers against a real cluster: a batch sent twice lands
//! once, a batch sent to the wrong broker is told where the leader is, and a
//! producer that skips ahead is stopped.
//!
//! Through the client-facing API, on a `Quorum` stream replicated three ways,
//! because that is the case the feature exists for: a `Quorum` publish whose
//! acknowledgement never arrived, re-sent without a second copy landing.
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_client::{PublishRefusalReason, PublishRefused};
use felix_cluster::{Cluster, ClusterConfig, StreamSpec, client};
use serial_test::serial;

const STREAM: &str = "orders";
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    }
}

/// Every record on the stream, replayed from the start on the leader. The
/// harness publishes one probe of its own while the cluster settles; that is
/// not the test's.
async fn replay(cluster: &Cluster, owner: &str, expected: usize) -> Result<Vec<Vec<u8>>> {
    let (_client, mut subscription) = cluster.replay_on(owner, STREAM).await?;
    let mut records = Vec::new();
    while records.len() < expected {
        let event = tokio::time::timeout(DELIVERY_TIMEOUT, subscription.next_event())
            .await
            .context("no delivery within the timeout")??
            .context("subscription closed")?;
        if event.payload.as_ref() == b"harness-probe" {
            continue;
        }
        records.push(event.payload.to_vec());
    }
    // One more would be a duplicate; give it a moment to show up.
    if let Ok(Ok(Some(extra))) =
        tokio::time::timeout(Duration::from_millis(500), subscription.next_event()).await
    {
        anyhow::bail!(
            "an extra record landed: {:?}",
            String::from_utf8_lossy(&extra.payload)
        );
    }
    Ok(records)
}

/// **A batch sent twice lands once**, and the second send is acknowledged
/// like the first: the producer cannot tell, which is the point.
///
/// The producer here goes through a cluster client connected wherever, so
/// the first batch is refused by a non-leader and followed to the leader;
/// then a batch is re-sent by hand under a sequence the leader already holds,
/// which is what a producer does after an acknowledgement went missing.
#[tokio::test]
#[serial]
async fn a_re_sent_batch_lands_once() -> Result<()> {
    let cluster = Cluster::start(config()).await?;
    let owner = cluster.owner(STREAM).await?;
    let cluster_client = client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await?;
    let cluster_client = Arc::new(cluster_client);
    let producer = cluster_client.idempotent_producer().await?;

    for i in 0..5u32 {
        producer
            .publish(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                format!("record-{i}").into_bytes(),
            )
            .await
            .with_context(|| format!("publish record-{i}"))?;
    }

    // The acknowledgement for sequence 2 "never arrived": send it again,
    // straight to the leader, under the same producer and sequence.
    let leader = cluster.node(&owner).context("leader node")?;
    let direct = client::connect(
        leader.client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await?;
    direct
        .publisher()
        .await?
        .publish_idempotent_batch(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            vec![b"record-2".to_vec()],
            producer.producer_id(),
            2,
        )
        .await
        .context("the re-send must be acknowledged, not refused")?;

    let records = replay(&cluster, &owner, 5).await?;
    let expected: Vec<Vec<u8>> = (0..5).map(|i| format!("record-{i}").into_bytes()).collect();
    assert_eq!(
        records, expected,
        "the re-sent batch landed twice, or out of order"
    );
    Ok(())
}

/// **A non-leader names the leader.** Only the leader holds the sequences,
/// so a batch arriving anywhere else is refused with where to go rather than
/// forwarded — forwarded, two ingress brokers could land one batch twice.
#[tokio::test]
#[serial]
async fn a_non_leader_names_the_leader() -> Result<()> {
    let cluster = Cluster::start(config()).await?;
    let owner = cluster.owner(STREAM).await?;
    let elsewhere = cluster
        .nodes
        .iter()
        .find(|node| node.node_id != owner)
        .context("a broker that is not the leader")?;
    let leader = cluster.node(&owner).context("leader node")?;

    let direct = client::connect(
        elsewhere.client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await?;
    let direct = Arc::new(direct);
    let producer = direct.idempotent_producer().await?;
    let err = producer
        .publish(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"misrouted".to_vec(),
        )
        .await
        .expect_err("a non-leader accepted an idempotent publish");
    let refused = err
        .downcast_ref::<PublishRefused>()
        .with_context(|| format!("not a typed refusal: {err:#}"))?;
    match &refused.reason {
        PublishRefusalReason::NotLeader { node_id, addr } => {
            assert_eq!(node_id, &owner);
            assert_eq!(
                addr.as_deref(),
                Some(leader.client_addr.to_string().as_str())
            );
        }
        other => panic!("expected not_leader, got {other:?}"),
    }

    // Nothing landed anywhere, and the producer is not ended: the batch can
    // go to the leader under the same sequence.
    let via_leader = client::connect(
        leader.client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await?;
    via_leader
        .publisher()
        .await?
        .publish_idempotent_batch(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            vec![b"routed".to_vec()],
            producer.producer_id(),
            0,
        )
        .await?;
    assert_eq!(replay(&cluster, &owner, 1).await?, vec![b"routed".to_vec()]);
    Ok(())
}

/// **A gap is refused, names what was expected, and appends nothing.** The
/// producer skipped a batch the leader never saw; continuing would leave a
/// hole the producer believes is filled.
#[tokio::test]
#[serial]
async fn a_gap_is_refused_with_the_expected_sequence() -> Result<()> {
    let cluster = Cluster::start(config()).await?;
    let owner = cluster.owner(STREAM).await?;
    let leader = cluster.node(&owner).context("leader node")?;
    let direct = client::connect(
        leader.client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await?;
    let producer_id = direct.producer_init().await?;
    let publisher = direct.publisher().await?;

    publisher
        .publish_idempotent_batch(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            vec![b"first".to_vec()],
            producer_id,
            0,
        )
        .await?;
    let err = publisher
        .publish_idempotent_batch(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            vec![b"skipped ahead".to_vec()],
            producer_id,
            5,
        )
        .await
        .expect_err("a gap was appended");
    let refused = err
        .downcast_ref::<PublishRefused>()
        .with_context(|| format!("not a typed refusal: {err:#}"))?;
    assert_eq!(
        refused.reason,
        PublishRefusalReason::SequenceGap { expected: 1 }
    );

    // And a producer the leader has never seen may not begin mid-way.
    let err = publisher
        .publish_idempotent_batch(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            vec![b"unknown".to_vec()],
            producer_id + 1,
            3,
        )
        .await
        .expect_err("an unknown producer was accepted mid-sequence");
    let refused = err
        .downcast_ref::<PublishRefused>()
        .with_context(|| format!("not a typed refusal: {err:#}"))?;
    assert_eq!(refused.reason, PublishRefusalReason::UnknownProducer);

    assert_eq!(replay(&cluster, &owner, 1).await?, vec![b"first".to_vec()]);
    Ok(())
}

/// A cancelled publish still runs to its answer, and the producer carries on.
///
/// The sequence mechanism makes a re-send safe *because the number does not
/// move*, which holds only while the producer knows whether the number was
/// used. The producer's own task finishes the send whether or not the caller
/// still waits, so dropping the future mid-flight neither leaves the sequence
/// in doubt nor lets the next batch go out under a spent number, where the
/// broker would answer it from memory without appending it.
#[tokio::test]
#[serial]
async fn a_cancelled_publish_still_lands_once_and_the_producer_carries_on() -> Result<()> {
    let cluster = Cluster::start(config()).await?;
    let owner = cluster.owner(STREAM).await?;
    let cluster_client = client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await?;
    let cluster_client = Arc::new(cluster_client);
    let producer = cluster_client.idempotent_producer().await?;

    // One that lands, so the producer is past its first sequence and the
    // cancellation below is not confused with a producer that never started.
    producer
        .publish(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"landed".to_vec(),
        )
        .await?;

    // Cancelled as tightly as possible: the future is dropped while the batch
    // is in flight, which is exactly what a `timeout` around a publish does.
    let cancelled = tokio::time::timeout(
        Duration::from_nanos(1),
        producer.publish(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"cancelled".to_vec(),
        ),
    )
    .await;
    assert!(cancelled.is_err(), "the publish was meant to be cancelled");

    // Different records next. Under a reused sequence these would be
    // answered `Ok` and dropped.
    producer
        .publish(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"next".to_vec(),
        )
        .await
        .context("the producer must carry on after a cancelled publish")?;

    // A tolerant drain rather than `replay`, so a missing cancelled batch
    // reads as that rather than as a short stream. It is handed to the
    // producer on the future's first poll, so it should be there.
    let (_client, mut subscription) = cluster.replay_on(&owner, STREAM).await?;
    let mut records: Vec<Vec<u8>> = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_millis(500), subscription.next_event()).await
    {
        records.push(event.payload.to_vec());
    }
    // The harness's own readiness probe may be on the stream too.
    records.retain(|record| record != b"harness-probe");
    assert_eq!(
        records,
        vec![b"landed".to_vec(), b"cancelled".to_vec(), b"next".to_vec()],
        "each batch once, in the order it was handed over",
    );

    cluster.shutdown().await;
    Ok(())
}

/// Publish `record-0..count` through `producer`, one batch each.
async fn publish_records(
    cluster: &Cluster,
    producer: &felix_client::IdempotentProducer,
    range: std::ops::Range<u32>,
) -> Result<()> {
    for i in range {
        producer
            .publish(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                format!("record-{i}").into_bytes(),
            )
            .await
            .with_context(|| format!("publish record-{i}"))?;
    }
    Ok(())
}

/// Re-send `record-{sequence}` under `sequence`, straight to `node_id`: what a
/// producer does when the acknowledgement for its last batch never arrived
/// and the leader changed meanwhile.
async fn re_send(cluster: &Cluster, node_id: &str, producer_id: u64, sequence: u64) -> Result<()> {
    let node = cluster.node(node_id).context("node")?;
    let direct = client::connect(
        node.client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await?;
    direct
        .publisher()
        .await?
        .publish_idempotent_batch(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            vec![format!("record-{sequence}").into_bytes()],
            producer_id,
            sequence,
        )
        .await
        .context("the new leader must answer the re-send as a duplicate")
        .map(|_| ())
}

/// **A producer's sequence moves with its shard.** The owner drains, the
/// shard moves, and the new owner answers a re-send of the last batch from
/// the log it received rather than appending it again. The producer then
/// carries on where it was instead of being told it is unknown.
#[tokio::test]
#[serial]
async fn a_producer_keeps_its_sequence_across_a_planned_move() -> Result<()> {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await?;
    let owner = cluster.owner(STREAM).await?;
    let cluster_client = client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await?;
    let cluster_client = Arc::new(cluster_client);
    let producer = cluster_client.idempotent_producer().await?;
    publish_records(&cluster, &producer, 0..5).await?;

    cluster.drain_node(&owner).await?;
    cluster
        .drain_until_empty(&owner, 1, Duration::from_secs(60))
        .await?;
    let new_owner = cluster.owner(STREAM).await?;
    assert_ne!(new_owner, owner);

    re_send(&cluster, &new_owner, producer.producer_id(), 4).await?;
    publish_records(&cluster, &producer, 5..7).await?;

    let records = replay(&cluster, &new_owner, 7).await?;
    let expected: Vec<Vec<u8>> = (0..7).map(|i| format!("record-{i}").into_bytes()).collect();
    assert_eq!(records, expected, "a batch landed twice, or out of order");
    cluster.shutdown().await;
    Ok(())
}

/// **A producer's sequence survives its leader.** The leader is killed after
/// a majority holds the producer's batches; the promoted replica answers a
/// re-send of the last one without appending it, and the producer carries on.
#[tokio::test]
#[serial]
async fn a_producer_keeps_its_sequence_when_its_leader_dies() -> Result<()> {
    let mut cluster = Cluster::start(config()).await?;
    let owner = cluster.owner(STREAM).await?;
    let survivors: Vec<std::net::SocketAddr> = cluster
        .nodes
        .iter()
        .filter(|node| node.node_id != owner)
        .map(|node| node.client_addr)
        .collect();
    let cluster_client =
        client::connect_cluster(&survivors, &cluster.tenant_id, &cluster.client_token()).await?;
    let cluster_client = Arc::new(cluster_client);
    let producer = cluster_client.idempotent_producer().await?;
    // Acknowledged only once a majority holds each batch.
    publish_records(&cluster, &producer, 0..5).await?;

    cluster.kill_node(&owner)?;
    let new_owner =
        felix_cluster::wait::until_some(Duration::from_secs(30), "a new leader", || async {
            cluster.place_shards().await;
            cluster
                .owner(STREAM)
                .await
                .ok()
                .filter(|leader| leader != &owner)
        })
        .await?;

    // The promoted broker learns of its promotion through its watch, so the
    // re-send is retried until it is the leader.
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the promoted leader to take the re-send",
        || async {
            re_send(&cluster, &new_owner, producer.producer_id(), 4)
                .await
                .is_ok()
        },
    )
    .await?;
    publish_records(&cluster, &producer, 5..7).await?;

    let records = replay(&cluster, &new_owner, 7).await?;
    let expected: Vec<Vec<u8>> = (0..7).map(|i| format!("record-{i}").into_bytes()).collect();
    assert_eq!(records, expected, "a batch landed twice, or out of order");
    cluster.shutdown().await;
    Ok(())
}

/// **A producer publishing through its leader's death loses and repeats
/// nothing.** The leader is killed while the producer is mid-stream; the
/// batch in flight is re-sent by the producer itself until the promoted
/// leader answers it, and every record is on the stream exactly once, in
/// order.
#[tokio::test]
#[serial]
async fn a_producer_publishing_through_its_leaders_death_loses_and_repeats_nothing() -> Result<()> {
    const RECORDS: u32 = 40;
    let mut cluster = Cluster::start(config()).await?;
    let owner = cluster.owner(STREAM).await?;
    let cluster_client = client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await?;
    let cluster_client = Arc::new(cluster_client);
    let producer = cluster_client.idempotent_producer().await?;
    let tenant = cluster.tenant_id.clone();
    let namespace = cluster.namespace.clone();
    let acked = std::sync::atomic::AtomicU32::new(0);

    let publishing = async {
        for i in 0..RECORDS {
            producer
                .publish(
                    &tenant,
                    &namespace,
                    STREAM,
                    format!("record-{i}").into_bytes(),
                )
                .await
                .with_context(|| format!("publish record-{i}"))?;
            acked.store(i + 1, std::sync::atomic::Ordering::Release);
        }
        anyhow::Ok(())
    };
    let failing_over = async {
        felix_cluster::wait::until(Duration::from_secs(30), "some records acked", || async {
            acked.load(std::sync::atomic::Ordering::Acquire) >= 10
        })
        .await?;
        cluster.kill_node(&owner)?;
        felix_cluster::wait::until_some(Duration::from_secs(30), "a new leader", || async {
            cluster.place_shards().await;
            cluster
                .owner(STREAM)
                .await
                .ok()
                .filter(|leader| leader != &owner)
        })
        .await
    };
    let (published, new_owner) = tokio::join!(publishing, failing_over);
    published?;
    let new_owner = new_owner?;

    let records = replay(&cluster, &new_owner, RECORDS as usize).await?;
    let expected: Vec<Vec<u8>> = (0..RECORDS)
        .map(|i| format!("record-{i}").into_bytes())
        .collect();
    assert_eq!(
        records, expected,
        "a record was lost, repeated, or reordered"
    );
    cluster.shutdown().await;
    Ok(())
}

/// **A pipelining producer loses and repeats nothing through its leader's
/// death.** Each call keeps a window of batches unanswered, so the kill lands
/// with several in flight; the producer re-sends from the first unanswered
/// one until the promoted leader answers, and every record is on the stream
/// exactly once, in order.
#[tokio::test]
#[serial]
async fn a_pipelining_producer_loses_and_repeats_nothing_through_its_leaders_death() -> Result<()> {
    const CALLS: u32 = 300;
    const BATCHES: u32 = 50;
    let mut cluster = Cluster::start(config()).await?;
    let owner = cluster.owner(STREAM).await?;
    let cluster_client = client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await?;
    let cluster_client = Arc::new(cluster_client);
    let producer = cluster_client.idempotent_producer().await?;
    let tenant = cluster.tenant_id.clone();
    let namespace = cluster.namespace.clone();
    let calls_done = std::sync::atomic::AtomicU32::new(0);
    let record = |call: u32, n: u32| format!("record-{call}-{n}").into_bytes();

    let publishing = async {
        for call in 0..CALLS {
            let batches = (0..BATCHES).map(|n| vec![record(call, n)]).collect();
            producer
                .publish_batches(&tenant, &namespace, STREAM, batches)
                .await
                .with_context(|| format!("publish call {call}"))?;
            calls_done.store(call + 1, std::sync::atomic::Ordering::Release);
        }
        anyhow::Ok(())
    };
    let failing_over = async {
        felix_cluster::wait::until(Duration::from_secs(30), "some calls acked", || async {
            calls_done.load(std::sync::atomic::Ordering::Acquire) >= 3
        })
        .await?;
        cluster.kill_node(&owner)?;
        felix_cluster::wait::until_some(Duration::from_secs(30), "a new leader", || async {
            cluster.place_shards().await;
            cluster
                .owner(STREAM)
                .await
                .ok()
                .filter(|leader| leader != &owner)
        })
        .await
    };
    let (published, new_owner) = tokio::join!(publishing, failing_over);
    published?;
    let new_owner = new_owner?;

    let expected: Vec<Vec<u8>> = (0..CALLS)
        .flat_map(|call| (0..BATCHES).map(move |n| record(call, n)))
        .collect();
    let records = replay(&cluster, &new_owner, expected.len()).await?;
    assert_eq!(
        records, expected,
        "a record was lost, repeated, or reordered"
    );
    cluster.shutdown().await;
    Ok(())
}
