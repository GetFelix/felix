//! The transport-free publish: the same fence, append and quorum wait the
//! worker runs, called without a connection.

use felix_broker::{ConsistencyLevel, DurableStorage, StreamMetadata};
use felix_replication::quorum::QuorumMarks;
use felix_storage::log::{FsyncMode, LogConfig};

use super::*;
use crate::serving::quic::handlers::publish::direct::{
    DirectPublishError, LocalTarget, Resolution, publish_direct, resolve_publish,
};
use crate::shards::routing::StreamEngine;

async fn durable_broker(dir: &std::path::Path, consistency: ConsistencyLevel) -> Broker {
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let broker = Broker::new(EphemeralCache::new().into())
        .with_durable_storage(DurableStorage::open(dir, config).expect("storage"));
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "stream",
            StreamMetadata {
                durable: true,
                shards: 1,
                consistency,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    broker
}

async fn local(broker: &Broker, ingress: &IngressRouter) -> LocalTarget {
    match resolve_publish(
        broker,
        Some(ingress),
        None,
        "t1",
        "ns",
        "stream",
        None,
        None,
    )
    .await
    .map_err(|refusal| refusal.message)
    {
        Ok(Resolution::Local(target)) => target,
        Ok(Resolution::Forward(owner)) => panic!("forwarded to {}", owner.node_id),
        Err(refusal) => panic!("refused: {refusal}"),
    }
}

/// A leader with an open fence appends and answers with the first offset.
#[tokio::test]
async fn a_leader_with_an_open_fence_publishes() {
    let dir = tempfile::tempdir().expect("dir");
    let broker = durable_broker(dir.path(), ConsistencyLevel::Leader).await;
    let ingress = ingress_for("broker-a", true);
    let payloads = [Bytes::from_static(b"a"), Bytes::from_static(b"b")];

    let mut first = Vec::new();
    for _ in 0..2 {
        let target = local(&broker, &ingress).await;
        let offset = publish_direct(
            &broker,
            Some(&ingress),
            &StreamEngine::default(),
            target,
            &payloads,
            None,
            Duration::from_secs(1),
        )
        .await
        .expect("published");
        first.push(offset);
    }
    assert_eq!(first, [Some(0), Some(2)]);
}

/// A shard another broker owns is not served here, and the owner is named.
#[tokio::test]
async fn a_shard_owned_elsewhere_resolves_to_its_owner() {
    let dir = tempfile::tempdir().expect("dir");
    let broker = durable_broker(dir.path(), ConsistencyLevel::Leader).await;
    let ingress = ingress_for("broker-b", false);
    match resolve_publish(
        &broker,
        Some(&ingress),
        None,
        "t1",
        "ns",
        "stream",
        None,
        None,
    )
    .await
    {
        Ok(Resolution::Forward(owner)) => assert_eq!(owner.node_id, "broker-b"),
        _ => panic!("expected a forward to the owner"),
    }
}

/// The fence closing between resolve and claim is a refusal that wrote
/// nothing.
#[tokio::test]
async fn a_closed_fence_is_not_written() {
    let dir = tempfile::tempdir().expect("dir");
    let broker = durable_broker(dir.path(), ConsistencyLevel::Leader).await;
    let open = ingress_for("broker-a", true);
    let mut target = local(&broker, &open).await;
    // Admission's guard would carry the write through; drop it so the claim
    // has to enter the fence itself, as a write that waited in a queue does.
    target.fenced = None;
    let closed = ingress_for("broker-a", false);
    let log = broker
        .durable_storage()
        .expect("durable")
        .open_stream("t1", "ns", "stream", 0)
        .expect("log");
    let before = log.flushes();

    let err = publish_direct(
        &broker,
        Some(&closed),
        &StreamEngine::default(),
        target,
        &[Bytes::from_static(b"a")],
        None,
        Duration::from_secs(1),
    )
    .await
    .expect_err("fenced");
    assert!(matches!(err, DirectPublishError::NotWritten(_)), "{err:?}");
    assert_eq!(log.flushes(), before, "a refused publish must not append");
}

/// A quorum wait that runs out leaves the batch on this broker's disk, so the
/// answer is "outcome unknown", not a refusal.
#[tokio::test(start_paused = true)]
async fn a_quorum_timeout_is_outcome_unknown() {
    let dir = tempfile::tempdir().expect("dir");
    let broker = durable_broker(dir.path(), ConsistencyLevel::Quorum).await;
    let ingress = Arc::new(ingress_with_follower());
    let engine = StreamEngine::new(
        Some(Arc::new(QuorumMarks::new())),
        Some(Arc::clone(&ingress)),
    );
    let target = local(&broker, &ingress).await;

    let err = publish_direct(
        &broker,
        Some(&ingress),
        &engine,
        target,
        &[Bytes::from_static(b"a")],
        None,
        Duration::from_millis(50),
    )
    .await
    .expect_err("no majority");
    match err {
        DirectPublishError::OutcomeUnknown(err) => {
            let timeout = err
                .downcast_ref::<felix_replication::quorum::QuorumError>()
                .expect("a quorum error");
            assert!(matches!(
                timeout,
                felix_replication::quorum::QuorumError::TimedOut { .. }
            ));
        }
        other => panic!("expected outcome unknown, got {other:?}"),
    }
}

/// A leader with a follower, so a `Quorum` write has someone to wait on.
fn ingress_with_follower() -> IngressRouter {
    ingress_with_replicas("broker-a", true, vec!["broker-b".to_string()])
}
