//! A follower refusing records for a stream its catalog has not registered
//! yet asks the catalog sync to poll now, so the leader waits for one poll,
//! not for the sync's interval.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;

use bytes::Bytes;
use felix_broker::Broker;
use felix_replication::ReplicaHandler;
use felix_router::{NodeRef, RegionRouter, RoutingTable, ShardRouter};
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::{InternalMessage, ReplicateRecords, ShardRef, batch_checksum};

use super::*;

fn stream() -> Stream {
    Stream {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shards: 1,
        durable: true,
        consistency: None,
        retention: None,
        incarnation: 7,
    }
}

/// The catalog, which has the stream only once `created` is set, counting
/// its polls of the stream feed in `polls`.
fn router(created: StdArc<AtomicBool>, polls: StdArc<AtomicUsize>) -> Router {
    Router::new()
        .route(
            "/v1/tenants/snapshot",
            get(|| async {
                Json(TenantSnapshotResponse {
                    items: vec![Tenant {
                        tenant_id: "t1".to_string(),
                    }],
                    next_seq: 1,
                })
            }),
        )
        .route(
            "/v1/namespaces/snapshot",
            get(|| async {
                Json(NamespaceSnapshotResponse {
                    items: vec![Namespace {
                        tenant_id: "t1".to_string(),
                        namespace: "ns".to_string(),
                    }],
                    next_seq: 1,
                })
            }),
        )
        .route(
            "/v1/caches/snapshot",
            get(|| async {
                Json(CacheSnapshotResponse {
                    items: vec![],
                    next_seq: 1,
                })
            }),
        )
        .route(
            "/v1/streams/snapshot",
            get(|| async {
                Json(StreamSnapshotResponse {
                    items: vec![],
                    next_seq: 1,
                    tombstones: Vec::new(),
                })
            }),
        )
        .route(
            "/v1/tenants/changes",
            get(|| async {
                Json(TenantChangesResponse {
                    items: vec![],
                    next_seq: 1,
                })
            }),
        )
        .route(
            "/v1/namespaces/changes",
            get(|| async {
                Json(NamespaceChangesResponse {
                    items: vec![],
                    next_seq: 1,
                })
            }),
        )
        .route(
            "/v1/caches/changes",
            get(|| async {
                Json(CacheChangesResponse {
                    items: vec![],
                    next_seq: 1,
                })
            }),
        )
        .route(
            "/v1/streams/changes",
            get(move || {
                polls.fetch_add(1, Ordering::SeqCst);
                let created = created.load(Ordering::SeqCst);
                async move {
                    Json(StreamChangesResponse {
                        items: if created {
                            vec![StreamChange {
                                seq: Some(1),
                                op: StreamChangeOp::Created,
                                key: StreamKey {
                                    tenant_id: "t1".to_string(),
                                    namespace: "ns".to_string(),
                                    stream: "orders".to_string(),
                                },
                                stream: Some(stream()),
                                deleted_through: None,
                            }]
                        } else {
                            vec![]
                        },
                        next_seq: if created { 2 } else { 1 },
                    })
                }
            }),
        )
}

fn replica_router() -> StdArc<ShardRouter> {
    let node = |id: &str, port| NodeRef {
        node_id: id.to_string(),
        advertise_addr: SocketAddr::from(([10, 0, 0, 1], port)),
        region: "us-west-2".to_string(),
        live: true,
    };
    let nodes: HashMap<String, NodeRef> = [
        ("broker-a".to_string(), node("broker-a", 7001)),
        ("broker-b".to_string(), node("broker-b", 7002)),
    ]
    .into_iter()
    .collect();
    let router = StdArc::new(ShardRouter::new(
        "broker-b",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let key = felix_router::ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: felix_router::ShardKind::Stream,
    };
    let table = RoutingTable::build(
        [(key, "broker-a".to_string(), vec!["broker-b".to_string()], 1)],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

fn records() -> ReplicateRecords {
    let payloads = vec![Bytes::from_static(b"a")];
    ReplicateRecords {
        correlation_id: 1,
        shard: ShardRef {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            generation: 1,
            incarnation: Some(7),
        },
        first_offset: 0,
        checksum: batch_checksum(&payloads, &[], &[]),
        payloads,
        marks: Vec::new(),
        commit_offset: None,
        generations: None,
        publishers: Vec::new(),
        times: None,
    }
}

#[tokio::test]
async fn a_refused_replication_polls_the_catalog_at_once() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(dir.path(), config)?;
    let broker =
        StdArc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    let created = StdArc::new(AtomicBool::new(false));
    let polls = StdArc::new(AtomicUsize::new(0));
    let (addr, shutdown_tx, handle) = serve_router(router(StdArc::clone(&created), polls)).await?;
    let (seeded_tx, seeded_rx) = tokio::sync::oneshot::channel();
    let sync = tokio::spawn(start_sync_with_signal(
        StdArc::clone(&broker),
        format!("http://{addr}"),
        // Far longer than the test may take.
        Duration::from_secs(600),
        Some(seeded_tx),
        None,
    ));
    seeded_rx.await.expect("seeded");

    // Created on the control plane; the leader ships before this broker's
    // next poll.
    created.store(true, Ordering::SeqCst);
    let handler = ReplicaHandler::new(StdArc::clone(&broker), replica_router());
    let answer = handler
        .apply(Some("broker-a"), records(), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateError(_)),
        "{:?}",
        answer.kind()
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !broker.stream_exists("t1", "ns", "orders").await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the refusal did not make the catalog poll"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let answer = handler
        .apply(Some("broker-a"), records(), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{:?}",
        answer.kind()
    );

    sync.abort();
    let _ = shutdown_tx.send(());
    let _ = handle.await;
    Ok(())
}

/// While the catalog has nothing new, refusals that keep asking for a poll get
/// polls further and further apart, not one every 100 ms.
#[tokio::test]
async fn asked_for_polls_back_off_while_they_bring_nothing() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(dir.path(), config)?;
    let broker =
        StdArc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    let polls = StdArc::new(AtomicUsize::new(0));
    // The stream never reaches the catalog.
    let (addr, shutdown_tx, handle) = serve_router(router(
        StdArc::new(AtomicBool::new(false)),
        StdArc::clone(&polls),
    ))
    .await?;
    let (seeded_tx, seeded_rx) = tokio::sync::oneshot::channel();
    let sync = tokio::spawn(start_sync_with_signal(
        StdArc::clone(&broker),
        format!("http://{addr}"),
        Duration::from_secs(600),
        Some(seeded_tx),
        None,
    ));
    seeded_rx.await.expect("seeded");
    let seeded_at = polls.load(Ordering::SeqCst);

    // A leader retrying all the while.
    let handler = ReplicaHandler::new(StdArc::clone(&broker), replica_router());
    let until = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < until {
        let answer = handler
            .apply(Some("broker-a"), records(), felix_broker::LogKind::Stream)
            .await;
        assert!(matches!(answer, InternalMessage::ReplicateError(_)));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // 100, 200, 400, 800 and 1600 ms apart: five polls in three seconds, not
    // thirty.
    let asked = polls.load(Ordering::SeqCst) - seeded_at;
    assert!((1..=7).contains(&asked), "{asked} polls in three seconds");

    sync.abort();
    let _ = shutdown_tx.send(());
    let _ = handle.await;
    Ok(())
}
