//! `GET /backup/offsets`: which led shards get a backup point, and why the
//! others are listed as skipped instead.

use std::sync::Arc;

use bytes::Bytes;
use felix_broker::{
    Broker, CacheMetadata, ConsistencyLevel, DurableStorage, ReadBound, ReadBounds, StreamMetadata,
};
use felix_common::random::HashMap;
use felix_router::{NodeRef, RegionRouter, ShardRouter};
use felix_storage::log::{FsyncMode, LogConfig};
use serde_json::Value;

use super::*;
use crate::shards::lifecycle::ShardLifecycle;
use crate::shards::routing::routing_table_from;
use crate::shards::watch::ShardAssignment;

#[derive(Debug)]
struct Fixed(ReadBound);

impl ReadBounds for Fixed {
    fn stream_bound(&self, _: &str, _: &str, _: &str, _: u32) -> ReadBound {
        self.0
    }
}

fn config() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn key(stream: &str, shard: u32, kind: ShardKind) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard,
        kind,
    }
}

fn assignment(key: ShardKey, leader: &str, generation: u64, replicas: &[&str]) -> ShardAssignment {
    ShardAssignment {
        key,
        leader: leader.to_string(),
        replicas: replicas.iter().map(|r| r.to_string()).collect(),
        generation,
        state: "active".to_string(),
        successor: None,
        routing: Default::default(),
        incarnation: 0,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
        target: None,
        targets_withheld: false,
        raft_formed: None,
        raft_voters: None,
        raft_bases: None,
        raft_seed: None,
        raft_floor: None,
        move_reason: None,
        placed: None,
    }
}

fn node(id: &str, port: u16) -> NodeRef {
    NodeRef {
        node_id: id.to_string(),
        advertise_addr: std::net::SocketAddr::from(([10, 0, 0, 4], port)),
        region: "us-west-2".to_string(),
        live: true,
    }
}

/// An ingress router on `broker-a` that sees `assignments`, every one of them
/// through its opening phase here.
fn ingress(assignments: &[ShardAssignment]) -> Arc<IngressRouter> {
    let nodes: HashMap<String, NodeRef> = [node("broker-a", 7001), node("broker-b", 7002)]
        .into_iter()
        .map(|n| (n.node_id.clone(), n))
        .collect();
    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let owned: HashMap<ShardKey, ShardAssignment> = assignments
        .iter()
        .map(|a| (a.key.clone(), a.clone()))
        .collect();
    router.publish(routing_table_from(&owned, &nodes), &nodes);
    let mut lifecycle = ShardLifecycle::new("broker-a");
    for assignment in assignments {
        lifecycle.observe(&assignment.key, Some(assignment));
        lifecycle.opened(&assignment.key, assignment.generation);
    }
    let ingress = IngressRouter::new(router, Arc::clone(lifecycle.fence()));
    ingress.publish_servable(lifecycle.servable());
    Arc::new(ingress)
}

/// A broker with durable streams and caches under `dir`, holding `orders`
/// (durable, three records), `scratch` (in memory) and the `quorum` cache.
async fn broker(dir: &std::path::Path) -> Arc<Broker> {
    broker_with(dir, ConsistencyLevel::Leader, None).await
}

/// [`broker`] with `orders` at `consistency`, read up to `bound` when given.
async fn broker_with(
    dir: &std::path::Path,
    consistency: ConsistencyLevel,
    bound: Option<ReadBound>,
) -> Arc<Broker> {
    let broker = Broker::new(Box::new(
        felix_storage::LogCache::open(dir.join("caches"), config()).expect("cache"),
    ))
    .with_durable_storage(DurableStorage::open(dir.join("streams"), config()).expect("storage"));
    if let Some(bound) = bound {
        broker.set_read_bounds(Arc::new(Fixed(bound)));
    }
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    for (name, durable) in [("orders", true), ("scratch", false)] {
        broker
            .register_stream(
                "t1",
                "ns",
                name,
                StreamMetadata {
                    durable,
                    consistency,
                    ..Default::default()
                },
            )
            .await
            .expect("stream");
    }
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    for payload in [b"a", b"b", b"c"] {
        broker
            .publish_batch_with_outcome(&handle, &[Bytes::from_static(payload)], None, None)
            .await
            .expect("publish");
    }
    broker
        .register_cache(
            "t1",
            "ns",
            "quorum",
            CacheMetadata {
                consistency: ConsistencyLevel::Quorum,
                ..Default::default()
            },
        )
        .await
        .expect("cache");
    Arc::new(broker)
}

fn source(broker: Arc<Broker>, ingress: Option<Arc<IngressRouter>>) -> BackupOffsets {
    BackupOffsets {
        node_id: Some("broker-a".to_string()),
        broker,
        ingress,
    }
}

async fn collect(source: &BackupOffsets) -> Value {
    serde_json::to_value(source.collect().await).expect("json")
}

/// **A broker with no cluster names no shard**: it leads nothing, so its
/// answer is its node id and two empty lists.
#[tokio::test]
async fn a_broker_without_a_cluster_answers_with_nothing_to_back_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let answer = collect(&source(broker(dir.path()).await, None)).await;
    assert_eq!(
        answer,
        serde_json::json!({"node_id": "broker-a", "shards": [], "skipped": []}),
        "raft_members is left out when empty"
    );
}

/// **A led durable stream is cut at its acknowledged tail**, tagged with the
/// leadership's generation, and with no Raft-only fields.
#[tokio::test]
async fn a_led_durable_stream_reports_its_acknowledged_tail() {
    let dir = tempfile::tempdir().expect("tempdir");
    let orders = key("orders", 0, ShardKind::Stream);
    let ingress = ingress(&[assignment(orders, "broker-a", 4, &[])]);
    let answer = collect(&source(broker(dir.path()).await, Some(ingress))).await;

    assert_eq!(answer["skipped"], serde_json::json!([]));
    let shards = answer["shards"].as_array().expect("shards");
    assert_eq!(shards.len(), 1, "{answer}");
    let shard = &shards[0];
    assert_eq!(shard["name"], "orders");
    assert_eq!(shard["kind"], "stream");
    assert_eq!(shard["shard"], 0);
    assert_eq!(shard["generation"], 4);
    assert_eq!(shard["logs"]["records"], 3);
    for absent in ["incarnation", "engine", "end_term"] {
        assert!(shard.get(absent).is_none(), "{absent} in {shard}");
    }
}

/// **Only shards led here are listed**: one whose leader is another broker
/// is neither backed up nor skipped.
#[tokio::test]
async fn a_shard_led_elsewhere_is_left_out() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ingress = ingress(&[assignment(
        key("orders", 0, ShardKind::Stream),
        "broker-b",
        2,
        &["broker-a"],
    )]);
    let answer = collect(&source(broker(dir.path()).await, Some(ingress))).await;
    assert_eq!(answer["shards"], serde_json::json!([]));
    assert_eq!(answer["skipped"], serde_json::json!([]));
}

/// **An in-memory stream is skipped as `not_durable`**, with no detail: there
/// is nothing on disk to copy, and asking again will not change that.
#[tokio::test]
async fn an_in_memory_stream_is_skipped_as_not_durable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ingress = ingress(&[assignment(
        key("scratch", 0, ShardKind::Stream),
        "broker-a",
        1,
        &[],
    )]);
    let answer = collect(&source(broker(dir.path()).await, Some(ingress))).await;
    assert_eq!(answer["shards"], serde_json::json!([]));
    assert_eq!(
        answer["skipped"],
        serde_json::json!([{
            "tenant_id": "t1", "namespace": "ns", "name": "scratch",
            "shard": 0, "kind": "stream", "reason": "not_durable",
        }])
    );
}

/// **A shard the broker cannot read is skipped as `error` with the cause**,
/// rather than failing the whole answer for the shards that can be read.
#[tokio::test]
async fn an_unreadable_shard_is_skipped_with_its_error_and_the_rest_still_answer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ingress = ingress(&[
        assignment(key("orders", 0, ShardKind::Stream), "broker-a", 1, &[]),
        assignment(key("missing", 0, ShardKind::Stream), "broker-a", 1, &[]),
    ]);
    let answer = collect(&source(broker(dir.path()).await, Some(ingress))).await;
    assert_eq!(answer["shards"].as_array().expect("shards").len(), 1);
    let skipped = answer["skipped"].as_array().expect("skipped");
    assert_eq!(skipped.len(), 1, "{answer}");
    assert_eq!(skipped[0]["name"], "missing");
    assert_eq!(skipped[0]["reason"], "error");
    assert!(
        skipped[0]["detail"].as_str().is_some_and(|d| !d.is_empty()),
        "{answer}"
    );
}

/// **A stream with no committed bound yet is `settling`, and one whose read
/// is refused is `refused`**: both are worth asking again, and neither is
/// given offsets that might include uncommitted records.
#[tokio::test]
async fn a_stream_without_a_readable_bound_is_skipped_with_the_reason() {
    for (bound, reason) in [
        (ReadBound::Settling, "settling"),
        (ReadBound::Refused, "refused"),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let broker = broker_with(dir.path(), ConsistencyLevel::Quorum, Some(bound)).await;
        let ingress = ingress(&[assignment(
            key("orders", 0, ShardKind::Stream),
            "broker-a",
            1,
            &[],
        )]);
        let answer = collect(&source(broker, Some(ingress))).await;
        assert_eq!(answer["shards"], serde_json::json!([]), "{reason}");
        assert_eq!(answer["skipped"][0]["reason"], reason, "{answer}");
        assert!(answer["skipped"][0].get("detail").is_none(), "{answer}");
    }
}

/// **A quorum stream is cut at its committed mark**, not at the leader's
/// tail, which may hold records no quorum has yet.
#[tokio::test]
async fn a_committed_bound_cuts_the_stream_below_its_tail() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_with(
        dir.path(),
        ConsistencyLevel::Quorum,
        Some(ReadBound::Committed(2)),
    )
    .await;
    let ingress = ingress(&[assignment(
        key("orders", 0, ShardKind::Stream),
        "broker-a",
        1,
        &[],
    )]);
    let answer = collect(&source(broker, Some(ingress))).await;
    assert_eq!(answer["shards"][0]["logs"]["records"], 2, "{answer}");
}

/// **A replicated quorum cache with no mark for this leadership is
/// `settling`**: the mark of an earlier leadership says nothing about this
/// one, so no offset is given until this one has its own.
#[tokio::test]
async fn a_replicated_quorum_cache_waits_for_its_own_mark() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ingress = ingress(&[assignment(
        key("quorum", 0, ShardKind::Cache),
        "broker-a",
        3,
        &["broker-b"],
    )]);
    let answer = collect(&source(broker(dir.path()).await, Some(ingress))).await;
    assert_eq!(answer["shards"], serde_json::json!([]), "{answer}");
    assert_eq!(answer["skipped"][0]["kind"], "cache", "{answer}");
    assert_eq!(answer["skipped"][0]["reason"], "settling", "{answer}");
}

/// **Shards are listed in key order**, so two answers from the same broker
/// compare line by line.
#[tokio::test]
async fn led_shards_are_listed_in_key_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ingress = ingress(&[
        assignment(key("scratch", 0, ShardKind::Stream), "broker-a", 1, &[]),
        assignment(key("quorum", 0, ShardKind::Cache), "broker-a", 1, &[]),
        assignment(key("audit", 0, ShardKind::Stream), "broker-a", 1, &[]),
        assignment(key("orders", 0, ShardKind::Stream), "broker-a", 1, &[]),
        assignment(key("missing", 0, ShardKind::Stream), "broker-a", 1, &[]),
    ]);
    let broker = broker(dir.path()).await;
    broker
        .register_stream(
            "t1",
            "ns",
            "audit",
            StreamMetadata {
                durable: true,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    let answer = collect(&source(broker, Some(ingress))).await;
    let shards: Vec<(&str, u64)> = answer["shards"]
        .as_array()
        .expect("shards")
        .iter()
        .map(|s| {
            (
                s["name"].as_str().expect("name"),
                s["shard"].as_u64().expect("shard"),
            )
        })
        .collect();
    assert_eq!(
        shards,
        [("audit", 0), ("orders", 0), ("quorum", 0)],
        "{answer}"
    );
    let names: Vec<&str> = answer["skipped"]
        .as_array()
        .expect("skipped")
        .iter()
        .map(|s| s["name"].as_str().expect("name"))
        .collect();
    assert_eq!(names, ["missing", "scratch"]);
}

/// **The route serves the same answer as JSON** on `GET /backup/offsets`.
#[tokio::test]
async fn the_route_serves_the_offsets_as_json() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ingress = ingress(&[assignment(
        key("orders", 0, ShardKind::Stream),
        "broker-a",
        1,
        &[],
    )]);
    let app = router(source(broker(dir.path()).await, Some(ingress)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (shutdown, server) = crate::test_support::spawn_axum_with_shutdown(listener, app);
    let client = crate::test_support::build_test_client().expect("client");
    let response = crate::test_support::get_with_context(
        &client,
        &format!("http://{addr}/backup/offsets"),
        "backup offsets",
    )
    .await
    .expect("get");
    assert!(response.status().is_success(), "{}", response.status());
    let body: Value = response.json().await.expect("json body");
    assert_eq!(body["node_id"], "broker-a");
    assert_eq!(body["shards"][0]["logs"]["records"], 3, "{body}");
    let _ = shutdown.send(());
    server.await.expect("server");
}
