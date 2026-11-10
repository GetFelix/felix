//! A follower's first contact for a shard opens its log off the runtime's
//! workers. Its own test binary: the fsync delay it sets is process-wide.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use felix_broker::{Broker, LogKind};
use felix_replication::ReplicaHandler;
use felix_router::{NodeRef, RegionRouter, RoutingTable, ShardKey, ShardKind, ShardRouter};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::{InternalMessage, ReplicateRecords, ShardRef, batch_checksum};

const LOCAL: &str = "broker-b";
const LEADER: &str = "broker-a";

fn node(node_id: &str, port: u16) -> NodeRef {
    NodeRef {
        node_id: node_id.to_string(),
        advertise_addr: format!("10.0.0.1:{port}").parse().expect("addr"),
        region: "us-west-2".to_string(),
        live: true,
    }
}

/// A router in which `LOCAL` follows `LEADER` for shard 0 of t1/ns/orders.
fn router(generation: u64) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes: HashMap<String, NodeRef> = [(LEADER, 7001), (LOCAL, 7002)]
        .into_iter()
        .map(|(id, port)| (id.to_string(), node(id, port)))
        .collect();
    let key = ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    };
    let table = RoutingTable::build(
        [(key, LEADER.to_string(), vec![LOCAL.to_string()], generation)],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

fn batch(generation: u64) -> ReplicateRecords {
    let payloads = vec![Bytes::from_static(b"a")];
    ReplicateRecords {
        writers: None,
        times: None,
        correlation_id: 1,
        shard: ShardRef {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            generation,
            incarnation: None,
        },
        first_offset: 0,
        checksum: batch_checksum(&payloads, &[], &[], &[]),
        payloads,
        marks: Vec::new(),
        commit_offset: None,
        generations: None,
        publishers: Vec::new(),
        headers: None,
        set_generation: None,
    }
}

/// **A slow disk does not stall the runtime while a follower opens a new
/// shard's log.** A bulk create hands every follower hundreds of new shards
/// at once, and each open takes several fsyncs. On a worker they would hold up
/// the lease heartbeat and every other task behind the disk. A
/// single-threaded runtime makes any flush on it visible as a gap in a ticker.
#[tokio::test]
async fn a_followers_first_batch_opens_its_log_off_the_runtime() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = felix_broker::DurableStorage::open(
        dir.path(),
        LogConfig {
            fsync_mode: FsyncMode::OnCommit,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    let handler = ReplicaHandler::new(Arc::clone(&broker), router(1));

    let flush = Duration::from_millis(100);
    felix_storage::fault::set_fsync_delay(flush);
    let ticks = Arc::new(Mutex::new(vec![Instant::now()]));
    let ticker = tokio::spawn({
        let ticks = Arc::clone(&ticks);
        async move {
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
                ticks.lock().expect("ticks").push(Instant::now());
            }
        }
    });
    let started = Instant::now();
    let answer = handler.apply(Some(LEADER), batch(1), LogKind::Stream).await;
    let end = Instant::now();
    let took = end - started;
    ticker.abort();
    felix_storage::fault::set_fsync_delay(Duration::ZERO);

    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "the batch was refused: {:?}",
        answer.kind()
    );
    let mut ticks = ticks.lock().expect("ticks").clone();
    // The batch's end counts as a tick, so a runtime that never got to tick
    // shows its whole stall.
    ticks.push(end);
    let longest = ticks
        .windows(2)
        .map(|pair| pair[1] - pair[0])
        .max()
        .unwrap_or_default();
    assert!(took >= flush, "the open flushed nothing: {took:?}");
    assert!(
        longest < flush / 2,
        "the runtime stalled {longest:?} during a first batch that took {took:?}"
    );
}
