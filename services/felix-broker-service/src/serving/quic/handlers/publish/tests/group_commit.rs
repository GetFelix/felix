//! Concurrent durable publishes through the real worker pool should share
//! device flushes, the way they do when they call the broker directly
//! (`felix-broker/tests/publish_concurrency.rs`). The worker claims serially;
//! if it also waited for each flush, fan-in would be one.

use felix_broker::{DurableStorage, StreamMetadata};
use felix_storage::log::{FsyncMode, LogConfig};
use tokio::sync::oneshot;

use crate::config::BrokerConfig;
use crate::serving::quic::ClusterContext;

use super::*;

const PUBLISHERS: usize = 8;
const PER_PUBLISHER: usize = 16;

async fn publish_through_workers(publishers: usize) -> (u64, u64) {
    let dir = tempfile::tempdir().expect("dir");
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let broker = Broker::new(EphemeralCache::new().into())
        .with_durable_storage(DurableStorage::open(dir.path(), config).expect("storage"));
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    let broker = Arc::new(broker);
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let log = broker
        .durable_storage()
        .expect("durable")
        .open_stream("t1", "ns", "orders", 0)
        .expect("log");
    let ctx = build_publish_context(
        Arc::clone(&broker),
        &BrokerConfig::default(),
        ClusterContext::default(),
    );

    let before = log.flushes();
    let mut tasks = Vec::new();
    for _ in 0..publishers {
        let worker = ctx.workers[handle.id() as usize % ctx.worker_count].clone();
        let handle = handle.clone();
        // Closed loop, one batch in flight per publisher: what a client that
        // waits for each commit ack looks like.
        tasks.push(tokio::spawn(async move {
            for _ in 0..PER_PUBLISHER {
                let (response, answer) = oneshot::channel();
                worker
                    .send(PublishJob {
                        target: PublishTarget::Resolved {
                            handle: handle.clone(),
                            shard: None,
                            generation: 0,
                            fenced: None,
                        },
                        payloads: vec![Bytes::from(vec![b'x'; 4096]); 16],
                        response: Some(response),
                        acked_on_enqueue: false,
                        admission_permit: None,
                        fenced: None,
                    })
                    .await
                    .expect("send");
                answer.await.expect("answer").expect("publish");
            }
        }));
    }
    for task in tasks {
        task.await.expect("publisher");
    }
    let appends = (publishers * PER_PUBLISHER) as u64;
    (appends, log.flushes() - before)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publishers_share_flushes_through_the_worker() {
    let (serial_appends, serial_flushes) = publish_through_workers(1).await;
    let (appends, flushes) = publish_through_workers(PUBLISHERS).await;
    eprintln!(
        "serial: {serial_appends} appends / {serial_flushes} flushes; \
         {PUBLISHERS} publishers: {appends} appends / {flushes} flushes \
         (fan-in {:.2})",
        appends as f64 / flushes.max(1) as f64
    );
    // One publisher waits alone every time, so it is the control.
    assert!(
        serial_flushes * 2 >= serial_appends,
        "a single publisher coalesced, so it is no control"
    );
    assert!(
        flushes * 2 <= appends,
        "{PUBLISHERS} concurrent publishers made {flushes} flushes for {appends} appends"
    );
}
