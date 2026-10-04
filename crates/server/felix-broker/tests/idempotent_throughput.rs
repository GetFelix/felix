//! Publish throughput on a durable stream, idempotent and plain, for
//! comparing builds. Not a correctness test, so ignored by default:
//!
//! ```text
//! cargo test --release -p felix-broker --test idempotent_throughput -- --ignored --nocapture
//! ```

use std::time::Instant;

use bytes::Bytes;
use felix_broker::{Broker, DurableStorage, SequenceReuse, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};

const BATCHES: u64 = 200_000;
const BATCH: usize = 8;
const PAYLOAD: usize = 128;

async fn broker(dir: &std::path::Path) -> Broker {
    let config = LogConfig {
        // The device is not what is being compared.
        fsync_mode: FsyncMode::None,
        ..LogConfig::default()
    };
    let storage = DurableStorage::open(dir, config).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    broker.register_tenant("t").await.expect("tenant");
    broker
        .register_namespace("t", "ns")
        .await
        .expect("namespace");
    for stream in ["plain", "idempotent"] {
        broker
            .register_stream(
                "t",
                "ns",
                stream,
                StreamMetadata {
                    durable: true,
                    ..Default::default()
                },
            )
            .await
            .expect("stream");
    }
    broker
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark"]
async fn durable_publish_throughput() {
    let dir = tempfile::tempdir().expect("dir");
    let broker = broker(dir.path()).await;
    let payloads: Vec<Bytes> = (0..BATCH)
        .map(|i| Bytes::from(vec![i as u8; PAYLOAD]))
        .collect();

    let plain = broker
        .resolve_stream_handle("t", "ns", "plain", 0)
        .await
        .expect("handle");
    let idempotent = broker
        .resolve_stream_handle("t", "ns", "idempotent", 0)
        .await
        .expect("handle");
    let producer = broker.new_producer_id();
    let mut sequence = 0u64;
    // Alternated and repeated, best of each kept, so warm-up and a noisy
    // moment do not decide the comparison.
    let (mut plain_secs, mut idempotent_secs) = (f64::MAX, f64::MAX);
    for _ in 0..3 {
        let started = Instant::now();
        for _ in 0..BATCHES {
            broker
                .publish_batch_with_outcome(&plain, &payloads, None)
                .await
                .expect("publish");
        }
        plain_secs = plain_secs.min(started.elapsed().as_secs_f64());

        let started = Instant::now();
        for _ in 0..BATCHES {
            broker
                .publish_batch_idempotent(
                    &idempotent,
                    producer,
                    sequence,
                    &payloads,
                    SequenceReuse::Refuse,
                )
                .await
                .expect("publish");
            sequence += 1;
        }
        idempotent_secs = idempotent_secs.min(started.elapsed().as_secs_f64());
    }

    // Re-sends of the last window, answered without appending.
    let started = Instant::now();
    for resent in (sequence - 64)..sequence {
        let outcome = broker
            .publish_batch_idempotent(
                &idempotent,
                producer,
                resent,
                &payloads,
                SequenceReuse::Refuse,
            )
            .await
            .expect("re-send");
        assert!(outcome.duplicate);
    }
    let resend_ns = started.elapsed().as_nanos() as f64 / 64.0;

    let records = (BATCHES * BATCH as u64) as f64;
    println!(
        "plain: {:.0} records/s; idempotent: {:.0} records/s; re-send answered in {resend_ns:.0} ns",
        records / plain_secs,
        records / idempotent_secs,
    );
}
