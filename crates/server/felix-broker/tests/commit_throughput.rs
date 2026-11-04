//! Commit throughput and latency on one durable shard, for comparing builds.
//! Not a correctness test, so ignored by default. Prints one JSON line per
//! cell: writers x fsync mode.
//!
//! ```text
//! cargo test --release -p felix-broker --test commit_throughput -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use felix_broker::{Broker, DurableStorage, StateOp, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};

const COMMITS_PER_WRITER: usize = 4_000;

async fn run(writers: usize, fsync_mode: FsyncMode) -> (f64, u128, u128) {
    let dir = tempfile::tempdir().expect("dir");
    let config = LogConfig {
        fsync_mode,
        ..LogConfig::default()
    };
    let storage = DurableStorage::open(dir.path(), config).expect("storage");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    broker.register_tenant("t").await.expect("tenant");
    broker.register_namespace("t", "ns").await.expect("ns");
    broker
        .register_stream(
            "t",
            "ns",
            "s",
            StreamMetadata {
                durable: true,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    let handle = Arc::new(
        broker
            .resolve_stream_handle("t", "ns", "s", 0)
            .await
            .expect("handle"),
    );
    let started = Instant::now();
    let tasks: Vec<_> = (0..writers)
        .map(|w| {
            let (broker, handle) = (broker.clone(), handle.clone());
            tokio::spawn(async move {
                let mut nanos = Vec::with_capacity(COMMITS_PER_WRITER);
                for i in 0..COMMITS_PER_WRITER {
                    let ops = vec![StateOp::Put {
                        key: format!("w{w}-k{}", i % 64),
                        value: Bytes::from_static(&[7u8; 64]),
                    }];
                    let at = Instant::now();
                    broker
                        .commit_to_handle(&handle, Bytes::from_static(&[1u8; 64]), ops, None)
                        .await
                        .expect("commit");
                    nanos.push(at.elapsed().as_nanos());
                }
                nanos
            })
        })
        .collect();
    let mut all = Vec::new();
    for task in tasks {
        all.extend(task.await.expect("writer"));
    }
    let secs = started.elapsed().as_secs_f64();
    all.sort_unstable();
    let pct = |p: f64| all[((all.len() - 1) as f64 * p) as usize];
    (all.len() as f64 / secs, pct(0.5), pct(0.99))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark"]
async fn commit_throughput() {
    for (mode, name) in [
        (FsyncMode::None, "none"),
        (FsyncMode::OnCommit, "on_commit"),
    ] {
        for writers in [1, 8] {
            let (rate, p50, p99) = run(writers, mode).await;
            println!(
                "{{\"cell\":\"commit_{name}_w{writers}\",\"commits_per_s\":{rate:.0},\"p50_ns\":{p50},\"p99_ns\":{p99}}}"
            );
        }
    }
}
