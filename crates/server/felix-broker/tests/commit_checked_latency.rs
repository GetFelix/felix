//! Latency of commits with a version check, alone and beside writers without
//! one. Not a correctness test, so ignored by default. Prints one JSON line
//! per cell.
//!
//! ```text
//! cargo test --release -p felix-broker --test commit_checked_latency -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use felix_broker::{Broker, DurableStorage, StateOp, StreamHandle, StreamMetadata, VersionCheck};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};

const CHECKED_COMMITS: usize = 2_000;

fn put(key: &str) -> Vec<StateOp> {
    vec![StateOp::Put {
        key: key.to_owned(),
        value: Bytes::from_static(&[7u8; 64]),
    }]
}

async fn open(fsync_mode: FsyncMode) -> (tempfile::TempDir, Arc<Broker>, StreamHandle) {
    let dir = tempfile::tempdir().expect("dir");
    let config = LogConfig {
        fsync_mode,
        ..LogConfig::default()
    };
    let storage = DurableStorage::open(dir.path(), config).expect("storage");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    broker.register_tenant("t").await.expect("tenant");
    broker.register_namespace("t", "ns").await.expect("ns");
    let metadata = StreamMetadata {
        durable: true,
        ..Default::default()
    };
    broker
        .register_stream("t", "ns", "s", metadata)
        .await
        .expect("stream");
    let handle = broker
        .resolve_stream_handle("t", "ns", "s", 0)
        .await
        .expect("handle");
    (dir, broker, handle)
}

/// One writer of checked commits, each requiring the version it last wrote,
/// beside `others` writers without checks. Returns the checked commits' p50
/// and p99 and the unchecked commits per second.
async fn run(others: usize, fsync_mode: FsyncMode) -> (u128, u128, f64) {
    let (_dir, broker, handle) = open(fsync_mode).await;
    let stop = Arc::new(AtomicBool::new(false));
    let unchecked = Arc::new(AtomicU64::new(0));
    let background: Vec<_> = (0..others)
        .map(|w| {
            let (broker, handle) = (broker.clone(), handle.clone());
            let (stop, unchecked) = (stop.clone(), unchecked.clone());
            tokio::spawn(async move {
                let mut i = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    broker
                        .commit_to_handle(
                            &handle,
                            Bytes::from_static(&[1u8; 64]),
                            put(&format!("w{w}-k{}", i % 64)),
                            None,
                        )
                        .await
                        .expect("commit");
                    unchecked.fetch_add(1, Ordering::Relaxed);
                    i += 1;
                }
            })
        })
        .collect();
    let started = Instant::now();
    let mut version = None;
    let mut nanos = Vec::with_capacity(CHECKED_COMMITS);
    for _ in 0..CHECKED_COMMITS {
        let check = [VersionCheck {
            key: "checked".to_owned(),
            required: version,
        }];
        let at = Instant::now();
        let outcome = broker
            .commit_to_handle_checked(
                &handle,
                Bytes::from_static(&[1u8; 64]),
                put("checked"),
                None,
                &check,
                None,
            )
            .await
            .expect("checked commit");
        nanos.push(at.elapsed().as_nanos());
        version = outcome.offsets.map(|(first, _)| first);
    }
    let secs = started.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    for task in background {
        task.await.expect("writer");
    }
    nanos.sort_unstable();
    let pct = |p: f64| nanos[((nanos.len() - 1) as f64 * p) as usize];
    let rate = unchecked.load(Ordering::Relaxed) as f64 / secs;
    (pct(0.5), pct(0.99), rate)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark"]
async fn commit_checked_latency() {
    for (mode, name) in [
        (FsyncMode::None, "none"),
        (FsyncMode::OnCommit, "on_commit"),
    ] {
        for others in [0, 7] {
            let (p50, p99, rate) = run(others, mode).await;
            println!(
                "{{\"cell\":\"checked_{name}_others{others}\",\"checked_p50_ns\":{p50},\"checked_p99_ns\":{p99},\"unchecked_per_s\":{rate:.0}}}"
            );
        }
    }
}
