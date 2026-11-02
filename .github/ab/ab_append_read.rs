//! Temporary A/B harness: DiskLog append and read cost for records without headers.

use std::time::Instant;

use felix_storage::DiskLogProvider;
use felix_storage::log::{AppendOnlyLog, AppendRecord, FsyncMode, LogConfig, ReadRange, ShardKey};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn record(payload: usize) -> AppendRecord {
    AppendRecord {
        payload: vec![b'x'; payload].into(),
        timestamp_micros: 1,
        mark: Default::default(),
        publisher: None,
        headers: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn ab_append_read() {
    let total = env_usize("AB_TOTAL", 400_000);
    let payload = env_usize("AB_PAYLOAD", 256);
    let tag = std::env::var("AB_TAG").unwrap_or_default();
    for batch in [1usize, 64] {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = LogConfig {
            segment_size_bytes: 64 * 1024 * 1024,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        };
        let provider = DiskLogProvider::new(dir.path().to_path_buf(), config).expect("provider");
        let shard = ShardKey { tenant: "t".into(), namespace: "ns".into(), stream: "s".into(), shard: 0 };
        let log = provider.open_shard(&shard).expect("open");
        let records: Vec<AppendRecord> = (0..batch).map(|_| record(payload)).collect();
        let n = if batch == 1 { total / 4 } else { total };
        let start = Instant::now();
        for _ in 0..n / batch {
            log.append(&records).await.expect("append");
        }
        let append = start.elapsed();
        let tail = log.tail_offset().await.expect("tail");
        let start = Instant::now();
        let mut next = 0;
        let mut read = 0usize;
        while next < tail {
            let page = log
                .read_range(ReadRange { start: next, max_bytes: 1 << 20 })
                .await
                .expect("read");
            read += page.records.len();
            next = page.next;
        }
        let read_time = start.elapsed();
        assert_eq!(read as u64, tail);
        println!(
            "AB {{\"tag\":\"{tag}\",\"batch\":{batch},\"records\":{n},\"append_rps\":{:.0},\"read_rps\":{:.0}}}",
            n as f64 / append.as_secs_f64(),
            read as f64 / read_time.as_secs_f64()
        );
        log.shutdown().await.expect("shutdown");
    }
}
