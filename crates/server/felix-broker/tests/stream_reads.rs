//! `Broker::read_range`: a bounded page of a durable stream shard, read
//! without subscribing.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use felix_broker::{Broker, BrokerError, DurableStorage, ReadPage, StreamMetadata};
use felix_storage::CommitSequencer;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use tempfile::{TempDir, tempdir};

const TENANT: &str = "t1";
const NAMESPACE: &str = "default";
const STREAM: &str = "matches";

fn log_config() -> LogConfig {
    LogConfig {
        // Small segments, so a few dozen records span several.
        segment_size_bytes: 4 * 1024,
        index_spacing_bytes: 256,
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

async fn broker(dir: &TempDir, config: LogConfig) -> (Arc<Broker>, DurableStorage) {
    let storage = DurableStorage::open(dir.path(), config).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage.clone());
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    for (stream, durable) in [(STREAM, true), ("memory", false)] {
        broker
            .register_stream(
                TENANT,
                NAMESPACE,
                stream,
                StreamMetadata {
                    durable,
                    shards: 1,
                    ..Default::default()
                },
            )
            .await
            .expect("register");
    }
    (Arc::new(broker), storage)
}

async fn publish(broker: &Broker, count: usize) {
    for i in 0..count {
        broker
            .publish(
                TENANT,
                NAMESPACE,
                STREAM,
                Bytes::from(format!("record-{i:04}-{}", "x".repeat(200))),
            )
            .await
            .expect("publish");
    }
}

async fn read(
    broker: &Broker,
    from: u64,
    end: Option<u64>,
    max_records: usize,
    max_bytes: usize,
) -> Result<ReadPage, BrokerError> {
    broker
        .read_range(
            TENANT,
            NAMESPACE,
            STREAM,
            0,
            from,
            end,
            max_records,
            max_bytes,
        )
        .await
}

/// Segment files anywhere under `root`.
fn segment_files(root: &std::path::Path) -> usize {
    let mut count = 0;
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|ext| ext == "log") {
                count += 1;
            }
        }
    }
    count
}

fn offsets(page: &ReadPage) -> Vec<u64> {
    page.records.iter().map(|record| record.offset).collect()
}

/// **Paging a range across segment boundaries returns every record once, in
/// order, and stops at `end`.**
#[tokio::test]
async fn a_range_across_segments_pages_to_its_end() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker(&dir, log_config()).await;
    publish(&broker, 60).await;
    let segments = segment_files(dir.path());
    assert!(
        segments > 2,
        "the range should span segments, got {segments}"
    );

    let mut seen = Vec::new();
    let mut from = 5;
    loop {
        let page = read(&broker, from, Some(50), 7, 1 << 20)
            .await
            .expect("read");
        assert!(page.records.len() <= 7);
        seen.extend(offsets(&page));
        assert!(page.next_offset > from, "a page must make progress");
        from = page.next_offset;
        if from >= 50 {
            break;
        }
    }
    assert_eq!(from, 50, "the last page ends at `end`, not past it");
    assert_eq!(seen, (5..50).collect::<Vec<_>>());
    let page = read(&broker, 5, Some(6), 10, 1 << 20).await.expect("read");
    assert_eq!(
        page.records[0].payload,
        Bytes::from(format!("record-0005-{}", "x".repeat(200)))
    );
}

/// **A start below what retention kept is reported, not served short.**
#[tokio::test]
async fn a_start_below_retention_is_too_old() {
    let dir = tempdir().expect("dir");
    let config = LogConfig {
        retention_bytes: Some(8 * 1024),
        retention_check_interval: Duration::from_secs(3600),
        ..log_config()
    };
    let (broker, storage) = broker(&dir, config).await;
    publish(&broker, 120).await;
    let log = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("log");
    assert!(log.enforce_retention_now().await.expect("retention") > 0);
    let base = log.base_offset();
    assert!(base > 0);

    match read(&broker, 0, Some(base + 5), 100, 1 << 20).await {
        Err(BrokerError::CursorTooOld { oldest, requested }) => {
            assert_eq!((oldest, requested), (base, 0));
        }
        other => panic!("expected CursorTooOld, got {other:?}"),
    }
    let page = read(&broker, base, Some(base + 5), 100, 1 << 20)
        .await
        .expect("the retained head reads");
    assert_eq!(offsets(&page), (base..base + 5).collect::<Vec<_>>());
}

/// **An end past the committed tail stops at the tail**, and a start at the
/// tail is an empty page rather than an error. Past the tail is an error.
#[tokio::test]
async fn an_end_past_the_tail_stops_at_it() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker(&dir, log_config()).await;
    publish(&broker, 10).await;

    let page = read(&broker, 6, Some(1_000), 100, 1 << 20)
        .await
        .expect("read");
    assert_eq!(offsets(&page), vec![6, 7, 8, 9]);
    assert_eq!(page.next_offset, 10);

    let at_tail = read(&broker, 10, None, 100, 1 << 20).await.expect("read");
    assert!(at_tail.records.is_empty());
    assert_eq!(at_tail.next_offset, 10);

    assert!(matches!(
        read(&broker, 11, None, 100, 1 << 20).await,
        Err(BrokerError::CursorInFuture {
            requested: 11,
            tail: 10
        })
    ));
}

/// **An empty range answers at once with nothing**, wherever it sits.
#[tokio::test]
async fn empty_ranges_are_empty_pages() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker(&dir, log_config()).await;
    publish(&broker, 10).await;

    for (from, end) in [(4, 4), (4, 2), (0, 0)] {
        let page = read(&broker, from, Some(end), 100, 1 << 20)
            .await
            .expect("read");
        assert!(page.records.is_empty(), "[{from}, {end}) returned records");
        assert_eq!(page.next_offset, from);
    }
    let none = read(&broker, 3, None, 0, 1 << 20).await.expect("read");
    assert!(none.records.is_empty());
    assert_eq!(none.next_offset, 3);
}

/// **The record count and byte budget both cap a page**, and a record larger
/// than the byte budget still comes back, alone.
#[tokio::test]
async fn a_page_is_capped_by_records_and_bytes() {
    let dir = tempdir().expect("dir");
    let config = LogConfig {
        max_records_per_read: 8,
        ..log_config()
    };
    let (broker, _storage) = broker(&dir, config).await;
    publish(&broker, 40).await;

    let by_count = read(&broker, 0, None, 3, 1 << 20).await.expect("read");
    assert_eq!(offsets(&by_count), vec![0, 1, 2]);
    assert_eq!(by_count.next_offset, 3);

    // The log's own per-read cap holds however many the caller asks for.
    let by_log = read(&broker, 0, None, 1_000, 1 << 20).await.expect("read");
    assert_eq!(by_log.records.len(), 8);
    assert_eq!(by_log.next_offset, 8);

    // Each record is a little over 200 bytes.
    let by_bytes = read(&broker, 0, None, 1_000, 500).await.expect("read");
    assert!(
        (1..=3).contains(&by_bytes.records.len()),
        "{} records in a 500-byte page",
        by_bytes.records.len()
    );
    assert_eq!(by_bytes.next_offset, by_bytes.records.len() as u64);

    let one = read(&broker, 0, None, 1_000, 1).await.expect("read");
    assert_eq!(offsets(&one), vec![0]);
}

/// **A record written but not yet synced is never returned** under
/// `FsyncMode::OnCommit`: its publish has not completed, and a crash now
/// would take it back.
#[tokio::test]
async fn a_record_not_yet_committed_is_never_returned() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker(&dir, log_config()).await;
    publish(&broker, 3).await;
    let log = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("log");

    let order = Arc::new(CommitSequencer::new(3));
    let (pending, turn) = log
        .begin_append(
            &[Bytes::from_static(b"in flight")],
            &[],
            felix_broker::append_time_now(),
            &order,
        )
        .await
        .expect("write");
    assert_eq!(
        log.tail_offset().await.expect("tail"),
        4,
        "the record is on disk"
    );

    let page = read(&broker, 0, None, 100, 1 << 20).await.expect("read");
    assert_eq!(offsets(&page), vec![0, 1, 2]);
    assert_eq!(page.next_offset, 3);
    let at = read(&broker, 3, None, 100, 1 << 20).await.expect("read");
    assert!(at.records.is_empty(), "returned a record not yet synced");

    log.commit(&pending).await.expect("commit");
    drop(turn);
    let page = read(&broker, 3, None, 100, 1 << 20).await.expect("read");
    assert_eq!(offsets(&page), vec![3]);
    assert_eq!(page.next_offset, 4);
}

/// **`next_offset` passes over a generation-start record**, which holds an
/// offset no client is given.
#[tokio::test]
async fn next_offset_skips_a_generation_start() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker(&dir, log_config()).await;
    publish(&broker, 2).await;
    broker
        .append_generation_start(TENANT, NAMESPACE, STREAM, 0, 2)
        .await
        .expect("generation start");
    publish(&broker, 1).await;

    let first = read(&broker, 0, None, 2, 1 << 20).await.expect("read");
    assert_eq!(offsets(&first), vec![0, 1]);
    assert_eq!(first.next_offset, 2);
    let over = read(&broker, 2, None, 10, 1 << 20).await.expect("read");
    assert_eq!(offsets(&over), vec![3]);
    assert_eq!(over.next_offset, 4);
    let hole = read(&broker, 2, Some(3), 10, 1 << 20).await.expect("read");
    assert!(hole.records.is_empty());
    assert_eq!(
        hole.next_offset, 3,
        "the range holding only the marker is done"
    );
}

/// **A read registers no subscriber, and an in-memory stream is refused.**
#[tokio::test]
async fn a_read_registers_nothing_and_needs_a_durable_stream() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker(&dir, log_config()).await;
    publish(&broker, 5).await;
    read(&broker, 0, None, 10, 1 << 20).await.expect("read");
    assert_eq!(
        broker
            .registered_subscribers(TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("count"),
        0
    );
    assert!(matches!(
        broker
            .read_range(TENANT, NAMESPACE, "memory", 0, 0, None, 10, 1 << 20)
            .await,
        Err(BrokerError::StreamNotDurable { .. })
    ));
}
