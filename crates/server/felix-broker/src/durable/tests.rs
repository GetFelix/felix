use felix_storage::log::FsyncMode;
use tempfile::tempdir;

use super::*;

fn config() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn payloads(values: &[&str]) -> Vec<Bytes> {
    values
        .iter()
        .map(|v| Bytes::copy_from_slice(v.as_bytes()))
        .collect()
}

#[tokio::test]
async fn appends_are_assigned_contiguous_offsets() {
    let dir = tempdir().expect("dir");
    let storage = DurableStorage::open(dir.path(), config()).expect("open");
    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("stream");

    let first = log.append(&payloads(&["a", "b"])).await.expect("append");
    assert_eq!((first.first_offset, first.last_offset), (0, 1));
    let second = log.append(&payloads(&["c"])).await.expect("append");
    assert_eq!((second.first_offset, second.last_offset), (2, 2));
    assert_eq!(log.tail_offset().await.expect("tail"), 3);
}

#[tokio::test]
async fn an_empty_batch_is_rejected() {
    let dir = tempdir().expect("dir");
    let storage = DurableStorage::open(dir.path(), config()).expect("open");
    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("stream");
    assert!(matches!(
        log.append(&[]).await.expect_err("empty"),
        BrokerError::Storage(_)
    ));
}

#[tokio::test]
async fn reopening_the_same_stream_shares_one_log() {
    let dir = tempdir().expect("dir");
    let storage = DurableStorage::open(dir.path(), config()).expect("open");
    let first = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("stream");
    let again = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("stream");

    first.append(&payloads(&["one"])).await.expect("append");
    let result = again.append(&payloads(&["two"])).await.expect("append");
    // A second writer over the same directory would have restarted at zero.
    assert_eq!(result.first_offset, 1);
}

#[tokio::test]
async fn different_streams_are_isolated() {
    let dir = tempdir().expect("dir");
    let storage = DurableStorage::open(dir.path(), config()).expect("open");
    let orders = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("stream");
    let events = storage
        .open_stream("t1", "default", "events", 0)
        .expect("stream");
    let shard_one = storage
        .open_stream("t1", "default", "orders", 1)
        .expect("stream");

    orders.append(&payloads(&["o"])).await.expect("append");
    assert_eq!(events.tail_offset().await.expect("tail"), 0);
    assert_eq!(shard_one.tail_offset().await.expect("tail"), 0);
}

#[tokio::test]
async fn records_are_readable_after_reopening_storage() {
    let dir = tempdir().expect("dir");
    {
        let storage = DurableStorage::open(dir.path(), config()).expect("open");
        let log = storage
            .open_stream("t1", "default", "orders", 0)
            .expect("stream");
        log.append(&payloads(&["persisted", "twice"]))
            .await
            .expect("append");
        storage.shutdown().await.expect("shutdown");
    }

    let storage = DurableStorage::open(dir.path(), config()).expect("reopen");
    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("stream");
    let records = log.read_from(0, usize::MAX).await.expect("read");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].payload, Bytes::from_static(b"persisted"));
    assert_eq!(records[1].payload, Bytes::from_static(b"twice"));
    assert_eq!(log.tail_offset().await.expect("tail"), 2);
}

#[tokio::test]
async fn on_commit_is_durable_by_the_time_append_returns() {
    let dir = tempdir().expect("dir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            fsync_mode: FsyncMode::OnCommit,
            ..config()
        },
    )
    .expect("open");
    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("stream");

    let result = log.append(&payloads(&["a", "b"])).await.expect("append");
    assert!(log.durable_offset() > result.last_offset);
    assert_eq!(log.unsynced_bytes(), 0);
}

#[tokio::test]
async fn offset_for_time_finds_the_first_record_at_or_after_a_time() {
    let dir = tempdir().expect("dir");
    let storage = DurableStorage::open(dir.path(), config()).expect("open");
    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("stream");
    let order = Arc::new(CommitSequencer::new(0));
    for (values, time) in [(&["a"][..], 100), (&["b", "c"][..], 200), (&["d"][..], 300)] {
        let (pending, turn) = log
            .begin_append(&payloads(values), &[], None, time, &order)
            .await
            .expect("append");
        log.commit(&pending).await.expect("commit");
        drop(turn);
    }
    let tail = log.tail_offset().await.expect("tail");
    assert_eq!(tail, 4);

    let at = |time| log.offset_for_time(time, tail);
    assert_eq!(at(0).await.expect("search"), Some((0, 100)));
    assert_eq!(at(100).await.expect("search"), Some((0, 100)));
    assert_eq!(at(101).await.expect("search"), Some((1, 200)));
    assert_eq!(at(250).await.expect("search"), Some((3, 300)));
    assert_eq!(at(301).await.expect("search"), None);
    // A record at or past the bound is not committed yet, so not an answer.
    assert_eq!(log.offset_for_time(250, 3).await.expect("search"), None);
    // Each record reads back with the time its batch was stamped with.
    let records = log.read_from(0, usize::MAX).await.expect("read");
    let times: Vec<u64> = records.iter().map(|r| r.timestamp_micros).collect();
    assert_eq!(times, [100, 200, 200, 300]);
}

/// One record of each v9 kind with a client record after it, in a v9
/// segment.
async fn append_v9_kinds(log: &StreamLog, after: &'static str) {
    let records: Vec<AppendRecord> = [
        (RecordMark::Membership, "set"),
        (RecordMark::CounterDelta, "add"),
        (RecordMark::CounterCheckpoint, "sums"),
        (RecordMark::None, after),
    ]
    .into_iter()
    .map(|(mark, payload)| AppendRecord {
        payload: Bytes::from(payload),
        timestamp_micros: 1,
        mark,
        publisher: None,
        headers: None,
    })
    .collect();
    log.append_v9_for_tests(&records, 1)
        .await
        .expect("v9 append");
}

/// **A replay never returns a membership or counter record.** Each keeps its
/// offset, so the records around them sit at their own offsets with a gap
/// between, and a read that starts on one of them returns the next client
/// record, not an empty page.
#[tokio::test]
async fn a_read_leaves_out_membership_and_counter_records() {
    let dir = tempdir().expect("dir");
    let storage = DurableStorage::open(dir.path(), config()).expect("open");
    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("stream");
    log.append(&payloads(&["a"])).await.expect("append");
    append_v9_kinds(&log, "b").await;

    let seen = |records: Vec<LogRecord>| -> Vec<(Offset, Bytes)> {
        records.into_iter().map(|r| (r.offset, r.payload)).collect()
    };
    assert_eq!(
        seen(log.read_from(0, usize::MAX).await.expect("read")),
        vec![(0, Bytes::from("a")), (4, Bytes::from("b"))]
    );
    for start in 1..=3 {
        assert_eq!(
            seen(log.read_from(start, 1).await.expect("read")),
            vec![(4, Bytes::from("b"))],
            "from {start}"
        );
    }
}

/// The client records end before trailing membership and counter records,
/// so a reader waiting for the tail is not left waiting on them.
#[tokio::test]
async fn the_event_end_steps_back_over_membership_and_counter_records() {
    let dir = tempdir().expect("dir");
    let storage = DurableStorage::open(dir.path(), config()).expect("open");
    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("stream");
    log.append(&payloads(&["a"])).await.expect("append");
    append_v9_kinds(&log, "b").await;
    let records: Vec<AppendRecord> = [RecordMark::Membership, RecordMark::CounterCheckpoint]
        .into_iter()
        .map(|mark| AppendRecord {
            payload: Bytes::from_static(b"x"),
            timestamp_micros: 1,
            mark,
            publisher: None,
            headers: None,
        })
        .collect();
    log.append_v9_for_tests(&records, 1)
        .await
        .expect("v9 append");

    assert_eq!(log.tail_offset().await.expect("tail"), 7);
    assert_eq!(log.event_end(0, 7).await.expect("end"), 5);
}
