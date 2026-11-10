//! Followers keep the generation that wrote each record, never their own.
//! The logs here are on segment format v9, where each record carries one.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use felix_storage::log::{Epoch, FsyncMode, LogConfig, LogRecord};
use felix_wire::internal::GenerationStart;
use tempfile::TempDir;

use super::*;
use crate::durable::DurableStorage;

/// A stream log whose segments are v9 from offset 1.
async fn v9_log(shard: u32) -> (StreamLog, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            segment_size_bytes: 256,
            index_spacing_bytes: 256,
            fsync_mode: FsyncMode::OnCommit,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let open = Arc::new(AtomicBool::new(true));
    storage.record_generations_when(move || open.load(Ordering::Acquire));
    let log = storage
        .open_stream(TENANT, NAMESPACE, STREAM, shard)
        .expect("open the shard log");
    // Only a segment started after the gate opened is v9. This record fills
    // the first one, so everything after it lands in v9 segments (offset 0).
    log.append(&[Bytes::from(vec![0u8; 512])])
        .await
        .expect("filler");
    (log, dir)
}

/// Make `log` lead `generation` from its tail, as a leader does before it serves.
async fn lead(log: &StreamLog, generation: u64) {
    log.accept_generation(generation, Some("a"))
        .await
        .expect("accept");
    let tail = log.tail_offset().await.expect("tail");
    log.record_generation(generation, tail).expect("record");
}

async fn read_all(log: &StreamLog) -> Vec<LogRecord> {
    log.read_from(0, 1 << 20).await.expect("read")
}

fn stamps(records: &[LogRecord]) -> Vec<Option<u64>> {
    records.iter().map(|record| record.generation).collect()
}

fn start(generation: u64, start_offset: u64) -> GenerationStart {
    GenerationStart {
        generation,
        start_offset,
    }
}

/// Ship `records` (a slice of a log) the way the shipper would, with the
/// writers their stamps give, into `to`.
async fn copy(
    to: &StreamLog,
    records: &[LogRecord],
    history: &[Epoch],
) -> std::result::Result<Applied, Divergence> {
    let (writers, covered) = writers_to_wire(records, history);
    assert_eq!(covered, records.len());
    let payloads: Vec<Bytes> = records.iter().map(|r| r.payload.clone()).collect();
    let checksum = felix_wire::internal::batch_checksum(&payloads, &[], &[], &[]);
    let times: Vec<u64> = records.iter().map(|r| r.timestamp_micros).collect();
    apply(
        to,
        records[0].offset,
        checksum,
        &payloads,
        &[],
        &[],
        &times,
        None,
        Some(&writers),
    )
    .await
    .expect("apply")
}

async fn leader_of(values: &[&str], generation: u64) -> (StreamLog, TempDir) {
    let (log, dir) = v9_log(0).await;
    lead(&log, generation).await;
    log.append(&batch(values)).await.expect("append");
    (log, dir)
}

/// A follower takes records 0..5 from the leader of generation 3, accepts
/// generation 4 from another node, and then gets 5..10 from that node, which
/// holds a copy of the leader's log. The records were all written at
/// generation 3, so that is what the follower must store for them.
#[tokio::test]
async fn a_follower_keeps_the_generation_that_wrote_the_records() {
    let values: Vec<String> = (0..10).map(|n| format!("r{n}")).collect();
    let refs: Vec<&str> = values.iter().map(String::as_str).collect();
    let (leader, _l) = leader_of(&refs, 3).await;
    let led = read_all(&leader).await[1..].to_vec();
    assert_eq!(stamps(&led), vec![Some(3); 10]);

    let (follower, _f) = v9_log(1).await;
    copy(&follower, &led[..5], &leader.generations())
        .await
        .expect("applied");
    follower
        .accept_generation(4, Some("n"))
        .await
        .expect("accept");
    follower.record_generation(4, 6).expect("record");

    // The other node holds a copy and ships what the follower lacks.
    let (node, _n) = v9_log(2).await;
    copy(&node, &led, &leader.generations())
        .await
        .expect("applied");
    let held = read_all(&node).await[1..].to_vec();
    copy(&follower, &held[5..], &node.generations())
        .await
        .expect("applied");

    assert_eq!(stamps(&read_all(&follower).await[1..]), stamps(&led));
}

/// A batch that does not say who wrote it is refused by a log that stamps,
/// and nothing is stored.
#[tokio::test]
async fn a_batch_without_writers_is_refused_by_a_stamping_log() {
    let (follower, _f) = v9_log(0).await;
    // It leads a generation of its own, which it must not stamp on the copy.
    lead(&follower, 4).await;
    let payloads = batch(&["a", "b"]);
    let checksum = felix_wire::internal::batch_checksum(&payloads, &[], &[], &[]);
    let refused = apply(&follower, 1, checksum, &payloads, &[], &[], &[], None, None).await;
    assert!(
        matches!(refused, Err(BrokerError::WriterUnknown(_))),
        "{refused:?}"
    );
    assert_eq!(follower.tail_offset().await.expect("tail"), 1);
}

/// A writer below the generation of the record before it is a divergence,
/// and nothing is stored.
#[tokio::test]
async fn a_writer_below_the_floor_is_a_divergence() {
    let (follower, _f) = v9_log(0).await;
    let payloads = batch(&["a", "b", "c"]);
    let checksum = felix_wire::internal::batch_checksum(&payloads, &[], &[], &[]);
    apply(
        &follower,
        1,
        checksum,
        &payloads,
        &[],
        &[],
        &[],
        None,
        Some(&[start(5, 1)]),
    )
    .await
    .expect("apply")
    .expect("stored");

    let payloads = batch(&["d", "e"]);
    let checksum = felix_wire::internal::batch_checksum(&payloads, &[], &[], &[]);
    let refused = apply(
        &follower,
        4,
        checksum,
        &payloads,
        &[],
        &[],
        &[],
        None,
        Some(&[start(4, 4)]),
    )
    .await
    .expect("apply")
    .expect_err("refused");
    assert_eq!(
        refused,
        Divergence::GenerationBelowFloor {
            offset: 4,
            generation: 4,
            floor: 5
        }
    );
    assert_eq!(follower.tail_offset().await.expect("tail"), 4);
}

fn record(offset: u64, generation: Option<u64>) -> LogRecord {
    LogRecord {
        offset,
        timestamp_micros: 0,
        checksum: 0,
        payload: Bytes::new(),
        mark: RecordMark::None,
        publisher: None,
        headers: None,
        generation,
    }
}

fn epoch(generation: u64, start_offset: u64) -> Epoch {
    Epoch {
        generation,
        start_offset,
    }
}

#[test]
fn writers_merge_equal_generations_and_start_at_the_first_record() {
    let records: Vec<_> = [(7, 2), (8, 2), (9, 3), (10, 3), (11, 5)]
        .into_iter()
        .map(|(offset, generation)| record(offset, Some(generation)))
        .collect();
    let (runs, covered) = writers_to_wire(&records, &[]);
    assert_eq!(runs, vec![start(2, 7), start(3, 9), start(5, 11)]);
    assert_eq!(covered, 5);
}

#[test]
fn unstamped_records_take_the_generation_the_history_gives() {
    let history = [epoch(1, 0), epoch(4, 12), epoch(6, 20)];
    let records: Vec<_> = (10..14).map(|offset| record(offset, None)).collect();
    let (runs, covered) = writers_to_wire(&records, &history);
    assert_eq!(runs, vec![start(1, 10), start(4, 12)]);
    assert_eq!(covered, 4);

    let (runs, _) = writers_to_wire(&records[..1], &[]);
    assert_eq!(runs, vec![start(0, 10)], "no history is generation 0");
}

#[test]
fn stamps_and_history_mix() {
    let history = [epoch(2, 0)];
    let records = vec![
        record(0, None),
        record(1, Some(2)),
        record(2, Some(3)),
        record(3, None),
    ];
    let (runs, covered) = writers_to_wire(&records, &history);
    // The last record has no stamp and the history says 2, below the 3 before it.
    assert_eq!(runs, vec![start(2, 0), start(3, 2)]);
    assert_eq!(covered, 3);
}

#[test]
fn a_fall_cuts_the_batch_before_it() {
    let records = vec![record(4, Some(3)), record(5, Some(3)), record(6, Some(2))];
    let (runs, covered) = writers_to_wire(&records, &[]);
    assert_eq!(runs, vec![start(3, 4)]);
    assert_eq!(covered, 2);
    assert_eq!(writers_to_wire(&[], &[]), (Vec::new(), 0));
}
