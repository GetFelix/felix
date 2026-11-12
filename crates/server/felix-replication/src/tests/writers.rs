//! The generation that wrote each record travels with it.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use felix_wire::internal::GenerationStart;

use super::*;

fn offers_writers() -> PeerCapabilities {
    PeerCapabilities::GENERATION_LABELS
        .union(PeerCapabilities::RECORD_TIMES)
        .union(PeerCapabilities::RECORD_HEADERS)
        .union(PeerCapabilities::WRITER_GENERATIONS)
}

/// A v9 leader log: a filler at offset 0, then two records written at
/// generation 3 and two at 5.
async fn log_over_two_generations() -> (StreamLog, TempDir, felix_broker::Broker) {
    log_over_two_generations_gated(Arc::new(AtomicBool::new(true))).await
}

/// The same log, moving onto v9 only while `open` says so.
async fn log_over_two_generations_gated(
    open: Arc<AtomicBool>,
) -> (StreamLog, TempDir, felix_broker::Broker) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            segment_size_bytes: 256,
            index_spacing_bytes: 256,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let broker = felix_broker::Broker::new(felix_storage::EphemeralCache::new().into())
        .with_durable_storage(storage);
    broker.record_generations_when(move || open.load(Ordering::Acquire));
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("open");
    // Only a segment started after the gate opened is v9. This record fills
    // the first one, so the records after it start a new segment.
    log.append(&[Bytes::from(vec![0u8; 512])])
        .await
        .expect("filler");
    for (generation, values) in [(3, ["a", "b"]), (5, ["c", "d"])] {
        log.accept_generation(generation, Some("broker-a"))
            .await
            .expect("accept");
        let tail = log.tail_offset().await.expect("tail");
        log.record_generation(generation, tail).expect("record");
        log.lead_at(generation);
        for value in values {
            log.append(&[Bytes::copy_from_slice(value.as_bytes())])
                .await
                .expect("append");
        }
    }
    (log, dir, broker)
}

/// **A follower that offers `WRITER_GENERATIONS` is sent the generation of the
/// writer of every record**, equal to the records' own stamps. Catch-up and
/// rebuild ship through the same path. One that does not is sent the frame it
/// always was.
#[tokio::test]
async fn the_writers_ride_with_the_records() {
    let (log, _dir, _broker) = log_over_two_generations().await;
    for (capabilities, expected) in [
        (
            offers_writers(),
            Some(vec![
                GenerationStart {
                    generation: 3,
                    start_offset: 1,
                },
                GenerationStart {
                    generation: 5,
                    start_offset: 3,
                },
            ]),
        ),
        (
            PeerCapabilities::GENERATION_LABELS
                .union(PeerCapabilities::RECORD_TIMES)
                .union(PeerCapabilities::RECORD_HEADERS),
            None,
        ),
    ] {
        let follower = ScriptedFollower::new([Ok(stored(5))]).offering(capabilities);
        let mut cursor = cursor(1);
        let progress = ship_once(
            &follower,
            &log,
            &shard(),
            felix_broker::LogKind::Stream,
            &mut cursor,
            BATCH_BYTES,
            &Rebuilds::disabled(),
        )
        .await;
        assert_eq!(progress, Progress::Stored { durable_offset: 5 });
        let batch = follower.batches().remove(0);
        assert_eq!(batch.writers, expected);
        assert_eq!(batch.payloads.len(), 4);
    }
}

/// **Until the log stamps records or its gate opens, a follower offering
/// `WRITER_GENERATIONS` is sent the frame it always was**: no writers, so no
/// extra bytes and no work to build them. Once the gate says yes, the same
/// records go as unknown writers: they sit in an older segment, and the
/// history's labels are not a record's writer.
#[tokio::test]
async fn no_writers_ride_until_the_log_stamps_records() {
    let open = Arc::new(AtomicBool::new(false));
    let (log, _dir, _broker) = log_over_two_generations_gated(Arc::clone(&open)).await;
    assert!(!log.stamps_generations());
    for (gate, expected) in [
        (false, None),
        // Written before the gate opened, so their writers are unknown.
        (
            true,
            Some(vec![GenerationStart {
                generation: felix_storage::log::UNKNOWN_WRITER,
                start_offset: 1,
            }]),
        ),
    ] {
        open.store(gate, Ordering::Release);
        let follower = ScriptedFollower::new([Ok(stored(5))]).offering(offers_writers());
        let mut cursor = cursor(1);
        let progress = ship_once(
            &follower,
            &log,
            &shard(),
            felix_broker::LogKind::Stream,
            &mut cursor,
            BATCH_BYTES,
            &Rebuilds::disabled(),
        )
        .await;
        assert_eq!(progress, Progress::Stored { durable_offset: 5 });
        let batch = follower.batches().remove(0);
        assert_eq!(batch.writers, expected, "gate {gate}");
        let kind = InternalMessage::ReplicateRecords(batch).kind();
        let stamped = felix_wire::internal::Kind::ReplicateStampedRecords;
        assert_eq!(kind == stamped, gate, "gate {gate}: {kind:?}");
    }
}

/// **A follower that heard the `record_generations` finalize before its
/// leader is sent writers once it refuses a batch without them.** Under
/// `Quorum` RF 3 with both followers ahead of the leader, neither would
/// otherwise store anything until the leader heard it too, and writes would
/// stall. With the gate still closed here, each follower is refused once and
/// stores the retry, which carries the records as unknown writers.
#[tokio::test]
async fn followers_ahead_of_the_finalize_get_writers_after_one_refusal() {
    let open = Arc::new(AtomicBool::new(false));
    let (log, _dir, _broker) = log_over_two_generations_gated(open).await;
    assert!(!log.stamps_generations());
    let wants = InternalMessage::ReplicateError(felix_wire::internal::ReplicateError {
        correlation_id: 0,
        code: ErrorCode::UnsupportedKind,
        expected_offset: 0,
        detail: "the leader must offer writer_generations".to_string(),
        reason: None,
    });
    for _follower in 0..2 {
        let follower =
            ScriptedFollower::new([Ok(wants.clone()), Ok(stored(5))]).offering(offers_writers());
        let mut cursor = cursor(1);
        for _pass in 0..2 {
            ship_once(
                &follower,
                &log,
                &shard(),
                felix_broker::LogKind::Stream,
                &mut cursor,
                BATCH_BYTES,
                &Rebuilds::disabled(),
            )
            .await;
        }
        assert_eq!(cursor.next_offset, 5, "the retry was stored");
        let batches = follower.batches();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].writers, None);
        assert_eq!(
            batches[1].writers,
            Some(vec![GenerationStart {
                generation: felix_storage::log::UNKNOWN_WRITER,
                start_offset: 1,
            }])
        );
    }
}
