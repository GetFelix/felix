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

/// A v9 leader log: two records written at generation 3, then two at 5.
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
            segment_size_bytes: 64 * 1024,
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
                    start_offset: 0,
                },
                GenerationStart {
                    generation: 5,
                    start_offset: 2,
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
        let follower = ScriptedFollower::new([Ok(stored(4))]).offering(capabilities);
        let mut cursor = cursor(0);
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
        assert_eq!(progress, Progress::Stored { durable_offset: 4 });
        let batch = follower.batches().remove(0);
        assert_eq!(batch.writers, expected);
        assert_eq!(batch.payloads.len(), 4);
    }
}

/// **Until the log stamps records or its gate opens, a follower offering
/// `WRITER_GENERATIONS` is sent the frame it always was**: no writers, so no
/// extra bytes and no work to build them. Once the gate says yes, the same
/// records go with the writers their history names.
#[tokio::test]
async fn no_writers_ride_until_the_log_stamps_records() {
    let open = Arc::new(AtomicBool::new(false));
    let (log, _dir, _broker) = log_over_two_generations_gated(Arc::clone(&open)).await;
    assert!(!log.stamps_generations());
    for (gate, expected) in [
        (false, None),
        (
            true,
            Some(vec![
                GenerationStart {
                    generation: 3,
                    start_offset: 0,
                },
                GenerationStart {
                    generation: 5,
                    start_offset: 2,
                },
            ]),
        ),
    ] {
        open.store(gate, Ordering::Release);
        let follower = ScriptedFollower::new([Ok(stored(4))]).offering(offers_writers());
        let mut cursor = cursor(0);
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
        assert_eq!(progress, Progress::Stored { durable_offset: 4 });
        let batch = follower.batches().remove(0);
        assert_eq!(batch.writers, expected, "gate {gate}");
        let kind = InternalMessage::ReplicateRecords(batch).kind();
        let stamped = felix_wire::internal::Kind::ReplicateStampedRecords;
        assert_eq!(kind == stamped, gate, "gate {gate}: {kind:?}");
    }
}
