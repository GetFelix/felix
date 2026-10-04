//! Per-record sequences: each record of a batch carries its own, and a batch
//! re-sent to the same log, a restarted one, or a replica lands once.

use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};

use super::{RECORD_SEQUENCE_WRAP, lift};
use crate::durable::StreamLog;
use crate::error::BrokerError;
use crate::{Broker, DurableStorage, StreamHandle, StreamMetadata};

async fn open(dir: &std::path::Path) -> (Broker, StreamHandle, DurableStorage) {
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = DurableStorage::open(dir, config).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage.clone());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            StreamMetadata {
                durable: true,
                ..Default::default()
            },
        )
        .await
        .expect("register");
    let handle = broker
        .resolve_stream_handle("t1", "default", "orders", 0)
        .await
        .expect("handle");
    (broker, handle, storage)
}

fn log(handle: &StreamHandle) -> &StreamLog {
    handle.state.durable.as_ref().expect("durable")
}

fn bytes(values: &[&'static str]) -> Vec<Bytes> {
    values
        .iter()
        .map(|v| Bytes::from_static(v.as_bytes()))
        .collect()
}

async fn stored(handle: &StreamHandle) -> Vec<Bytes> {
    log(handle)
        .read_from(0, usize::MAX)
        .await
        .expect("read")
        .into_iter()
        .map(|record| record.payload)
        .collect()
}

/// Ship `leader`'s log from `from` to `follower` as replication does,
/// stopping after `count` records.
async fn ship(
    leader: &StreamHandle,
    broker: &Broker,
    follower: &StreamHandle,
    from: u64,
    count: usize,
) {
    let records: Vec<_> = log(leader)
        .read_from(from, usize::MAX)
        .await
        .expect("read")
        .into_iter()
        .take(count)
        .collect();
    let payloads: Vec<Bytes> = records.iter().map(|r| r.payload.clone()).collect();
    let marks: Vec<_> = records
        .iter()
        .map(|r| crate::replication::mark_to_wire(r.mark))
        .collect();
    let checksum = felix_wire::internal::batch_checksum(&payloads, &marks, &[]);
    let applied = crate::replication::apply(log(follower), from, checksum, &payloads, &marks, &[])
        .await
        .expect("apply")
        .expect("in order");
    broker
        .adopt_replicated("t1", "default", "orders", 0, applied.durable_offset)
        .await
        .expect("adopt");
}

/// **A batch's base sequence advances by its record count, and a re-send is
/// a duplicate.** Two batches of three and two records carry sequences 0 and
/// 3; sending either again writes nothing and answers with its offsets.
#[tokio::test]
async fn sequences_count_records_and_a_re_send_lands_once() {
    let dir = tempfile::tempdir().expect("dir");
    let (broker, handle, _storage) = open(dir.path()).await;

    let first = broker
        .publish_records_idempotent(&handle, 7, 0, &bytes(&["a", "b", "c"]))
        .await
        .expect("0");
    let second = broker
        .publish_records_idempotent(&handle, 7, 3, &bytes(&["d", "e"]))
        .await
        .expect("3");
    assert!(!first.duplicate && !second.duplicate);
    assert_eq!(first.outcome.offsets, Some((0, 2)));
    assert_eq!(second.outcome.offsets, Some((3, 4)));

    for (sequence, batch, offsets) in [
        (0, ["a", "b", "c"].as_slice(), (0, 2)),
        (3, &["d", "e"], (3, 4)),
    ] {
        let again = broker
            .publish_records_idempotent(&handle, 7, sequence, &bytes(batch))
            .await
            .expect("re-send");
        assert!(again.duplicate, "sequence {sequence} was appended again");
        assert_eq!(again.outcome.offsets, Some(offsets));
    }
    let next = broker
        .publish_records_idempotent(&handle, 7, 5, &bytes(&["f"]))
        .await
        .expect("5");
    assert!(!next.duplicate);
    assert_eq!(
        stored(&handle).await,
        bytes(&["a", "b", "c", "d", "e", "f"])
    );
}

#[tokio::test]
async fn gaps_unknown_producers_and_expired_re_sends_are_refused() {
    let dir = tempfile::tempdir().expect("dir");
    let (broker, handle, _storage) = open(dir.path()).await;
    broker
        .publish_records_idempotent(&handle, 7, 0, &bytes(&["a", "b"]))
        .await
        .expect("0");

    let gap = broker
        .publish_records_idempotent(&handle, 7, 5, &bytes(&["x"]))
        .await;
    assert!(
        matches!(gap, Err(BrokerError::SequenceGap { expected: 2 })),
        "{gap:?}"
    );
    let unknown = broker
        .publish_records_idempotent(&handle, 8, 4, &bytes(&["x"]))
        .await;
    assert!(
        matches!(
            unknown,
            Err(BrokerError::UnknownProducer { producer_id: 8 })
        ),
        "{unknown:?}"
    );

    // Push the first batch out of the window the log remembers.
    let filler: Vec<Bytes> = (0..100).map(|i| Bytes::from(format!("f{i}"))).collect();
    broker
        .publish_records_idempotent(&handle, 7, 2, &filler)
        .await
        .expect("filler");
    let expired = broker
        .publish_records_idempotent(&handle, 7, 0, &bytes(&["a", "b"]))
        .await;
    assert!(
        matches!(expired, Err(BrokerError::SequenceExpired { sequence: 0 })),
        "{expired:?}"
    );
    assert_eq!(
        stored(&handle).await.len(),
        102,
        "a refusal wrote something"
    );
}

/// **The sequences are the log's.** A restarted broker answers a re-send
/// from what it reads back, and the producer carries on after it.
#[tokio::test]
async fn a_restarted_leader_answers_a_re_send_from_its_log() {
    let dir = tempfile::tempdir().expect("dir");
    let first = {
        let (broker, handle, storage) = open(dir.path()).await;
        let first = broker
            .publish_records_idempotent(&handle, 7, 0, &bytes(&["a", "b"]))
            .await
            .expect("0");
        storage.shutdown().await.expect("shutdown");
        first
    };

    let (broker, handle, _storage) = open(dir.path()).await;
    let again = broker
        .publish_records_idempotent(&handle, 7, 0, &bytes(&["a", "b"]))
        .await
        .expect("re-send");
    assert!(again.duplicate, "the re-send was appended again");
    assert_eq!(again.outcome.offsets, first.outcome.offsets);
    broker
        .publish_records_idempotent(&handle, 7, 2, &bytes(&["c"]))
        .await
        .expect("2");
    assert_eq!(stored(&handle).await, bytes(&["a", "b", "c"]));
}

/// A replica holds only what it was shipped, and that is enough: promoted,
/// it answers a re-send of a whole batch as a duplicate, and finishes one it
/// holds only the start of without writing the start twice.
#[tokio::test]
async fn a_replica_answers_re_sends_of_what_it_was_shipped() {
    let leader_dir = tempfile::tempdir().expect("dir");
    let follower_dir = tempfile::tempdir().expect("dir");
    let (leader, on_leader, _l) = open(leader_dir.path()).await;
    let (follower, on_follower, _f) = open(follower_dir.path()).await;

    leader
        .publish_records_idempotent(&on_leader, 7, 0, &bytes(&["a", "b"]))
        .await
        .expect("0");
    leader
        .publish_records_idempotent(&on_leader, 7, 2, &bytes(&["c", "d", "e"]))
        .await
        .expect("2");
    // The second batch arrives only in part: the leader died mid-shipment.
    ship(&on_leader, &follower, &on_follower, 0, 3).await;

    let whole = follower
        .publish_records_idempotent(&on_follower, 7, 0, &bytes(&["a", "b"]))
        .await
        .expect("re-send");
    assert!(whole.duplicate, "the replica appended a batch it holds");
    assert_eq!(whole.outcome.offsets, Some((0, 1)));

    let finished = follower
        .publish_records_idempotent(&on_follower, 7, 2, &bytes(&["c", "d", "e"]))
        .await
        .expect("re-send");
    assert!(!finished.duplicate, "part of it was new");
    assert_eq!(finished.outcome.offsets, Some((2, 4)));
    assert_eq!(
        stored(&on_follower).await,
        bytes(&["a", "b", "c", "d", "e"])
    );
}

#[test]
fn a_wrapped_sequence_lifts_to_the_count_nearest_what_is_owed() {
    let wrap = RECORD_SEQUENCE_WRAP;
    assert_eq!(lift(5, None), 5);
    assert_eq!(lift(5, Some(5)), 5);
    // Just past the wrap: the producer owes 2^31 + 1 and sends 1.
    assert_eq!(lift(1, Some(wrap + 1)), wrap + 1);
    // A re-send from just before the wrap, arriving after it.
    assert_eq!(lift(wrap - 2, Some(wrap + 3)), wrap - 2);
    // A batch that starts before the wrap while the producer owes it.
    assert_eq!(lift(wrap - 1, Some(wrap - 1)), wrap - 1);
    assert_eq!(lift(0, Some(wrap)), wrap);
}
