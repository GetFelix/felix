//! Which batches carry the generations that wrote them.

use super::*;

/// **A cache shard's batches are labelled only for a follower that fences
/// caches.** It is the cache fence that compares their labels; a follower
/// with labels but not the cache fence gets cache batches as it always has.
#[test]
fn cache_and_counter_batches_are_labelled_for_a_follower_with_the_cache_fence() {
    use felix_broker::LogKind;
    let labels = PeerCapabilities::GENERATION_LABELS;
    let cache_fence = labels.union(PeerCapabilities::CACHE_FENCE);
    let labelled = |log_kind, offered: PeerCapabilities| {
        labels_needed(log_kind).is_some_and(|needed| offered.contains(needed))
    };

    assert!(labelled(LogKind::Stream, labels));
    for log_kind in [LogKind::Cache, LogKind::Counters] {
        assert!(!labelled(log_kind, labels), "{log_kind:?}");
        assert!(labelled(log_kind, cache_fence), "{log_kind:?}");
    }
    for log_kind in [LogKind::GroupCursors, LogKind::GroupDeadLetters] {
        assert!(!labelled(log_kind, cache_fence), "{log_kind:?}");
    }
}

/// **A cache or counter batch holding a generation-start record keeps its
/// mark on the wire.** Their own kinds have no mark section; sent as one, the
/// record would reach the follower as a cache op it cannot read, and the
/// checksum, which covers marks, would refuse the batch for ever.
#[test]
fn a_cache_batch_with_a_start_record_travels_in_a_layout_with_marks() {
    use felix_broker::LogKind;
    use felix_wire::internal::{ShardRef, batch_checksum};
    let payloads = vec![Bytes::from_static(&[0, 0, 0, 0, 0, 0, 0, 5])];
    let marks = vec![felix_broker::replication::mark_to_wire(
        felix_storage::log::RecordMark::GenerationStart,
    )];
    let mut batch = ReplicateRecords {
        correlation_id: 1,
        shard: ShardRef {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "sessions".to_string(),
            shard: 0,
            generation: 5,
        },
        first_offset: 3,
        checksum: batch_checksum(&payloads, &marks, &[]),
        payloads,
        marks,
        publishers: Vec::new(),
        commit_offset: None,
        generations: None,
    };

    carry_marks(&mut batch, LogKind::Counters, false);
    let sent = InternalMessage::ReplicateCounterRecords(batch);
    let decoded = InternalMessage::decode(sent.encode().expect("encode")).expect("decode");

    let InternalMessage::ReplicateCounterRecords(received) = decoded else {
        panic!("decoded as {:?}", decoded.kind());
    };
    let InternalMessage::ReplicateCounterRecords(sent) = sent else {
        unreachable!()
    };
    assert_eq!(received.marks, sent.marks);
    assert_eq!(
        received.checksum,
        batch_checksum(&received.payloads, &received.marks, &received.publishers)
    );
}
