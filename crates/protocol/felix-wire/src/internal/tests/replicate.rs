use super::*;

/// **The checksum covers each payload's length as well as its bytes.** Without
/// the length, a batch resplit in transit hashes the same as the original, and
/// a resplit batch is a different set of records — exactly the divergence the
/// checksum exists to catch.
#[test]
fn the_batch_checksum_separates_a_different_split_of_the_same_bytes() {
    let one = batch_checksum(&[Bytes::from_static(b"ab"), Bytes::from_static(b"c")], &[]);
    let other = batch_checksum(&[Bytes::from_static(b"a"), Bytes::from_static(b"bc")], &[]);

    assert_ne!(one, other);
}

#[test]
fn the_batch_checksum_is_stable_and_order_sensitive() {
    let batch = [Bytes::from_static(b"a"), Bytes::from_static(b"bb")];
    let reversed = [Bytes::from_static(b"bb"), Bytes::from_static(b"a")];

    assert_eq!(batch_checksum(&batch, &[]), batch_checksum(&batch, &[]));
    assert_ne!(batch_checksum(&batch, &[]), batch_checksum(&reversed, &[]));
    assert_eq!(batch_checksum(&[], &[]), batch_checksum(&[], &[]));
}

/// The cache variants share a body with the stream ones and must still be told
/// apart. Same bytes but a different kind means the follower writes into the
/// wrong log — a cache's records appended to the stream of the same name.
#[test]
fn a_cache_replication_batch_is_not_a_stream_one() {
    let body = ReplicateRecords {
        correlation_id: 42,
        shard: shard(),
        first_offset: 100,
        checksum: 0x0102_0304,
        payloads: vec![Bytes::from_static(b"a")],
        marks: Vec::new(),
        commit_offset: None,
    };
    let stream = InternalMessage::ReplicateRecords(body.clone());
    let cache = InternalMessage::ReplicateCacheRecords(body);

    let stream_bytes = stream.encode().expect("encode");
    let cache_bytes = cache.encode().expect("encode");
    assert_ne!(stream_bytes, cache_bytes, "only the kind separates them");

    assert_eq!(
        InternalMessage::decode(stream_bytes).expect("decode"),
        stream
    );
    assert_eq!(InternalMessage::decode(cache_bytes).expect("decode"), cache);
}

/// A shard has more than one log, and all four replication kinds carry the
/// same body. Only the kind separates them, so a follower that read the kind
/// wrong would write a stream's records into its cursor log — or the offsets a
/// group gave up on into the positions it resumes from.
#[test]
fn the_four_replication_kinds_are_distinguishable() {
    let body = ReplicateRecords {
        correlation_id: 42,
        shard: shard(),
        first_offset: 1,
        checksum: 7,
        payloads: vec![Bytes::from_static(b"x")],
        marks: Vec::new(),
        commit_offset: None,
    };
    let encoded: Vec<_> = [
        InternalMessage::ReplicateRecords(body.clone()),
        InternalMessage::ReplicateCacheRecords(body.clone()),
        InternalMessage::ReplicateGroupRecords(body.clone()),
        InternalMessage::ReplicateDeadLetterRecords(body),
    ]
    .into_iter()
    .map(|m| (m.clone(), m.encode().expect("encode")))
    .collect();

    for (message, bytes) in &encoded {
        assert_eq!(
            &InternalMessage::decode(bytes.clone()).expect("decode"),
            message
        );
    }
    let distinct: std::collections::HashSet<_> = encoded.iter().map(|(_, b)| b).collect();
    assert_eq!(distinct.len(), 4, "two kinds encode identically");
}

#[test]
fn marks_are_covered_by_the_checksum_and_absent_marks_change_nothing() {
    use super::super::ProducerMark;
    let batch = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
    let opens = ProducerMark::Opens {
        producer_id: 1,
        sequence: 2,
        len: 2,
    };
    let marked = [opens, ProducerMark::Continues];
    assert_ne!(batch_checksum(&batch, &marked), batch_checksum(&batch, &[]));
    assert_ne!(
        batch_checksum(&batch, &marked),
        batch_checksum(&batch, &[opens, ProducerMark::None])
    );
}

/// Without a commit offset a batch is byte for byte what it always was, so a
/// follower that predates the field reads it unchanged.
#[test]
fn a_batch_without_a_commit_offset_keeps_its_old_kind() {
    let message = replicate();
    assert_eq!(message.kind(), Kind::ReplicateRecords);
    let bytes = message.encode().expect("encode");
    let header = InternalHeader::decode(&bytes).expect("header");
    assert_eq!(header.kind, Kind::ReplicateRecords);
}

/// With one it travels as its own kind, which names the log: an older
/// follower refuses it instead of storing records and dropping the offset,
/// and a newer one files the records in the right log.
#[test]
fn a_commit_offset_travels_as_the_committed_kind_for_every_log() {
    let body = ReplicateRecords {
        correlation_id: 42,
        shard: shard(),
        first_offset: 10,
        checksum: 1,
        payloads: vec![Bytes::from_static(b"x")],
        marks: Vec::new(),
        commit_offset: Some(8),
    };
    for message in [
        InternalMessage::ReplicateRecords(body.clone()),
        InternalMessage::ReplicateCacheRecords(body.clone()),
        InternalMessage::ReplicateGroupRecords(body.clone()),
        InternalMessage::ReplicateDeadLetterRecords(body.clone()),
        InternalMessage::ReplicateCounterRecords(body.clone()),
    ] {
        assert_eq!(message.kind(), Kind::ReplicateCommittedRecords);
        let bytes = message.encode().expect("encode");
        let header = InternalHeader::decode(&bytes).expect("header");
        assert_eq!(header.kind, Kind::ReplicateCommittedRecords);
        assert_eq!(InternalMessage::decode(bytes).expect("decode"), message);
    }
}
