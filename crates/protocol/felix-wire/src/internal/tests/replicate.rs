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
        generations: None,
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
        generations: None,
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
        generations: None,
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

/// Generations travel as their own kind for every log, with or without a
/// commit offset or marks, and an older follower refuses the kind rather
/// than labelling the records with the sender's generation.
#[test]
fn generations_travel_as_the_labelled_kind_for_every_log() {
    let labelled = |commit_offset, marks: Vec<ProducerMark>| ReplicateRecords {
        correlation_id: 42,
        shard: shard(),
        first_offset: 10,
        checksum: 1,
        payloads: vec![Bytes::from_static(b"x")],
        marks,
        commit_offset,
        generations: Some(vec![
            GenerationStart {
                generation: 3,
                start_offset: 4,
            },
            GenerationStart {
                generation: 5,
                start_offset: 11,
            },
        ]),
    };
    let opens = ProducerMark::Opens {
        producer_id: 7,
        sequence: 1,
        len: 1,
    };
    for commit_offset in [None, Some(8)] {
        let body = labelled(commit_offset, Vec::new());
        for message in [
            InternalMessage::ReplicateRecords(body.clone()),
            InternalMessage::ReplicateMarkedRecords(labelled(commit_offset, vec![opens])),
            InternalMessage::ReplicateCacheRecords(body.clone()),
            InternalMessage::ReplicateGroupRecords(body.clone()),
            InternalMessage::ReplicateDeadLetterRecords(body.clone()),
            InternalMessage::ReplicateCounterRecords(body.clone()),
        ] {
            assert_eq!(message.kind(), Kind::ReplicateLabelledRecords);
            let bytes = message.encode().expect("encode");
            let header = InternalHeader::decode(&bytes).expect("header");
            assert_eq!(header.kind, Kind::ReplicateLabelledRecords);
            assert_eq!(InternalMessage::decode(bytes).expect("decode"), message);
        }
    }
}

/// A labelled batch with no commit offset still carries the field, zeroed. A
/// nonzero value there is refused rather than dropped, or the frame decodes to
/// a message that re-encodes to different bytes.
#[test]
fn an_absent_commit_offset_must_be_zero() {
    let message = InternalMessage::ReplicateCounterRecords(ReplicateRecords {
        correlation_id: 42,
        shard: shard(),
        first_offset: 10,
        checksum: 1,
        payloads: vec![Bytes::from_static(b"x")],
        marks: Vec::new(),
        commit_offset: None,
        generations: Some(Vec::new()),
    });
    let encoded = message.encode().expect("encode");
    // The commit offset is the u64 just before the empty generations count.
    let commit_at = encoded.len() - 4 - 8;
    assert_eq!(&encoded[commit_at..commit_at + 8], &[0; 8]);
    assert_eq!(
        InternalMessage::decode(encoded.clone()).expect("decode"),
        message
    );

    let mut tampered = BytesMut::from(encoded.as_ref());
    tampered[commit_at + 1] = 0x59;
    assert!(matches!(
        InternalMessage::decode(tampered.freeze()),
        Err(Error::Incomplete)
    ));
}

/// A labelled fetch is its own kind with the plain fetch's body.
#[test]
fn a_labelled_fetch_is_its_own_kind() {
    let fetch = |labelled| {
        InternalMessage::ReplicateFetch(ReplicateFetch {
            correlation_id: 42,
            shard: shard(),
            log: ReplicaLog::Stream,
            from_offset: 100,
            max_bytes: 1 << 20,
            labelled,
        })
    };
    assert_eq!(fetch(false).kind(), Kind::ReplicateFetch);
    assert_eq!(fetch(true).kind(), Kind::ReplicateLabelledFetch);
    let plain = fetch(false).encode().expect("encode");
    let labelled = fetch(true).encode().expect("encode");
    assert_eq!(
        plain[InternalHeader::LEN..],
        labelled[InternalHeader::LEN..],
        "the kind is the only difference"
    );
    assert_eq!(
        InternalMessage::decode(labelled).expect("decode"),
        fetch(true)
    );
}

/// A generation-start record's mark round-trips, and a peer that predates it
/// refuses the batch rather than storing the record as a client's.
#[test]
fn a_generation_start_mark_round_trips_and_an_unknown_mark_is_refused() {
    let batch = ReplicateRecords {
        correlation_id: 42,
        shard: shard(),
        first_offset: 10,
        checksum: 1,
        payloads: vec![Bytes::from_static(b"x"), Bytes::from_static(b"y")],
        marks: vec![ProducerMark::GenerationStart, ProducerMark::None],
        commit_offset: None,
        generations: None,
    };
    let message = InternalMessage::ReplicateMarkedRecords(batch);
    let bytes = message.encode().expect("encode");
    assert_eq!(InternalMessage::decode(bytes).expect("decode"), message);

    let mut body = bytes::BytesMut::new();
    crate::internal::replicate::put_marks(&mut body, &[ProducerMark::GenerationStart]);
    body[0] = 4;
    assert!(matches!(
        crate::internal::replicate::take_marks(&mut body.freeze(), 1),
        Err(Error::UnknownInternalProducerMark(4))
    ));
}
