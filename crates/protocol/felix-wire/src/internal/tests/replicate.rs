use super::*;

/// **The checksum covers each payload's length as well as its bytes.** Without
/// the length, a batch resplit in transit hashes the same as the original, and
/// a resplit batch is a different set of records — exactly the divergence the
/// checksum exists to catch.
#[test]
fn the_batch_checksum_separates_a_different_split_of_the_same_bytes() {
    let one = batch_checksum(
        &[Bytes::from_static(b"ab"), Bytes::from_static(b"c")],
        &[],
        &[],
    );
    let other = batch_checksum(
        &[Bytes::from_static(b"a"), Bytes::from_static(b"bc")],
        &[],
        &[],
    );

    assert_ne!(one, other);
}

#[test]
fn the_batch_checksum_is_stable_and_order_sensitive() {
    let batch = [Bytes::from_static(b"a"), Bytes::from_static(b"bb")];
    let reversed = [Bytes::from_static(b"bb"), Bytes::from_static(b"a")];

    assert_eq!(
        batch_checksum(&batch, &[], &[]),
        batch_checksum(&batch, &[], &[])
    );
    assert_ne!(
        batch_checksum(&batch, &[], &[]),
        batch_checksum(&reversed, &[], &[])
    );
    assert_eq!(batch_checksum(&[], &[], &[]), batch_checksum(&[], &[], &[]));
}

/// The cache variants share a body with the stream ones and must still be told
/// apart. Same bytes but a different kind means the follower writes into the
/// wrong log — a cache's records appended to the stream of the same name.
#[test]
fn a_cache_replication_batch_is_not_a_stream_one() {
    let body = ReplicateRecords {
        times: None,
        correlation_id: 42,
        shard: shard(),
        first_offset: 100,
        checksum: 0x0102_0304,
        payloads: vec![Bytes::from_static(b"a")],
        marks: Vec::new(),
        commit_offset: None,
        generations: None,
        publishers: Vec::new(),
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
        times: None,
        correlation_id: 42,
        shard: shard(),
        first_offset: 1,
        checksum: 7,
        payloads: vec![Bytes::from_static(b"x")],
        marks: Vec::new(),
        commit_offset: None,
        generations: None,
        publishers: Vec::new(),
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
    assert_ne!(
        batch_checksum(&batch, &marked, &[]),
        batch_checksum(&batch, &[], &[])
    );
    assert_ne!(
        batch_checksum(&batch, &marked, &[]),
        batch_checksum(&batch, &[opens, ProducerMark::None], &[])
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
        times: None,
        correlation_id: 42,
        shard: shard(),
        first_offset: 10,
        checksum: 1,
        payloads: vec![Bytes::from_static(b"x")],
        marks: Vec::new(),
        commit_offset: Some(8),
        generations: None,
        publishers: Vec::new(),
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
        times: None,
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
        publishers: Vec::new(),
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
        times: None,
        correlation_id: 42,
        shard: shard(),
        first_offset: 10,
        checksum: 1,
        payloads: vec![Bytes::from_static(b"x")],
        marks: Vec::new(),
        commit_offset: None,
        generations: Some(Vec::new()),
        publishers: Vec::new(),
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
            timed: false,
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
        times: None,
        correlation_id: 42,
        shard: shard(),
        first_offset: 10,
        checksum: 1,
        payloads: vec![Bytes::from_static(b"x"), Bytes::from_static(b"y")],
        marks: vec![ProducerMark::GenerationStart, ProducerMark::Commit],
        commit_offset: None,
        generations: None,
        publishers: Vec::new(),
    };
    let message = InternalMessage::ReplicateMarkedRecords(batch);
    let bytes = message.encode().expect("encode");
    assert_eq!(InternalMessage::decode(bytes).expect("decode"), message);

    let mut body = bytes::BytesMut::new();
    crate::internal::replicate::put_marks(&mut body, &[ProducerMark::GenerationStart], &[]);
    body[0] = 5;
    assert!(matches!(
        crate::internal::replicate::take_marks(&mut body.freeze(), 1),
        Err(Error::UnknownInternalProducerMark(5))
    ));
}

/// Publishers ride the marks section: a batch with any goes as a kind that
/// has one, round-trips with them in place, and is checksummed with them. A
/// batch without any is unchanged.
#[test]
fn publishers_travel_with_the_marks_and_are_checksummed() {
    use super::super::ProducerMark;
    let payloads = vec![
        Bytes::from_static(b"a"),
        Bytes::from_static(b"b"),
        Bytes::from_static(b"c"),
    ];
    let publishers = vec![Some(Bytes::from_static(b"alice")), None, Some(Bytes::new())];
    let opens = ProducerMark::Opens {
        producer_id: 1,
        sequence: 0,
        len: 1,
    };
    for marks in [
        Vec::new(),
        vec![ProducerMark::None, opens, ProducerMark::None],
    ] {
        let records = ReplicateRecords {
            times: None,
            correlation_id: 3,
            shard: shard(),
            first_offset: 10,
            checksum: batch_checksum(&payloads, &marks, &publishers),
            payloads: payloads.clone(),
            marks: marks.clone(),
            commit_offset: None,
            generations: None,
            publishers: publishers.clone(),
        };
        let message = if marks.is_empty() {
            InternalMessage::ReplicateRecords(records)
        } else {
            InternalMessage::ReplicateMarkedRecords(records)
        };
        assert_eq!(message.kind(), Kind::ReplicateMarkedRecords);
        let decoded = InternalMessage::decode(message.encode().expect("encode")).expect("decode");
        assert_eq!(decoded, message);
    }

    assert_ne!(
        batch_checksum(&payloads, &[], &publishers),
        batch_checksum(&payloads, &[], &[])
    );
    assert_ne!(
        batch_checksum(&payloads, &[], &publishers),
        batch_checksum(
            &payloads,
            &[],
            &[None, None, Some(Bytes::from_static(b"alice"))]
        )
    );
}

fn timed(times: Option<Vec<u64>>, generations: Option<Vec<GenerationStart>>) -> ReplicateRecords {
    ReplicateRecords {
        correlation_id: 42,
        shard: shard(),
        first_offset: 10,
        checksum: 1,
        payloads: vec![Bytes::from_static(b"x"), Bytes::from_static(b"y")],
        marks: Vec::new(),
        commit_offset: Some(8),
        generations,
        publishers: Vec::new(),
        times,
    }
}

/// A labelled batch with times is its own kind, the labelled body followed by
/// one time per record, and round-trips whichever log it is for.
#[test]
fn a_timed_batch_is_its_own_kind_and_round_trips() {
    let generations = Some(vec![GenerationStart {
        generation: 3,
        start_offset: 4,
    }]);
    let body = timed(
        Some(vec![1_700_000_000_000_001, 1_700_000_000_000_002]),
        generations.clone(),
    );
    for message in [
        InternalMessage::ReplicateRecords(body.clone()),
        InternalMessage::ReplicateCacheRecords(body.clone()),
        InternalMessage::ReplicateCounterRecords(body.clone()),
    ] {
        assert_eq!(message.kind(), Kind::ReplicateTimedRecords);
        let bytes = message.encode().expect("encode");
        assert_eq!(
            InternalHeader::decode(&bytes).expect("header").kind,
            Kind::ReplicateTimedRecords
        );
        assert_eq!(InternalMessage::decode(bytes).expect("decode"), message);
    }

    // The times follow the labelled body unchanged.
    let labelled = InternalMessage::ReplicateRecords(timed(None, generations))
        .encode()
        .expect("encode");
    let with_times = InternalMessage::ReplicateRecords(body)
        .encode()
        .expect("encode");
    assert_eq!(
        with_times[InternalHeader::LEN..labelled.len()],
        labelled[InternalHeader::LEN..]
    );
    assert_eq!(
        &with_times[labelled.len()..][..8],
        &1_700_000_000_000_001u64.to_be_bytes()
    );
}

/// Times go only on a labelled batch. Without generations the batch keeps the
/// kind an older follower reads, and the times are left behind.
#[test]
fn times_without_generations_are_not_sent() {
    let message = InternalMessage::ReplicateRecords(timed(Some(vec![1, 2]), None));
    assert_eq!(message.kind(), Kind::ReplicateCommittedRecords);
    let decoded = InternalMessage::decode(message.encode().expect("encode")).expect("decode");
    let InternalMessage::ReplicateRecords(decoded) = decoded else {
        panic!("{decoded:?}");
    };
    assert_eq!(decoded.times, None);
    assert_eq!(decoded, timed(None, None));
}

/// A timed batch cut short in its times is refused, not read as fewer times.
#[test]
fn a_timed_batch_missing_times_is_refused() {
    let message = InternalMessage::ReplicateRecords(timed(Some(vec![1, 2]), Some(Vec::new())));
    let encoded = message.encode().expect("encode");
    let mut short = BytesMut::from(&encoded[..encoded.len() - 8]);
    let length = (short.len() - InternalHeader::LEN) as u32;
    short[8..12].copy_from_slice(&length.to_be_bytes());
    assert!(matches!(
        InternalMessage::decode(short.freeze()),
        Err(Error::Incomplete)
    ));
}

/// A timed fetch is its own kind with the plain fetch's body, and is always
/// labelled.
#[test]
fn a_timed_fetch_is_its_own_kind() {
    let fetch = |labelled, timed| {
        InternalMessage::ReplicateFetch(ReplicateFetch {
            correlation_id: 42,
            shard: shard(),
            log: ReplicaLog::Stream,
            from_offset: 100,
            max_bytes: 1 << 20,
            labelled,
            timed,
        })
    };
    assert_eq!(fetch(true, true).kind(), Kind::ReplicateTimedFetch);
    // Times ride the labelled layout, so an unlabelled fetch cannot ask.
    assert_eq!(fetch(false, true).kind(), Kind::ReplicateFetch);
    let plain = fetch(false, false).encode().expect("encode");
    let timed = fetch(true, true).encode().expect("encode");
    assert_eq!(plain[InternalHeader::LEN..], timed[InternalHeader::LEN..]);
    assert_eq!(
        InternalMessage::decode(timed).expect("decode"),
        fetch(true, true)
    );
}
