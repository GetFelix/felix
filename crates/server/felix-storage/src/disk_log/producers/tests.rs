use super::*;

/// The payloads of `producer`'s batch `sequence`, distinct for every batch.
fn payloads(producer: u64, sequence: u64, len: usize) -> Vec<Vec<u8>> {
    (0..len)
        .map(|i| format!("{producer}/{sequence}/{i}").into_bytes())
        .collect()
}

/// Observe a whole batch from `producer` at `first`, as an append would.
fn batch(state: &mut ProducerState, producer: u64, sequence: u64, first: Offset, len: usize) {
    let marks = RecordMark::for_batch(producer, sequence, len);
    for ((offset, mark), payload) in (first..).zip(marks).zip(payloads(producer, sequence, len)) {
        state.observe(offset, mark, marked_digest(mark, &payload));
    }
}

/// What a batch [`batch`] observed classifies as.
fn held(producer: u64, sequence: u64, first: Offset, last: Offset) -> ProducerSequence {
    let len = (last - first + 1) as usize;
    ProducerSequence::Held {
        first,
        last,
        digest: Some(PayloadDigest::of(payloads(producer, sequence, len))),
    }
}

#[test]
fn a_held_batch_is_found_and_the_next_is_expected() {
    let mut state = ProducerState::default();
    batch(&mut state, 7, 0, 0, 2);
    batch(&mut state, 7, 1, 2, 1);

    assert_eq!(state.classify(7, 0), held(7, 0, 0, 1));
    assert_eq!(state.classify(7, 1), held(7, 1, 2, 2));
    assert_eq!(state.classify(7, 2), ProducerSequence::Next);
    assert_eq!(state.classify(7, 5), ProducerSequence::Gap { expected: 2 });
    assert_eq!(state.classify(8, 0), ProducerSequence::Unknown);
    assert_eq!(state.next_sequence(7), Some(2));
    assert_eq!(state.next_sequence(8), None);
}

/// The digest is what tells a re-send from a different batch under the same
/// sequence, so it has to cover every record's bytes, their order and their
/// boundaries.
#[test]
fn a_held_batch_digest_tells_its_payloads_from_any_other() {
    let mut state = ProducerState::default();
    batch(&mut state, 7, 0, 0, 2);
    let ProducerSequence::Held {
        digest: Some(digest),
        ..
    } = state.classify(7, 0)
    else {
        panic!("held with a digest");
    };

    assert_eq!(digest, PayloadDigest::of(payloads(7, 0, 2)));
    for other in [
        vec![b"7/0/0".to_vec(), b"7/0/X".to_vec()],
        vec![b"7/0/1".to_vec(), b"7/0/0".to_vec()],
        vec![b"7/0/07/0/1".to_vec()],
        vec![b"7/0/0".to_vec(), b"7/0/1".to_vec(), Vec::new()],
        vec![b"7/0/0".to_vec()],
    ] {
        assert_ne!(digest, PayloadDigest::of(&other), "{other:?}");
    }
}

#[test]
fn a_sequence_older_than_the_window_is_expired() {
    let mut state = ProducerState::default();
    for sequence in 0..(WINDOW as u64 + 3) {
        batch(&mut state, 1, sequence, sequence, 1);
    }
    assert_eq!(state.classify(1, 2), ProducerSequence::Expired);
    assert_eq!(state.classify(1, 3), held(1, 3, 3, 3));
}

#[test]
fn a_batch_is_held_only_once_every_record_is() {
    let mut state = ProducerState::default();
    let marks: Vec<_> = RecordMark::for_batch(3, 0, 3).collect();
    let sent = payloads(3, 0, 3);
    state.observe(10, marks[0], marked_digest(marks[0], &sent[0]));
    state.observe(11, marks[1], marked_digest(marks[1], &sent[1]));
    assert_eq!(
        state.classify(3, 0),
        ProducerSequence::Partial {
            first: 10,
            held: 2,
            len: 3,
            digest: Some(PayloadDigest::of(&sent[..2])),
        }
    );
    assert!(state.is_open());

    state.observe(12, marks[2], marked_digest(marks[2], &sent[2]));
    assert!(!state.is_open());
    assert_eq!(state.classify(3, 0), held(3, 0, 10, 12));
}

/// A batch whose leader stopped partway is abandoned by whatever the log
/// holds next, and its sequence is owed again rather than half-held.
#[test]
fn a_record_that_does_not_continue_an_open_batch_abandons_it() {
    for next in [
        RecordMark::None,
        RecordMark::Opens(ProducerBatch {
            producer_id: 9,
            sequence: 0,
            len: 1,
        }),
    ] {
        let mut state = ProducerState::default();
        batch(&mut state, 3, 0, 0, 1);
        state.observe(1, RecordMark::for_batch(3, 1, 2).next().expect("first"), 0);
        state.observe(2, next, 0);
        assert!(!state.is_open());
        assert_eq!(
            state.classify(3, 1),
            ProducerSequence::Next,
            "after {next:?}"
        );
    }

    // Unmarked records are not observed while nothing is open, so the tail
    // moving past an open batch has to close it too.
    let mut state = ProducerState::default();
    state.observe(0, RecordMark::for_batch(3, 0, 2).next().expect("first"), 0);
    state.settle(1);
    assert!(state.is_open(), "nothing has been written after it yet");
    state.settle(2);
    assert_eq!(state.classify(3, 0), ProducerSequence::Unknown);
}

#[test]
fn a_continuation_without_its_opening_record_is_ignored() {
    let mut state = ProducerState::default();
    state.observe(4, RecordMark::Continues, 0);
    assert_eq!(state, ProducerState::default());
}

/// Retention decides how long a producer is remembered: once every batch it
/// wrote is gone, so is it, on every replica alike.
#[test]
fn a_producer_whose_batches_were_all_trimmed_is_forgotten() {
    let mut state = ProducerState::default();
    batch(&mut state, 1, 0, 0, 1);
    batch(&mut state, 2, 0, 1, 1);
    batch(&mut state, 2, 1, 2, 1);

    state.prune(2);
    assert_eq!(state.classify(1, 1), ProducerSequence::Unknown);
    assert_eq!(state.classify(2, 0), ProducerSequence::Expired);
    assert_eq!(state.classify(2, 1), held(2, 1, 2, 2));
    assert_eq!(state.classify(2, 2), ProducerSequence::Next);
}

#[test]
fn past_the_bound_the_producer_written_longest_ago_is_forgotten() {
    let mut state = ProducerState::default();
    for producer in 0..MAX_PRODUCERS as u64 {
        batch(&mut state, producer, 0, producer, 1);
    }
    // Producer 0 writes again, so producer 1 is now the coldest.
    batch(&mut state, 0, 1, MAX_PRODUCERS as u64, 1);
    batch(&mut state, u64::MAX, 0, MAX_PRODUCERS as u64 + 1, 1);

    assert_eq!(state.len(), MAX_PRODUCERS);
    assert_eq!(state.classify(1, 1), ProducerSequence::Unknown);
    assert_eq!(state.classify(0, 2), ProducerSequence::Next);
    assert_eq!(state.classify(u64::MAX, 1), ProducerSequence::Next);
}

#[test]
fn a_sequence_that_skips_restarts_the_window() {
    let mut state = ProducerState::default();
    batch(&mut state, 1, 0, 0, 1);
    batch(&mut state, 1, 5, 1, 1);
    assert_eq!(state.classify(1, 0), ProducerSequence::Expired);
    assert_eq!(state.classify(1, 5), held(1, 5, 1, 1));
}

#[test]
fn the_snapshot_round_trips_and_refuses_damage() {
    let mut state = ProducerState::default();
    batch(&mut state, 1, 0, 0, 2);
    batch(&mut state, 2, 4, 2, 1);
    state.observe(3, RecordMark::for_batch(1, 1, 3).next().expect("first"), 5);
    // A batch known only from a version 1 snapshot has no digest, and has to
    // keep having none rather than read back as some value.
    state
        .producers
        .get_mut(&2)
        .expect("producer 2")
        .recent
        .back_mut()
        .expect("batch")
        .digest = None;

    let bytes = encode(&state, 4);
    let (decoded, as_of) = decode(&bytes).expect("decodes");
    assert_eq!((&decoded, as_of), (&state, 4));
    assert_eq!(decoded.classify(1, 0), held(1, 0, 0, 1));
    assert_eq!(
        decoded.classify(2, 4),
        ProducerSequence::Held {
            first: 2,
            last: 2,
            digest: None
        }
    );
    assert!(matches!(
        decoded.classify(1, 1),
        ProducerSequence::Partial {
            digest: Some(_),
            ..
        }
    ));

    let mut flipped = bytes.clone();
    let last = flipped.len() - 1;
    flipped[last] ^= 1;
    assert_eq!(decode(&flipped), None);
    assert_eq!(decode(&bytes[..bytes.len() - 1]), None);
    assert_eq!(decode(&[]), None);
}

/// A snapshot written before digests were kept still opens without a rescan,
/// and every batch in it reads as having no digest, which the broker answers
/// the way it always did.
#[test]
fn a_version_1_snapshot_reads_as_batches_without_digests() {
    // Laid out by hand, as a build without digests wrote it: one open batch
    // and one producer holding two batches.
    let mut body = vec![1u8];
    body.extend_from_slice(&1u64.to_be_bytes()); // producer_id
    body.extend_from_slice(&1u64.to_be_bytes()); // sequence
    body.extend_from_slice(&3u32.to_be_bytes()); // len
    body.extend_from_slice(&3u64.to_be_bytes()); // first
    body.extend_from_slice(&1u32.to_be_bytes()); // held
    body.extend_from_slice(&7u64.to_be_bytes()); // producer id
    body.extend_from_slice(&1u64.to_be_bytes()); // last_sequence
    body.extend_from_slice(&2u16.to_be_bytes()); // batches
    for (first, len) in [(0u64, 2u32), (2, 1)] {
        body.extend_from_slice(&first.to_be_bytes());
        body.extend_from_slice(&len.to_be_bytes());
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&SNAPSHOT_MAGIC.to_be_bytes());
    bytes.extend_from_slice(&1u16.to_be_bytes());
    bytes.extend_from_slice(&0u16.to_be_bytes());
    bytes.extend_from_slice(&6u64.to_be_bytes()); // as_of
    bytes.extend_from_slice(&1u32.to_be_bytes()); // producers
    bytes.extend_from_slice(&crate::segment::format::crc32(&[&body]).to_be_bytes());
    bytes.extend_from_slice(&body);

    let (state, as_of) = decode(&bytes).expect("a version 1 snapshot decodes");
    assert_eq!(as_of, 6);
    assert_eq!(
        state.classify(7, 0),
        ProducerSequence::Held {
            first: 0,
            last: 1,
            digest: None
        }
    );
    assert_eq!(
        state.classify(7, 1),
        ProducerSequence::Held {
            first: 2,
            last: 2,
            digest: None
        }
    );
    assert_eq!(
        state.classify(1, 1),
        ProducerSequence::Partial {
            first: 3,
            held: 1,
            len: 3,
            digest: None
        }
    );
    assert_eq!(state.classify(7, 2), ProducerSequence::Next);

    // Written back, it is the current version, still without digests.
    let rewritten = encode(&state, as_of);
    assert_eq!(&rewritten[4..6], &SNAPSHOT_VERSION.to_be_bytes());
    assert_eq!(decode(&rewritten), Some((state, as_of)));

    let mut unknown = bytes;
    unknown[4..6].copy_from_slice(&(SNAPSHOT_VERSION + 1).to_be_bytes());
    assert_eq!(decode(&unknown), None);
}
