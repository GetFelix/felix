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

/// Observe the records of `producer`'s batch `sequence` of `len` at the
/// given positions and offsets, marked as a Raft log marks them.
fn pieces(state: &mut ProducerState, producer: u64, sequence: u64, len: u32, at: &[(u32, Offset)]) {
    let payloads = payloads(producer, sequence, len as usize);
    for &(position, offset) in at {
        let mark = match position {
            0 => RecordMark::Opens(ProducerBatch {
                producer_id: producer,
                sequence,
                len,
            }),
            _ => RecordMark::Piece(BatchPiece {
                producer_id: producer,
                sequence,
                position,
                len,
            }),
        };
        let digest = marked_digest(mark, &payloads[position as usize]);
        state.observe(offset, mark, digest);
    }
}

/// A batch cut by another record is remembered with what the log holds of
/// it, and the pieces still owed finish it wherever they land. Forgotten,
/// the batch would read as the producer's next and be written again whole.
#[test]
fn a_cut_batch_is_finished_by_the_pieces_it_was_owed() {
    let mut state = ProducerState::keeping_cuts();
    batch(&mut state, 3, 0, 0, 1);
    pieces(&mut state, 3, 1, 3, &[(0, 1), (1, 2)]);
    state.observe(3, RecordMark::None, 0);
    assert!(!state.is_open());
    assert_eq!(
        state.unfinished(3, 1),
        Some(UnfinishedBatch {
            first: Some(1),
            held: 2,
            len: 3,
            digest: Some(PayloadDigest::of(&payloads(3, 1, 3)[..2])),
        })
    );
    assert_eq!(state.unfinished(3, 2), None);
    assert_eq!(state.unfinished(4, 1), None);

    pieces(&mut state, 3, 1, 3, &[(2, 4)]);
    assert_eq!(state.unfinished(3, 1), None);
    assert_eq!(
        state.classify(3, 1),
        ProducerSequence::Held {
            first: 1,
            last: 4,
            digest: Some(PayloadDigest::of(payloads(3, 1, 3))),
        }
    );
    assert_eq!(state.classify(3, 2), ProducerSequence::Next);
}

/// A log that begins inside a cut batch still holds some of its records,
/// so it must go on knowing the batch is there, as one it cannot finish.
/// Dropped with its first record, the batch would be taken as new and the
/// records still here written a second time. A log pruned to a base and a
/// log read from that base agree.
#[test]
fn a_cut_batch_outlives_its_first_record() {
    let mut state = ProducerState::keeping_cuts();
    pieces(&mut state, 3, 0, 3, &[(0, 1), (1, 2)]);
    state.observe(3, RecordMark::None, 0);

    state.prune(2);
    let headless = UnfinishedBatch {
        first: None,
        held: 2,
        len: 3,
        digest: None,
    };
    assert_eq!(state.unfinished(3, 0), Some(headless));
    let mut suffix = ProducerState::keeping_cuts();
    pieces(&mut suffix, 3, 0, 3, &[(1, 2)]);
    suffix.observe(3, RecordMark::None, 0);
    assert_eq!(state, suffix);

    // A snapshot keeps it.
    let (mut saved, as_of) = decode(&encode(&state, 4)).expect("decodes");
    saved.keeps_cuts = true;
    assert_eq!((saved, as_of), (state.clone(), 4));

    // Once the last record held of it is gone, its re-send is still refused.
    state.prune(3);
    assert_refused(&state, 3, 0);
}

/// The re-send of a refused batch: the stub a forgotten cut leaves.
const FORGOTTEN: UnfinishedBatch = UnfinishedBatch {
    first: None,
    held: 0,
    len: 0,
    digest: None,
};

fn assert_refused(state: &ProducerState, producer: u64, sequence: u64) {
    assert_eq!(state.unfinished(producer, sequence), Some(FORGOTTEN));
    assert_eq!(
        state.classify(producer, sequence),
        ProducerSequence::Expired
    );
}

/// A purge past the last record of a producer's first batch, cut, leaves
/// the producer unknown. Taken as new, the re-send of sequence 0 would
/// write the records the log held of it a second time. It is refused,
/// across a snapshot, until the producer opens a batch again.
#[test]
fn a_purge_past_a_cut_first_batch_refuses_its_resend() {
    let mut state = ProducerState::keeping_cuts();
    pieces(&mut state, 3, 0, 3, &[(0, 0), (1, 1)]);
    state.observe(2, RecordMark::None, 0);
    state.prune(3);
    assert_refused(&state, 3, 0);
    assert_eq!(state.classify(3, 1), ProducerSequence::Unknown);
    assert_eq!(state.classify(4, 0), ProducerSequence::Unknown);

    let (mut saved, _) = decode(&encode(&state, 3)).expect("decodes");
    saved.keeps_cuts = true;
    assert_eq!(saved, state);

    // A later batch does not lift the refusal while a truncation could
    // still remove it. A purge past it does: purged records are committed.
    batch(&mut state, 3, 1, 5, 1);
    assert_refused(&state, 3, 0);
    assert_eq!(state.classify(3, 1), held(3, 1, 5, 5));
    state.prune(6);
    assert!(state.forgotten().is_empty());
}

/// A piece of a forgotten batch, written by a leader that still held the
/// rest, leaves the refusal in place.
#[test]
fn a_piece_of_a_forgotten_batch_does_not_lift_the_refusal() {
    let mut state = ProducerState::keeping_cuts();
    pieces(&mut state, 3, 0, 3, &[(0, 0), (1, 1)]);
    state.observe(2, RecordMark::None, 0);
    state.prune(3);
    pieces(&mut state, 3, 0, 3, &[(2, 3)]);
    assert_eq!(state.classify(3, 0), ProducerSequence::Expired);
}

/// A version 3 snapshot has cut batches and no forgotten ones.
#[test]
fn a_version_3_snapshot_reads_with_nothing_forgotten() {
    let mut state = ProducerState::keeping_cuts();
    pieces(&mut state, 3, 0, 3, &[(0, 0), (1, 1)]);
    state.observe(2, RecordMark::None, 0);
    let current = encode(&state, 3);
    // With no producers the forgotten, evicted and carried counts are the
    // last twelve bytes.
    let body = &current[SNAPSHOT_HEADER_LEN..current.len() - 12];
    let mut bytes = current[..4].to_vec();
    bytes.extend_from_slice(&3u16.to_be_bytes());
    bytes.extend_from_slice(&current[6..20]);
    bytes.extend_from_slice(&crate::segment::format::crc32(&[body]).to_be_bytes());
    bytes.extend_from_slice(body);

    let (mut read, as_of) = decode(&bytes).expect("a version 3 snapshot decodes");
    read.keeps_cuts = true;
    assert_eq!(as_of, 3);
    assert_eq!(read, state);
    assert!(read.forgotten().is_empty());
    assert!(
        read.unfinished(3, 0)
            .is_some_and(|cut| cut.first == Some(0))
    );
}

/// Forgotten batches round trip through the snapshot and through their own
/// file, and a damaged file is refused.
#[test]
fn forgotten_batches_round_trip() {
    let mut state = ProducerState::keeping_cuts();
    pieces(&mut state, 3, 4, 3, &[(0, 0), (1, 1)]);
    state.observe(2, RecordMark::None, 0);
    batch(&mut state, 5, 0, 3, 2);
    state.prune(3);
    assert_eq!(state.forgotten().get(&3), Some(&4));

    let bytes = encode(&state, 5);
    assert_eq!(&bytes[4..6], &5u16.to_be_bytes());
    let (mut read, as_of) = decode(&bytes).expect("decodes");
    read.keeps_cuts = true;
    assert_eq!((read, as_of), (state.clone(), 5));

    let mut file = encode_forgotten(state.forgotten());
    assert_eq!(decode_forgotten(&file).as_ref(), Some(state.forgotten()));
    *file.last_mut().expect("byte") ^= 1;
    assert_eq!(decode_forgotten(&file), None);

    // The file also keeps the carried batches and the installed base, and
    // one written before them carries every batch it forgot.
    state.install(9, ForgottenCuts([(4, 1)].into_iter().collect()));
    let saved = state.saved();
    assert_eq!(saved.carried.get(&4), Some(&1));
    let file = encode_saved(&saved);
    assert_eq!(decode_saved(&file), Some(saved.clone()));
    let old = decode_saved(&encode_forgotten(&saved.forgotten)).expect("decodes");
    assert_eq!(old.carried, saved.forgotten);
    assert!(old.installed.is_none());
    assert_eq!(old.forgotten, saved.forgotten);
}

/// An open batch whose records a purge removed is refused the same way.
#[test]
fn a_purge_past_an_open_batch_refuses_its_resend() {
    let mut state = ProducerState::keeping_cuts();
    pieces(&mut state, 3, 0, 3, &[(0, 0)]);
    assert!(state.is_open());
    state.prune(1);
    assert!(!state.is_open());
    assert_refused(&state, 3, 0);
}

/// The same for a batch the log holds whole but for its first record: its
/// re-send must not be written again while pieces of it are still read.
#[test]
fn a_finished_batch_outlives_its_first_record() {
    let mut state = ProducerState::keeping_cuts();
    pieces(&mut state, 3, 0, 3, &[(0, 1), (1, 2)]);
    state.observe(3, RecordMark::None, 0);
    pieces(&mut state, 3, 0, 3, &[(2, 4)]);
    assert!(matches!(
        state.classify(3, 0),
        ProducerSequence::Held {
            first: 1,
            last: 4,
            ..
        }
    ));

    state.prune(2);
    let headless = UnfinishedBatch {
        first: None,
        held: 3,
        len: 3,
        digest: None,
    };
    assert_eq!(state.unfinished(3, 0), Some(headless));
    let mut suffix = ProducerState::keeping_cuts();
    pieces(&mut suffix, 3, 0, 3, &[(1, 2)]);
    suffix.observe(3, RecordMark::None, 0);
    pieces(&mut suffix, 3, 0, 3, &[(2, 4)]);
    assert_eq!(state, suffix);

    state.prune(5);
    assert_refused(&state, 3, 0);
}

/// Pieces that do not follow one another leave a batch nothing can finish:
/// a record of it is missing in the middle.
#[test]
fn a_batch_missing_a_piece_cannot_be_finished() {
    let mut state = ProducerState::keeping_cuts();
    pieces(&mut state, 3, 0, 4, &[(0, 0), (2, 1)]);
    assert_eq!(
        state.unfinished(3, 0),
        Some(UnfinishedBatch {
            first: None,
            held: 3,
            len: 4,
            digest: None,
        })
    );
    pieces(&mut state, 3, 0, 4, &[(3, 2)]);
    assert_eq!(state.unfinished(3, 0).map(|batch| batch.first), Some(None));
    assert_eq!(state.classify(3, 0), ProducerSequence::Unknown);
}

/// A producer that opens a batch again has given up on its cut one.
#[test]
fn a_producer_that_starts_over_abandons_its_cut_batch() {
    let mut state = ProducerState::keeping_cuts();
    pieces(&mut state, 3, 0, 3, &[(0, 0), (1, 1)]);
    state.observe(2, RecordMark::None, 0);
    batch(&mut state, 3, 0, 3, 1);
    assert_eq!(state.unfinished(3, 0), None);
    assert_eq!(state.classify(3, 0), held(3, 0, 3, 3));
}

/// Past the bound the oldest cut batch is kept only by name, and its
/// re-send is refused. Read as the producer's next batch, or as a new
/// producer's first, it would be written whole after the start that is
/// still in the log.
#[test]
fn past_the_bound_the_oldest_cut_batch_is_still_refused() {
    let mut state = ProducerState::keeping_cuts();
    batch(&mut state, 0, 0, 0, 1);
    let mut offset = 1;
    for producer in 0..MAX_CUTS as u64 {
        pieces(&mut state, producer, 1, 2, &[(0, offset)]);
        state.observe(offset + 1, RecordMark::None, 0);
        offset += 2;
    }
    assert!(state.unfinished(0, 1).is_some());
    assert_eq!(state.classify(0, 1), ProducerSequence::Next);

    pieces(&mut state, MAX_CUTS as u64, 1, 2, &[(0, offset)]);
    state.observe(offset + 1, RecordMark::None, 0);
    assert_refused(&state, 0, 1);
    assert_eq!(state.classify(0, 0), ProducerSequence::Expired);
    assert!(state.unfinished(1, 1).is_some());
}

/// The same for a first batch: a producer the cap forgot is not taken as
/// new at sequence 0.
#[test]
fn past_the_bound_a_cut_first_batch_is_still_refused() {
    let mut state = ProducerState::keeping_cuts();
    let mut offset = 0;
    for producer in 0..=MAX_CUTS as u64 {
        pieces(&mut state, producer, 0, 2, &[(0, offset)]);
        state.observe(offset + 1, RecordMark::None, 0);
        offset += 2;
    }
    assert_refused(&state, 0, 0);
    assert!(
        state
            .unfinished(1, 0)
            .is_some_and(|cut| cut.first.is_some())
    );
}

/// Off a Raft log nothing finishes a cut batch, so none is kept.
#[test]
fn a_log_that_writes_no_pieces_keeps_no_cut_batch() {
    let mut state = ProducerState::default();
    pieces(&mut state, 3, 0, 3, &[(0, 0)]);
    state.observe(1, RecordMark::None, 0);
    assert_eq!(state, ProducerState::default());
}

/// A snapshot written before cut batches were kept still opens without a
/// rescan: its batches are one run each, and nothing in it is cut.
#[test]
fn a_version_2_snapshot_reads_as_batches_in_one_run() {
    let digest = |body: &mut Vec<u8>, bits: u64| {
        body.push(1);
        body.extend_from_slice(&bits.to_be_bytes());
    };
    let mut body = vec![1u8];
    body.extend_from_slice(&1u64.to_be_bytes()); // producer_id
    body.extend_from_slice(&1u64.to_be_bytes()); // sequence
    body.extend_from_slice(&3u32.to_be_bytes()); // len
    body.extend_from_slice(&5u64.to_be_bytes()); // first
    body.extend_from_slice(&2u32.to_be_bytes()); // held
    digest(&mut body, 11);
    body.extend_from_slice(&7u64.to_be_bytes()); // producer id
    body.extend_from_slice(&0u64.to_be_bytes()); // last_sequence
    body.extend_from_slice(&1u16.to_be_bytes()); // batches
    body.extend_from_slice(&2u64.to_be_bytes()); // first
    body.extend_from_slice(&3u32.to_be_bytes()); // len
    digest(&mut body, 12);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&SNAPSHOT_MAGIC.to_be_bytes());
    bytes.extend_from_slice(&2u16.to_be_bytes());
    bytes.extend_from_slice(&0u16.to_be_bytes());
    bytes.extend_from_slice(&7u64.to_be_bytes()); // as_of
    bytes.extend_from_slice(&1u32.to_be_bytes()); // producers
    bytes.extend_from_slice(&crate::segment::format::crc32(&[&body]).to_be_bytes());
    bytes.extend_from_slice(&body);

    let (mut state, as_of) = decode(&bytes).expect("a version 2 snapshot decodes");
    assert_eq!(as_of, 7);
    assert_eq!(
        state.classify(7, 0),
        ProducerSequence::Held {
            first: 2,
            last: 4,
            digest: Some(PayloadDigest::from_bits(12)),
        }
    );
    assert_eq!(
        state.classify(1, 1),
        ProducerSequence::Partial {
            first: 5,
            held: 2,
            len: 3,
            digest: Some(PayloadDigest::from_bits(11)),
        }
    );
    // Its open batch continues where the old layout said it would.
    state.observe(7, RecordMark::Continues, 0);
    assert!(matches!(
        state.classify(1, 1),
        ProducerSequence::Held {
            first: 5,
            last: 7,
            ..
        }
    ));

    let (state, _) = decode(&bytes).expect("decodes");
    let rewritten = encode(&state, as_of);
    assert_eq!(&rewritten[4..6], &SNAPSHOT_VERSION.to_be_bytes());
    assert_eq!(decode(&rewritten), Some((state, as_of)));
}

/// A cut batch evicted past the bound is forgotten at once, but carried only
/// once a purge passes its records: until then a copy that starts below it
/// reads them itself.
#[test]
fn an_evicted_cut_is_carried_once_purged() {
    let mut state = ProducerState::keeping_cuts();
    let mut offset = 0;
    for producer in 0..=MAX_CUTS as u64 {
        pieces(&mut state, producer, 0, 3, &[(0, offset)]);
        state.observe(offset + 1, RecordMark::None, 0);
        offset += 2;
    }
    assert_refused(&state, 0, 0);
    assert!(state.carried().is_empty());
    state.prune(1);
    assert_eq!(state.carried().get(&0), Some(&0));
    assert_eq!(state.carried().len(), 1);
}
