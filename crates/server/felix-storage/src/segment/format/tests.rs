use super::*;

#[test]
fn segment_header_round_trip() {
    let header = SegmentHeader::new(42, 1_700_000_000_000_000);
    let decoded = SegmentHeader::decode(&header.encode()).expect("decode");
    assert_eq!(decoded, header);
}

#[test]
fn segment_header_is_a_stable_golden_vector() {
    // Pinned bytes: a change here is a format change and must bump the
    // version and update docs/storage-format.md.
    let bytes = SegmentHeader::new(1, 2).encode();
    assert_eq!(
        bytes,
        [
            0x46, 0x4C, 0x53, 0x47, // magic "FLSG"
            0x00, 0x03, // version
            0x00, 0x00, // flags
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // base_offset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // created_at_micros
            0xAD, 0xEB, 0x65, 0xE9, // header crc32
            0x00, 0x00, 0x00, 0x00, // reserved
        ]
    );
    // A segment that holds a generation-start record.
    let bytes = SegmentHeader::at_version(1, 2, 4).encode();
    assert_eq!(bytes[4..6], [0x00, 0x04]);
    assert_eq!(bytes[24..28], [0xE7, 0xD5, 0xEE, 0xA2]);
}

#[test]
fn record_is_a_stable_golden_vector() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 7, 9, b"hi", &Default::default());
    assert_eq!(
        buf,
        vec![
            0x00, 0x00, 0x00, 0x02, // payload_len
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // offset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, // timestamp
            0xC6, 0x54, 0xDE, 0x27, // header crc32 (bytes 0..20)
            0x24, 0x02, 0x15, 0x2C, // checksum (bytes 0..24 + payload)
            b'h', b'i',
        ]
    );
}

#[test]
fn record_round_trips_every_field() {
    let mut buf = Vec::new();
    let written = encode_record(&mut buf, 9, 1234, b"payload", &Default::default());
    let (decoded, consumed) = decode_record(&buf).expect("decode");
    assert_eq!(written, consumed);
    assert_eq!(consumed, buf.len() as u64);
    assert_eq!(decoded.header.offset, 9);
    assert_eq!(decoded.header.timestamp_micros, 1234);
    assert_eq!(decoded.header.payload_len, 7);
    assert_eq!(decoded.payload, Bytes::from_static(b"payload"));
}

#[test]
fn zero_length_payload_round_trips() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 0, b"", &Default::default());
    let (decoded, consumed) = decode_record(&buf).expect("decode");
    assert_eq!(consumed, RECORD_HEADER_LEN);
    assert!(decoded.payload.is_empty());
}

#[test]
fn records_decode_back_to_back() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 1, b"a", &Default::default());
    encode_record(&mut buf, 1, 2, b"bb", &Default::default());
    let (first, consumed) = decode_record(&buf).expect("first");
    assert_eq!(first.header.offset, 0);
    let (second, _) = decode_record(&buf[consumed as usize..]).expect("second");
    assert_eq!(second.header.offset, 1);
    assert_eq!(second.payload, Bytes::from_static(b"bb"));
}

#[test]
fn truncation_at_every_boundary_reports_truncated() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 3, 4, b"abcd", &Default::default());
    for cut in 0..buf.len() {
        let err = decode_record(&buf[..cut]).expect_err("short buffer");
        assert!(err.is_truncation(), "cut {cut} gave {err}");
    }
    assert!(decode_record(&buf).is_ok());
}

#[test]
fn payload_corruption_fails_the_checksum() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 0, b"payload", &Default::default());
    let last = buf.len() - 1;
    buf[last] ^= 0xFF;
    let err = decode_record(&buf).expect_err("corrupt payload");
    assert!(matches!(err.kind, CorruptionKind::RecordChecksum { .. }));
    assert!(!err.is_truncation());
}

#[test]
fn header_corruption_fails_the_header_checksum() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 0, b"payload", &Default::default());
    // Flip a bit in the timestamp. The header checksum covers it, so this
    // is caught without reading the payload at all.
    buf[12] ^= 0x01;
    let err = decode_record(&buf).expect_err("corrupt header");
    assert!(matches!(
        err.kind,
        CorruptionKind::RecordHeaderChecksum { .. }
    ));
}

#[test]
fn a_corrupt_length_is_caught_by_the_header_checksum() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 4, 5, b"payload", &Default::default());
    // Damage only the length field. Before the header checksum this was
    // indistinguishable from an unfinished write: the claimed extent ran
    // past the data, so recovery truncated an acknowledged record. Now the
    // header itself reports the damage.
    buf[0..4].copy_from_slice(&9_999u32.to_be_bytes());
    let err = decode_record(&buf).expect_err("corrupt length");
    assert!(
        matches!(err.kind, CorruptionKind::RecordHeaderChecksum { .. }),
        "expected a header checksum failure, got {err}"
    );
}

#[test]
fn oversized_length_is_rejected_before_allocating() {
    // A header whose length is impossible but whose checksum is valid: the
    // shape that would reach an allocation if the bound were not checked.
    // The largest length the flag bits leave room for.
    let impossible = (1u32 << 28) - 1;
    let mut buf = vec![0u8; RECORD_HEADER_LEN as usize];
    buf[0..4].copy_from_slice(&impossible.to_be_bytes());
    let header_crc = crc32(&[&buf[0..20]]);
    buf[20..24].copy_from_slice(&header_crc.to_be_bytes());

    let err = decode_record(&buf).expect_err("oversized");
    assert!(
        matches!(
            err.kind,
            CorruptionKind::RecordTooLarge {
                payload_len,
                limit: MAX_PAYLOAD_BYTES,
            } if payload_len == impossible
        ),
        "got {err}"
    );
}

#[test]
fn a_garbage_header_is_rejected_before_its_length_is_believed() {
    // Random bytes almost never carry a valid header checksum, so the
    // length they happen to encode is never acted on.
    let buf = vec![0u8; RECORD_HEADER_LEN as usize];
    let err = decode_record(&buf).expect_err("garbage");
    assert!(matches!(
        err.kind,
        CorruptionKind::RecordHeaderChecksum { .. }
    ));
}

#[test]
fn max_size_payload_round_trips() {
    let payload = vec![0xA5u8; MAX_PAYLOAD_BYTES as usize];
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 0, &payload, &Default::default());
    let (decoded, _) = decode_record(&buf).expect("decode");
    assert_eq!(decoded.header.payload_len, MAX_PAYLOAD_BYTES);
    assert_eq!(decoded.payload.len(), payload.len());
}

#[test]
fn a_header_alone_locates_the_next_record() {
    let mut buf = Vec::new();
    let written = encode_record(&mut buf, 0, 0, b"some payload", &Default::default());
    // Only the header bytes are available, yet the next position is known
    // without ever touching the payload.
    let header = RecordHeader::decode(&buf[..RECORD_HEADER_LEN as usize]).expect("header");
    assert_eq!(header.encoded_len(), written);
}

#[test]
fn segment_header_rejects_bad_magic_version_and_flags() {
    let good = SegmentHeader::new(0, 0).encode();

    let mut bad = good;
    bad[0] ^= 0xFF;
    assert!(matches!(
        SegmentHeader::decode(&bad).expect_err("magic").kind,
        CorruptionKind::SegmentMagic { .. }
    ));

    let mut bad = good;
    bad[4..6].copy_from_slice(&99u16.to_be_bytes());
    assert!(matches!(
        SegmentHeader::decode(&bad).expect_err("version").kind,
        CorruptionKind::SegmentVersion { found: 99 }
    ));

    let mut bad = good;
    bad[6..8].copy_from_slice(&1u16.to_be_bytes());
    assert!(matches!(
        SegmentHeader::decode(&bad).expect_err("flags").kind,
        CorruptionKind::SegmentFlags { found: 1 }
    ));

    let mut bad = good;
    bad[8] ^= 0xFF;
    assert!(matches!(
        SegmentHeader::decode(&bad).expect_err("checksum").kind,
        CorruptionKind::SegmentHeaderChecksum { .. }
    ));
}

#[test]
fn segment_header_truncation_is_reported_as_truncation() {
    let good = SegmentHeader::new(0, 0).encode();
    for cut in 0..good.len() {
        assert!(
            SegmentHeader::decode(&good[..cut])
                .expect_err("short")
                .is_truncation()
        );
    }
}

#[test]
fn index_header_and_entry_round_trip() {
    let header = IndexHeader { base_offset: 17 };
    assert_eq!(IndexHeader::decode(&header.encode()).expect("hdr"), header);

    let entry = IndexEntry {
        offset: 5,
        position: 64,
    };
    assert_eq!(IndexEntry::decode(&entry.encode()).expect("entry"), entry);
}

#[test]
fn index_header_rejects_bad_magic_and_version() {
    let good = IndexHeader { base_offset: 0 }.encode();
    let mut bad = good;
    bad[0] ^= 0xFF;
    assert!(matches!(
        IndexHeader::decode(&bad).expect_err("magic").kind,
        CorruptionKind::IndexMagic { .. }
    ));
    let mut bad = good;
    bad[4..6].copy_from_slice(&7u16.to_be_bytes());
    assert!(matches!(
        IndexHeader::decode(&bad).expect_err("version").kind,
        CorruptionKind::IndexVersion { found: 7 }
    ));
}

#[test]
fn offset_continuity_is_checked() {
    assert!(check_offset_continuity(4, 4).is_ok());
    let err = check_offset_continuity(4, 6).expect_err("gap");
    assert!(matches!(
        err.kind,
        CorruptionKind::OffsetOutOfOrder {
            expected: 4,
            found: 6
        }
    ));
}

#[test]
fn a_producer_mark_round_trips_and_is_covered_by_the_checksum() {
    let opens = RecordMark::Opens(ProducerBatch {
        producer_id: 7,
        sequence: 3,
        len: 2,
    });
    let mut buf = Vec::new();
    let first = encode_record(&mut buf, 10, 1, b"a", &opens);
    assert_eq!(first, RECORD_HEADER_LEN + PRODUCER_TAG_LEN + 1);
    encode_record(&mut buf, 11, 1, b"b", &RecordMark::Continues);

    let (decoded, consumed) = decode_record(&buf).expect("first");
    assert_eq!(
        (decoded.mark, decoded.payload.as_ref(), consumed),
        (opens, &b"a"[..], first)
    );
    let (decoded, _) = decode_record(&buf[first as usize..]).expect("second");
    assert_eq!(decoded.mark, RecordMark::Continues);

    // A flipped bit in the tag is damage, like one in the payload.
    let mut damaged = buf.clone();
    damaged[RECORD_HEADER_LEN as usize + 3] ^= 1;
    assert!(matches!(
        decode_record(&damaged).expect_err("damaged tag").kind,
        CorruptionKind::RecordChecksum { .. }
    ));
}

#[test]
fn a_v2_segment_header_is_still_read() {
    let mut header = SegmentHeader::new(5, 6);
    header.version = 2;
    let decoded = SegmentHeader::decode(&header.encode()).expect("v2 decodes");
    assert!(!decoded.holds_marks());
    assert!(SegmentHeader::new(5, 6).holds_marks());
}

#[test]
fn a_generation_start_round_trips_and_is_one_kind_only() {
    let mut buf = Vec::new();
    let len = encode_record(
        &mut buf,
        4,
        1,
        &7u64.to_be_bytes(),
        &RecordMark::GenerationStart,
    );
    assert_eq!(len, RECORD_HEADER_LEN + 8);
    let (decoded, consumed) = decode_record(&buf).expect("decode");
    assert_eq!(
        (decoded.mark, decoded.payload.as_ref(), consumed),
        (RecordMark::GenerationStart, &7u64.to_be_bytes()[..], len)
    );

    // Two kinds at once is not a record this build wrote.
    let mut both = Vec::new();
    encode_record(&mut both, 4, 1, b"x", &RecordMark::Continues);
    both[0] |= 0x20;
    let crc = crc32(&[&both[0..20]]);
    both[20..24].copy_from_slice(&crc.to_be_bytes());
    assert!(matches!(
        decode_record(&both).expect_err("two kinds").kind,
        CorruptionKind::RecordFlags { .. }
    ));
}

#[test]
fn a_commit_record_round_trips_and_is_one_kind_only() {
    let mut buf = Vec::new();
    let len = encode_record(&mut buf, 9, 1, b"commit", &RecordMark::Commit);
    assert_eq!(len, RECORD_HEADER_LEN + 6);
    let (decoded, consumed) = decode_record(&buf).expect("decode");
    assert_eq!(
        (decoded.mark, decoded.payload.as_ref(), consumed),
        (RecordMark::Commit, &b"commit"[..], len)
    );

    let mut both = buf.clone();
    both[0] |= 0x20;
    let crc = crc32(&[&both[0..20]]);
    both[20..24].copy_from_slice(&crc.to_be_bytes());
    assert!(matches!(
        decode_record(&both).expect_err("two kinds").kind,
        CorruptionKind::RecordFlags { .. }
    ));
}

/// A v4 build refuses a v5 segment outright, rather than reading the commit
/// flag as part of an impossible length. Only a segment that holds a commit
/// is written at v5.
#[test]
fn only_a_v5_segment_holds_commits() {
    assert!(SegmentHeader::at_version(5, 6, 5).holds_commits());
    assert!(!SegmentHeader::at_version(5, 6, 4).holds_commits());
    assert!(SegmentHeader::decode(&SegmentHeader::at_version(5, 6, 5).encode()).is_ok());
}

#[test]
fn only_a_v4_segment_holds_generation_starts() {
    assert!(SegmentHeader::at_version(5, 6, 4).holds_generation_starts());
    let header = SegmentHeader::new(5, 6);
    let decoded = SegmentHeader::decode(&header.encode()).expect("v3 decodes");
    assert!(decoded.holds_marks());
    assert!(!decoded.holds_generation_starts());
}
