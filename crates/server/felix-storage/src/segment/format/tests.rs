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
    encode_record(&mut buf, 7, 9, b"hi", None, None, &Default::default());
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
    let written = encode_record(
        &mut buf,
        9,
        1234,
        b"payload",
        None,
        None,
        &Default::default(),
    );
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
    encode_record(&mut buf, 0, 0, b"", None, None, &Default::default());
    let (decoded, consumed) = decode_record(&buf).expect("decode");
    assert_eq!(consumed, RECORD_HEADER_LEN);
    assert!(decoded.payload.is_empty());
}

#[test]
fn records_decode_back_to_back() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 1, b"a", None, None, &Default::default());
    encode_record(&mut buf, 1, 2, b"bb", None, None, &Default::default());
    let (first, consumed) = decode_record(&buf).expect("first");
    assert_eq!(first.header.offset, 0);
    let (second, _) = decode_record(&buf[consumed as usize..]).expect("second");
    assert_eq!(second.header.offset, 1);
    assert_eq!(second.payload, Bytes::from_static(b"bb"));
}

#[test]
fn truncation_at_every_boundary_reports_truncated() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 3, 4, b"abcd", None, None, &Default::default());
    for cut in 0..buf.len() {
        let err = decode_record(&buf[..cut]).expect_err("short buffer");
        assert!(err.is_truncation(), "cut {cut} gave {err}");
    }
    assert!(decode_record(&buf).is_ok());
}

#[test]
fn payload_corruption_fails_the_checksum() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 0, b"payload", None, None, &Default::default());
    let last = buf.len() - 1;
    buf[last] ^= 0xFF;
    let err = decode_record(&buf).expect_err("corrupt payload");
    assert!(matches!(err.kind, CorruptionKind::RecordChecksum { .. }));
    assert!(!err.is_truncation());
}

#[test]
fn header_corruption_fails_the_header_checksum() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 0, b"payload", None, None, &Default::default());
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
    encode_record(&mut buf, 4, 5, b"payload", None, None, &Default::default());
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
    // The largest length the flag bits leave room for before v8. From v8
    // bit 26 is a flag, so no length is too large.
    let impossible = (1u32 << 27) - 1;
    let mut buf = vec![0u8; RECORD_HEADER_LEN as usize];
    buf[0..4].copy_from_slice(&impossible.to_be_bytes());
    let header_crc = crc32(&[&buf[0..20]]);
    buf[20..24].copy_from_slice(&header_crc.to_be_bytes());

    for version in OLDEST_READABLE_VERSION..HEADERS_VERSION {
        let err = RecordHeader::decode_in(&buf, version).expect_err("oversized");
        assert!(
            matches!(
                err.kind,
                CorruptionKind::RecordTooLarge {
                    payload_len,
                    limit: MAX_PAYLOAD_BYTES,
                } if payload_len == impossible
            ),
            "v{version}: {err}"
        );
    }
    let header = RecordHeader::decode(&buf).expect("v8");
    assert_eq!(header.payload_len, MAX_PAYLOAD_BYTES - 1);
    assert!(header.has_headers);
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
    encode_record(&mut buf, 0, 0, &payload, None, None, &Default::default());
    let (decoded, _) = decode_record(&buf).expect("decode");
    assert_eq!(decoded.header.payload_len, MAX_PAYLOAD_BYTES);
    assert_eq!(decoded.payload.len(), payload.len());
}

#[test]
fn a_header_alone_locates_the_next_record() {
    let mut buf = Vec::new();
    let written = encode_record(
        &mut buf,
        0,
        0,
        b"some payload",
        None,
        None,
        &Default::default(),
    );
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
    bad[4..6].copy_from_slice(&10u16.to_be_bytes());
    assert!(matches!(
        IndexHeader::decode(&bad).expect_err("version").kind,
        CorruptionKind::IndexVersion { found: 10 }
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
    let first = encode_record(&mut buf, 10, 1, b"a", None, None, &opens);
    assert_eq!(first, RECORD_HEADER_LEN + PRODUCER_TAG_LEN + 1);
    encode_record(&mut buf, 11, 1, b"b", None, None, &RecordMark::Continues);

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
        None,
        None,
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
    encode_record(&mut both, 4, 1, b"x", None, None, &RecordMark::Continues);
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
    let len = encode_record(&mut buf, 9, 1, b"commit", None, None, &RecordMark::Commit);
    assert_eq!(len, RECORD_HEADER_LEN + 6);
    let (decoded, consumed) = decode_record(&buf).expect("decode");
    assert_eq!(
        (decoded.mark, decoded.payload.as_ref(), consumed),
        (RecordMark::Commit, &b"commit"[..], len)
    );

    // Bit 29 with bit 28 is the trim code (see the trim tests below), so
    // pair the commit with bit 30.
    let mut both = buf.clone();
    both[0] |= 0x40;
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

/// The publisher rides at the end of the body, under the record checksum, and
/// combines with any kind.
#[test]
fn a_record_carries_its_publisher_with_any_mark() {
    let opens = RecordMark::Opens(ProducerBatch {
        producer_id: 1,
        sequence: 2,
        len: 1,
    });
    for mark in [RecordMark::None, opens, RecordMark::Commit] {
        let mut buf = Vec::new();
        let len = encode_record(&mut buf, 3, 4, b"event", None, Some(b"alice"), &mark);
        assert_eq!(len, record_len(body_len(5, None, Some(b"alice")), &mark));
        let (decoded, consumed) = decode_record(&buf).expect("decode");
        assert_eq!(consumed, len);
        assert_eq!(decoded.payload.as_ref(), b"event");
        assert_eq!(decoded.publisher.as_deref(), Some(&b"alice"[..]));
        assert_eq!(decoded.mark, mark);
    }

    // An empty publisher is still a publisher: one length byte.
    let mut buf = Vec::new();
    let len = encode_record(&mut buf, 0, 0, b"x", None, Some(b""), &RecordMark::None);
    assert_eq!(len, RECORD_HEADER_LEN + 2);
    let (decoded, _) = decode_record(&buf).expect("decode");
    assert_eq!(decoded.publisher.as_deref(), Some(&b""[..]));
}

/// A record without a publisher is byte for byte what v5 wrote.
#[test]
fn a_record_without_a_publisher_is_unchanged() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 7, 9, b"hi", None, None, &RecordMark::None);
    assert_eq!(buf[0..4], [0x00, 0x00, 0x00, 0x02]);
    let (decoded, _) = decode_record(&buf).expect("decode");
    assert_eq!(decoded.publisher, None);
}

/// A length byte claiming more than the body holds is corruption, never a
/// short publisher.
#[test]
fn a_publisher_longer_than_its_body_is_refused() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 0, b"", None, Some(b"ab"), &RecordMark::None);
    let last = buf.len() - 1;
    buf[last] = 9;
    let crc = crc32(&[&buf[0..24], &buf[RECORD_HEADER_LEN as usize..]]);
    buf[24..28].copy_from_slice(&crc.to_be_bytes());
    assert!(matches!(
        decode_record(&buf).expect_err("misfit").kind,
        CorruptionKind::RecordPublisher { body_len: 3 }
    ));
}

#[test]
fn only_a_v6_segment_holds_publishers() {
    assert!(SegmentHeader::at_version(5, 6, 6).holds_publishers());
    assert!(!SegmentHeader::at_version(5, 6, 5).holds_publishers());
    assert!(SegmentHeader::decode(&SegmentHeader::at_version(5, 6, 6).encode()).is_ok());
}

/// Pinned bytes for a record with a publisher. A change here is a format
/// change: see docs/storage-format.md, "Golden vectors".
#[test]
fn a_record_with_a_publisher_is_a_stable_golden_vector() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 7, 9, b"hi", None, Some(b"al"), &RecordMark::None);
    assert_eq!(
        buf,
        [
            0x08, 0x00, 0x00, 0x05, // body length 5, bit 27
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // offset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, // timestamp
            0x7B, 0x1D, 0x74, 0xDE, // header crc32
            0x51, 0x5E, 0x81, 0x63, // checksum
            0x68, 0x69, // payload "hi"
            0x61, 0x6C, 0x02, // publisher "al", its length
        ]
    );
}

/// Pinned bytes for every kind an older build writes, from before the trim
/// record existed. Adding v7 must not change any of them.
#[test]
fn every_older_record_kind_is_a_stable_golden_vector() {
    let prefix = |kind: u8, len: u8| {
        [
            [kind, 0x00, 0x00, len],
            [0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x07],
            [0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x09],
        ]
        .concat()
    };
    let opens = RecordMark::Opens(ProducerBatch {
        producer_id: 3,
        sequence: 5,
        len: 2,
    });
    type Case = (RecordMark, Option<&'static [u8]>, Vec<u8>);
    let cases: [Case; 5] = [
        (
            opens,
            None,
            [
                prefix(0x80, 0x02),
                vec![0xAD, 0xA5, 0x9E, 0x0B, 0x8A, 0x63, 0xA6, 0xD3],
                vec![0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 2],
                b"hi".to_vec(),
            ]
            .concat(),
        ),
        (
            RecordMark::Continues,
            None,
            [
                prefix(0x40, 0x02),
                vec![0xF3, 0xAC, 0x7E, 0x31, 0x99, 0xEB, 0x39, 0x4A],
                b"hi".to_vec(),
            ]
            .concat(),
        ),
        (
            RecordMark::GenerationStart,
            None,
            [
                prefix(0x20, 0x02),
                vec![0xDC, 0xA8, 0x8E, 0x2C, 0x43, 0x06, 0xD9, 0x7F],
                b"hi".to_vec(),
            ]
            .concat(),
        ),
        (
            RecordMark::Commit,
            None,
            [
                prefix(0x10, 0x02),
                vec![0x26, 0x92, 0x75, 0x02, 0x5A, 0xAD, 0x52, 0xA2],
                b"hi".to_vec(),
            ]
            .concat(),
        ),
        (
            RecordMark::Commit,
            Some(b"al"),
            [
                prefix(0x18, 0x05),
                vec![0x9B, 0xDB, 0xDF, 0xFB, 0x43, 0xA4, 0xD0, 0x2C],
                b"hial\x02".to_vec(),
            ]
            .concat(),
        ),
    ];
    for (mark, publisher, expected) in cases {
        let mut buf = Vec::new();
        encode_record(&mut buf, 7, 9, b"hi", None, publisher, &mark);
        assert_eq!(buf, expected, "{mark:?}");
        let (decoded, _) = decode_record(&buf).expect("decode");
        assert_eq!(decoded.mark, mark);
    }
}

/// The header's version and CRC for every version this build reads.
#[test]
fn every_segment_header_version_is_a_stable_golden_vector() {
    let crcs: [(u16, [u8; 4]); 8] = [
        (2, [0x7A, 0x09, 0xE5, 0xB1]),
        (3, [0xAD, 0xEB, 0x65, 0xE9]),
        (4, [0xE7, 0xD5, 0xEE, 0xA2]),
        (5, [0x30, 0x37, 0x6E, 0xFA]),
        (6, [0x93, 0x61, 0xE8, 0x53]),
        (7, [0x44, 0x83, 0x68, 0x0B]),
        (8, [0x07, 0x1C, 0xFE, 0xC5]),
        (9, [0xD0, 0xFE, 0x7E, 0x9D]),
    ];
    for (version, crc) in crcs {
        let bytes = SegmentHeader::at_version(1, 2, version).encode();
        assert_eq!(bytes[4..6], version.to_be_bytes(), "v{version}");
        assert_eq!(bytes[24..28], crc, "v{version}");
        let decoded = SegmentHeader::decode(&bytes).expect("decodes");
        assert_eq!(decoded.holds_trims(), version >= TRIM_VERSION, "v{version}");
        assert_eq!(
            decoded.holds_generations(),
            version >= GENERATION_VERSION,
            "v{version}"
        );
    }
    let newer = SegmentHeader::at_version(1, 2, FORMAT_VERSION + 1);
    assert!(matches!(
        SegmentHeader::decode(&newer.encode())
            .expect_err("newer")
            .kind,
        CorruptionKind::SegmentVersion { found: 10 }
    ));
}

/// Pinned bytes for a v9 record: the generation sits between the timestamp
/// and the header checksum, which covers it.
#[test]
fn a_v9_record_is_a_stable_golden_vector() {
    let expected = [
        0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0xEC, 0x4D,
        0x6F, 0x2B, 0x01, 0xBF, 0x2C, 0xE8, 0x68, 0x69,
    ];
    let mut buf = Vec::new();
    let len = encode_record_at(
        &mut buf,
        Some(5),
        7,
        9,
        b"hi",
        None,
        None,
        &RecordMark::None,
    );
    assert_eq!(buf, expected);
    assert_eq!(len, GENERATION_HEADER_LEN + 2);
    let (decoded, consumed) = decode_record_in(&buf, GENERATION_VERSION).expect("decode");
    assert_eq!(consumed, len);
    assert_eq!(decoded.header.generation, Some(5));
    assert_eq!(decoded.header.offset, 7);
    assert_eq!(decoded.payload, Bytes::from_static(b"hi"));
}

/// Every kind and body shape round-trips through the v9 layout, and the
/// generation comes back with it.
#[test]
fn every_record_shape_round_trips_at_v9() {
    let marks = [
        RecordMark::None,
        RecordMark::Opens(ProducerBatch {
            producer_id: 3,
            sequence: 4,
            len: 2,
        }),
        RecordMark::Continues,
        RecordMark::GenerationStart,
        RecordMark::Commit,
        RecordMark::Trim { floor: 6 },
    ];
    for mark in marks {
        for headers in [None, Some(&b"k\x00h"[..])] {
            for publisher in [None, Some(&b"alice"[..])] {
                let mut buf = Vec::new();
                let len = encode_record_at(
                    &mut buf,
                    Some(u64::MAX - 1),
                    11,
                    12,
                    b"payload",
                    headers,
                    publisher,
                    &mark,
                );
                let body = body_len(7, headers, publisher);
                assert_eq!(len, record_len_in(body, &mark, GENERATION_VERSION));
                assert_eq!(len, record_len(body, &mark) + 8);
                let (decoded, consumed) =
                    decode_record_in(&buf, GENERATION_VERSION).expect("decode");
                assert_eq!(consumed, len, "{mark:?}");
                assert_eq!(decoded.mark, mark);
                assert_eq!(decoded.header.generation, Some(u64::MAX - 1));
                assert_eq!(decoded.header.encoded_len(), len);
                assert_eq!(decoded.payload, Bytes::from_static(b"payload"));
                assert_eq!(decoded.headers.as_deref(), headers);
                assert_eq!(decoded.publisher.as_deref(), publisher);
            }
        }
    }
}

/// A v8 record read as v9, or a v9 one read as v8, fails its header
/// checksum: the two layouts never misparse as each other.
#[test]
fn the_v8_and_v9_layouts_do_not_read_as_each_other() {
    let mut v8 = Vec::new();
    encode_record(
        &mut v8,
        1,
        2,
        b"some payload",
        None,
        None,
        &RecordMark::None,
    );
    assert!(matches!(
        decode_record_in(&v8, GENERATION_VERSION)
            .expect_err("v8 as v9")
            .kind,
        CorruptionKind::RecordHeaderChecksum { .. }
    ));
    let mut v9 = Vec::new();
    encode_record_at(
        &mut v9,
        Some(3),
        1,
        2,
        b"some payload",
        None,
        None,
        &RecordMark::None,
    );
    assert!(matches!(
        decode_record_in(&v9, HEADERS_VERSION)
            .expect_err("v9 as v8")
            .kind,
        CorruptionKind::RecordHeaderChecksum { .. }
    ));
    assert_eq!(
        decode_record_in(&v8, HEADERS_VERSION)
            .expect("v8")
            .0
            .header
            .generation,
        None
    );
}

/// The generation is inside the header checksum: a flipped bit there is
/// caught before any field is used.
#[test]
fn a_flipped_generation_bit_fails_the_header_checksum() {
    let mut clean = Vec::new();
    encode_record_at(
        &mut clean,
        Some(9),
        1,
        2,
        b"x",
        None,
        None,
        &RecordMark::None,
    );
    for bit in 20 * 8..28 * 8 {
        let mut buf = clean.clone();
        buf[bit / 8] ^= 1 << (bit % 8);
        assert!(
            matches!(
                decode_record_in(&buf, GENERATION_VERSION)
                    .expect_err("flipped")
                    .kind,
                CorruptionKind::RecordHeaderChecksum { .. }
            ),
            "bit {bit}"
        );
    }
}

/// A v9 header cut anywhere, the generation field included, asks for the
/// whole 36 bytes.
#[test]
fn a_short_v9_header_is_truncated() {
    let mut buf = Vec::new();
    encode_record_at(&mut buf, Some(9), 1, 2, b"x", None, None, &RecordMark::None);
    for cut in 0..GENERATION_HEADER_LEN as usize {
        assert!(
            matches!(
                RecordHeader::decode_in(&buf[..cut], GENERATION_VERSION)
                    .expect_err("short")
                    .kind,
                CorruptionKind::Truncated {
                    needed: GENERATION_HEADER_LEN,
                    ..
                }
            ),
            "cut at {cut}"
        );
    }
}

/// A build that reads up to v8 refuses a v9 segment by its header, and says
/// a newer build wrote it.
#[test]
fn a_v8_reader_refuses_a_v9_segment() {
    let bytes = SegmentHeader::at_version(0, 1, GENERATION_VERSION).encode();
    // The check a v8 build makes, with its newest version.
    let v8_reads = OLDEST_READABLE_VERSION..=HEADERS_VERSION;
    let found = u16::from_be_bytes([bytes[4], bytes[5]]);
    assert!(!v8_reads.contains(&found));
    let message = CorruptionKind::SegmentVersion {
        found: FORMAT_VERSION + 1,
    }
    .to_string();
    assert!(message.contains("newer build"), "{message}");
    assert_eq!(
        CorruptionKind::SegmentVersion { found: 1 }.to_string(),
        "unsupported segment version 1"
    );
}

/// Pinned bytes for a trim record: bits 29 and 28, the floor as an 8-byte
/// tag, then the caller's headers as the body.
#[test]
fn a_trim_record_is_a_stable_golden_vector() {
    let mut buf = Vec::new();
    let len = encode_record(
        &mut buf,
        7,
        9,
        b"hi",
        None,
        None,
        &RecordMark::Trim { floor: 5 },
    );
    assert_eq!(len, RECORD_HEADER_LEN + TRIM_TAG_LEN + 2);
    assert_eq!(
        buf,
        [
            0x30, 0x00, 0x00, 0x02, // body length 2, bits 29 and 28
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // offset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, // timestamp
            0x3C, 0x6E, 0x25, 0x09, // header crc32
            0x23, 0xC7, 0x73, 0x0E, // checksum
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, // floor
            0x68, 0x69, // headers
        ]
    );
}

#[test]
fn a_trim_record_round_trips_with_and_without_a_publisher() {
    for publisher in [None, Some(&b"alice"[..])] {
        let mark = RecordMark::Trim { floor: 40 };
        let mut buf = Vec::new();
        let len = encode_record(&mut buf, 41, 3, b"headers", None, publisher, &mark);
        assert_eq!(len, record_len(body_len(7, None, publisher), &mark));
        let (decoded, consumed) = decode_record(&buf).expect("decode");
        assert_eq!(consumed, len);
        assert_eq!(decoded.header.kind, RecordKind::Trim);
        assert_eq!(decoded.header.encoded_len(), len);
        assert_eq!(decoded.mark, mark);
        assert!(decoded.mark.is_trim());
        assert_eq!(decoded.payload.as_ref(), b"headers");
        assert_eq!(decoded.publisher.as_deref(), publisher);
        assert!(decoded.header.check_version(TRIM_VERSION).is_ok());
    }
}

/// The floor is under the record checksum like a producer tag.
#[test]
fn a_damaged_trim_floor_fails_the_checksum() {
    let mut buf = Vec::new();
    encode_record(
        &mut buf,
        4,
        1,
        b"",
        None,
        None,
        &RecordMark::Trim { floor: 2 },
    );
    buf[RECORD_HEADER_LEN as usize + 7] ^= 1;
    assert!(matches!(
        decode_record(&buf).expect_err("damaged floor").kind,
        CorruptionKind::RecordChecksum { .. }
    ));
}

/// Before v7 the trim code was two kinds at once. A segment older than v7
/// holding it reads as `RecordFlags`, exactly as its own build reported it,
/// and every other pair or triple of kind bits is still `RecordFlags`.
#[test]
fn a_trim_is_record_flags_before_v7_and_other_kind_pairs_always_are() {
    let mut buf = Vec::new();
    encode_record(
        &mut buf,
        4,
        1,
        b"x",
        None,
        Some(b"a"),
        &RecordMark::Trim { floor: 2 },
    );
    let header = RecordHeader::decode(&buf).expect("decode");
    for version in OLDEST_READABLE_VERSION..TRIM_VERSION {
        assert!(matches!(
            header.check_version(version).expect_err("older").kind,
            CorruptionKind::RecordFlags { found } if found == read_u32(&buf, 0)
        ));
    }
    assert!(header.check_version(TRIM_VERSION).is_ok());

    for kinds in [0xC0u8, 0xA0, 0x90, 0x60, 0x50, 0x70, 0xB0, 0xF0] {
        let mut both = Vec::new();
        encode_record(&mut both, 4, 1, b"x", None, None, &RecordMark::None);
        both[0] |= kinds;
        let crc = crc32(&[&both[0..20]]);
        both[20..24].copy_from_slice(&crc.to_be_bytes());
        assert!(
            matches!(
                RecordHeader::decode(&both).expect_err("kinds").kind,
                CorruptionKind::RecordFlags { .. }
            ),
            "{kinds:#x}"
        );
    }
}

/// Before v7 a trim's bits are judged before the length, as an older build
/// judged them: with an impossible length they are still `RecordFlags`,
/// which recovery never repairs, and not `RecordTooLarge`, which it may.
#[test]
fn an_oversized_trim_before_v7_is_record_flags_not_too_large() {
    let mut buf = Vec::new();
    encode_record(
        &mut buf,
        4,
        1,
        b"",
        None,
        None,
        &RecordMark::Trim { floor: 2 },
    );
    let oversized = (MAX_PAYLOAD_BYTES + 1) | 0x3000_0000;
    buf[0..4].copy_from_slice(&oversized.to_be_bytes());
    let crc = crc32(&[&buf[0..20]]);
    buf[20..24].copy_from_slice(&crc.to_be_bytes());
    for version in OLDEST_READABLE_VERSION..TRIM_VERSION {
        assert!(
            matches!(
                RecordHeader::decode_in(&buf, version).expect_err("older").kind,
                CorruptionKind::RecordFlags { found } if found == oversized
            ),
            "v{version}"
        );
    }
    assert!(matches!(
        RecordHeader::decode_in(&buf, TRIM_VERSION)
            .expect_err("v7")
            .kind,
        CorruptionKind::RecordTooLarge { .. }
    ));
}

/// Pinned bytes for records with a key and headers: bit 26, then the block's
/// length and the block ahead of the payload. A change here is a format
/// change: see docs/storage-format.md, "Golden vectors".
#[test]
fn a_record_with_headers_is_a_stable_golden_vector() {
    let prefix = |len_and_flags: [u8; 4], crcs: [u8; 8]| {
        [
            &len_and_flags[..],
            &[0, 0, 0, 0, 0, 0, 0, 7],
            &[0, 0, 0, 0, 0, 0, 0, 9],
            &crcs,
        ]
        .concat()
    };
    type Case = (&'static [u8], Option<&'static [u8]>, Vec<u8>);
    let cases: [Case; 3] = [
        (
            b"k",
            None,
            [
                prefix(
                    [0x04, 0x00, 0x00, 0x07],
                    [0x7C, 0x17, 0xD9, 0xF2, 0x7B, 0xA6, 0xA5, 0xA2],
                ),
                b"\0\0\0\x01khi".to_vec(),
            ]
            .concat(),
        ),
        (
            b"k",
            Some(b"al"),
            [
                prefix(
                    [0x0C, 0x00, 0x00, 0x0A],
                    [0xF3, 0x72, 0xAC, 0x52, 0x91, 0xA0, 0x06, 0x9F],
                ),
                b"\0\0\0\x01khial\x02".to_vec(),
            ]
            .concat(),
        ),
        (
            b"",
            None,
            [
                prefix(
                    [0x04, 0x00, 0x00, 0x06],
                    [0xFB, 0xB1, 0x12, 0xB1, 0xEA, 0xC8, 0xB3, 0x3D],
                ),
                b"\0\0\0\0hi".to_vec(),
            ]
            .concat(),
        ),
    ];
    for (headers, publisher, expected) in cases {
        let mut buf = Vec::new();
        let len = encode_record(
            &mut buf,
            7,
            9,
            b"hi",
            Some(headers),
            publisher,
            &RecordMark::None,
        );
        assert_eq!(buf, expected, "{headers:?} {publisher:?}");
        assert_eq!(
            len,
            record_len(body_len(2, Some(headers), publisher), &RecordMark::None)
        );
        let (decoded, _) = decode_record(&buf).expect("decode");
        assert_eq!(decoded.payload, &b"hi"[..]);
        assert_eq!(decoded.headers.as_deref(), Some(headers));
        assert_eq!(decoded.publisher.as_deref(), publisher);
    }
}

/// Headers combine with every mark, and a record without them is read back
/// with none.
#[test]
fn headers_round_trip_with_any_mark() {
    let opens = RecordMark::Opens(ProducerBatch {
        producer_id: 1,
        sequence: 2,
        len: 3,
    });
    for mark in [
        RecordMark::None,
        opens,
        RecordMark::Continues,
        RecordMark::Commit,
        RecordMark::Trim { floor: 4 },
    ] {
        for headers in [None, Some(&b"key and headers"[..])] {
            let mut buf = Vec::new();
            encode_record(&mut buf, 3, 4, b"payload", headers, Some(b"p"), &mark);
            let (decoded, used) = decode_record(&buf).expect("decode");
            assert_eq!(used, buf.len() as u64);
            assert_eq!(decoded.mark, mark);
            assert_eq!(decoded.headers.as_deref(), headers);
            assert_eq!(decoded.payload, &b"payload"[..]);
            assert_eq!(decoded.publisher.as_deref(), Some(&b"p"[..]));
        }
    }
}

/// Bit 26 with no length under it is a 2^26-byte body any older build could
/// write, in any segment, never an empty record with headers.
#[test]
fn a_legacy_max_size_body_is_not_read_as_headers() {
    let payload = vec![0u8; MAX_PAYLOAD_BYTES as usize];
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 0, &payload, None, None, &RecordMark::None);
    assert_eq!(buf[0..4], [0x04, 0x00, 0x00, 0x00]);
    for version in [3, 7, 8] {
        let header = RecordHeader::decode_in(&buf, version).expect("decode");
        assert!(!header.has_headers, "v{version}");
        assert_eq!(header.payload_len, MAX_PAYLOAD_BYTES);
    }
    let (decoded, _) = decode_record(&buf).expect("decode");
    assert_eq!(decoded.headers, None);
    // With headers the body stops a byte short, so the two never meet.
    assert!(body_fits(MAX_PAYLOAD_BYTES as usize, false));
    assert!(!body_fits(MAX_PAYLOAD_BYTES as usize, true));
    assert!(body_fits(MAX_PAYLOAD_BYTES as usize - 1, true));
}

/// Before v8, bit 26 over a non-zero length is the impossible length an older
/// build refused as too large, so recovery judges it the same way.
#[test]
fn headers_before_v8_are_too_large() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 0, b"hi", Some(b"k"), None, &RecordMark::None);
    for version in [3, 6, 7] {
        assert!(
            matches!(
                RecordHeader::decode_in(&buf, version)
                    .expect_err("old")
                    .kind,
                CorruptionKind::RecordTooLarge {
                    payload_len: 0x0400_0007,
                    ..
                }
            ),
            "v{version}"
        );
        let header = RecordHeader::decode(&buf).expect("decode");
        assert!(matches!(
            header.check_version(version).expect_err("old").kind,
            CorruptionKind::RecordTooLarge {
                payload_len: 0x0400_0007,
                ..
            }
        ));
    }
    let header = RecordHeader::decode_in(&buf, HEADERS_VERSION).expect("v8");
    assert!(header.has_headers);
    header
        .check_version(HEADERS_VERSION)
        .expect("v8 holds headers");
}

/// A block length claiming more than the body holds is corruption, never a
/// short payload.
#[test]
fn headers_longer_than_their_body_are_refused() {
    for publisher in [None, Some(&b"al"[..])] {
        let mut buf = Vec::new();
        encode_record(
            &mut buf,
            0,
            0,
            b"hi",
            Some(b"k"),
            publisher,
            &RecordMark::None,
        );
        let at = RECORD_HEADER_LEN as usize;
        buf[at..at + 4].copy_from_slice(&4u32.to_be_bytes());
        let crc = crc32(&[&buf[0..24], &buf[at..]]);
        buf[24..28].copy_from_slice(&crc.to_be_bytes());
        let body_len = (buf.len() - at) as u32;
        assert!(matches!(
            decode_record(&buf).expect_err("misfit").kind,
            CorruptionKind::RecordHeaders { body_len: found } if found == body_len
        ));
    }
}

/// The v9 kinds: bits 30 and 29, 30 and 28, and 30, 29 and 28. Each round
/// trips in a v9 segment with no tag and its body intact.
#[test]
fn the_v9_kinds_round_trip_in_a_v9_segment() {
    for (mark, kind, bits) in [
        (RecordMark::Membership, RecordKind::Membership, 0x60u8),
        (RecordMark::CounterDelta, RecordKind::CounterDelta, 0x50),
        (
            RecordMark::CounterCheckpoint,
            RecordKind::CounterCheckpoint,
            0x70,
        ),
    ] {
        let mut buf = Vec::new();
        let len = encode_record_at(
            &mut buf,
            Some(3),
            4,
            5,
            b"body",
            Some(b"h"),
            Some(b"p"),
            &mark,
        );
        assert_eq!(buf[0] & 0xF0, bits, "{mark:?}");
        assert_eq!(
            len,
            record_len_in(
                body_len(4, Some(b"h"), Some(b"p")),
                &mark,
                GENERATION_VERSION
            )
        );
        let (decoded, used) = decode_record_in(&buf, GENERATION_VERSION).expect("decode");
        assert_eq!(used, len);
        assert_eq!(decoded.header.kind, kind);
        assert_eq!(decoded.header.kind.tag_len(), 0);
        assert_eq!(decoded.header.generation, Some(3));
        assert_eq!(decoded.mark, mark);
        assert_eq!(decoded.payload, Bytes::from_static(b"body"));
        assert_eq!(decoded.headers, Some(Bytes::from_static(b"h")));
        assert_eq!(decoded.publisher, Some(Bytes::from_static(b"p")));
        assert!(decoded.header.check_version(GENERATION_VERSION).is_ok());
    }
}

/// Before v9 the new kinds' bits are `RecordFlags`, as every older build
/// reads them, both when decoding and when checking a decoded header against
/// an older version. The pairs v9 leaves unused stay refused in v9 too.
#[test]
fn the_v9_kinds_are_record_flags_before_v9() {
    for mark in [
        RecordMark::Membership,
        RecordMark::CounterDelta,
        RecordMark::CounterCheckpoint,
    ] {
        let mut buf = Vec::new();
        encode_record(&mut buf, 4, 1, b"x", None, None, &mark);
        for version in OLDEST_READABLE_VERSION..GENERATION_VERSION {
            assert!(
                matches!(
                    RecordHeader::decode_in(&buf, version)
                        .expect_err("older")
                        .kind,
                    CorruptionKind::RecordFlags { .. }
                ),
                "{mark:?} at v{version}"
            );
        }
        let mut v9 = Vec::new();
        encode_record_at(&mut v9, Some(1), 4, 1, b"x", None, None, &mark);
        let header = RecordHeader::decode_in(&v9, GENERATION_VERSION).expect("v9");
        for version in OLDEST_READABLE_VERSION..GENERATION_VERSION {
            assert!(
                matches!(
                    header.check_version(version).expect_err("older").kind,
                    CorruptionKind::RecordFlags { .. }
                ),
                "{mark:?} checked at v{version}"
            );
        }
    }
    for kinds in [0xC0u8, 0xA0, 0x90, 0xB0, 0xD0, 0xE0, 0xF0] {
        let mut buf = Vec::new();
        encode_record_at(&mut buf, Some(1), 4, 1, b"x", None, None, &RecordMark::None);
        buf[0] |= kinds;
        let crc = crc32(&[&buf[0..28]]);
        buf[28..32].copy_from_slice(&crc.to_be_bytes());
        assert!(
            matches!(
                RecordHeader::decode_in(&buf, GENERATION_VERSION)
                    .expect_err("kinds")
                    .kind,
                CorruptionKind::RecordFlags { .. }
            ),
            "{kinds:#x}"
        );
    }
}
