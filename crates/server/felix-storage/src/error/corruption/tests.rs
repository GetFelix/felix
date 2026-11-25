use super::*;

#[test]
fn corruption_display_includes_the_site() {
    let err = Corruption::new(CorruptionKind::RecordChecksum {
        expected: 1,
        found: 2,
    })
    .in_segment("t/ns/s/0", 3)
    .at_position(128);
    let rendered = err.to_string();
    assert!(rendered.contains("shard=t/ns/s/0"), "{rendered}");
    assert!(rendered.contains("segment=3"), "{rendered}");
    assert!(rendered.contains("position=128"), "{rendered}");
}

#[test]
fn at_position_keeps_the_innermost_site() {
    let err = Corruption::new(CorruptionKind::Truncated {
        needed: 8,
        available: 2,
    })
    .at_position(10)
    .at_position(999);
    assert_eq!(err.site.position, Some(10));
}

#[test]
fn corruption_display_without_a_site_has_no_parentheses() {
    let err = Corruption::new(CorruptionKind::IndexVersion { found: 2 });
    assert_eq!(err.to_string(), "unsupported index version 2");
}

/// **A shard alone is shown without a segment or position.**
#[test]
fn a_corruption_in_a_shard_names_only_the_shard() {
    let err = Corruption::new(CorruptionKind::CacheRecordKeyNotUtf8).in_shard("t/ns/c/1");
    assert_eq!(
        err.to_string(),
        "cache record key is not valid UTF-8 (shard=t/ns/c/1)"
    );
}

/// **A segment from a newer build is refused with a reason that says so**,
/// distinct from a version that was never valid.
#[test]
fn a_newer_segment_version_says_a_newer_build_wrote_it() {
    let newer = crate::segment::format::FORMAT_VERSION + 1;
    let message = CorruptionKind::SegmentVersion { found: newer }.to_string();
    assert!(message.contains("written by a newer build"), "{message}");
    assert!(message.contains("refuses to open the log"), "{message}");

    let older = CorruptionKind::SegmentVersion { found: 0 }.to_string();
    assert_eq!(older, "unsupported segment version 0");
}

/// **Each decode failure names the invariant and the values it found**, so a
/// report is actionable without a hex dump.
#[test]
fn every_corruption_kind_names_the_values_it_found() {
    let cases = [
        (CorruptionKind::SegmentMagic { found: 0xdead }, "0x0000dead"),
        (CorruptionKind::SegmentFlags { found: 0x80 }, "0x0080"),
        (
            CorruptionKind::SegmentHeaderChecksum {
                expected: 1,
                found: 2,
            },
            "expected 0x00000001, found 0x00000002",
        ),
        (
            CorruptionKind::IndexMagic { found: 7 },
            "bad index magic 0x00000007",
        ),
        (
            CorruptionKind::RecordFlags { found: 3 },
            "impossible record flags 0x00000003",
        ),
        (
            CorruptionKind::RecordPublisher { body_len: 12 },
            "publisher does not fit its 12-byte body",
        ),
        (
            CorruptionKind::RecordHeaders { body_len: 30 },
            "do not fit its 30-byte body",
        ),
        (
            CorruptionKind::RecordHeaderChecksum {
                expected: 4,
                found: 5,
            },
            "record header checksum mismatch (expected 0x00000004, found 0x00000005)",
        ),
        (
            CorruptionKind::RecordTooLarge {
                payload_len: 900,
                limit: 800,
            },
            "payload 900 exceeds limit 800",
        ),
        (
            CorruptionKind::OffsetOutOfOrder {
                expected: 10,
                found: 12,
            },
            "expected 10, found 12",
        ),
        (
            CorruptionKind::GenerationFell {
                previous: 6,
                found: 5,
            },
            "previous 6, found 5",
        ),
        (
            CorruptionKind::CacheRecordTooShort {
                len: 3,
                header_len: 9,
            },
            "is 3 bytes, shorter than its 9-byte header",
        ),
        (
            CorruptionKind::CacheRecordVersion {
                found: 4,
                expected: 2,
            },
            "version 4 is not readable by this build (reads up to 2)",
        ),
        (
            CorruptionKind::CacheRecordOp { found: 77 },
            "unknown cache record op 77",
        ),
        (
            CorruptionKind::CacheRecordKeyLength {
                claimed: 64,
                available: 10,
            },
            "claims a 64-byte key but carries 10 bytes",
        ),
    ];
    for (kind, needle) in cases {
        let message = kind.to_string();
        assert!(message.contains(needle), "{needle:?} not in {message:?}");
    }
}
