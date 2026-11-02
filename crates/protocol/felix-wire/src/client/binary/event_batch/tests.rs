use bytes::{Bytes, BytesMut};

use crate::binary::EventBatchMeta;
use crate::error::Error;
use crate::{FLAG_BINARY_EVENT_BATCH, FLAG_BINARY_EVENT_BATCH_SHARED, Frame, FrameHeader, binary};

#[test]
fn binary_event_batch_round_trip() {
    let payloads = vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")];
    let encoded = binary::encode_event_batch_bytes(7, &payloads).expect("encode");
    let frame = Frame::decode(encoded).expect("decode");
    assert_eq!(frame.header.flags, FLAG_BINARY_EVENT_BATCH);
    let decoded = binary::decode_event_batch(&frame).expect("decode batch");
    assert_eq!(decoded.subscription_id, 7);
    assert_eq!(decoded.payloads, payloads);
}

#[test]
fn shared_binary_event_batch_round_trip() {
    let payloads = vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")];
    let encoded = binary::encode_shared_event_batch_bytes(&payloads).expect("encode");
    let frame = Frame::decode(encoded).expect("decode");
    assert_eq!(frame.header.flags, FLAG_BINARY_EVENT_BATCH_SHARED);
    let decoded = binary::decode_shared_event_batch(&frame).expect("decode batch");
    assert_eq!(decoded.payloads, payloads);
}

#[test]
fn binary_event_batch_rejects_incomplete_payload() {
    let frame = Frame::new(FLAG_BINARY_EVENT_BATCH, Bytes::from_static(b"short")).expect("frame");
    let err = binary::decode_event_batch(&frame).expect_err("incomplete");
    assert!(matches!(err, Error::Incomplete));
}

#[test]
fn binary_event_batch_parts_match_full_encoding_single() {
    let payloads = vec![Bytes::from_static(b"hello world")];
    let encoded = binary::encode_event_batch_bytes(42, &payloads).expect("encode");
    let parts = binary::encode_event_batch_parts(42, &payloads).expect("parts");

    let mut flattened = BytesMut::with_capacity(parts.frame_len());
    for segment in parts.segments() {
        flattened.extend_from_slice(segment.as_ref());
    }

    assert_eq!(flattened.freeze(), encoded);
}

#[test]
fn binary_event_batch_parts_match_full_encoding_multi_payload() {
    fn payload(seed: u8, len: usize) -> Bytes {
        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            out.push((i as u8).wrapping_mul(31) ^ seed);
        }
        Bytes::from(out)
    }

    for case in 0..16u8 {
        let payloads = vec![
            payload(case, (case as usize) * 7),
            payload(case.wrapping_add(3), 17 + case as usize),
            payload(case.wrapping_add(9), 257 + (case as usize * 13)),
        ];
        let encoded =
            binary::encode_event_batch_bytes(10_000 + case as u64, &payloads).expect("encode");
        let parts =
            binary::encode_event_batch_parts(10_000 + case as u64, &payloads).expect("parts");

        let mut flattened = BytesMut::with_capacity(parts.frame_len());
        for segment in parts.segments() {
            flattened.extend_from_slice(segment.as_ref());
        }
        assert_eq!(flattened.freeze(), encoded);
    }
}

#[test]
fn binary_decode_event_batch_rejects_oversized_payload_count() {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_u64(1); // subscription id
    buf.put_u32(u32::MAX); // count, with no payload bytes following
    let frame = Frame::new(FLAG_BINARY_EVENT_BATCH, buf.freeze()).expect("frame");
    let err = binary::decode_event_batch(&frame).expect_err("oversized count");
    assert!(matches!(err, Error::Incomplete));
}

#[test]
fn binary_decode_shared_event_batch_rejects_oversized_payload_count() {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_u32(u32::MAX); // count, with no payload bytes following
    let frame = Frame::new(FLAG_BINARY_EVENT_BATCH_SHARED, buf.freeze()).expect("frame");
    let err = binary::decode_shared_event_batch(&frame).expect_err("oversized count");
    assert!(matches!(err, Error::Incomplete));
}

// The bound must reject only counts the frame cannot back, never a legitimate
// batch sitting exactly at the limit.
#[test]
fn binary_decode_event_batch_accepts_maximum_supportable_count() {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_u64(7);
    buf.put_u32(3); // three zero-length payloads: 3 * 4 bytes of prefix follow
    for _ in 0..3 {
        buf.put_u32(0);
    }
    let frame = Frame::new(FLAG_BINARY_EVENT_BATCH, buf.freeze()).expect("frame");
    let batch = binary::decode_event_batch(&frame).expect("decode");
    assert_eq!(batch.subscription_id, 7);
    assert_eq!(batch.payloads.len(), 3);
}

#[test]
fn binary_event_batch_empty_payloads() {
    let payloads = vec![];
    let encoded = binary::encode_event_batch_bytes(1, &payloads).expect("encode");
    let frame = Frame::decode(encoded).expect("decode");
    let decoded = binary::decode_event_batch(&frame).expect("decode batch");
    assert_eq!(decoded.subscription_id, 1);
    assert_eq!(decoded.payloads, payloads);
}

#[test]
fn binary_encode_event_batch_large() {
    let payloads: Vec<Bytes> = (0..100)
        .map(|i| Bytes::from(format!("payload{}", i)))
        .collect();
    let result = binary::encode_event_batch_bytes(42, &payloads);
    assert!(result.is_ok());
    let bytes = result.unwrap();
    let frame = Frame::decode(bytes).expect("decode frame");
    let decoded = binary::decode_event_batch(&frame).expect("decode batch");
    assert_eq!(decoded.subscription_id, 42);
    assert_eq!(decoded.payloads.len(), 100);
}

#[test]
fn peek_base_offset_matches_the_decoded_batch() {
    let payloads = vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")];
    let own = Frame::decode(
        binary::encode_event_batch_bytes_with_offset(7, &payloads, 41).expect("encode"),
    )
    .expect("frame");
    assert_eq!(binary::peek_event_batch_base_offset(&own), Some(41));
    let shared = Frame::decode(
        binary::encode_shared_event_batch_bytes_with_offset(&payloads, 42).expect("encode"),
    )
    .expect("frame");
    assert_eq!(binary::peek_event_batch_base_offset(&shared), Some(42));

    // No offsets negotiated, or a short frame: nothing to report.
    let plain = Frame::decode(binary::encode_event_batch_bytes(7, &payloads).expect("encode"))
        .expect("frame");
    assert_eq!(binary::peek_event_batch_base_offset(&plain), None);
    let short = Frame::new(
        FLAG_BINARY_EVENT_BATCH | crate::FLAG_EVENT_BATCH_OFFSETS,
        Bytes::from_static(b"short"),
    )
    .expect("frame");
    assert_eq!(binary::peek_event_batch_base_offset(&short), None);
}

#[test]
fn skip_count_round_trips_on_both_batch_kinds() {
    let payloads = vec![Bytes::from_static(b"after")];

    let frame = Frame::decode(
        binary::encode_event_batch_bytes_with_skip(9, &payloads, 41, 2).expect("encode"),
    )
    .expect("frame");
    assert_eq!(
        frame.header.flags,
        FLAG_BINARY_EVENT_BATCH | crate::FLAG_EVENT_BATCH_OFFSETS | crate::FLAG_EVENT_BATCH_SKIPPED
    );
    let batch = binary::decode_event_batch(&frame).expect("decode");
    assert_eq!(
        (
            batch.subscription_id,
            batch.base_offset,
            batch.skipped_before
        ),
        (9, Some(41), 2)
    );
    assert_eq!(batch.payloads, payloads);
    assert_eq!(binary::peek_event_batch_base_offset(&frame), Some(41));

    let frame = Frame::decode(
        binary::encode_shared_event_batch_bytes_with_skip(&payloads, 41, 2).expect("encode"),
    )
    .expect("frame");
    let batch = binary::decode_shared_event_batch(&frame).expect("decode");
    assert_eq!((batch.base_offset, batch.skipped_before), (Some(41), 2));
    assert_eq!(batch.payloads, payloads);
    assert_eq!(binary::peek_event_batch_base_offset(&frame), Some(41));
}

#[test]
fn a_zero_skip_is_the_offsets_only_frame() {
    // Every batch without a skip must stay byte-identical, so a subscriber
    // that negotiated the bit costs nothing extra on ordinary batches.
    let payloads = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
    assert_eq!(
        binary::encode_event_batch_bytes_with_skip(3, &payloads, 10, 0).expect("encode"),
        binary::encode_event_batch_bytes_with_offset(3, &payloads, 10).expect("encode"),
    );
    assert_eq!(
        binary::encode_shared_event_batch_bytes_with_skip(&payloads, 10, 0).expect("encode"),
        binary::encode_shared_event_batch_bytes_with_offset(&payloads, 10).expect("encode"),
    );
    let frame = Frame::decode(
        binary::encode_shared_event_batch_bytes_with_offset(&payloads, 10).expect("encode"),
    )
    .expect("frame");
    let batch = binary::decode_shared_event_batch(&frame).expect("decode");
    assert_eq!(batch.skipped_before, 0);
}

#[test]
fn a_skip_count_without_offsets_is_rejected() {
    let frame = Frame::new(
        FLAG_BINARY_EVENT_BATCH_SHARED | crate::FLAG_EVENT_BATCH_SKIPPED,
        Bytes::from_static(&[0; 32]),
    )
    .expect("frame");
    assert!(matches!(
        binary::decode_shared_event_batch(&frame),
        Err(Error::UnknownFlags(_))
    ));
}

#[test]
fn a_publisher_rides_either_batch_with_or_without_offsets() {
    let payloads = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
    for base_offset in [None, Some(10)] {
        let meta = binary::EventBatchMeta {
            base_offset,
            skipped_before: 2,
            publisher: Some(b"alice"),
            timestamps: None,
            record_headers: None,
        };
        let frame =
            Frame::decode(binary::encode_event_batch_bytes_with_meta(7, &payloads, meta).unwrap())
                .unwrap();
        assert_ne!(frame.header.flags & crate::FLAG_EVENT_BATCH_PUBLISHER, 0);
        let batch = binary::decode_event_batch(&frame).expect("decode");
        assert_eq!(batch.subscription_id, 7);
        assert_eq!(batch.base_offset, base_offset);
        assert_eq!(batch.skipped_before, base_offset.map_or(0, |_| 2));
        assert_eq!(batch.publisher.as_deref(), Some(&b"alice"[..]));
        assert_eq!(batch.payloads, payloads);

        let frame = Frame::decode(
            binary::encode_shared_event_batch_bytes_with_meta(&payloads, meta).unwrap(),
        )
        .unwrap();
        let batch = binary::decode_shared_event_batch(&frame).expect("decode");
        assert_eq!(batch.base_offset, base_offset);
        assert_eq!(batch.publisher.as_deref(), Some(&b"alice"[..]));
        assert_eq!(batch.payloads, payloads);
        assert_eq!(binary::peek_event_batch_base_offset(&frame), base_offset);
    }
}

/// A batch with no publisher is byte for byte the frame a client that never
/// negotiated the bit gets.
#[test]
fn no_publisher_is_the_frame_without_the_bit() {
    let payloads = vec![Bytes::from_static(b"a")];
    let none = binary::EventBatchMeta::default();
    assert_eq!(
        binary::encode_event_batch_bytes_with_meta(3, &payloads, none).unwrap(),
        binary::encode_event_batch_bytes(3, &payloads).unwrap()
    );
    assert_eq!(
        binary::encode_shared_event_batch_bytes_with_meta(&payloads, none).unwrap(),
        binary::encode_shared_event_batch_bytes(&payloads).unwrap()
    );
    let offsets = binary::EventBatchMeta {
        base_offset: Some(4),
        ..none
    };
    assert_eq!(
        binary::encode_shared_event_batch_bytes_with_meta(&payloads, offsets).unwrap(),
        binary::encode_shared_event_batch_bytes_with_offset(&payloads, 4).unwrap()
    );
}

#[test]
fn a_publisher_past_its_length_is_incomplete() {
    // Flags say a publisher follows; its length claims more than remains.
    let frame = Frame::new(
        FLAG_BINARY_EVENT_BATCH_SHARED | crate::FLAG_EVENT_BATCH_PUBLISHER,
        Bytes::from_static(&[9, b'a', 0, 0, 0, 0]),
    )
    .expect("frame");
    assert!(matches!(
        binary::decode_shared_event_batch(&frame),
        Err(Error::Incomplete)
    ));
    let long = vec![b'p'; binary::MAX_PUBLISHER_BYTES + 1];
    assert!(
        binary::encode_shared_event_batch_bytes_with_meta(
            &[],
            binary::EventBatchMeta {
                publisher: Some(&long),
                ..Default::default()
            }
        )
        .is_err()
    );
}

#[test]
fn timestamps_ride_either_batch_with_every_other_field() {
    let payloads = vec![Bytes::from_static(b"a"), Bytes::from_static(b"bc")];
    let times = [1_700_000_000_000_001u64, 1_700_000_000_000_002];
    for base_offset in [None, Some(10)] {
        let meta = binary::EventBatchMeta {
            base_offset,
            skipped_before: 2,
            publisher: Some(b"alice"),
            timestamps: Some(&times),
            record_headers: None,
        };
        let frame =
            Frame::decode(binary::encode_event_batch_bytes_with_meta(7, &payloads, meta).unwrap())
                .unwrap();
        assert_ne!(frame.header.flags & crate::FLAG_EVENT_BATCH_TIMESTAMPS, 0);
        let batch = binary::decode_event_batch(&frame).expect("decode");
        assert_eq!(batch.base_offset, base_offset);
        assert_eq!(batch.publisher.as_deref(), Some(&b"alice"[..]));
        assert_eq!(batch.timestamps.as_deref(), Some(&times[..]));
        assert_eq!(batch.payloads, payloads);

        let frame = Frame::decode(
            binary::encode_shared_event_batch_bytes_with_meta(&payloads, meta).unwrap(),
        )
        .unwrap();
        let batch = binary::decode_shared_event_batch(&frame).expect("decode");
        assert_eq!(batch.timestamps.as_deref(), Some(&times[..]));
        assert_eq!(batch.payloads, payloads);
        assert_eq!(binary::peek_event_batch_base_offset(&frame), base_offset);
    }
}

/// Without timestamps the frame is the one a peer that predates the bit
/// sends and expects, byte for byte. Pinned as literal bytes so a change to
/// the shared encoder cannot move both sides of the comparison at once.
#[test]
fn no_timestamps_is_the_frame_an_older_peer_knows() {
    let payloads = vec![Bytes::from_static(b"hi")];
    let meta = binary::EventBatchMeta {
        base_offset: Some(5),
        publisher: Some(b"p"),
        ..Default::default()
    };
    let encoded = binary::encode_event_batch_bytes_with_meta(3, &payloads, meta).unwrap();
    let mut expected = BytesMut::new();
    crate::FrameHeader::new(
        FLAG_BINARY_EVENT_BATCH
            | crate::FLAG_EVENT_BATCH_OFFSETS
            | crate::FLAG_EVENT_BATCH_PUBLISHER,
        8 + 8 + 2 + 4 + 4 + 2,
    )
    .encode(&mut expected);
    expected.extend_from_slice(&3u64.to_be_bytes());
    expected.extend_from_slice(&5u64.to_be_bytes());
    expected.extend_from_slice(&[1, b'p']);
    expected.extend_from_slice(&1u32.to_be_bytes());
    expected.extend_from_slice(&2u32.to_be_bytes());
    expected.extend_from_slice(b"hi");
    assert_eq!(encoded, expected.freeze());
    let frame = Frame::decode(encoded).unwrap();
    assert_eq!(binary::decode_event_batch(&frame).unwrap().timestamps, None);

    assert_eq!(
        binary::encode_shared_event_batch_bytes_with_meta(&payloads, Default::default()).unwrap(),
        binary::encode_shared_event_batch_bytes(&payloads).unwrap()
    );
}

#[test]
fn timestamps_must_match_the_payloads() {
    let payloads = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
    let meta = binary::EventBatchMeta {
        timestamps: Some(&[1]),
        ..Default::default()
    };
    assert!(binary::encode_shared_event_batch_bytes_with_meta(&payloads, meta).is_err());
    // Flags say a time precedes each payload; the frame stops inside it.
    let frame = Frame::new(
        FLAG_BINARY_EVENT_BATCH_SHARED | crate::FLAG_EVENT_BATCH_TIMESTAMPS,
        Bytes::from_static(&[0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]),
    )
    .expect("frame");
    assert!(matches!(
        binary::decode_shared_event_batch(&frame),
        Err(Error::Incomplete)
    ));
}

/// No key, one header `a: 1`.
const BLOCK: &[u8] = &[
    0x00, // flags
    0x00, 0x00, 0x00, 0x01, // count
    0x00, 0x01, b'a', 0x00, 0x00, 0x00, 0x01, b'1', // a: 1
];

fn block() -> Bytes {
    Bytes::from_static(BLOCK)
}

fn golden_event_frame() -> Vec<u8> {
    [
        &[
            0x46, 0x4C, 0x58, 0x31, // magic
            0x00, 0x01, // version
            0xC0, 0x22, // EXTENDED | TIMESTAMPS | OFFSETS | EVENT_BATCH
            0x00, 0x00, 0x00, 0x48, // length
            0x00, 0x00, 0x00, 0x01, // extension word: EXT_RECORD_HEADERS
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, // subscription 5
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, // base offset 9
            0x00, 0x00, 0x00, 0x02, // count
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // event 0: timestamp
            0x00, 0x00, 0x00, 0x0D, // event 0: block length
        ][..],
        BLOCK,
        &[
            0x00, 0x00, 0x00, 0x01, b'e', // event 0: payload
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, // event 1: timestamp
            0x00, 0x00, 0x00, 0x00, // event 1: no block
            0x00, 0x00, 0x00, 0x02, b'f', b'g', // event 1: payload
        ],
    ]
    .concat()
}

fn golden_meta<'a>(timestamps: &'a [u64], headers: &'a [Option<Bytes>]) -> EventBatchMeta<'a> {
    EventBatchMeta {
        base_offset: Some(9),
        timestamps: Some(timestamps),
        record_headers: Some(headers),
        ..EventBatchMeta::default()
    }
}

#[test]
fn an_event_batch_with_offsets_timestamps_and_headers_is_these_bytes() {
    let payloads = [Bytes::from_static(b"e"), Bytes::from_static(b"fg")];
    let timestamps = [0x0102_0304_0506_0708, 0x0A];
    let headers = [Some(block()), None];
    let meta = golden_meta(&timestamps, &headers);

    let bytes = binary::encode_event_batch_bytes_with_meta(5, &payloads, meta).expect("encode");
    assert_eq!(bytes.as_ref(), golden_event_frame());

    let frame = Frame::decode(bytes).expect("frame");
    // The offset is found behind the extension word and the subscription id.
    assert_eq!(binary::peek_event_batch_base_offset(&frame), Some(9));
    let decoded = binary::decode_event_batch(&frame).expect("decode");
    assert_eq!(decoded.subscription_id, 5);
    assert_eq!(decoded.base_offset, Some(9));
    assert_eq!(decoded.timestamps, Some(vec![0x0102_0304_0506_0708, 0x0A]));
    assert_eq!(decoded.payloads, payloads);
    assert_eq!(decoded.record_headers, Some(vec![Some(block()), None]));
}

#[test]
fn the_parts_of_an_event_batch_with_headers_are_its_bytes() {
    let payloads = [Bytes::from_static(b"e"), Bytes::from_static(b"fg")];
    let timestamps = [0x0102_0304_0506_0708, 0x0A];
    let headers = [Some(block()), None];
    let meta = golden_meta(&timestamps, &headers);

    let parts = binary::encode_event_batch_parts_with_meta(5, &payloads, meta).expect("parts");
    let flat: Vec<u8> = parts.segments().iter().flat_map(|s| s.to_vec()).collect();
    assert_eq!(flat, golden_event_frame());
    assert_eq!(parts.frame_len(), flat.len());
    // The block and the payloads are the caller's bytes, not copies.
    assert!(
        parts
            .segments()
            .iter()
            .any(|s| s.as_ptr() == headers[0].as_ref().expect("block").as_ptr())
    );
    assert!(
        parts
            .segments()
            .iter()
            .any(|s| s.as_ptr() == payloads[0].as_ptr())
    );
}

#[test]
fn parts_without_meta_are_unchanged() {
    let payloads = [Bytes::from_static(b"e"), Bytes::from_static(b"fg")];
    let plain = binary::encode_event_batch_parts(5, &payloads).expect("plain");
    let with = binary::encode_event_batch_parts_with_meta(5, &payloads, EventBatchMeta::default())
        .expect("with");
    assert_eq!(plain.segments(), with.segments());
    assert_eq!(plain.frame_len(), with.frame_len());
}

#[test]
fn a_shared_event_batch_with_headers_is_these_bytes() {
    let payloads = [Bytes::from_static(b"e")];
    let headers = [Some(block())];
    let meta = EventBatchMeta {
        record_headers: Some(&headers),
        ..EventBatchMeta::default()
    };
    let bytes = binary::encode_shared_event_batch_bytes_with_meta(&payloads, meta).expect("encode");
    let expected = [
        &[
            0x46, 0x4C, 0x58, 0x31, // magic
            0x00, 0x01, // version
            0x80, 0x04, // EXTENDED | EVENT_BATCH_SHARED
            0x00, 0x00, 0x00, 0x1E, // length
            0x00, 0x00, 0x00, 0x01, // extension word
            0x00, 0x00, 0x00, 0x01, // count
            0x00, 0x00, 0x00, 0x0D, // block length
        ][..],
        BLOCK,
        &[0x00, 0x00, 0x00, 0x01, b'e'],
    ]
    .concat();
    assert_eq!(bytes.as_ref(), expected);

    let frame = Frame::decode(bytes).expect("frame");
    let decoded = binary::decode_shared_event_batch(&frame).expect("decode");
    assert_eq!(decoded.payloads, payloads);
    assert_eq!(decoded.record_headers, Some(vec![Some(block())]));
    assert_eq!(decoded.timestamps, None);
}

#[test]
fn a_shared_batch_peeks_its_offset_behind_the_extension_word() {
    let payloads = [Bytes::from_static(b"e")];
    let headers = [Some(block())];
    let bytes = binary::encode_shared_event_batch_bytes_with_meta(
        &payloads,
        EventBatchMeta {
            base_offset: Some(77),
            record_headers: Some(&headers),
            ..EventBatchMeta::default()
        },
    )
    .expect("encode");
    let frame = Frame::decode(bytes).expect("frame");
    assert_eq!(binary::peek_event_batch_base_offset(&frame), Some(77));
    assert_eq!(
        binary::decode_shared_event_batch(&frame)
            .expect("decode")
            .base_offset,
        Some(77)
    );
}

#[test]
fn headers_that_are_all_none_leave_the_frame_as_it_was() {
    let payloads = [Bytes::from_static(b"e"), Bytes::from_static(b"fg")];
    let timestamps = [1, 2];
    let none = [None, None];
    for (publisher, skipped) in [(None, 0), (Some(b"alice".as_slice()), 3)] {
        let plain = EventBatchMeta {
            base_offset: Some(9),
            skipped_before: skipped,
            publisher,
            timestamps: Some(&timestamps),
            record_headers: None,
        };
        let with = EventBatchMeta {
            record_headers: Some(&none),
            ..plain
        };
        assert_eq!(
            binary::encode_event_batch_bytes_with_meta(5, &payloads, with).expect("with"),
            binary::encode_event_batch_bytes_with_meta(5, &payloads, plain).expect("plain"),
        );
        assert_eq!(
            binary::encode_shared_event_batch_bytes_with_meta(&payloads, with).expect("with"),
            binary::encode_shared_event_batch_bytes_with_meta(&payloads, plain).expect("plain"),
        );
        let frame = Frame::decode(
            binary::encode_event_batch_bytes_with_meta(5, &payloads, with).expect("with"),
        )
        .expect("frame");
        assert_eq!(frame.header.flags & crate::FLAG_EXTENDED, 0);
        assert_eq!(
            binary::decode_event_batch(&frame)
                .expect("decode")
                .record_headers,
            None
        );
    }
}

#[test]
fn headers_that_do_not_line_up_with_the_payloads_are_refused() {
    let payloads = [Bytes::from_static(b"e"), Bytes::from_static(b"fg")];
    let one = [Some(block())];
    let meta = EventBatchMeta {
        record_headers: Some(&one),
        ..EventBatchMeta::default()
    };
    assert!(matches!(
        binary::encode_event_batch_bytes_with_meta(5, &payloads, meta),
        Err(Error::Incomplete)
    ));
    assert!(matches!(
        binary::encode_event_batch_parts_with_meta(5, &payloads, meta),
        Err(Error::Incomplete)
    ));
    let empty = [Some(Bytes::new()), None];
    let meta = EventBatchMeta {
        record_headers: Some(&empty),
        ..EventBatchMeta::default()
    };
    assert!(matches!(
        binary::encode_shared_event_batch_bytes_with_meta(&payloads, meta),
        Err(Error::InvalidRecordHeaders(_))
    ));
}

fn extended_event(flags: u16, ext: u32, rest: &[u8]) -> Frame {
    let mut payload = ext.to_be_bytes().to_vec();
    payload.extend_from_slice(rest);
    Frame::new(flags | crate::FLAG_EXTENDED, Bytes::from(payload)).expect("frame")
}

#[test]
fn a_bad_extension_word_is_refused_on_both_event_frames() {
    let body = &golden_event_frame()[FrameHeader::LEN + 4..];
    for ext in [0u32, 0x2, 0x8000_0001] {
        let per_subscriber = extended_event(FLAG_BINARY_EVENT_BATCH, ext, body);
        assert!(matches!(
            binary::decode_event_batch(&per_subscriber),
            Err(Error::UnknownExtension(word)) if word == ext
        ));
        let shared = extended_event(FLAG_BINARY_EVENT_BATCH_SHARED, ext, &[0, 0, 0, 0]);
        assert!(matches!(
            binary::decode_shared_event_batch(&shared),
            Err(Error::UnknownExtension(word)) if word == ext
        ));
    }
}

#[test]
fn an_event_block_that_is_cut_short_or_malformed_is_refused() {
    let full = golden_event_frame();
    let flags = FLAG_BINARY_EVENT_BATCH
        | crate::FLAG_EVENT_BATCH_OFFSETS
        | crate::FLAG_EVENT_BATCH_TIMESTAMPS
        | crate::FLAG_EXTENDED;
    // Event 0's block starts after the header (12), ext (4), sub (8), offset
    // (8), count (4), timestamp (8) and block length (4).
    let block_at = FrameHeader::LEN + 4 + 8 + 8 + 4 + 8 + 4;

    for cut in [block_at - 3, block_at + 5, block_at + 12] {
        let frame =
            Frame::new(flags, Bytes::copy_from_slice(&full[FrameHeader::LEN..cut])).expect("frame");
        assert!(binary::decode_event_batch(&frame).is_err(), "cut {cut}");
    }

    let mut reserved = full.clone();
    reserved[block_at] = 0x04;
    let frame = Frame::decode(Bytes::from(reserved)).expect("frame");
    assert!(matches!(
        binary::decode_event_batch(&frame),
        Err(Error::InvalidRecordHeaders(_))
    ));

    let mut bad_name = full.clone();
    bad_name[block_at + 7] = 0xFF;
    let frame = Frame::decode(Bytes::from(bad_name)).expect("frame");
    assert!(matches!(
        binary::decode_event_batch(&frame),
        Err(Error::InvalidUtf8)
    ));

    let mut past_end = full;
    past_end[block_at - 4..block_at].copy_from_slice(&u32::MAX.to_be_bytes());
    let frame = Frame::decode(Bytes::from(past_end)).expect("frame");
    assert!(matches!(
        binary::decode_event_batch(&frame),
        Err(Error::Incomplete)
    ));
}

#[test]
fn an_event_block_the_decoder_would_refuse_is_not_encoded() {
    let payloads = [Bytes::from_static(b"e")];
    for bad in [
        &[0x02u8, 0, 0, 0, 0][..],
        &[0u8, 0, 0, 0, 5][..],
        &[0u8; 5][..],
    ] {
        let blocks = [Some(Bytes::copy_from_slice(bad))];
        let meta = EventBatchMeta {
            record_headers: Some(&blocks),
            ..EventBatchMeta::default()
        };
        assert!(binary::encode_event_batch_bytes_with_meta(1, &payloads, meta).is_err());
        assert!(binary::encode_event_batch_parts_with_meta(1, &payloads, meta).is_err());
        assert!(binary::encode_shared_event_batch_bytes_with_meta(&payloads, meta).is_err());
    }
}
