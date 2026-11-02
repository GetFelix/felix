use bytes::{BufMut, Bytes, BytesMut};

use crate::error::Error;
use crate::{FLAG_BINARY_PUBLISH_BATCH, FLAG_BINARY_PUBLISH_KEYED, Frame, FrameHeader, binary};

#[test]
fn binary_publish_batch_round_trip() {
    let payloads = vec![b"one".to_vec(), b"two".to_vec()];
    let frame = binary::encode_publish_batch("t1", "default", "orders", &payloads).expect("encode");
    assert_eq!(frame.header.flags, FLAG_BINARY_PUBLISH_BATCH);
    let decoded = binary::decode_publish_batch(&frame).expect("decode");
    assert_eq!(decoded.tenant_id, "t1");
    assert_eq!(decoded.namespace, "default");
    assert_eq!(decoded.stream, "orders");
    assert_eq!(decoded.payloads, payloads);
}

// Decoded records point into the frame's payload instead of being copied out.
#[test]
fn decoded_payloads_are_slices_of_the_frame() {
    let payloads = vec![b"first".to_vec(), Vec::new(), vec![7u8; 4096]];
    let frame = binary::encode_publish_batch("t1", "default", "orders", &payloads).expect("encode");
    let frame_range = frame.payload.as_ptr_range();
    let decoded = binary::decode_publish_batch(&frame).expect("decode");
    assert_eq!(decoded.payloads, payloads);
    for payload in decoded.payloads.iter().filter(|p| !p.is_empty()) {
        let range = payload.as_ptr_range();
        assert!(
            frame_range.start <= range.start && range.end <= frame_range.end,
            "payload of {} bytes was copied out of the frame",
            payload.len()
        );
    }
}

#[test]
fn keyed_publish_batch_round_trip() {
    let payloads = vec![b"one".to_vec(), b"two".to_vec()];
    let frame =
        binary::encode_publish_batch_keyed(Some(b"orders-7"), "t1", "default", "orders", &payloads)
            .expect("encode");
    assert_eq!(
        frame.header.flags,
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_KEYED
    );
    let decoded = binary::decode_publish_batch(&frame).expect("decode");
    assert_eq!(decoded.key.as_deref(), Some(b"orders-7".as_slice()));
    assert_eq!(decoded.tenant_id, "t1");
    assert_eq!(decoded.namespace, "default");
    assert_eq!(decoded.stream, "orders");
    assert_eq!(decoded.payloads, payloads);
}

// An empty key is a key: it hashes to a shard like any other, which is not what
// an unkeyed publish does.
#[test]
fn an_empty_key_is_not_an_absent_key() {
    let payloads = vec![b"one".to_vec()];
    let keyed = binary::encode_publish_batch_keyed(Some(b""), "t1", "default", "orders", &payloads)
        .expect("encode");
    assert_eq!(
        binary::decode_publish_batch(&keyed).expect("decode").key,
        Some(Bytes::new())
    );
    let unkeyed =
        binary::encode_publish_batch("t1", "default", "orders", &payloads).expect("encode");
    assert_eq!(
        binary::decode_publish_batch(&unkeyed).expect("decode").key,
        None
    );
}

// The keyed bit changes where the body starts, so a decoder that ignored it
// would read the key length as a tenant length. This is the frame that catches
// that: the same bytes, read both ways.
#[test]
fn the_keyed_bit_is_what_shifts_the_body() {
    let payloads = vec![b"one".to_vec()];
    let keyed =
        binary::encode_publish_batch_keyed(Some(b"k1"), "t1", "default", "orders", &payloads)
            .expect("encode");
    let as_unkeyed = Frame::new(FLAG_BINARY_PUBLISH_BATCH, keyed.payload.clone()).expect("frame");
    let misread = binary::decode_publish_batch(&as_unkeyed);
    assert!(
        misread.is_err() || misread.expect("decoded").tenant_id != "t1",
        "the key prefix must not parse as a tenant id"
    );
}

#[test]
fn keyed_publish_batch_rejects_truncated_key() {
    // A key length of four with only two bytes behind it.
    let mut buf = BytesMut::new();
    buf.put_u16(4);
    buf.extend_from_slice(b"ab");
    let frame = Frame::new(
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_KEYED,
        buf.freeze(),
    )
    .expect("frame");
    assert!(matches!(
        binary::decode_publish_batch(&frame).expect_err("truncated key"),
        Error::Incomplete
    ));
}

#[test]
fn binary_publish_batch_rejects_incomplete_payload() {
    let frame = Frame::new(FLAG_BINARY_PUBLISH_BATCH, Bytes::from_static(b"\x00")).expect("frame");
    let err = binary::decode_publish_batch(&frame).expect_err("incomplete");
    assert!(matches!(err, Error::Incomplete));
}

#[test]
fn binary_decode_publish_batch_invalid_utf8_tenant() {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_u16(2); // tenant_id len
    buf.extend_from_slice(&[0xFF, 0xFE]); // Invalid UTF-8
    let frame = Frame::new(FLAG_BINARY_PUBLISH_BATCH, buf.freeze()).expect("frame");
    let result = binary::decode_publish_batch(&frame);
    assert!(result.is_err());
}

#[test]
fn binary_decode_publish_batch_invalid_utf8_namespace() {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_u16(2); // tenant_id len
    buf.extend_from_slice(b"t1");
    buf.put_u16(2); // namespace len
    buf.extend_from_slice(&[0xFF, 0xFE]); // Invalid UTF-8
    let frame = Frame::new(FLAG_BINARY_PUBLISH_BATCH, buf.freeze()).expect("frame");
    let result = binary::decode_publish_batch(&frame);
    assert!(result.is_err());
}

#[test]
fn binary_decode_publish_batch_invalid_utf8_stream() {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_u16(2); // tenant_id len
    buf.extend_from_slice(b"t1");
    buf.put_u16(2); // namespace len
    buf.extend_from_slice(b"ns");
    buf.put_u16(2); // stream len
    buf.extend_from_slice(&[0xFF, 0xFE]); // Invalid UTF-8
    let frame = Frame::new(FLAG_BINARY_PUBLISH_BATCH, buf.freeze()).expect("frame");
    let result = binary::decode_publish_batch(&frame);
    assert!(result.is_err());
}

// A declared payload count is attacker-controlled and was previously passed
// straight to `Vec::with_capacity`. These frames are ~20 bytes but claim
// `u32::MAX` payloads; before the bound each reserved 95-127 GiB of address
// space, and enough concurrent ones turn that into an abort. See
// `checked_payload_count`.
#[test]
fn binary_decode_publish_batch_rejects_oversized_payload_count() {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_u16(2); // tenant_id len
    buf.extend_from_slice(b"t1");
    buf.put_u16(2); // namespace len
    buf.extend_from_slice(b"ns");
    buf.put_u16(2); // stream len
    buf.extend_from_slice(b"st");
    buf.put_u32(u32::MAX); // count, with no payload bytes following
    let frame = Frame::new(FLAG_BINARY_PUBLISH_BATCH, buf.freeze()).expect("frame");
    let err = binary::decode_publish_batch(&frame).expect_err("oversized count");
    assert!(matches!(err, Error::Incomplete));
}

#[test]
fn binary_publish_batch_empty_payloads() {
    let payloads = vec![];
    let frame = binary::encode_publish_batch("t1", "default", "orders", &payloads).expect("encode");
    let decoded = binary::decode_publish_batch(&frame).expect("decode");
    assert_eq!(decoded.payloads, payloads);
}

#[test]
fn binary_encode_publish_batch_stats() {
    let payloads = vec![
        b"payload1".to_vec(),
        b"payload2".to_vec(),
        b"payload3".to_vec(),
    ];
    let result = binary::encode_publish_batch_bytes_with_stats("t1", "ns", "stream", &payloads);
    assert!(result.is_ok());
    let (bytes, stats) = result.unwrap();
    assert!(!bytes.is_empty());
    // Stats should be valid (reallocs is a usize, so always >= 0)
    let _ = stats.reallocs;
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

/// A plain batch whose first record has a block and whose second has none.
fn golden_headers_frame() -> Vec<u8> {
    [
        &[
            0x46, 0x4C, 0x58, 0x31, // magic
            0x00, 0x01, // version
            0x80, 0x01, // FLAG_EXTENDED | FLAG_BINARY_PUBLISH_BATCH
            0x00, 0x00, 0x00, 0x32, // length
            0x00, 0x00, 0x00, 0x01, // extension word: EXT_RECORD_HEADERS
            0x00, 0x01, b't', 0x00, 0x01, b'n', 0x00, 0x01, b's', // names
            0x00, 0x00, 0x00, 0x02, // count
            0x00, 0x00, 0x00, 0x0D, // record 0: block length
        ][..],
        BLOCK,
        &[
            0x00, 0x00, 0x00, 0x02, b'p', b'1', // record 0: payload
            0x00, 0x00, 0x00, 0x00, // record 1: no block
            0x00, 0x00, 0x00, 0x02, b'p', b'2', // record 1: payload
        ],
    ]
    .concat()
}

#[test]
fn a_publish_batch_with_headers_is_these_bytes() {
    let payloads = vec![b"p1".to_vec(), b"p2".to_vec()];
    let bytes = binary::encode_publish_batch_bytes_with_headers(
        None,
        None,
        None,
        "t",
        "n",
        "s",
        &payloads,
        &[Some(block()), None],
    )
    .expect("encode");
    assert_eq!(bytes.as_ref(), golden_headers_frame());

    let frame = Frame::decode(bytes).expect("frame");
    let decoded = binary::decode_publish_batch(&frame).expect("decode");
    assert_eq!(decoded.payloads, vec![Bytes::from("p1"), Bytes::from("p2")]);
    assert_eq!(decoded.record_headers, Some(vec![Some(block()), None]));
    assert_eq!(decoded.key, None);
}

#[test]
fn a_keyed_publish_batch_with_headers_round_trips() {
    let payloads = vec![Bytes::from_static(b"p1")];
    let bytes = binary::encode_publish_batch_bytes_with_headers(
        None,
        None,
        Some(b"k"),
        "t",
        "n",
        "s",
        &payloads,
        &[Some(block())],
    )
    .expect("encode");
    let frame = Frame::decode(bytes).expect("frame");
    assert_eq!(
        frame.header.flags,
        crate::FLAG_EXTENDED | FLAG_BINARY_PUBLISH_BATCH | crate::FLAG_BINARY_PUBLISH_KEYED
    );
    let decoded = binary::decode_publish_batch(&frame).expect("decode");
    assert_eq!(decoded.key.as_deref(), Some(b"k".as_slice()));
    assert_eq!(decoded.record_headers, Some(vec![Some(block())]));
}

#[test]
fn a_batch_whose_headers_are_all_none_is_the_plain_frame() {
    let payloads = vec![b"p1".to_vec(), b"p2".to_vec()];
    let none = [None, None];
    for key in [None, Some(b"k".as_slice())] {
        let (plain, _) =
            binary::encode_publish_batch_bytes_with_stats_keyed(key, "t", "n", "s", &payloads)
                .expect("plain");
        let with = binary::encode_publish_batch_bytes_with_headers(
            None, None, key, "t", "n", "s", &payloads, &none,
        )
        .expect("with headers");
        assert_eq!(with, plain);
        let frame = Frame::decode(with).expect("frame");
        assert_eq!(frame.header.flags & crate::FLAG_EXTENDED, 0);
        assert_eq!(
            binary::decode_publish_batch(&frame)
                .expect("decode")
                .record_headers,
            None
        );
    }
}

#[test]
fn headers_that_do_not_line_up_with_the_payloads_are_refused() {
    let payloads = vec![b"p1".to_vec(), b"p2".to_vec()];
    let encode = |headers: &[Option<Bytes>]| {
        binary::encode_publish_batch_bytes_with_headers(
            None, None, None, "t", "n", "s", &payloads, headers,
        )
    };
    assert!(matches!(encode(&[Some(block())]), Err(Error::Incomplete)));
    assert!(matches!(
        encode(&[Some(Bytes::new()), None]),
        Err(Error::InvalidRecordHeaders(_))
    ));
}

#[test]
fn a_plain_decode_of_an_extended_frame_reads_past_the_extension_word() {
    let frame = Frame::new(
        crate::FLAG_EXTENDED | FLAG_BINARY_PUBLISH_BATCH,
        Bytes::from(golden_headers_frame()[FrameHeader::LEN..].to_vec()),
    )
    .expect("frame");
    assert!(binary::decode_publish_batch(&frame).is_ok());
}

fn extended(ext: u32, rest: &[u8]) -> Frame {
    let mut payload = ext.to_be_bytes().to_vec();
    payload.extend_from_slice(rest);
    Frame::new(
        crate::FLAG_EXTENDED | FLAG_BINARY_PUBLISH_BATCH,
        Bytes::from(payload),
    )
    .expect("frame")
}

#[test]
fn an_extension_word_of_zero_is_refused() {
    let body = &golden_headers_frame()[FrameHeader::LEN + 4..];
    assert!(matches!(
        binary::decode_publish_batch(&extended(0, body)),
        Err(Error::UnknownExtension(0))
    ));
}

#[test]
fn an_unknown_extension_bit_is_refused() {
    let body = &golden_headers_frame()[FrameHeader::LEN + 4..];
    for ext in [0x2, 0x8000_0000, 0x3] {
        assert!(
            matches!(
                binary::decode_publish_batch(&extended(ext, body)),
                Err(Error::UnknownExtension(word)) if word == ext
            ),
            "{ext:#x}"
        );
    }
}

#[test]
fn a_truncated_extension_word_is_refused() {
    let frame = Frame::new(
        crate::FLAG_EXTENDED | FLAG_BINARY_PUBLISH_BATCH,
        Bytes::from_static(&[0, 0, 1]),
    )
    .expect("frame");
    assert!(matches!(
        binary::decode_publish_batch(&frame),
        Err(Error::Incomplete)
    ));
}

#[test]
fn a_block_with_a_reserved_flag_bit_is_refused_in_a_frame() {
    let mut bytes = golden_headers_frame();
    // The first byte of record 0's block.
    bytes[FrameHeader::LEN + 4 + 9 + 4 + 4] = 0x02;
    let frame = Frame::decode(Bytes::from(bytes)).expect("frame");
    assert!(matches!(
        binary::decode_publish_batch(&frame),
        Err(Error::InvalidRecordHeaders(_))
    ));
}

#[test]
fn a_block_with_a_bad_utf8_name_is_refused_in_a_frame() {
    let mut bytes = golden_headers_frame();
    // The name byte of record 0's only header.
    bytes[FrameHeader::LEN + 4 + 9 + 4 + 4 + 7] = 0xFF;
    let frame = Frame::decode(Bytes::from(bytes)).expect("frame");
    assert!(matches!(
        binary::decode_publish_batch(&frame),
        Err(Error::InvalidUtf8)
    ));
}

#[test]
fn a_truncated_block_is_refused_in_a_frame() {
    let bytes = golden_headers_frame();
    // Cut the frame inside record 0's block and fix up the header length.
    let cut = FrameHeader::LEN + 4 + 9 + 4 + 4 + 6;
    let frame = Frame::new(
        crate::FLAG_EXTENDED | FLAG_BINARY_PUBLISH_BATCH,
        Bytes::copy_from_slice(&bytes[FrameHeader::LEN..cut]),
    )
    .expect("frame");
    assert!(matches!(
        binary::decode_publish_batch(&frame),
        Err(Error::Incomplete)
    ));
}

#[test]
fn a_block_length_past_the_end_is_refused() {
    let mut bytes = golden_headers_frame();
    let at = FrameHeader::LEN + 4 + 9 + 4;
    bytes[at..at + 4].copy_from_slice(&u32::MAX.to_be_bytes());
    let frame = Frame::decode(Bytes::from(bytes)).expect("frame");
    assert!(matches!(
        binary::decode_publish_batch(&frame),
        Err(Error::Incomplete)
    ));
}

#[test]
fn a_block_that_is_not_a_block_is_refused_even_when_the_frame_adds_up() {
    // One record whose block is a single reserved-flag byte.
    let mut bytes = golden_headers_frame();
    bytes.truncate(FrameHeader::LEN + 4 + 9 + 4);
    bytes.extend_from_slice(&[0, 0, 0, 1, 0x80, 0, 0, 0, 0]);
    // count 2 -> 1
    bytes[FrameHeader::LEN + 4 + 9 + 3] = 1;
    let frame = Frame::new(
        crate::FLAG_EXTENDED | FLAG_BINARY_PUBLISH_BATCH,
        Bytes::copy_from_slice(&bytes[FrameHeader::LEN..]),
    )
    .expect("frame");
    assert!(binary::decode_publish_batch(&frame).is_err());
}

#[test]
fn a_block_the_decoder_would_refuse_is_not_encoded() {
    let payloads = vec![b"p1".to_vec()];
    for bad in [
        &[0x02u8, 0, 0, 0, 0][..],
        &[0u8, 0, 0, 0, 5][..],
        &[0u8; 5][..],
    ] {
        assert!(matches!(
            binary::encode_publish_batch_bytes_with_headers(
                None,
                None,
                None,
                "t",
                "n",
                "s",
                &payloads,
                &[Some(Bytes::copy_from_slice(bad))],
            ),
            Err(Error::InvalidRecordHeaders(_) | Error::Incomplete)
        ));
    }
}
