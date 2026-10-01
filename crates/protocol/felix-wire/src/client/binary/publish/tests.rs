use bytes::{BufMut, Bytes, BytesMut};

use crate::error::Error;
use crate::{FLAG_BINARY_PUBLISH_BATCH, FLAG_BINARY_PUBLISH_KEYED, Frame, binary};

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
