use bytes::{BufMut, Bytes, BytesMut};

use crate::binary::{
    ProducerSequence, decode_acked_publish_batch, decode_publish_batch,
    encode_idempotent_publish_batch_bytes, peek_acked_publish_prefix,
};
use crate::error::Error;
use crate::{
    AckMode, FLAG_BINARY_PUBLISH_ACKED, FLAG_BINARY_PUBLISH_BATCH, FLAG_BINARY_PUBLISH_KEYED,
    Frame, FrameHeader, binary,
};

#[test]
fn acked_keyed_publish_batch_round_trip() {
    let payloads = vec![b"one".to_vec()];
    let bytes = binary::encode_acked_publish_batch_bytes_keyed(
        42,
        AckMode::PerBatch,
        Some(b"orders-7"),
        "t1",
        "default",
        "orders",
        &payloads,
    )
    .expect("encode");
    let frame = Frame::decode(bytes).expect("frame");
    assert_eq!(
        frame.header.flags,
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED | FLAG_BINARY_PUBLISH_KEYED
    );
    // The correlation prefix still reads at offset 0 with a key behind it.
    let (request_id, ack) = binary::peek_acked_publish_prefix(&frame).expect("peek");
    assert_eq!(request_id, 42);
    assert_eq!(ack, AckMode::PerBatch);
    let decoded = binary::decode_acked_publish_batch(&frame).expect("decode");
    assert_eq!(decoded.batch.key.as_deref(), Some(b"orders-7".as_slice()));
    assert_eq!(decoded.batch.payloads, payloads);
}

#[test]
fn acked_publish_batch_round_trip() {
    let payloads = vec![b"one".to_vec(), b"two".to_vec()];
    for ack in [AckMode::PerMessage, AckMode::PerBatch] {
        let bytes =
            binary::encode_acked_publish_batch_bytes(42, ack, "t1", "default", "orders", &payloads)
                .expect("encode");
        let frame = Frame::decode(bytes).expect("frame");
        // Both bits are set: the acked frame is a publish batch that also owes an ack.
        assert_eq!(
            frame.header.flags,
            FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED
        );
        let decoded = binary::decode_acked_publish_batch(&frame).expect("decode");
        assert_eq!(decoded.request_id, 42);
        assert_eq!(decoded.ack, ack);
        assert_eq!(decoded.batch.tenant_id, "t1");
        assert_eq!(decoded.batch.namespace, "default");
        assert_eq!(decoded.batch.stream, "orders");
        assert_eq!(decoded.batch.payloads, payloads);
    }
}

// The prefix must be readable on its own, because that is what lets the broker
// answer a corrupt body with an error the client can still correlate.
#[test]
fn acked_publish_prefix_readable_without_valid_body() {
    let mut buf = BytesMut::new();
    buf.put_u64(7);
    buf.put_u8(2); // PerBatch
    buf.extend_from_slice(b"\xff\xff garbage body");
    let frame = Frame::new(
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED,
        buf.freeze(),
    )
    .expect("frame");
    let (request_id, ack) = binary::peek_acked_publish_prefix(&frame).expect("peek");
    assert_eq!(request_id, 7);
    assert_eq!(ack, AckMode::PerBatch);
    // The body is still garbage, so the full decode must fail.
    assert!(binary::decode_acked_publish_batch(&frame).is_err());
}

#[test]
fn acked_publish_batch_rejects_truncated_prefix() {
    // Eight bytes: a full request_id but no ack-mode byte.
    let frame = Frame::new(
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED,
        Bytes::from_static(b"\x00\x00\x00\x00\x00\x00\x00\x00"),
    )
    .expect("frame");
    assert!(matches!(
        binary::decode_acked_publish_batch(&frame).expect_err("truncated"),
        Error::Incomplete
    ));
}

#[test]
fn acked_publish_batch_rejects_invalid_ack_mode() {
    let mut buf = BytesMut::new();
    buf.put_u64(1);
    buf.put_u8(0); // AckMode::None is not representable in this encoding
    let frame = Frame::new(
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED,
        buf.freeze(),
    )
    .expect("frame");
    assert!(binary::decode_acked_publish_batch(&frame).is_err());
}

#[test]
fn acked_publish_batch_rejects_none_ack_mode_on_encode() {
    // AckMode::None must go through `encode_publish_batch` instead, so there is
    // exactly one wire encoding per mode.
    assert!(
        binary::encode_acked_publish_batch_bytes(
            1,
            AckMode::None,
            "t1",
            "default",
            "orders",
            &[b"x".to_vec()],
        )
        .is_err()
    );
}

// The body parser is shared with the unacked path, so the payload-count bound
// must still apply once the prefix has been stripped.
#[test]
fn acked_publish_batch_rejects_oversized_payload_count() {
    let mut buf = BytesMut::new();
    buf.put_u64(1);
    buf.put_u8(1);
    buf.put_u16(2);
    buf.extend_from_slice(b"t1");
    buf.put_u16(2);
    buf.extend_from_slice(b"ns");
    buf.put_u16(2);
    buf.extend_from_slice(b"st");
    buf.put_u32(u32::MAX); // count, with no payload bytes following
    let frame = Frame::new(
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED,
        buf.freeze(),
    )
    .expect("frame");
    assert!(matches!(
        binary::decode_acked_publish_batch(&frame).expect_err("oversized count"),
        Error::Incomplete
    ));
}

const PRODUCER: ProducerSequence = ProducerSequence {
    producer_id: 0xdead_beef,
    sequence: 7,
};

#[test]
fn round_trips_keyed_and_unkeyed() {
    for key in [None, Some(&b"user-1"[..])] {
        let bytes = encode_idempotent_publish_batch_bytes(
            9,
            PRODUCER,
            key,
            "t1",
            "ns",
            "orders",
            &[b"a".to_vec(), b"bc".to_vec()],
        )
        .expect("encode");
        let frame = Frame::decode(bytes).expect("frame");
        // The request id stays at offset 0, so a malformed body can still
        // be answered against the right request.
        assert_eq!(
            peek_acked_publish_prefix(&frame).expect("peek"),
            (9, AckMode::PerBatch)
        );
        let decoded = decode_acked_publish_batch(&frame).expect("decode");
        assert_eq!(decoded.request_id, 9);
        assert_eq!(decoded.producer, Some(PRODUCER));
        assert_eq!(decoded.batch.key.as_deref(), key);
        assert_eq!(decoded.batch.stream, "orders");
        assert_eq!(decoded.batch.payloads, vec![b"a".to_vec(), b"bc".to_vec()]);
    }
}

/// Read as an unacked batch, the producer id would be taken for the
/// tenant length. It is refused instead.
#[test]
fn the_unacked_decoder_refuses_it() {
    let bytes = encode_idempotent_publish_batch_bytes(
        1,
        PRODUCER,
        None,
        "t1",
        "ns",
        "orders",
        &[b"a".to_vec()],
    )
    .expect("encode");
    let frame = Frame::decode(bytes).expect("frame");
    assert!(decode_publish_batch(&frame).is_err());
}

#[test]
fn a_truncated_producer_prefix_is_incomplete() {
    let bytes = encode_idempotent_publish_batch_bytes(1, PRODUCER, None, "t1", "ns", "orders", &[])
        .expect("encode");
    let frame = Frame::decode(bytes).expect("frame");
    let cut = Frame {
        header: frame.header,
        payload: frame.payload.slice(..9 + 8),
    };
    assert!(decode_acked_publish_batch(&cut).is_err());
}

/// An acked batch read by the plain decoder would take its request id for the
/// tenant length. That is what a uni stream would do with `BATCH|ACKED`.
#[test]
fn the_plain_decoder_refuses_an_acked_batch() {
    let bytes = binary::encode_acked_publish_batch_bytes(
        u64::MAX,
        AckMode::PerBatch,
        "t1",
        "ns",
        "orders",
        &[b"a".to_vec()],
    )
    .expect("encode");
    let frame = Frame::decode(bytes).expect("frame");
    assert!(frame.header.flags & FLAG_BINARY_PUBLISH_ACKED != 0);
    assert!(decode_publish_batch(&frame).is_err());
    assert!(decode_acked_publish_batch(&frame).is_ok());
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

fn golden_acked_headers_frame() -> Vec<u8> {
    [
        &[
            0x46, 0x4C, 0x58, 0x31, // magic
            0x00, 0x01, // version
            0x80, 0x09, // FLAG_EXTENDED | FLAG_BINARY_PUBLISH_ACKED | _BATCH
            0x00, 0x00, 0x00, 0x30, // length
            0x00, 0x00, 0x00, 0x01, // extension word: EXT_RECORD_HEADERS
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x02, // request 7, per batch
            0x00, 0x01, b't', 0x00, 0x01, b'n', 0x00, 0x01, b's', // names
            0x00, 0x00, 0x00, 0x01, // count
            0x00, 0x00, 0x00, 0x0D, // block length
        ][..],
        BLOCK,
        &[0x00, 0x00, 0x00, 0x01, b'p'],
    ]
    .concat()
}

#[test]
fn an_acked_publish_with_headers_is_these_bytes() {
    let bytes = binary::encode_publish_batch_bytes_with_headers(
        Some((7, AckMode::PerBatch)),
        None,
        None,
        "t",
        "n",
        "s",
        &[b"p".to_vec()],
        &[Some(block())],
    )
    .expect("encode");
    assert_eq!(bytes.as_ref(), golden_acked_headers_frame());

    let frame = Frame::decode(bytes).expect("frame");
    // The correlation prefix is still readable without the body, behind the
    // extension word.
    assert_eq!(
        peek_acked_publish_prefix(&frame).expect("peek"),
        (7, AckMode::PerBatch)
    );
    let decoded = decode_acked_publish_batch(&frame).expect("decode");
    assert_eq!(decoded.request_id, 7);
    assert_eq!(decoded.producer, None);
    assert_eq!(decoded.batch.payloads, vec![Bytes::from_static(b"p")]);
    assert_eq!(decoded.batch.record_headers, Some(vec![Some(block())]));
}

#[test]
fn an_idempotent_keyed_publish_with_headers_round_trips() {
    let producer = ProducerSequence {
        producer_id: 11,
        sequence: 12,
    };
    let bytes = binary::encode_publish_batch_bytes_with_headers(
        Some((7, AckMode::PerBatch)),
        Some(producer),
        Some(b"k"),
        "t",
        "n",
        "s",
        &[Bytes::from_static(b"p"), Bytes::from_static(b"q")],
        &[None, Some(block())],
    )
    .expect("encode");
    let frame = Frame::decode(bytes).expect("frame");
    assert_eq!(
        frame.header.flags,
        crate::FLAG_EXTENDED
            | FLAG_BINARY_PUBLISH_BATCH
            | FLAG_BINARY_PUBLISH_ACKED
            | crate::FLAG_BINARY_PUBLISH_IDEMPOTENT
            | FLAG_BINARY_PUBLISH_KEYED
    );
    let decoded = decode_acked_publish_batch(&frame).expect("decode");
    assert_eq!(decoded.producer, Some(producer));
    assert_eq!(decoded.batch.key.as_deref(), Some(b"k".as_slice()));
    assert_eq!(
        decoded.batch.record_headers,
        Some(vec![None, Some(block())])
    );
}

#[test]
fn acked_batches_whose_headers_are_all_none_are_the_frames_they_always_were() {
    let payloads = vec![b"one".to_vec(), b"two".to_vec()];
    let none = [None, None];

    let plain = binary::encode_acked_publish_batch_bytes_keyed(
        9,
        AckMode::PerMessage,
        Some(b"k"),
        "t",
        "n",
        "s",
        &payloads,
    )
    .expect("plain");
    let with = binary::encode_publish_batch_bytes_with_headers(
        Some((9, AckMode::PerMessage)),
        None,
        Some(b"k"),
        "t",
        "n",
        "s",
        &payloads,
        &none,
    )
    .expect("with");
    assert_eq!(with, plain);

    let producer = ProducerSequence {
        producer_id: 1,
        sequence: 2,
    };
    let plain = encode_idempotent_publish_batch_bytes(9, producer, None, "t", "n", "s", &payloads)
        .expect("plain");
    let with = binary::encode_publish_batch_bytes_with_headers(
        Some((9, AckMode::PerBatch)),
        Some(producer),
        None,
        "t",
        "n",
        "s",
        &payloads,
        &none,
    )
    .expect("with");
    assert_eq!(with, plain);
}

#[test]
fn a_producer_needs_an_ack_per_batch() {
    let producer = ProducerSequence {
        producer_id: 1,
        sequence: 2,
    };
    for ack in [
        None,
        Some((1, AckMode::PerMessage)),
        Some((1, AckMode::None)),
    ] {
        assert!(
            binary::encode_publish_batch_bytes_with_headers(
                ack,
                Some(producer),
                None,
                "t",
                "n",
                "s",
                &[b"p".to_vec()],
                &[None],
            )
            .is_err()
        );
    }
}

#[test]
fn an_acked_frame_with_a_bad_extension_word_is_refused_but_still_peekable() {
    for ext in [0u32, 0x2] {
        let mut bytes = golden_acked_headers_frame();
        bytes[FrameHeader::LEN..FrameHeader::LEN + 4].copy_from_slice(&ext.to_be_bytes());
        let frame = Frame::decode(Bytes::from(bytes)).expect("frame");
        assert!(matches!(
            decode_acked_publish_batch(&frame),
            Err(Error::UnknownExtension(word)) if word == ext
        ));
        // The broker can still address the error to the right request.
        assert_eq!(
            peek_acked_publish_prefix(&frame).expect("peek"),
            (7, AckMode::PerBatch)
        );
    }
}
