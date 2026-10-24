use bytes::BytesMut;

use crate::{AckMode, Frame, Message};

// The publish-batch JSON is written by hand rather than by serde, so nothing but
// a test keeps it in step with the decoder. It drifted: the `request_id` prefix
// carried a leading `"` that belongs after a string field, not after the
// `payloads` array, so every acked JSON batch serialised to `]","request_id"` and
// failed to deserialise. Cover every combination of the optional fields.
#[test]
fn text_publish_batch_json_round_trips_through_decoder() {
    let payloads = vec![b"payload".to_vec(), b"second".to_vec()];
    let cases = [
        (None, None),
        (Some(1u64), None),
        (None, Some(AckMode::PerBatch)),
        (Some(7), Some(AckMode::PerMessage)),
        (Some(u64::MAX), Some(AckMode::PerBatch)),
    ];
    for (request_id, ack) in cases {
        let mut buf = BytesMut::new();
        crate::text::write_publish_batch_json(
            &mut buf, "t1", "default", "updates", &payloads, None, None, request_id, ack,
        )
        .expect("write");

        // The declared length must match what was actually written, or callers
        // that pre-reserve using it will mis-size the frame.
        let declared = crate::text::publish_batch_json_len(
            "t1", "default", "updates", &payloads, None, None, request_id, ack,
        )
        .expect("len");
        assert_eq!(
            declared,
            buf.len(),
            "length mismatch for {request_id:?}/{ack:?}"
        );

        let frame = Frame::new(0, buf.freeze()).expect("frame");
        match Message::decode(frame).expect("decode") {
            Message::PublishBatch {
                tenant_id,
                namespace,
                stream,
                payloads: decoded,
                request_id: decoded_id,
                ack: decoded_ack,
                key: None,
                shard: None,
            } => {
                assert_eq!(tenant_id, "t1");
                assert_eq!(namespace, "default");
                assert_eq!(stream, "updates");
                assert_eq!(decoded, payloads);
                assert_eq!(decoded_id, request_id);
                assert_eq!(decoded_ack, ack);
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }
}

#[test]
fn text_publish_batch_json_with_special_chars() {
    // Test encoding with special characters that need escaping
    let payloads = vec![b"payload\nwith\nnewlines".to_vec()];
    let message = Message::PublishBatch {
        tenant_id: "tenant".to_string(),
        namespace: "ns".to_string(),
        stream: "stream".to_string(),
        payloads,
        request_id: Some(123),
        ack: Some(AckMode::PerBatch),
        key: None,
        shard: None,
    };
    let frame = message.encode().expect("encode");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(message, decoded);
}

/// **The hand-written batch encoder carries the routing key.** It, not serde, is
/// what the client's writer task uses, so a key omitted here is a key that never
/// reaches the broker — and the record lands on shard 0 with nothing to show for
/// it. Exactly the kind of silent divergence a fast path invites.
#[test]
fn the_fast_batch_encoder_carries_the_routing_key() {
    let payloads = vec![b"one".to_vec(), b"two".to_vec()];
    let key = b"customer-42".to_vec();

    let mut buf = BytesMut::new();
    crate::text::write_publish_batch_json(
        &mut buf,
        "t1",
        "default",
        "updates",
        &payloads,
        Some(&key),
        None,
        Some(7),
        Some(AckMode::PerMessage),
    )
    .expect("write");

    let declared = crate::text::publish_batch_json_len(
        "t1",
        "default",
        "updates",
        &payloads,
        Some(&key),
        None,
        Some(7),
        Some(AckMode::PerMessage),
    )
    .expect("len");
    assert_eq!(
        declared,
        buf.len(),
        "a declared length that ignores the key mis-sizes the frame",
    );

    let decoded = Message::decode(Frame::new(0, buf.freeze()).expect("frame")).expect("decode");
    match decoded {
        Message::PublishBatch { key: decoded, .. } => {
            assert_eq!(decoded.as_deref(), Some(&key[..]), "the key was dropped");
        }
        other => panic!("expected a publish batch, got {other:?}"),
    }
}

/// A batch with no key encodes the bytes it always did, so an old broker parses
/// a new client's unkeyed publish unchanged.
#[test]
fn an_unkeyed_batch_omits_the_field() {
    let payloads = vec![b"one".to_vec()];
    let mut buf = BytesMut::new();
    crate::text::write_publish_batch_json(
        &mut buf, "t1", "default", "updates", &payloads, None, None, None, None,
    )
    .expect("write");
    let json = std::str::from_utf8(&buf).expect("utf8");
    assert!(
        !json.contains("shard"),
        "an absent shard must not appear: {json}"
    );
    assert!(
        !json.contains("key"),
        "an absent key must not appear: {json}"
    );
}

/// **The hand-written batch encoder carries an explicit shard**, for the
/// reason it carries the key: without it the broker routes by the key and the
/// record lands on another shard.
#[test]
fn the_fast_batch_encoder_carries_the_shard() {
    let payloads = vec![b"one".to_vec()];
    for (key, shard) in [
        (None, 0u32),
        (Some(b"d03".to_vec()), 3),
        (Some(Vec::new()), u32::MAX),
    ] {
        let mut buf = BytesMut::new();
        crate::text::write_publish_batch_json(
            &mut buf,
            "t1",
            "default",
            "updates",
            &payloads,
            key.as_deref(),
            Some(shard),
            Some(9),
            Some(AckMode::PerBatch),
        )
        .expect("write");
        let declared = crate::text::publish_batch_json_len(
            "t1",
            "default",
            "updates",
            &payloads,
            key.as_deref(),
            Some(shard),
            Some(9),
            Some(AckMode::PerBatch),
        )
        .expect("len");
        assert_eq!(declared, buf.len());
        match Message::decode(Frame::new(0, buf.freeze()).expect("frame")).expect("decode") {
            Message::PublishBatch {
                key: decoded_key,
                shard: decoded,
                ..
            } => {
                assert_eq!(decoded, Some(shard));
                assert_eq!(decoded_key.as_deref(), key.as_deref());
            }
            other => panic!("expected a publish batch, got {other:?}"),
        }
    }
}
