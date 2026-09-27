use super::*;

#[test]
fn every_message_round_trips() {
    for message in every_message() {
        let encoded = message.encode().expect("encode");
        let decoded = InternalMessage::decode(encoded).expect("decode");
        assert_eq!(decoded, message);
    }
}

#[test]
fn every_message_carries_a_correlation_id() {
    for message in every_message() {
        assert_eq!(
            message.correlation_id(),
            42,
            "a response with no correlation could never be matched or discarded",
        );
    }
}

/// The allocation trap the client binary path documents: a tiny body declaring
/// an enormous payload count must be rejected before `with_capacity` sees it.
#[test]
fn a_declared_payload_count_is_bounded_by_the_body() {
    let mut body = BytesMut::new();
    body.put_u64(1);
    for value in ["t1", "ns", "orders"] {
        body.put_u32(value.len() as u32);
        body.extend_from_slice(value.as_bytes());
    }
    body.put_u32(0);
    body.put_u64(0);
    body.put_u8(0);
    body.put_u32(u32::MAX); // "there are four billion payloads after this"

    let mut frame = BytesMut::new();
    InternalHeader {
        kind: Kind::ForwardPublish,
        length: body.len() as u32,
    }
    .encode(&mut frame);
    frame.extend_from_slice(&body);

    assert!(
        matches!(
            InternalMessage::decode(frame.freeze()),
            Err(Error::Incomplete)
        ),
        "a declared count must be checked against the bytes that remain",
    );
}

/// Truncation at every byte must be an error, never a panic. This is the test
/// that says a hostile or buggy peer cannot take a broker down.
#[test]
fn truncation_at_every_byte_is_an_error_not_a_panic() {
    for message in every_message() {
        let encoded = message.encode().expect("encode");
        for cut in 0..encoded.len() {
            let truncated = encoded.slice(0..cut);
            assert!(
                InternalMessage::decode(truncated).is_err(),
                "{:?} truncated to {cut} bytes must not decode",
                message.kind(),
            );
        }
    }
}

/// Every single-byte corruption must either decode to something well formed or
/// error. Neither may panic.
#[test]
fn single_byte_corruption_never_panics() {
    for message in every_message() {
        let encoded = message.encode().expect("encode");
        for index in 0..encoded.len() {
            for bit in 0..8u32 {
                let mut corrupted = encoded.to_vec();
                corrupted[index] ^= 1 << bit;
                let _ = InternalMessage::decode(Bytes::from(corrupted));
            }
        }
    }
}

#[test]
fn trailing_bytes_are_rejected() {
    let mut encoded = forward().encode().expect("encode").to_vec();
    encoded.push(0);
    // The header's length no longer matches the body, which is the first thing
    // checked.
    assert!(InternalMessage::decode(Bytes::from(encoded)).is_err());
}

#[test]
fn a_non_utf8_identifier_is_rejected() {
    let mut body = BytesMut::new();
    body.put_u64(1);
    body.put_u32(2);
    body.extend_from_slice(&[0xff, 0xfe]);

    let mut frame = BytesMut::new();
    InternalHeader {
        kind: Kind::NotLeader,
        length: body.len() as u32,
    }
    .encode(&mut frame);
    frame.extend_from_slice(&body);

    assert!(matches!(
        InternalMessage::decode(frame.freeze()),
        Err(Error::InvalidUtf8),
    ));
}

#[test]
fn an_over_long_identifier_is_refused_on_encode() {
    let message = InternalMessage::NotLeader(NotLeader {
        correlation_id: 1,
        node_id: "n".repeat(MAX_IDENT_BYTES + 1),
        advertise_addr: "h:1".to_string(),
        generation: 1,
    });
    assert!(matches!(message.encode(), Err(Error::FrameTooLarge)));
}

#[test]
fn an_over_large_batch_is_refused_on_encode() {
    let message = InternalMessage::ForwardPublish(ForwardPublish {
        correlation_id: 1,
        shard: shard(),
        ack: AckMode::None,
        payloads: vec![Bytes::new(); MAX_BATCH_PAYLOADS + 1],
        credential: String::new(),
    });
    assert!(matches!(message.encode(), Err(Error::FrameTooLarge)));
}

#[test]
fn an_empty_batch_round_trips() {
    let message = InternalMessage::ForwardPublish(ForwardPublish {
        correlation_id: 5,
        shard: shard(),
        ack: AckMode::None,
        payloads: Vec::new(),
        credential: String::new(),
    });
    let decoded = InternalMessage::decode(message.encode().expect("encode")).expect("decode");
    assert_eq!(decoded, message);
}

#[test]
fn unknown_enum_values_are_rejected() {
    assert!(Kind::from_u16(0).is_err());
    // One past the highest kind: an unknown kind must be rejected rather than
    // skipped, because the kind is what selects how to read the body.
    assert!(Kind::from_u16(27).is_err());
    assert!(ReplicaLog::from_u8(0).is_err());
    assert!(ReplicaLog::from_u8(6).is_err());
    assert!(ErrorCode::from_u16(0).is_err());
    assert!(ErrorCode::from_u16(999).is_err());
    assert!(AckMode::from_u8(9).is_err());
    assert!(CacheOpKind::from_u8(0).is_err());
    assert!(CacheOpKind::from_u8(6).is_err());
}
