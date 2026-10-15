use super::*;

/// A miss and a stored empty value are different answers, and the presence byte
/// is the only thing separating them. Encode them as the same bytes and a
/// cached empty value reads back as "not there" forever.
#[test]
fn an_empty_cached_value_is_not_a_miss() {
    let empty = InternalMessage::ForwardCacheOk(ForwardCacheOk {
        correlation_id: 42,
        value: Some(Bytes::new()),
        outcome: None,
    });
    let miss = InternalMessage::ForwardCacheOk(ForwardCacheOk {
        correlation_id: 42,
        value: None,
        outcome: None,
    });

    let empty_bytes = empty.encode().expect("encode");
    let miss_bytes = miss.encode().expect("encode");
    assert_ne!(empty_bytes, miss_bytes);

    assert_eq!(InternalMessage::decode(empty_bytes).expect("decode"), empty);
    assert_eq!(InternalMessage::decode(miss_bytes).expect("decode"), miss);
}

/// The presence byte has two meanings and no third. A peer sending anything
/// else is not a peer this can guess for.
#[test]
fn an_unknown_cache_value_presence_byte_is_refused() {
    let ok = InternalMessage::ForwardCacheOk(ForwardCacheOk {
        correlation_id: 42,
        value: None,
        outcome: None,
    });
    let mut bytes = ok.encode().expect("encode").to_vec();
    let last = bytes.len() - 1;
    bytes[last] = 2;

    assert!(InternalMessage::decode(Bytes::from(bytes)).is_err());
}

#[test]
fn an_unknown_cache_operation_is_refused() {
    let op = InternalMessage::ForwardCacheOp(ForwardCacheOp {
        correlation_id: 42,
        shard: shard(),
        op: CacheOpKind::Get,
        key: "k".to_string(),
        value: Bytes::new(),
        ttl_ms: 0,
        credential: String::new(),
        condition: None,
    });
    let bytes = op.encode().expect("encode").to_vec();
    let position = bytes
        .windows(1)
        .position(|w| w == [CacheOpKind::Get as u8])
        .expect("the op byte is in there somewhere");
    let mut broken = bytes;
    broken[position] = 9;

    assert!(InternalMessage::decode(Bytes::from(broken)).is_err());
}

/// The credential picks the kind, so a forwarder holding one cannot send it
/// on the kind an owner does not check.
#[test]
fn a_credential_selects_the_authorized_kind() {
    assert_eq!(forward().kind(), Kind::ForwardPublish);
    assert_eq!(authorized_forward().kind(), Kind::AuthorizedForwardPublish);
    let decoded =
        InternalMessage::decode(authorized_forward().encode().expect("encode")).expect("decode");
    assert_eq!(decoded, authorized_forward());
}

/// The legacy layout is a prefix of the authorized one, so an authorized frame
/// relabelled as the legacy kind has trailing bytes and is refused -- a peer
/// cannot downgrade a frame by rewriting its kind.
#[test]
fn an_authorized_frame_relabelled_as_legacy_is_refused() {
    let mut bytes = authorized_forward().encode().expect("encode").to_vec();
    // kind sits after the 4-byte magic and 2-byte version
    bytes[6..8].copy_from_slice(&(Kind::ForwardPublish as u16).to_be_bytes());
    assert!(InternalMessage::decode(Bytes::from(bytes)).is_err());
}

/// And the other way: a legacy frame relabelled as authorized has no
/// credential to read, and an authorized kind with an empty one is refused
/// rather than read as "no credential".
#[test]
fn an_authorized_kind_without_a_credential_is_refused() {
    let mut bytes = forward().encode().expect("encode").to_vec();
    bytes[6..8].copy_from_slice(&(Kind::AuthorizedForwardPublish as u16).to_be_bytes());
    assert!(InternalMessage::decode(Bytes::from(bytes.clone())).is_err());

    // Explicitly zero-length, with the header length fixed up to match.
    bytes.extend_from_slice(&0u32.to_be_bytes());
    let body_len = (bytes.len() - InternalHeader::LEN) as u32;
    bytes[8..12].copy_from_slice(&body_len.to_be_bytes());
    assert!(InternalMessage::decode(Bytes::from(bytes)).is_err());
}

#[test]
fn an_over_long_credential_is_refused_on_encode() {
    let message = InternalMessage::ForwardPublish(ForwardPublish {
        correlation_id: 1,
        shard: shard(),
        ack: AckMode::None,
        payloads: Vec::new(),
        credential: "x".repeat(MAX_CREDENTIAL_BYTES + 1),
    });
    assert!(matches!(message.encode(), Err(Error::FrameTooLarge)));
}

/// A stream with no log is answered with an empty range, which no written
/// batch has, so it cannot be read as a batch at offset 0.
#[test]
fn a_forward_ok_without_offsets_round_trips_as_none() {
    let ok = ForwardPublishOk::new(42, None);
    assert_eq!(ok.offsets(), None);
    let message = InternalMessage::ForwardPublishOk(ok);
    let decoded = InternalMessage::decode(message.encode().expect("encode")).expect("decode");
    let InternalMessage::ForwardPublishOk(decoded) = decoded else {
        panic!("expected ForwardPublishOk, got {decoded:?}");
    };
    assert_eq!(decoded.offsets(), None);
}

#[test]
fn a_forward_ok_carries_the_batch_offsets() {
    let message = InternalMessage::ForwardPublishOk(ForwardPublishOk::new(42, Some((4, 6))));
    let decoded = InternalMessage::decode(message.encode().expect("encode")).expect("decode");
    let InternalMessage::ForwardPublishOk(decoded) = decoded else {
        panic!("expected ForwardPublishOk, got {decoded:?}");
    };
    assert_eq!(decoded.offsets(), Some((4, 6)));
    assert_eq!(
        ForwardPublishOk::new(1, Some((0, 0))).offsets(),
        Some((0, 0))
    );
}

/// A conditional request travels as its own kind, with or without a
/// credential, and its answer as another; an unconditional one keeps the
/// kinds and bytes it always had.
#[test]
fn a_conditional_cache_op_round_trips_as_its_own_kind() {
    let conditional = |credential: &str, condition| ForwardCacheOp {
        correlation_id: 42,
        shard: shard(),
        op: CacheOpKind::Put,
        key: "lease".to_string(),
        value: Bytes::from_static(b"worker-1"),
        ttl_ms: 30_000,
        credential: credential.to_string(),
        condition: Some(condition),
    };
    for message in [
        conditional("", ForwardCacheCondition::Absent),
        conditional("eyJ.token.sig", ForwardCacheCondition::Version(7)),
        conditional("eyJ.token.sig", ForwardCacheCondition::Unconditional),
    ] {
        let message = InternalMessage::ForwardCacheOp(message);
        assert_eq!(message.kind(), Kind::ConditionalForwardCacheOp);
        let decoded = InternalMessage::decode(message.encode().expect("encode")).expect("decode");
        assert_eq!(decoded, message);
    }

    for outcome in [
        ForwardCacheOutcome {
            applied: true,
            version: Some(9),
        },
        ForwardCacheOutcome {
            applied: false,
            version: None,
        },
    ] {
        let answer = InternalMessage::ForwardCacheOk(ForwardCacheOk {
            correlation_id: 42,
            value: Some(Bytes::from_static(b"v")),
            outcome: Some(outcome),
        });
        assert_eq!(answer.kind(), Kind::ConditionalForwardCacheOk);
        let decoded = InternalMessage::decode(answer.encode().expect("encode")).expect("decode");
        assert_eq!(decoded, answer);
    }

    let plain = InternalMessage::ForwardCacheOk(ForwardCacheOk {
        correlation_id: 42,
        value: None,
        outcome: None,
    });
    assert_eq!(plain.kind(), Kind::ForwardCacheOk);
}
