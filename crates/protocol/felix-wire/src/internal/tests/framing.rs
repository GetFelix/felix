use super::*;

/// The whole point of a distinct magic: a client frame must not decode here,
/// and an internal frame must not decode as a client frame.
#[test]
fn client_and_internal_frames_do_not_decode_as_each_other() {
    let internal = forward().encode().expect("encode");
    assert!(
        matches!(
            crate::Frame::decode(internal.clone()),
            Err(Error::InvalidMagic)
        ),
        "an internal frame must not parse as a client frame",
    );

    let client = crate::Frame::new(0x1, Bytes::from_static(b"hello"))
        .expect("frame")
        .encode();
    assert!(
        matches!(InternalMessage::decode(client), Err(Error::InvalidMagic)),
        "a client frame must not parse as an internal frame",
    );
}

/// An unknown kind is rejected rather than skipped: the kind selects how to
/// read the body, so ignoring one means confidently misparsing it.
#[test]
fn an_unknown_kind_is_rejected() {
    let mut frame = BytesMut::new();
    frame.put_u32(INTERNAL_MAGIC);
    frame.put_u16(INTERNAL_VERSION);
    frame.put_u16(999);
    frame.put_u32(0);
    assert!(matches!(
        InternalMessage::decode(frame.freeze()),
        Err(Error::UnsupportedInternalKind(999)),
    ));
}

#[test]
fn an_unknown_version_is_rejected() {
    let mut frame = BytesMut::new();
    frame.put_u32(INTERNAL_MAGIC);
    frame.put_u16(INTERNAL_VERSION + 1);
    frame.put_u16(Kind::ForwardPublishOk as u16);
    frame.put_u32(24);
    assert!(matches!(
        InternalMessage::decode(frame.freeze()),
        Err(Error::UnsupportedVersion(_)),
    ));
}

/// The kinds already on the wire keep their discriminants. A peer mid-upgrade
/// decodes by number, so renumbering one silently reinterprets every frame of
/// that kind.
#[test]
fn the_existing_kind_discriminants_are_unchanged() {
    for (value, kind) in [
        (1, Kind::ForwardPublish),
        (2, Kind::ForwardPublishOk),
        (3, Kind::ForwardPublishError),
        (4, Kind::NotLeader),
        (5, Kind::Hello),
        (6, Kind::HelloOk),
        (7, Kind::ReplicateRecords),
        (8, Kind::ReplicateOk),
        (9, Kind::ReplicateError),
        (10, Kind::ReplicateBootstrap),
        (11, Kind::ForwardCacheOp),
        (12, Kind::ForwardCacheOk),
        (13, Kind::ForwardCacheError),
        (14, Kind::ReplicateCacheRecords),
        (15, Kind::ReplicateCacheBootstrap),
        (16, Kind::ReplicateGroupRecords),
        (17, Kind::ReplicateGroupBootstrap),
        (18, Kind::ReplicateDeadLetterRecords),
        (19, Kind::ReplicateDeadLetterBootstrap),
        (20, Kind::ReplicateCounterRecords),
        (21, Kind::ReplicateCounterBootstrap),
        (22, Kind::AuthorizedForwardPublish),
        (23, Kind::AuthorizedForwardCacheOp),
        (24, Kind::ReplicateRebuild),
        (25, Kind::ReplicateMarkedRecords),
        (26, Kind::ReplicateCommittedRecords),
        (27, Kind::HelloCapable),
        (28, Kind::HelloCapableOk),
        (29, Kind::Fence),
        (30, Kind::FenceOk),
        (31, Kind::ReplicateFetch),
    ] {
        assert_eq!(Kind::from_u16(value).expect("known"), kind);
        assert_eq!(kind as u16, value);
    }
}

/// **Every body begins with its correlation id.**
///
/// A responder that meets a kind it does not know steps over the body and
/// answers anyway, and it can only do that because the correlation id is at a
/// fixed place in every body. Adding a field before it would make a refusal
/// unmatchable and leave dropping the connection as the only option — so this
/// is a protocol invariant, not a convention.
#[test]
fn every_body_begins_with_its_correlation_id() {
    let shard = ShardRef {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        generation: 4,
    };
    let id = 0x0102_0304_0506_0708u64;
    let messages = vec![
        InternalMessage::Hello(Hello {
            correlation_id: id,
            node_id: "broker-a".to_string(),
            capabilities: None,
        }),
        InternalMessage::HelloOk(HelloOk {
            correlation_id: id,
            node_id: "broker-a".to_string(),
            capabilities: None,
        }),
        InternalMessage::Hello(Hello {
            correlation_id: id,
            node_id: "broker-a".to_string(),
            capabilities: Some(PeerCapabilities::FENCE),
        }),
        InternalMessage::HelloOk(HelloOk {
            correlation_id: id,
            node_id: "broker-a".to_string(),
            capabilities: Some(PeerCapabilities::FENCE),
        }),
        InternalMessage::Fence(Fence {
            correlation_id: id,
            shard: shard.clone(),
            log: ReplicaLog::Stream,
        }),
        InternalMessage::FenceOk(FenceOk {
            correlation_id: id,
            log_end: 1,
            commit_offset: 1,
            last_generation: 1,
        }),
        InternalMessage::ReplicateFetch(ReplicateFetch {
            timed: false,
            correlation_id: id,
            shard: shard.clone(),
            log: ReplicaLog::Stream,
            from_offset: 1,
            max_bytes: 1,
            labelled: false,
        }),
        InternalMessage::ReplicateRecords(ReplicateRecords {
            times: None,
            correlation_id: id,
            shard: shard.clone(),
            first_offset: 10,
            checksum: 7,
            payloads: vec![Bytes::from_static(b"r")],
            marks: Vec::new(),
            commit_offset: None,
            generations: None,
            publishers: Vec::new(),
        }),
        InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: id,
            durable_offset: 11,
        }),
        InternalMessage::ReplicateError(ReplicateError {
            correlation_id: id,
            code: ErrorCode::LogGap,
            expected_offset: 3,
            detail: "gap".to_string(),
        }),
        InternalMessage::ReplicateBootstrap(ReplicateBootstrap {
            correlation_id: id,
            shard,
            base_offset: 100,
        }),
        InternalMessage::ForwardPublishError(ForwardPublishError {
            correlation_id: id,
            code: ErrorCode::Unavailable,
            detail: "no".to_string(),
        }),
    ];

    for message in messages {
        let encoded = message.encode().expect("encode");
        let body = &encoded[InternalHeader::LEN..];
        assert_eq!(
            correlation_id_in(body),
            Some(id),
            "{message:?} does not begin with its correlation id, so a peer that \
             does not know its kind could not answer it",
        );
    }
}

/// The envelope is readable even when the kind is not.
///
/// This is what lets an older peer step over a frame from a newer one rather
/// than dropping the stream every in-flight request is sharing.
#[test]
fn an_unknown_kind_still_has_a_readable_envelope() {
    let mut frame = BytesMut::new();
    frame.put_u32(INTERNAL_MAGIC);
    frame.put_u16(INTERNAL_VERSION);
    frame.put_u16(60_000); // A kind from some later build.
    frame.put_u32(9);
    frame.extend_from_slice(&[0u8; 9]);
    let frame = frame.freeze();

    let envelope = FrameEnvelope::decode(&frame).expect("the envelope is frozen and readable");
    assert_eq!(envelope.kind, 60_000);
    assert_eq!(envelope.length, 9);
    // And the typed decode still refuses it, which is what selects the body
    // layout and must never guess.
    assert!(InternalMessage::decode(frame).is_err());
}

/// A wrong magic or version is *not* steppable.
///
/// The envelope's own fields are the thing being trusted, so the framing has to
/// be ours before a length from it means anything.
#[test]
fn a_frame_that_is_not_ours_has_no_envelope_to_read() {
    let mut alien = BytesMut::new();
    alien.put_u32(0xDEAD_BEEF);
    alien.put_u16(INTERNAL_VERSION);
    alien.put_u16(1);
    alien.put_u32(0);
    assert!(FrameEnvelope::decode(&alien.freeze()).is_err());

    let mut future = BytesMut::new();
    future.put_u32(INTERNAL_MAGIC);
    future.put_u16(INTERNAL_VERSION + 1);
    future.put_u16(1);
    future.put_u32(0);
    assert!(FrameEnvelope::decode(&future.freeze()).is_err());
}
