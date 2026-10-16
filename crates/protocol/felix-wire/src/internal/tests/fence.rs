//! The capability handshake and the fence's bodies.

use super::*;

fn hello(capabilities: Option<PeerCapabilities>) -> InternalMessage {
    InternalMessage::Hello(Hello {
        correlation_id: 42,
        node_id: "broker-a".to_string(),
        capabilities,
    })
}

/// **A `Hello` without capabilities is the original frame, byte for byte.** A
/// peer that predates the bits is answered and greeted exactly as it always
/// was; the bits travel only on the kind it refuses.
#[test]
fn a_hello_without_capabilities_is_the_original_frame() {
    let mut expected = BytesMut::new();
    expected.put_u32(INTERNAL_MAGIC);
    expected.put_u16(INTERNAL_VERSION);
    expected.put_u16(Kind::Hello as u16);
    expected.put_u32(8 + 4 + 8);
    expected.put_u64(42);
    expected.put_u32(8);
    expected.put_slice(b"broker-a");

    assert_eq!(hello(None).encode().expect("encode"), expected.freeze());
    assert_eq!(hello(None).kind(), Kind::Hello);
}

#[test]
fn capabilities_travel_on_their_own_kind() {
    let capable = hello(Some(PeerCapabilities::FENCE));
    assert_eq!(capable.kind(), Kind::HelloCapable);
    let encoded = capable.encode().expect("encode");
    assert_eq!(
        InternalMessage::decode(encoded).expect("decode"),
        capable,
        "the capabilities did not survive the trip",
    );

    let answer = InternalMessage::HelloOk(HelloOk {
        correlation_id: 42,
        node_id: "broker-b".to_string(),
        capabilities: Some(PeerCapabilities::NONE),
        identity: None,
        incarnation: None,
    });
    assert_eq!(answer.kind(), Kind::HelloCapableOk);
    let decoded = InternalMessage::decode(answer.encode().expect("encode")).expect("decode");
    assert_eq!(
        decoded, answer,
        "an answer of no capabilities must still be told apart from an old peer's",
    );
}

fn capable_ok(capabilities: PeerCapabilities, identity: Option<[u8; 16]>) -> InternalMessage {
    InternalMessage::HelloOk(HelloOk {
        correlation_id: 42,
        node_id: "broker-b".to_string(),
        capabilities: Some(capabilities),
        identity,
        incarnation: None,
    })
}

fn capable_ok_frame(bits: u64, trailing: &[u8]) -> Bytes {
    let mut expected = BytesMut::new();
    expected.put_u32(INTERNAL_MAGIC);
    expected.put_u16(INTERNAL_VERSION);
    expected.put_u16(Kind::HelloCapableOk as u16);
    expected.put_u32((8 + 4 + 8 + 8 + trailing.len()) as u32);
    expected.put_u64(42);
    expected.put_u32(8);
    expected.put_slice(b"broker-b");
    expected.put_u64(bits);
    expected.put_slice(trailing);
    expected.freeze()
}

#[test]
fn a_storage_identity_travels_after_the_bits() {
    let identity = [7; 16];
    let answer = capable_ok(
        PeerCapabilities::FENCE.union(PeerCapabilities::STORAGE_IDENTITY),
        Some(identity),
    );
    let encoded = answer.encode().expect("encode");
    assert_eq!(
        encoded,
        capable_ok_frame((1 << 0) | (1 << 10), &identity),
        "the identity is the 16 bytes after the capability bits",
    );
    assert_eq!(InternalMessage::decode(encoded).expect("decode"), answer);
}

/// **Without the bit the answer is today's frame, byte for byte**, so a
/// caller that predates storage identities reads it as it always has.
#[test]
fn an_answer_without_the_identity_bit_is_unchanged() {
    let answer = capable_ok(PeerCapabilities::FENCE, None);
    assert_eq!(
        answer.encode().expect("encode"),
        capable_ok_frame(1 << 0, &[])
    );
}

#[test]
fn the_identity_bit_without_its_bytes_is_refused() {
    let frame = capable_ok_frame(PeerCapabilities::STORAGE_IDENTITY.bits(), &[1; 15]);
    assert!(matches!(
        InternalMessage::decode(frame),
        Err(Error::Incomplete)
    ));
    let frame = capable_ok_frame(PeerCapabilities::FENCE.bits(), &[1; 16]);
    assert!(
        matches!(InternalMessage::decode(frame), Err(Error::Incomplete)),
        "identity bytes without the bit are trailing garbage",
    );
    assert!(
        capable_ok(PeerCapabilities::STORAGE_IDENTITY, None)
            .encode()
            .is_err(),
        "the bit and the bytes must agree on the way out too",
    );
}

fn incarnated_ok(identity: Option<[u8; 16]>, incarnation: Option<u64>) -> InternalMessage {
    let mut capabilities = PeerCapabilities::FENCE.union(PeerCapabilities::NODE_INCARNATION);
    if identity.is_some() {
        capabilities = capabilities.union(PeerCapabilities::STORAGE_IDENTITY);
    }
    InternalMessage::HelloOk(HelloOk {
        correlation_id: 42,
        node_id: "broker-b".to_string(),
        capabilities: Some(capabilities),
        identity,
        incarnation,
    })
}

#[test]
fn a_node_incarnation_travels_after_the_identity() {
    let identity = [7; 16];
    let answer = incarnated_ok(Some(identity), Some(9));
    let encoded = answer.encode().expect("encode");
    let mut trailing = identity.to_vec();
    trailing.push(1);
    trailing.extend_from_slice(&9u64.to_be_bytes());
    assert_eq!(
        encoded,
        capable_ok_frame((1 << 0) | (1 << 10) | (1 << 11), &trailing),
        "a byte saying it is there, then the incarnation, after the identity",
    );
    assert_eq!(InternalMessage::decode(encoded).expect("decode"), answer);

    // Zero is a real incarnation: a node's first registration.
    let first = incarnated_ok(None, Some(0));
    let encoded = first.encode().expect("encode");
    assert_eq!(
        encoded,
        capable_ok_frame((1 << 0) | (1 << 11), &[1, 0, 0, 0, 0, 0, 0, 0, 0]),
    );
    assert_eq!(InternalMessage::decode(encoded).expect("decode"), first);
}

/// **Before it registers a responder answers the bit with none**, which is
/// not incarnation zero.
#[test]
fn an_unregistered_responder_answers_with_no_incarnation() {
    let answer = incarnated_ok(None, None);
    let encoded = answer.encode().expect("encode");
    assert_eq!(encoded, capable_ok_frame((1 << 0) | (1 << 11), &[0]));
    assert_eq!(InternalMessage::decode(encoded).expect("decode"), answer);
}

/// **Without the bit the answer is the frame a peer that predates it reads**,
/// identity and all.
#[test]
fn an_answer_without_the_incarnation_bit_is_unchanged() {
    let identity = [7; 16];
    let answer = capable_ok(
        PeerCapabilities::FENCE.union(PeerCapabilities::STORAGE_IDENTITY),
        Some(identity),
    );
    assert_eq!(
        answer.encode().expect("encode"),
        capable_ok_frame((1 << 0) | (1 << 10), &identity)
    );
}

#[test]
fn a_malformed_incarnation_is_refused() {
    let bits = PeerCapabilities::NODE_INCARNATION.bits();
    for trailing in [&[][..], &[2][..], &[1, 0, 0, 0][..], &[0, 0][..]] {
        assert!(
            matches!(
                InternalMessage::decode(capable_ok_frame(bits, trailing)),
                Err(Error::Incomplete)
            ),
            "{trailing:?} was read as an incarnation",
        );
    }
    assert!(
        matches!(
            InternalMessage::decode(capable_ok_frame(PeerCapabilities::FENCE.bits(), &[0])),
            Err(Error::Incomplete)
        ),
        "an incarnation byte without the bit is trailing garbage",
    );
    assert!(
        InternalMessage::HelloOk(HelloOk {
            correlation_id: 42,
            node_id: "broker-b".to_string(),
            capabilities: Some(PeerCapabilities::FENCE),
            identity: None,
            incarnation: Some(1),
        })
        .encode()
        .is_err(),
        "an incarnation without the bit would not read back",
    );
}

/// **A capability bit this build does not know is not a refusal.** Each bit
/// says a peer answers some request; none changes how a body is read. A later
/// build offering more must still be spoken to with what the two share.
#[test]
fn an_unknown_capability_bit_is_ignored() {
    let mut body = BytesMut::new();
    body.put_u64(42);
    body.put_u32(8);
    body.put_slice(b"broker-a");
    body.put_u64(PeerCapabilities::FENCE.bits() | 1 << 40);
    let mut frame = BytesMut::new();
    InternalHeader {
        kind: Kind::HelloCapable,
        length: body.len() as u32,
    }
    .encode(&mut frame);
    frame.extend_from_slice(&body);

    let frame = frame.freeze();
    let decoded = InternalMessage::decode(frame.clone()).expect("decode");
    let InternalMessage::Hello(Hello {
        capabilities: Some(capabilities),
        ..
    }) = &decoded
    else {
        panic!("expected a capable Hello, got {decoded:?}");
    };
    assert!(capabilities.contains(PeerCapabilities::FENCE));
    assert_eq!(decoded.encode().expect("encode"), frame);
}

#[test]
fn the_fence_names_the_shard_its_generation_and_its_log() {
    let fence = InternalMessage::Fence(Fence {
        correlation_id: 42,
        shard: shard(),
        log: ReplicaLog::Cache,
    });
    let encoded = fence.encode().expect("encode");
    assert_eq!(InternalMessage::decode(encoded).expect("decode"), fence);
}

#[test]
fn a_fence_naming_an_unknown_log_is_refused() {
    let fence = InternalMessage::Fence(Fence {
        correlation_id: 42,
        shard: shard(),
        log: ReplicaLog::Stream,
    });
    let mut frame = BytesMut::from(&fence.encode().expect("encode")[..]);
    let last = frame.len() - 1;
    frame[last] = 99;

    assert!(matches!(
        InternalMessage::decode(frame.freeze()),
        Err(Error::UnknownInternalReplicaLog(99))
    ));
}

#[test]
fn a_fence_answer_carries_where_the_replica_stands() {
    let ok = InternalMessage::FenceOk(FenceOk {
        correlation_id: 42,
        log_end: 120,
        commit_offset: 100,
        last_generation: 6,
    });
    let encoded = ok.encode().expect("encode");
    assert_eq!(encoded.len(), InternalHeader::LEN + 32);
    assert_eq!(InternalMessage::decode(encoded).expect("decode"), ok);
}

fn fence(incarnation: Option<u64>) -> InternalMessage {
    InternalMessage::Fence(Fence {
        correlation_id: 9,
        shard: ShardRef {
            incarnation,
            ..shard()
        },
        log: ReplicaLog::Stream,
    })
}

/// A request without an incarnation is its own kind, byte for byte; one with
/// an incarnation travels in an `Incarnated` envelope around it.
#[test]
fn an_incarnation_travels_in_an_envelope_around_the_request() {
    let plain = fence(None).encode().expect("encode");
    assert_eq!(
        InternalHeader::decode(&plain).expect("header").kind,
        Kind::Fence
    );

    let incarnated = fence(Some(77));
    let encoded = incarnated.encode().expect("encode");
    assert_eq!(
        InternalHeader::decode(&encoded).expect("header").kind,
        Kind::Incarnated
    );
    let mut expected = BytesMut::new();
    expected.put_u64(9);
    expected.put_u64(77);
    expected.put_u16(Kind::Fence as u16);
    expected.extend_from_slice(&plain[InternalHeader::LEN..]);
    assert_eq!(&encoded[InternalHeader::LEN..], &expected[..]);
    assert_eq!(
        InternalMessage::decode(encoded).expect("decode"),
        incarnated
    );
}

#[test]
fn an_envelope_must_hold_one_replication_request_with_its_own_correlation() {
    let wrap = |correlation: u64, kind: Kind, inner: &[u8]| {
        let mut body = BytesMut::new();
        body.put_u64(correlation);
        body.put_u64(77);
        body.put_u16(kind as u16);
        body.extend_from_slice(inner);
        let mut frame = BytesMut::new();
        InternalHeader {
            kind: Kind::Incarnated,
            length: body.len() as u32,
        }
        .encode(&mut frame);
        frame.extend_from_slice(&body);
        frame.freeze()
    };
    let fence_body = fence(None)
        .encode()
        .expect("encode")
        .slice(InternalHeader::LEN..);
    let hello_body = hello(None)
        .encode()
        .expect("encode")
        .slice(InternalHeader::LEN..);
    let nested = fence(Some(5))
        .encode()
        .expect("encode")
        .slice(InternalHeader::LEN..);

    assert!(InternalMessage::decode(wrap(9, Kind::Fence, &fence_body)).is_ok());
    assert!(InternalMessage::decode(wrap(8, Kind::Fence, &fence_body)).is_err());
    assert!(InternalMessage::decode(wrap(42, Kind::Hello, &hello_body)).is_err());
    assert!(InternalMessage::decode(wrap(9, Kind::Incarnated, &nested)).is_err());
}
