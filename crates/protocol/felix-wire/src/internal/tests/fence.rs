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
    });
    assert_eq!(answer.kind(), Kind::HelloCapableOk);
    let decoded = InternalMessage::decode(answer.encode().expect("encode")).expect("decode");
    assert_eq!(
        decoded, answer,
        "an answer of no capabilities must still be told apart from an old peer's",
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
