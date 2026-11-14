//! The seal's messages: round trips, the capability they need, and the
//! layouts an older peer must never be sent.

use super::*;

const SEALER: [u8; 16] = [7; 16];

fn fence() -> Fence {
    Fence {
        correlation_id: 9,
        shard: shard(),
        log: ReplicaLog::Stream,
        election: None,
    }
}

fn ok() -> FenceOk {
    FenceOk {
        correlation_id: 9,
        log_end: 40,
        commit_offset: 38,
        last_generation: 6,
        kept_ballot: true,
        attestation: None,
    }
}

fn batch() -> ReplicateRecords {
    ReplicateRecords {
        correlation_id: 11,
        shard: ShardRef {
            incarnation: Some(5),
            ..shard()
        },
        first_offset: 3,
        checksum: 0x0102_0304,
        payloads: vec![Bytes::from_static(b"a")],
        marks: Vec::new(),
        publishers: Vec::new(),
        commit_offset: Some(2),
        generations: None,
        times: None,
        headers: None,
        writers: None,
        set_generation: Some(4),
    }
}

fn messages() -> Vec<InternalMessage> {
    vec![
        InternalMessage::SealFence(SealFence {
            fence: fence(),
            sealer: SEALER,
        }),
        InternalMessage::SealFenceOk(SealFenceOk {
            ok: ok(),
            starts: SealStarts {
                log: 3,
                dead_letters: 1,
                cursors: 0,
            },
        }),
        InternalMessage::SealAck(SealAck {
            correlation_id: 10,
            shard: shard(),
            sealer: SEALER,
            offset: 40,
            timestamp_micros: 1_700_000,
            payload: Bytes::from_static(b"the seal's base"),
        }),
        InternalMessage::SealAckOk(SealAckOk {
            correlation_id: 10,
            copy: [8; 16],
        }),
        InternalMessage::FromCopy(FromCopy {
            copy: SEALER,
            message: Box::new(InternalMessage::ReplicateRecords(batch())),
        }),
    ]
}

#[test]
fn seal_messages_round_trip() {
    for message in messages() {
        let frame = message.encode().expect("encode");
        assert_eq!(InternalMessage::decode(frame).expect("decode"), message);
    }
}

/// **The seal's requests carry the stream's incarnation**, so a seal for a
/// deleted stream never promises its recreated namesake.
#[test]
fn seal_requests_carry_the_incarnation() {
    let incarnated = ShardRef {
        incarnation: Some(5),
        ..shard()
    };
    for message in messages() {
        let message = match message {
            InternalMessage::SealFence(mut m) => {
                m.fence.shard = incarnated.clone();
                InternalMessage::SealFence(m)
            }
            InternalMessage::SealAck(mut m) => {
                m.shard = incarnated.clone();
                InternalMessage::SealAck(m)
            }
            _ => continue,
        };
        let frame = message.encode().expect("encode");
        assert_eq!(InternalMessage::decode(frame).expect("decode"), message);
    }
}

#[test]
fn seal_requests_need_the_switch_seal_capability() {
    for message in messages() {
        let needed = message.required_capability();
        match message {
            InternalMessage::SealFence(_) | InternalMessage::SealAck(_) => {
                assert_eq!(needed, Some(PeerCapabilities::SWITCH_SEAL));
            }
            // A stamped batch also needs what the batch inside needs.
            InternalMessage::FromCopy(_) => assert_eq!(
                needed,
                Some(PeerCapabilities::SWITCH_SEAL.union(PeerCapabilities::ELECTION))
            ),
            _ => {}
        }
    }
    assert_eq!(PeerCapabilities::SWITCH_SEAL.bits(), 1 << 24);
}

#[test]
fn a_stamped_batch_is_the_plain_batch_behind_the_copy_id() {
    let plain = InternalMessage::ReplicateRecords(batch())
        .encode()
        .expect("encode");
    let stamped = InternalMessage::FromCopy(FromCopy {
        copy: SEALER,
        message: Box::new(InternalMessage::ReplicateRecords(batch())),
    })
    .encode()
    .expect("encode");
    let header = InternalHeader::decode(&stamped).expect("header");
    assert_eq!(header.kind, Kind::FromCopy);
    let body = &stamped[InternalHeader::LEN..];
    assert_eq!(&body[..8], &11u64.to_be_bytes());
    assert_eq!(&body[8..24], &SEALER);
    let inner = InternalHeader::decode(&plain).expect("header");
    assert_eq!(&body[24..26], &(inner.kind as u16).to_be_bytes());
    assert_eq!(&body[26..], &plain[InternalHeader::LEN..]);
}

/// The copy id's 26 bytes count toward the frame limit: a stamped batch
/// fills the frame exactly, and one byte more is refused before it is sent,
/// though the batch alone would still fit.
#[test]
fn a_stamped_batch_never_exceeds_the_frame_limit() {
    let sized = |len: usize| ReplicateRecords {
        payloads: vec![Bytes::from(vec![7u8; len])],
        ..batch()
    };
    let stamped = |len: usize| {
        InternalMessage::FromCopy(FromCopy {
            copy: SEALER,
            message: Box::new(InternalMessage::ReplicateRecords(sized(len))),
        })
    };
    let plain = InternalMessage::ReplicateRecords(sized(0))
        .encode()
        .expect("encode");
    let empty = stamped(0).encode().expect("encode");
    assert_eq!(empty.len() - plain.len(), FROM_COPY_BYTES as usize);
    let at_limit = MAX_BODY_BYTES as usize - (empty.len() - InternalHeader::LEN);

    let message = stamped(at_limit);
    let encoded = message.encode().expect("a stamped batch at the limit");
    assert_eq!(
        InternalHeader::decode(&encoded).expect("header").length,
        MAX_BODY_BYTES
    );
    assert_eq!(InternalMessage::decode(encoded).expect("decode"), message);

    assert!(matches!(
        stamped(at_limit + 1).encode(),
        Err(Error::FrameTooLarge)
    ));
    InternalMessage::ReplicateRecords(sized(at_limit + 1))
        .encode()
        .expect("the batch alone fits");
}

#[test]
fn a_fence_without_the_seal_is_sent_as_before() {
    // The seal rides its own kinds, so a plain fence and its answer keep
    // the layouts an older peer reads.
    let plain = InternalMessage::Fence(fence()).encode().expect("encode");
    assert_eq!(
        InternalHeader::decode(&plain).expect("header").kind,
        Kind::Fence
    );
    let sealed = InternalMessage::SealFence(SealFence {
        fence: fence(),
        sealer: SEALER,
    })
    .encode()
    .expect("encode");
    assert_eq!(
        &sealed[InternalHeader::LEN..sealed.len() - 16],
        &plain[InternalHeader::LEN..]
    );
}

#[test]
fn malformed_seal_frames_are_refused() {
    // Election fields and an attestation have no seal layout.
    let mut elected = fence();
    elected.election = Some(FenceElection {
        candidate: true,
        set: ReplicaSetView {
            generation: 1,
            members: Vec::new(),
        },
        leaving: None,
        earlier: Vec::new(),
    });
    assert!(
        InternalMessage::SealFence(SealFence {
            fence: elected,
            sealer: SEALER,
        })
        .encode()
        .is_err()
    );
    let mut attested = ok();
    attested.attestation = Some(FenceAttestation {
        kept_from: 0,
        trusted: true,
        by_record: false,
    });
    assert!(
        InternalMessage::SealFenceOk(SealFenceOk {
            ok: attested,
            starts: SealStarts::default(),
        })
        .encode()
        .is_err()
    );
    // A stamp inside a stamp.
    let twice = InternalMessage::FromCopy(FromCopy {
        copy: SEALER,
        message: Box::new(InternalMessage::FromCopy(FromCopy {
            copy: SEALER,
            message: Box::new(InternalMessage::ReplicateRecords(batch())),
        })),
    });
    assert!(twice.encode().is_err());
    let mut body = BytesMut::new();
    body.put_u64(1);
    body.put_slice(&SEALER);
    body.put_u16(Kind::FromCopy as u16);
    assert!(InternalMessage::decode(frame(Kind::FromCopy, &body)).is_err());
    // A correlation id that is not the inner request's.
    let stamped = messages().pop().expect("stamped").encode().expect("encode");
    let mut body = stamped[InternalHeader::LEN..].to_vec();
    body[7] ^= 1;
    assert!(InternalMessage::decode(frame(Kind::FromCopy, &body)).is_err());
    // An unknown flag in the seal fence's answer, and every truncation.
    let answer = messages()[1].encode().expect("encode");
    let mut body = answer[InternalHeader::LEN..].to_vec();
    body[32] = 2;
    assert!(InternalMessage::decode(frame(Kind::SealFenceOk, &body)).is_err());
    for message in messages() {
        let encoded = message.encode().expect("encode");
        let kind = InternalHeader::decode(&encoded).expect("header").kind;
        let body = &encoded[InternalHeader::LEN..];
        // A seal ack's payload runs to the end, so only its fixed part can
        // be cut short.
        let shortest = if kind == Kind::SealAck {
            body.len() - 15
        } else {
            body.len()
        };
        for cut in 0..shortest {
            assert!(
                InternalMessage::decode(frame(kind, &body[..cut])).is_err(),
                "{kind:?} cut at {cut}"
            );
        }
    }
}

fn frame(kind: Kind, body: &[u8]) -> Bytes {
    let mut frame = BytesMut::new();
    InternalHeader {
        kind,
        length: body.len() as u32,
    }
    .encode(&mut frame);
    frame.extend_from_slice(body);
    frame.freeze()
}
