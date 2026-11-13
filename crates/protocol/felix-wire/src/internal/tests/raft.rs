use super::*;

fn call() -> InternalMessage {
    InternalMessage::Raft(RaftCall {
        correlation_id: 9,
        body: Bytes::from_static(b"\x01\x02\x03"),
    })
}

#[test]
fn raft_messages_match_their_golden_vectors() {
    let answer = InternalMessage::RaftAnswer(RaftAnswer {
        correlation_id: 9,
        body: Bytes::from_static(b"\x01\x02\x03"),
    });
    for (message, kind) in [(call(), 57u8), (answer, 58u8)] {
        let expected: Vec<u8> = [
            &[0x46, 0x4C, 0x58, 0x49][..],
            &[0x00, 0x01][..],
            &[0x00, kind][..],
            &[0x00, 0x00, 0x00, 0x0B][..],
            &[0, 0, 0, 0, 0, 0, 0, 9][..],
            // the body, as given
            &[1, 2, 3][..],
        ]
        .concat();
        let encoded = message.encode().expect("encode");
        assert_eq!(encoded.as_ref(), expected.as_slice());
        assert_eq!(InternalMessage::decode(encoded).expect("decode"), message);
    }
}

#[test]
fn a_raft_body_may_be_empty_but_the_correlation_id_may_not() {
    let empty = InternalMessage::RaftAnswer(RaftAnswer {
        correlation_id: 9,
        body: Bytes::new(),
    });
    let encoded = empty.encode().expect("encode");
    assert_eq!(InternalMessage::decode(encoded).expect("decode"), empty);

    let short = frame(Kind::Raft, &[0, 0, 0, 0, 0, 0, 9]);
    assert!(InternalMessage::decode(short).is_err());
}

#[test]
fn raft_messages_need_the_raft_capability() {
    let answer = InternalMessage::RaftAnswer(RaftAnswer {
        correlation_id: 9,
        body: Bytes::new(),
    });
    for message in [call(), answer] {
        assert_eq!(message.required_capability(), Some(PeerCapabilities::RAFT));
    }
    // A bit of its own, not one an older peer already offers.
    assert_eq!(PeerCapabilities::RAFT.bits(), 1 << 23);
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
