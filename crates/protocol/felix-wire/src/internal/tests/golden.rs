use super::*;

/// Byte-exact, so a layout change has to be deliberate. Every field is at a
/// known offset; a reordering or a width change fails here rather than in a
/// cluster.
#[test]
fn forward_publish_matches_its_golden_vector() {
    let encoded = forward().encode().expect("encode");
    let expected: Vec<u8> = [
        // header: magic "FLXI", version 1, kind 1, length
        &[0x46, 0x4C, 0x58, 0x49][..],
        &[0x00, 0x01][..],
        &[0x00, 0x01][..],
        &[0x00, 0x00, 0x00, 0x3A][..],
        // correlation_id 42
        &[0, 0, 0, 0, 0, 0, 0, 42][..],
        // "t1", "ns", "orders"
        &[0, 0, 0, 2][..],
        b"t1",
        &[0, 0, 0, 2][..],
        b"ns",
        &[0, 0, 0, 6][..],
        b"orders",
        // shard 3, generation 7, ack on-commit
        &[0, 0, 0, 3][..],
        &[0, 0, 0, 0, 0, 0, 0, 7][..],
        &[2][..],
        // two payloads
        &[0, 0, 0, 2][..],
        &[0, 0, 0, 1][..],
        b"a",
        &[0, 0, 0, 2][..],
        b"bb",
    ]
    .concat();
    assert_eq!(encoded.as_ref(), expected.as_slice());
}

#[test]
fn not_leader_matches_its_golden_vector() {
    let message = InternalMessage::NotLeader(NotLeader {
        correlation_id: 1,
        node_id: "b".to_string(),
        advertise_addr: "h:1".to_string(),
        generation: 2,
    });
    let expected: Vec<u8> = [
        &[0x46, 0x4C, 0x58, 0x49][..],
        &[0x00, 0x01][..],
        &[0x00, 0x04][..],
        &[0x00, 0x00, 0x00, 0x1C][..],
        &[0, 0, 0, 0, 0, 0, 0, 1][..],
        &[0, 0, 0, 1][..],
        b"b",
        &[0, 0, 0, 3][..],
        b"h:1",
        &[0, 0, 0, 0, 0, 0, 0, 2][..],
    ]
    .concat();
    assert_eq!(
        message.encode().expect("encode").as_ref(),
        expected.as_slice()
    );
}

#[test]
fn replicate_records_matches_its_golden_vector() {
    let encoded = replicate().encode().expect("encode");
    let expected: Vec<u8> = [
        // header: magic "FLXI", version 1, kind 7, length
        &[0x46, 0x4C, 0x58, 0x49][..],
        &[0x00, 0x01][..],
        &[0x00, 0x07][..],
        &[0x00, 0x00, 0x00, 0x49][..],
        // correlation_id 42
        &[0, 0, 0, 0, 0, 0, 0, 42][..],
        // "t1", "ns", "orders"
        &[0, 0, 0, 2][..],
        b"t1",
        &[0, 0, 0, 2][..],
        b"ns",
        &[0, 0, 0, 6][..],
        b"orders",
        // shard 3, generation 7
        &[0, 0, 0, 3][..],
        &[0, 0, 0, 0, 0, 0, 0, 7][..],
        // first_offset 100, checksum 0x01020304
        &[0, 0, 0, 0, 0, 0, 0, 100][..],
        &[0, 0, 0, 0, 0x01, 0x02, 0x03, 0x04][..],
        // two payloads
        &[0, 0, 0, 2][..],
        &[0, 0, 0, 1][..],
        b"a",
        &[0, 0, 0, 2][..],
        b"bb",
    ]
    .concat();
    assert_eq!(encoded.as_ref(), expected.as_slice());
}

#[test]
fn ping_and_pong_match_their_golden_vectors() {
    for (message, kind) in [
        (InternalMessage::Ping(Ping { correlation_id: 9 }), 38u8),
        (InternalMessage::Pong(Pong { correlation_id: 9 }), 39u8),
    ] {
        let expected: Vec<u8> = [
            &[0x46, 0x4C, 0x58, 0x49][..],
            &[0x00, 0x01][..],
            &[0x00, kind][..],
            // the correlation id is the whole body
            &[0x00, 0x00, 0x00, 0x08][..],
            &[0, 0, 0, 0, 0, 0, 0, 9][..],
        ]
        .concat();
        assert_eq!(
            message.encode().expect("encode").as_ref(),
            expected.as_slice()
        );
    }
}
