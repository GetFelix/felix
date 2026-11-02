use super::*;

#[test]
fn every_byte_is_either_a_code_or_not() {
    let mut count = 0;
    for byte in 0..=255u8 {
        if let Some(code) = ReasonCode::from_code(byte) {
            assert_eq!(code.code(), byte);
            count += 1;
        }
    }
    assert_eq!(count, 43);
}

#[test]
fn success_is_below_0x80() {
    assert!(ReasonCode::Success.is_success());
    assert!(ReasonCode::GrantedQos2.is_success());
    assert!(ReasonCode::ContinueAuthentication.is_success());
    assert!(!ReasonCode::UnspecifiedError.is_success());
    assert!(!ReasonCode::WildcardSubscriptionsNotSupported.is_success());
}

#[test]
fn v311_connack_codes() {
    for code in 0..=5u8 {
        let reason = ReasonCode::from_v311_connack(code).unwrap();
        assert_eq!(reason.to_v311_connack(), Some(code));
    }
    assert_eq!(ReasonCode::from_v311_connack(6), None);
    assert_eq!(ReasonCode::ServerBusy.to_v311_connack(), None);
}

/// The specification's per-packet tables, written out in full, so a code added
/// to `permitted_in` by mistake shows up here.
#[test]
fn permitted_codes_match_the_specification() {
    let table: &[(PacketType, &[u8])] = &[
        (
            PacketType::ConnAck,
            &[
                0x00, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8C, 0x90,
                0x95, 0x97, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9F,
            ],
        ),
        (
            PacketType::PubAck,
            &[0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99],
        ),
        (
            PacketType::PubRec,
            &[0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99],
        ),
        (PacketType::PubRel, &[0x00, 0x92]),
        (PacketType::PubComp, &[0x00, 0x92]),
        (
            PacketType::SubAck,
            &[
                0x00, 0x01, 0x02, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2,
            ],
        ),
        (
            PacketType::UnsubAck,
            &[0x00, 0x11, 0x80, 0x83, 0x87, 0x8F, 0x91],
        ),
        (
            PacketType::Disconnect,
            &[
                0x00, 0x04, 0x80, 0x81, 0x82, 0x83, 0x87, 0x89, 0x8B, 0x8D, 0x8E, 0x8F, 0x90, 0x93,
                0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1,
                0xA2,
            ],
        ),
        (PacketType::Auth, &[0x00, 0x18, 0x19]),
    ];
    for (packet, allowed) in table {
        let got: Vec<u8> = (0..=255u8)
            .filter(|b| ReasonCode::from_code(*b).is_some_and(|r| r.permitted_in(*packet)))
            .collect();
        assert_eq!(&got, allowed, "{packet}");
    }
    // Packets without reason codes permit none.
    for packet in [
        PacketType::Connect,
        PacketType::Publish,
        PacketType::PingReq,
    ] {
        assert!(!ReasonCode::Success.permitted_in(packet));
    }
}
