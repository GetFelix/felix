//! Every way a packet can be malformed or a protocol error that the codec can
//! see from the packet alone. Each case cites the specification statement it
//! comes from: `[MQTT-x.y.z-n]` for 3.1.1 and 5.0, which share most numbers.

use bytes::BytesMut;
use felix_mqtt_wire::{DecodeError, Limits, Packet, PacketType, ReasonCode, TopicError, Version};

const V3: Option<Version> = Some(Version::V311);
const V5: Option<Version> = Some(Version::V5);

fn decode(version: Option<Version>, bytes: &[u8]) -> Result<Option<Packet>, DecodeError> {
    Packet::decode(&mut BytesMut::from(bytes), version, &Limits::default())
}

/// The error for `bytes`, which must be a whole packet.
fn refused(version: Option<Version>, bytes: &[u8]) -> DecodeError {
    match decode(version, bytes) {
        Err(e) => e,
        other => panic!("expected a refusal, got {other:?} for {bytes:02x?}"),
    }
}

fn accepted(version: Option<Version>, bytes: &[u8]) -> Packet {
    decode(version, bytes)
        .unwrap_or_else(|e| panic!("refused {bytes:02x?}: {e}"))
        .expect("whole packet")
}

/// A packet from its first byte and body.
fn packet(first: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![first];
    let mut n = body.len();
    loop {
        let byte = (n % 128) as u8;
        n /= 128;
        out.push(if n > 0 { byte | 0x80 } else { byte });
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(body);
    out
}

fn string(s: &[u8]) -> Vec<u8> {
    let mut out = (s.len() as u16).to_be_bytes().to_vec();
    out.extend_from_slice(s);
    out
}

fn cat(parts: &[&[u8]]) -> Vec<u8> {
    parts.concat()
}

/// A property block: its length, then `props`.
fn props(props: &[u8]) -> Vec<u8> {
    cat(&[&[props.len() as u8], props])
}

// ---- fixed header

#[test]
fn reserved_packet_types() {
    assert_eq!(
        refused(V3, &[0x00, 0x00]),
        DecodeError::InvalidPacketType(0)
    );
    assert_eq!(
        refused(V5, &[0x00, 0x00]),
        DecodeError::InvalidPacketType(0)
    );
    // AUTH does not exist in 3.1.1, so type 15 is reserved there.
    assert_eq!(
        refused(V3, &[0xF0, 0x00]),
        DecodeError::InvalidPacketType(15)
    );
    assert_eq!(
        refused(None, &[0x00, 0x00]),
        DecodeError::InvalidPacketType(0)
    );
}

#[test]
fn fixed_flags_must_match_the_specification() {
    // [MQTT-2.1.3-1]: reserved flag bits must be as the table says.
    let wrong = [
        (0x11, PacketType::Connect, None),
        (0x21, PacketType::ConnAck, V3),
        (0x41, PacketType::PubAck, V3),
        (0x51, PacketType::PubRec, V3),
        (0x60, PacketType::PubRel, V3), // [MQTT-3.6.1-1]
        (0x71, PacketType::PubComp, V3),
        (0x80, PacketType::Subscribe, V3), // [MQTT-3.8.1-1]
        (0x91, PacketType::SubAck, V3),
        (0xA0, PacketType::Unsubscribe, V3), // [MQTT-3.10.1-1]
        (0xB1, PacketType::UnsubAck, V3),
        (0xC1, PacketType::PingReq, V3),
        (0xD1, PacketType::PingResp, V3),
        (0xE1, PacketType::Disconnect, V3), // [MQTT-3.14.1-1]
        (0xF1, PacketType::Auth, V5),
    ];
    for (first, kind, version) in wrong {
        let e = refused(version, &[first, 0x00]);
        assert!(
            matches!(e, DecodeError::InvalidFlags { packet, .. } if packet == kind),
            "{first:#04x}: {e}"
        );
    }
}

#[test]
fn remaining_length_syntax() {
    // [MQTT-1.5.5-1]: shortest form only, and four bytes at most.
    assert_eq!(
        refused(V3, &[0xC0, 0x80, 0x00]),
        DecodeError::MalformedLength
    );
    assert_eq!(
        refused(V3, &[0x30, 0xFF, 0x80, 0x00]),
        DecodeError::MalformedLength
    );
    assert_eq!(
        refused(V3, &[0x30, 0x80, 0x80, 0x80, 0x80, 0x01]),
        DecodeError::MalformedLength
    );
    // The fifth byte is never read: this is already an error.
    assert_eq!(
        refused(V3, &[0x30, 0xFF, 0xFF, 0xFF, 0xFF]),
        DecodeError::MalformedLength
    );
}

#[test]
fn oversized_packets_are_refused_before_the_body_arrives() {
    // Only the fixed header is present; the 2 MiB it announces is never waited for.
    let header = [0x30, 0x80, 0x80, 0x80, 0x01];
    assert!(matches!(
        refused(V3, &header),
        DecodeError::PacketTooLarge { size, max } if size == 2_097_157 && max == 1_048_576
    ));
    // The limit is on the whole packet, fixed header included.
    let mut limits = Limits::default();
    limits.max_packet_size = 10;
    let mut body = vec![0, 1, b't'];
    body.resize(8, 0);
    let fits = packet(0x30, &body); // 10 bytes in all
    assert!(Packet::decode(&mut BytesMut::from(&fits[..]), V3, &limits).is_ok());
    body.push(0);
    let over = packet(0x30, &body);
    assert!(matches!(
        Packet::decode(&mut BytesMut::from(&over[..]), V3, &limits),
        Err(DecodeError::PacketTooLarge { size: 11, max: 10 })
    ));
}

#[test]
fn only_connect_may_open_a_connection() {
    // [MQTT-3.1.0-1]
    assert_eq!(
        refused(None, &[0xC0, 0x00]),
        DecodeError::FirstPacketNotConnect(PacketType::PingReq)
    );
    assert_eq!(
        refused(None, &packet(0x30, &cat(&[&string(b"t")]))),
        DecodeError::FirstPacketNotConnect(PacketType::Publish)
    );
}

#[test]
fn incomplete_input_waits_and_consumes_nothing() {
    let whole = packet(0x30, &cat(&[&string(b"topic"), b"payload"]));
    for cut in 0..whole.len() {
        let mut src = BytesMut::from(&whole[..cut]);
        assert_eq!(
            Packet::decode(&mut src, V3, &Limits::default()).unwrap(),
            None,
            "cut at {cut}"
        );
        assert_eq!(src.len(), cut, "consumed bytes at {cut}");
    }
}

#[test]
fn a_body_shorter_than_its_fields_is_truncated() {
    // The remaining length is honest but the string inside claims more.
    let bytes = packet(0x30, &[0x00, 0x09, b't']);
    assert_eq!(refused(V3, &bytes), DecodeError::Truncated);
    // A PUBLISH at QoS 1 with no room for the identifier.
    assert_eq!(
        refused(V3, &packet(0x32, &string(b"t"))),
        DecodeError::Truncated
    );
}

#[test]
fn bytes_after_the_last_field_are_refused() {
    assert_eq!(
        refused(V3, &[0xC0, 0x01, 0x00]),
        DecodeError::TrailingBytes(1)
    );
    assert_eq!(
        refused(V3, &[0xD0, 0x02, 0x00, 0x00]),
        DecodeError::TrailingBytes(2)
    );
    assert_eq!(
        refused(V3, &[0x40, 0x03, 0x00, 0x01, 0x00]),
        DecodeError::TrailingBytes(1)
    );
    assert_eq!(
        refused(V3, &[0xE0, 0x01, 0x00]),
        DecodeError::TrailingBytes(1)
    );
}

#[test]
fn pipelined_packets_decode_one_at_a_time() {
    let mut src = BytesMut::new();
    src.extend_from_slice(&[0xC0, 0x00, 0xD0, 0x00, 0xC0]);
    let limits = Limits::default();
    assert_eq!(
        Packet::decode(&mut src, V3, &limits).unwrap(),
        Some(Packet::PingReq)
    );
    assert_eq!(
        Packet::decode(&mut src, V3, &limits).unwrap(),
        Some(Packet::PingResp)
    );
    assert_eq!(Packet::decode(&mut src, V3, &limits).unwrap(), None);
    assert_eq!(src.len(), 1);
}

// ---- CONNECT

fn connect_body(level: u8, flags: u8, rest: &[u8]) -> Vec<u8> {
    cat(&[&string(b"MQTT"), &[level, flags, 0x00, 0x3C], rest])
}

fn connect(level: u8, flags: u8, rest: &[u8]) -> Vec<u8> {
    packet(0x10, &connect_body(level, flags, rest))
}

#[test]
fn connect_protocol_name_and_level() {
    let body = |name: &[u8], level: u8| {
        packet(
            0x10,
            &cat(&[&string(name), &[level, 0x02, 0x00, 0x3C], &string(b"c")]),
        )
    };
    // [MQTT-3.1.2-1]
    assert_eq!(
        refused(None, &body(b"MQTX", 4)),
        DecodeError::InvalidProtocolName
    );
    assert_eq!(
        refused(None, &body(b"mqtt", 4)),
        DecodeError::InvalidProtocolName
    );
    assert_eq!(
        refused(None, &body(b"", 4)),
        DecodeError::InvalidProtocolName
    );
    // [MQTT-3.1.2-2]: levels other than 4 and 5, and 3.1's own name.
    assert_eq!(
        refused(None, &body(b"MQTT", 3)),
        DecodeError::UnsupportedProtocolVersion(3)
    );
    assert_eq!(
        refused(None, &body(b"MQTT", 6)),
        DecodeError::UnsupportedProtocolVersion(6)
    );
    assert_eq!(
        refused(None, &body(b"MQTT", 0)),
        DecodeError::UnsupportedProtocolVersion(0)
    );
    assert_eq!(
        refused(None, &body(b"MQIsdp", 3)),
        DecodeError::UnsupportedProtocolVersion(3)
    );
    accepted(None, &body(b"MQTT", 4));
    accepted(
        None,
        &packet(
            0x10,
            &cat(&[
                &string(b"MQTT"),
                &[5, 0x02, 0x00, 0x3C, 0x00],
                &string(b"c"),
            ]),
        ),
    );
}

#[test]
fn connect_flags() {
    let id = string(b"c");
    // [MQTT-3.1.2-3]: the reserved bit.
    for level in [4, 5] {
        let rest = cat(&[if level == 5 { &[0u8][..] } else { &[][..] }, &id]);
        assert!(matches!(
            refused(None, &connect(level, 0x03, &rest)),
            DecodeError::InvalidConnectFlags(0x03)
        ));
    }
    // [MQTT-3.1.2-11]: without a will, will QoS must be 0; [MQTT-3.1.2-13]: and retain.
    assert!(matches!(
        refused(None, &connect(4, 0x0A, &id)),
        DecodeError::InvalidConnectFlags(_)
    ));
    assert!(matches!(
        refused(None, &connect(4, 0x22, &id)),
        DecodeError::InvalidConnectFlags(_)
    ));
    // [MQTT-3.1.2-14]: will QoS 3.
    let will = cat(&[&id, &string(b"w/t"), &string(b"m")]);
    assert_eq!(
        refused(None, &connect(4, 0x1E, &will)),
        DecodeError::InvalidQos
    );
    // [MQTT-3.1.2-22]: a password needs a user name under 3.1.1, not under 5.0.
    let pw = cat(&[&id, &string(b"secret")]);
    assert!(matches!(
        refused(None, &connect(4, 0x42, &pw)),
        DecodeError::InvalidConnectFlags(0x42)
    ));
    accepted(None, &connect(5, 0x42, &cat(&[&[0u8], &pw])));
}

#[test]
fn connect_strings() {
    let level4 = |rest: &[u8]| connect(4, 0x02, rest);
    // [MQTT-1.5.4-1]: UTF-8 that is not well formed, including surrogates and overlong forms.
    for bad in [
        &[0xFF][..],
        &[0xC0, 0x80],
        &[0xED, 0xA0, 0x80],
        &[0xE2, 0x82],
    ] {
        assert_eq!(
            refused(None, &level4(&string(bad))),
            DecodeError::InvalidString(felix_mqtt_wire::StringError::InvalidUtf8)
        );
    }
    // [MQTT-1.5.4-2]: U+0000.
    assert_eq!(
        refused(None, &level4(&string(b"a\0b"))),
        DecodeError::InvalidString(felix_mqtt_wire::StringError::ContainsNull)
    );
    // The client identifier limit.
    let long = vec![b'x'; 257];
    assert!(matches!(
        refused(None, &level4(&string(&long))),
        DecodeError::LimitExceeded { max: 256, .. }
    ));
    accepted(None, &level4(&string(&long[..256])));
    // [MQTT-3.1.3-5]: ids of up to 23 alphanumerics are always valid; empty is the session's call.
    accepted(None, &level4(&string(b"")));
}

#[test]
fn connect_will_topic_and_fields() {
    let id = string(b"c");
    let with_will = |topic: &[u8]| connect(4, 0x06, &cat(&[&id, &string(topic), &string(b"m")]));
    // [MQTT-3.1.3-10]: the will topic is a topic name, so no wildcards.
    for topic in [&b"a/+"[..], b"#", b"a/b#"] {
        assert_eq!(
            refused(None, &with_will(topic)),
            DecodeError::InvalidTopicName(TopicError::Wildcard)
        );
    }
    assert_eq!(
        refused(None, &with_will(b"")),
        DecodeError::InvalidTopicName(TopicError::Empty)
    );
    // Truncated: the flags promise a user name that is not there.
    assert_eq!(
        refused(None, &connect(4, 0x82, &id)),
        DecodeError::Truncated
    );
    // Truncated will payload.
    let cut = cat(&[&id, &string(b"w"), &[0x00, 0x05, b'x']]);
    assert_eq!(
        refused(None, &connect(4, 0x06, &cut)),
        DecodeError::Truncated
    );
}

#[test]
fn connect_v5_properties() {
    let id = string(b"c");
    let v5 = |properties: &[u8]| connect(5, 0x02, &cat(&[&props(properties), &id]));
    // [MQTT-3.1.2-25]: a property the packet may not carry (Topic Alias).
    assert!(matches!(
        refused(None, &v5(&[0x23, 0x00, 0x01])),
        DecodeError::PropertyNotAllowed { id: 0x23, .. }
    ));
    // Will Delay Interval belongs to the will, not to CONNECT.
    assert!(matches!(
        refused(None, &v5(&[0x18, 0, 0, 0, 1])),
        DecodeError::PropertyNotAllowed { id: 0x18, .. }
    ));
    // [MQTT-3.1.2-26]: duplicate Session Expiry Interval.
    assert_eq!(
        refused(None, &v5(&[0x11, 0, 0, 0, 1, 0x11, 0, 0, 0, 2])),
        DecodeError::DuplicateProperty(0x11)
    );
    // [MQTT-3.1.2-29] and [MQTT-3.1.2-30]: zero Receive Maximum, zero Maximum Packet Size.
    assert_eq!(
        refused(None, &v5(&[0x21, 0, 0])),
        DecodeError::InvalidPropertyValue(0x21)
    );
    assert_eq!(
        refused(None, &v5(&[0x27, 0, 0, 0, 0])),
        DecodeError::InvalidPropertyValue(0x27)
    );
    // [MQTT-3.1.2-28] and [MQTT-3.1.2-29]: Request Problem/Response Information are 0 or 1.
    assert_eq!(
        refused(None, &v5(&[0x17, 2])),
        DecodeError::InvalidPropertyValue(0x17)
    );
    assert_eq!(
        refused(None, &v5(&[0x19, 2])),
        DecodeError::InvalidPropertyValue(0x19)
    );
    // [MQTT-3.1.2-30]: Authentication Data needs an Authentication Method.
    assert_eq!(
        refused(None, &v5(&[0x16, 0, 1, 7])),
        DecodeError::MissingProperty(0x15)
    );
    // An identifier the specification does not define.
    assert_eq!(
        refused(None, &v5(&[0x30, 0])),
        DecodeError::UnknownProperty(0x30)
    );
    // A property that runs past the property length.
    assert_eq!(refused(None, &v5(&[0x11, 0, 0])), DecodeError::Truncated);
    // The property length itself runs past the packet.
    assert_eq!(
        refused(None, &connect(5, 0x02, &[0x7F, 0x11])),
        DecodeError::Truncated
    );
}

#[test]
fn connect_v5_will_properties() {
    let id = string(b"c");
    let rest = |will_props: &[u8]| {
        cat(&[
            &props(&[]),
            &id,
            &props(will_props),
            &string(b"w/t"),
            &string(b"m"),
        ])
    };
    accepted(None, &connect(5, 0x06, &rest(&[0x18, 0, 0, 0, 9, 0x01, 1])));
    // Session Expiry Interval is for CONNECT, not the will.
    assert!(matches!(
        refused(None, &connect(5, 0x06, &rest(&[0x11, 0, 0, 0, 1]))),
        DecodeError::PropertyNotAllowed { id: 0x11, .. }
    ));
    // [MQTT-3.1.3-13]: a Payload Format Indicator other than 0 or 1.
    assert_eq!(
        refused(None, &connect(5, 0x06, &rest(&[0x01, 2]))),
        DecodeError::InvalidPropertyValue(0x01)
    );
    // [MQTT-3.3.2-14]: a wildcard in the Response Topic.
    let mut response = vec![0x08, 0, 3];
    response.extend_from_slice(b"a/#");
    assert_eq!(
        refused(None, &connect(5, 0x06, &rest(&response))),
        DecodeError::InvalidPropertyValue(0x08)
    );
}

// ---- CONNACK

#[test]
fn connack() {
    // [MQTT-3.2.2-1]: bits 7 to 1 of the acknowledge flags are reserved.
    assert!(matches!(
        refused(V3, &[0x20, 0x02, 0x02, 0x00]),
        DecodeError::InvalidConnectFlags(0x02)
    ));
    // [MQTT-3.2.2-4]: session present with a refusal.
    assert!(matches!(
        refused(V3, &[0x20, 0x02, 0x01, 0x05]),
        DecodeError::InvalidConnectFlags(0x01)
    ));
    assert!(matches!(
        refused(V5, &[0x20, 0x03, 0x01, 0x87, 0x00]),
        DecodeError::InvalidConnectFlags(0x01)
    ));
    // 3.1.1 return codes stop at 5.
    assert!(matches!(
        refused(V3, &[0x20, 0x02, 0x00, 0x06]),
        DecodeError::InvalidReasonCode {
            packet: PacketType::ConnAck,
            code: 6
        }
    ));
    // A reason code another packet owns, and one nothing defines.
    for code in [0x01, 0x10, 0x91, 0x03] {
        assert!(matches!(
            refused(V5, &[0x20, 0x03, 0x00, code, 0x00]),
            DecodeError::InvalidReasonCode {
                packet: PacketType::ConnAck,
                ..
            }
        ));
    }
    // 5.0 always carries a property length.
    assert_eq!(
        refused(V5, &[0x20, 0x02, 0x00, 0x00]),
        DecodeError::Truncated
    );
    assert_eq!(refused(V3, &[0x20, 0x01, 0x00]), DecodeError::Truncated);
    // Properties that only CONNECT may carry.
    assert!(matches!(
        refused(V5, &[0x20, 0x05, 0x00, 0x00, 0x02, 0x17, 0x01]),
        DecodeError::PropertyNotAllowed { id: 0x17, .. }
    ));
    // [MQTT-3.2.2-14]: Maximum QoS is 0 or 1.
    assert_eq!(
        refused(V5, &[0x20, 0x05, 0x00, 0x00, 0x02, 0x24, 0x02]),
        DecodeError::InvalidPropertyValue(0x24)
    );
}

// ---- PUBLISH

fn publish(flags: u8, topic: &[u8], rest: &[u8]) -> Vec<u8> {
    packet(0x30 | flags, &cat(&[&string(topic), rest]))
}

#[test]
fn publish_flags() {
    // [MQTT-3.3.1-4]: QoS 3.
    assert_eq!(
        refused(V3, &publish(0b0110, b"t", &[0, 1])),
        DecodeError::InvalidQos
    );
    // [MQTT-3.3.1-2]: DUP with QoS 0.
    assert_eq!(
        refused(V3, &publish(0b1000, b"t", &[])),
        DecodeError::DupOnQos0
    );
    // [MQTT-2.2.1-3]: a QoS above 0 needs a non-zero identifier.
    assert_eq!(
        refused(V3, &publish(0b0010, b"t", &[0, 0])),
        DecodeError::ZeroPacketId
    );
    assert_eq!(
        refused(V3, &publish(0b0100, b"t", &[0, 0])),
        DecodeError::ZeroPacketId
    );
}

#[test]
fn publish_topic() {
    let t = |topic: &[u8]| refused(V3, &publish(0, topic, &[]));
    // [MQTT-3.3.2-2]: no wildcards in a topic name.
    assert_eq!(
        t(b"a/+/b"),
        DecodeError::InvalidTopicName(TopicError::Wildcard)
    );
    assert_eq!(
        t(b"a/#"),
        DecodeError::InvalidTopicName(TopicError::Wildcard)
    );
    // [MQTT-3.3.2-1]: well-formed UTF-8, no U+0000.
    assert!(matches!(t(b"a\0"), DecodeError::InvalidString(_)));
    assert!(matches!(t(&[0xFF, 0xFE]), DecodeError::InvalidString(_)));
    // [MQTT-4.7.3-1]: a topic is at least one character under 3.1.1.
    assert_eq!(t(b""), DecodeError::InvalidTopicName(TopicError::Empty));
    // 5.0 allows it only with a Topic Alias.
    assert_eq!(
        refused(V5, &publish(0, b"", &[0x00])),
        DecodeError::InvalidTopicName(TopicError::Empty)
    );
    accepted(V5, &publish(0, b"", &props(&[0x23, 0, 1])));
    // Control characters and non-characters.
    assert_eq!(
        t(b"a\x01b"),
        DecodeError::InvalidTopicName(TopicError::DiscouragedCharacter)
    );
    assert_eq!(
        t(b"a\x7Fb"),
        DecodeError::InvalidTopicName(TopicError::DiscouragedCharacter)
    );
    assert_eq!(
        t("a\u{FFFF}".as_bytes()),
        DecodeError::InvalidTopicName(TopicError::DiscouragedCharacter)
    );
    assert_eq!(
        t("a\u{FDD0}".as_bytes()),
        DecodeError::InvalidTopicName(TopicError::DiscouragedCharacter)
    );
    // The listener's bounds: 1024 bytes and 16 levels.
    let long = vec![b'a'; 1025];
    assert_eq!(
        t(&long),
        DecodeError::InvalidTopicName(TopicError::TooLong { max: 1024 })
    );
    accepted(V3, &publish(0, &long[..1024], &[]));
    let deep = "a/".repeat(16) + "a";
    assert_eq!(
        t(deep.as_bytes()),
        DecodeError::InvalidTopicName(TopicError::TooManyLevels { max: 16 })
    );
    accepted(
        V3,
        &publish(0, format!("{}a", "a/".repeat(15)).as_bytes(), &[]),
    );
}

#[test]
fn publish_v5_properties() {
    let p = |properties: &[u8]| refused(V5, &publish(0, b"t", &props(properties)));
    // [MQTT-3.3.2-8]: Topic Alias zero.
    assert_eq!(p(&[0x23, 0, 0]), DecodeError::InvalidPropertyValue(0x23));
    // [MQTT-3.3.4-5]: Subscription Identifier zero.
    assert_eq!(p(&[0x0B, 0]), DecodeError::InvalidPropertyValue(0x0B));
    // [MQTT-3.3.2-4]: Payload Format Indicator is 0 or 1.
    assert_eq!(p(&[0x01, 2]), DecodeError::InvalidPropertyValue(0x01));
    // A Subscription Identifier that is not a minimal variable byte integer.
    assert_eq!(p(&[0x0B, 0x81, 0x00]), DecodeError::MalformedLength);
    // Session Expiry belongs to CONNECT, CONNACK and DISCONNECT.
    assert!(matches!(
        p(&[0x11, 0, 0, 0, 1]),
        DecodeError::PropertyNotAllowed { id: 0x11, .. }
    ));
    assert_eq!(
        p(&[0x02, 0, 0, 0, 1, 0x02, 0, 0, 0, 2]),
        DecodeError::DuplicateProperty(0x02)
    );
    // [MQTT-3.3.2-13]: a Response Topic is a topic name.
    let mut response = vec![0x08, 0, 3];
    response.extend_from_slice(b"a/+");
    assert_eq!(p(&response), DecodeError::InvalidPropertyValue(0x08));
    // Several Subscription Identifiers are fine in a PUBLISH.
    let ok = accepted(V5, &publish(0, b"t", &props(&[0x0B, 1, 0x0B, 2])));
    let Packet::Publish(ok) = ok else { panic!() };
    assert_eq!(ok.properties.subscription_identifiers, [1, 2]);
}

#[test]
fn repeated_properties_are_counted() {
    let mut limits = Limits::default();
    limits.max_repeated_properties = 3;
    let user = [0x26, 0, 1, b'k', 0, 1, b'v'];
    let four: Vec<u8> = user.iter().copied().cycle().take(user.len() * 4).collect();
    let three = &four[..user.len() * 3];
    let ok = publish(0, b"t", &props(three));
    assert!(Packet::decode(&mut BytesMut::from(&ok[..]), V5, &limits).is_ok());
    let over = publish(0, b"t", &props(&four));
    assert!(matches!(
        Packet::decode(&mut BytesMut::from(&over[..]), V5, &limits),
        Err(DecodeError::LimitExceeded { max: 3, .. })
    ));
}

// ---- PUBACK, PUBREC, PUBREL, PUBCOMP

#[test]
fn acknowledgements() {
    for (first, kind) in [
        (0x40, PacketType::PubAck),
        (0x50, PacketType::PubRec),
        (0x62, PacketType::PubRel),
        (0x70, PacketType::PubComp),
    ] {
        // [MQTT-2.2.1-3]: identifier zero.
        assert_eq!(refused(V3, &[first, 2, 0, 0]), DecodeError::ZeroPacketId);
        assert_eq!(refused(V5, &[first, 2, 0, 0]), DecodeError::ZeroPacketId);
        // Too short for the identifier.
        assert_eq!(refused(V3, &[first, 1, 0]), DecodeError::Truncated);
        // 3.1.1 has nothing after the identifier.
        assert_eq!(
            refused(V3, &[first, 3, 0, 1, 0]),
            DecodeError::TrailingBytes(1)
        );
        // A code nothing defines, and one that belongs to another packet.
        for code in [0x03, 0x04, 0x85, 0xFF] {
            assert!(
                matches!(
                    refused(V5, &[first, 3, 0, 1, code]),
                    DecodeError::InvalidReasonCode { packet, .. } if packet == kind
                ),
                "{kind} accepted {code:#04x}"
            );
        }
        // A property the acknowledgements cannot carry.
        assert!(matches!(
            refused(V5, &[first, 7, 0, 1, 0, 3, 0x23, 0, 1]),
            DecodeError::PropertyNotAllowed { id: 0x23, .. }
        ));
    }
    // Which codes belong to which packet.
    assert!(matches!(
        refused(V5, &[0x62, 3, 0, 1, 0x10]),
        DecodeError::InvalidReasonCode { .. }
    ));
    assert!(matches!(
        refused(V5, &[0x40, 3, 0, 1, 0x92]),
        DecodeError::InvalidReasonCode { .. }
    ));
    accepted(V5, &[0x62, 3, 0, 1, 0x92]);
    accepted(V5, &[0x70, 3, 0, 1, 0x92]);
    accepted(V5, &[0x40, 3, 0, 1, 0x10]);
    accepted(V5, &[0x50, 3, 0, 1, 0x99]);
}

// ---- SUBSCRIBE and SUBACK

fn subscribe(version: Option<Version>, body: &[u8]) -> Result<Option<Packet>, DecodeError> {
    decode(version, &packet(0x82, body))
}

fn sub_err(version: Option<Version>, body: &[u8]) -> DecodeError {
    subscribe(version, body).expect_err("should refuse")
}

fn filter_body(filter: &[u8], options: u8) -> Vec<u8> {
    cat(&[&[0, 1], &string(filter), &[options]])
}

fn filter_body_v5(filter: &[u8], options: u8) -> Vec<u8> {
    cat(&[&[0, 1, 0], &string(filter), &[options]])
}

#[test]
fn subscribe_shape() {
    // [MQTT-3.8.3-3]: at least one filter.
    assert_eq!(
        sub_err(V3, &[0, 1]),
        DecodeError::EmptyList(PacketType::Subscribe)
    );
    assert_eq!(
        sub_err(V5, &[0, 1, 0]),
        DecodeError::EmptyList(PacketType::Subscribe)
    );
    assert_eq!(
        sub_err(V3, &[0, 0, 0, 1, b'a', 0]),
        DecodeError::ZeroPacketId
    );
    // A filter with no options byte after it.
    assert_eq!(
        sub_err(V3, &cat(&[&[0, 1], &string(b"a")])),
        DecodeError::Truncated
    );
}

#[test]
fn subscribe_options() {
    // [MQTT-3.8.3-4]: QoS 3, and the reserved bits under 3.1.1.
    for options in [0x03, 0x04, 0x08, 0x10, 0x80] {
        assert!(
            matches!(
                sub_err(V3, &filter_body(b"a", options)),
                DecodeError::InvalidSubscriptionOptions(_)
            ),
            "3.1.1 accepted {options:#04x}"
        );
    }
    // [MQTT-3.8.3-5]: under 5.0 the top two bits are reserved, and retain handling 3 is.
    for options in [0x03, 0x30, 0x40, 0x80, 0xC1] {
        assert!(
            matches!(
                sub_err(V5, &filter_body_v5(b"a", options)),
                DecodeError::InvalidSubscriptionOptions(_)
            ),
            "5.0 accepted {options:#04x}"
        );
    }
    // Everything legal.
    accepted(V5, &packet(0x82, &filter_body_v5(b"a", 0b0010_1110)));
}

#[test]
fn subscribe_filters() {
    let bad = |filter: &[u8]| sub_err(V3, &filter_body(filter, 0));
    // [MQTT-4.7.1-1] and [MQTT-4.7.1-2]: wildcard placement.
    for filter in [
        &b"a/#/b"[..],
        b"a#",
        b"#a",
        b"a/b#",
        b"a+",
        b"+a",
        b"a/+b",
        b"a/b+/c",
        b"##",
        b"+#",
    ] {
        assert_eq!(
            bad(filter),
            DecodeError::InvalidTopicFilter(TopicError::BadWildcard),
            "{}",
            String::from_utf8_lossy(filter)
        );
    }
    // [MQTT-4.7.3-1]: at least one character.
    assert_eq!(bad(b""), DecodeError::InvalidTopicFilter(TopicError::Empty));
    for filter in [
        &b"#"[..],
        b"+",
        b"+/+",
        b"a/+/b",
        b"a/#",
        b"/",
        b"/a",
        b"a/",
        b"$SYS/#",
        b"+/#",
    ] {
        accepted(V3, &packet(0x82, &filter_body(filter, 0)));
    }
    assert_eq!(
        bad(&vec![b'a'; 1025]),
        DecodeError::InvalidTopicFilter(TopicError::TooLong { max: 1024 })
    );
    assert_eq!(
        bad("+/".repeat(16).as_bytes()),
        DecodeError::InvalidTopicFilter(TopicError::TooManyLevels { max: 16 })
    );
}

#[test]
fn shared_subscription_filters_are_5_only() {
    let v5 = |filter: &[u8]| sub_err(V5, &filter_body_v5(filter, 0));
    // 5.0 [MQTT-4.8.2-1] and [MQTT-4.8.2-2]: a share name, a separator, then a filter.
    for filter in [
        &b"$share/"[..],
        b"$share/g",
        b"$share//a",
        b"$share/g/",
        b"$share/g+/a",
        b"$share/g#/a",
        b"$share/g/a#",
    ] {
        assert!(
            matches!(v5(filter), DecodeError::InvalidTopicFilter(_)),
            "{}",
            String::from_utf8_lossy(filter)
        );
    }
    for filter in [&b"$share/g/a"[..], b"$share/g/+/a", b"$share/g/#"] {
        accepted(V5, &packet(0x82, &filter_body_v5(filter, 0)));
    }
    // [MQTT-3.8.3-4]: No Local on a shared subscription.
    assert_eq!(
        sub_err(V5, &filter_body_v5(b"$share/g/a", 0b0100)),
        DecodeError::NoLocalOnSharedSubscription
    );
    accepted(V5, &packet(0x82, &filter_body_v5(b"$share/g/a", 0b1000)));
    // Under 3.1.1 the prefix means nothing.
    accepted(V3, &packet(0x82, &filter_body(b"$share/g", 0)));
}

#[test]
fn subscribe_counts_are_bounded() {
    let mut limits = Limits::default();
    limits.max_subscriptions = 2;
    let filters = |n: usize| {
        let mut body = vec![0, 1];
        for _ in 0..n {
            body.extend_from_slice(&string(b"a"));
            body.push(0);
        }
        packet(0x82, &body)
    };
    assert!(Packet::decode(&mut BytesMut::from(&filters(2)[..]), V3, &limits).is_ok());
    assert!(matches!(
        Packet::decode(&mut BytesMut::from(&filters(3)[..]), V3, &limits),
        Err(DecodeError::LimitExceeded { max: 2, .. })
    ));
}

#[test]
fn subscribe_v5_properties() {
    let with = |properties: &[u8]| {
        sub_err(
            V5,
            &cat(&[&[0, 1], &props(properties), &string(b"a"), &[0]]),
        )
    };
    // [MQTT-3.8.2.1.2]: Subscription Identifier zero, or given twice.
    assert_eq!(with(&[0x0B, 0]), DecodeError::InvalidPropertyValue(0x0B));
    assert_eq!(
        with(&[0x0B, 1, 0x0B, 2]),
        DecodeError::DuplicateProperty(0x0B)
    );
    assert!(matches!(
        with(&[0x23, 0, 1]),
        DecodeError::PropertyNotAllowed { id: 0x23, .. }
    ));
}

#[test]
fn suback() {
    // [MQTT-3.9.3-1]: 3.1.1 allows 0, 1, 2 and 0x80.
    for code in [0x03, 0x81, 0x83, 0x87] {
        assert!(matches!(
            refused(V3, &[0x90, 3, 0, 1, code]),
            DecodeError::InvalidReasonCode {
                packet: PacketType::SubAck,
                ..
            }
        ));
    }
    assert!(matches!(
        refused(V5, &[0x90, 4, 0, 1, 0, 0x84]),
        DecodeError::InvalidReasonCode {
            packet: PacketType::SubAck,
            code: 0x84
        }
    ));
    assert_eq!(
        refused(V3, &[0x90, 2, 0, 1]),
        DecodeError::EmptyList(PacketType::SubAck)
    );
    assert_eq!(
        refused(V5, &[0x90, 3, 0, 1, 0]),
        DecodeError::EmptyList(PacketType::SubAck)
    );
    assert_eq!(refused(V3, &[0x90, 3, 0, 0, 0]), DecodeError::ZeroPacketId);
    accepted(V5, &[0x90, 5, 0, 1, 0, 0x9E, 0xA2]);
}

// ---- UNSUBSCRIBE and UNSUBACK

#[test]
fn unsubscribe() {
    let unsub = |version, body: &[u8]| refused(version, &packet(0xA2, body));
    // [MQTT-3.10.3-2]: at least one filter.
    assert_eq!(
        unsub(V3, &[0, 1]),
        DecodeError::EmptyList(PacketType::Unsubscribe)
    );
    assert_eq!(
        unsub(V5, &[0, 1, 0]),
        DecodeError::EmptyList(PacketType::Unsubscribe)
    );
    assert_eq!(
        unsub(V3, &cat(&[&[0, 0], &string(b"a")])),
        DecodeError::ZeroPacketId
    );
    assert_eq!(
        unsub(V3, &cat(&[&[0, 1], &string(b"a/#/b")])),
        DecodeError::InvalidTopicFilter(TopicError::BadWildcard)
    );
    assert_eq!(
        unsub(V5, &cat(&[&[0, 1, 0], &string(b"$share/g")])),
        DecodeError::InvalidTopicFilter(TopicError::BadShare)
    );
    accepted(
        V3,
        &packet(0xA2, &cat(&[&[0, 1], &string(b"a/+"), &string(b"b")])),
    );
}

#[test]
fn unsuback() {
    assert_eq!(
        refused(V3, &[0xB0, 3, 0, 1, 0]),
        DecodeError::TrailingBytes(1)
    );
    assert_eq!(refused(V3, &[0xB0, 2, 0, 0]), DecodeError::ZeroPacketId);
    assert_eq!(
        refused(V5, &[0xB0, 3, 0, 1, 0]),
        DecodeError::EmptyList(PacketType::UnsubAck)
    );
    for code in [0x10, 0x01, 0x92] {
        assert!(matches!(
            refused(V5, &[0xB0, 4, 0, 1, 0, code]),
            DecodeError::InvalidReasonCode {
                packet: PacketType::UnsubAck,
                ..
            }
        ));
    }
    accepted(V5, &[0xB0, 5, 0, 1, 0, 0x00, 0x11]);
}

// ---- PINGREQ, PINGRESP, DISCONNECT, AUTH

#[test]
fn pings_have_no_body() {
    // [MQTT-3.12.1] and [MQTT-3.13.1]
    assert_eq!(refused(V3, &[0xC0, 1, 0]), DecodeError::TrailingBytes(1));
    assert_eq!(refused(V5, &[0xD0, 1, 0]), DecodeError::TrailingBytes(1));
}

#[test]
fn disconnect() {
    // [MQTT-3.14.1]: 3.1.1's DISCONNECT is empty.
    assert_eq!(refused(V3, &[0xE0, 1, 0]), DecodeError::TrailingBytes(1));
    for code in [0x01, 0x10, 0x88, 0x91, 0xFF] {
        assert!(
            matches!(
                refused(V5, &[0xE0, 1, code]),
                DecodeError::InvalidReasonCode {
                    packet: PacketType::Disconnect,
                    ..
                }
            ),
            "accepted {code:#04x}"
        );
    }
    // Session Expiry Interval is allowed here; Topic Alias is not.
    assert!(matches!(
        refused(V5, &[0xE0, 5, 0x00, 3, 0x23, 0, 1]),
        DecodeError::PropertyNotAllowed { id: 0x23, .. }
    ));
    accepted(V5, &[0xE0, 7, 0x00, 5, 0x11, 0, 0, 0, 0]);
}

#[test]
fn auth() {
    let method = [0x15, 0, 1, b'm'];
    // [MQTT-3.15.2-1]: Success, Continue Authentication and Re-authenticate only.
    for code in [0x01, 0x04, 0x80, 0x87] {
        let body = cat(&[&[code], &props(&method)]);
        assert!(matches!(
            refused(V5, &packet(0xF0, &body)),
            DecodeError::InvalidReasonCode {
                packet: PacketType::Auth,
                ..
            }
        ));
    }
    // [MQTT-3.15.2-2]: the method is required.
    assert_eq!(
        refused(V5, &packet(0xF0, &cat(&[&[0x18], &props(&[])]))),
        DecodeError::MissingProperty(0x15)
    );
    assert_eq!(
        refused(
            V5,
            &packet(0xF0, &cat(&[&[0x18], &props(&[0x16, 0, 1, 1])]))
        ),
        DecodeError::MissingProperty(0x15)
    );
    // The short forms the acknowledgements have are not allowed here.
    assert_eq!(refused(V5, &[0xF0, 0]), DecodeError::Truncated);
    assert_eq!(refused(V5, &[0xF0, 1, 0x18]), DecodeError::Truncated);
    // Properties AUTH cannot carry.
    let body = cat(&[&[0x18], &props(&cat(&[&method, &[0x11, 0, 0, 0, 1]]))]);
    assert!(matches!(
        refused(V5, &packet(0xF0, &body)),
        DecodeError::PropertyNotAllowed { id: 0x11, .. }
    ));
    accepted(V5, &packet(0xF0, &cat(&[&[0x19], &props(&method)])));
}

#[test]
fn errors_map_to_reason_codes() {
    assert_eq!(
        DecodeError::PacketTooLarge { size: 2, max: 1 }.reason_code(),
        ReasonCode::PacketTooLarge
    );
    assert_eq!(
        DecodeError::Truncated.reason_code(),
        ReasonCode::MalformedPacket
    );
    assert_eq!(
        DecodeError::ZeroPacketId.reason_code(),
        ReasonCode::ProtocolError
    );
    assert_eq!(
        DecodeError::InvalidTopicName(TopicError::Wildcard).reason_code(),
        ReasonCode::TopicNameInvalid
    );
    assert_eq!(
        DecodeError::InvalidTopicFilter(TopicError::Empty).reason_code(),
        ReasonCode::TopicFilterInvalid
    );
}
