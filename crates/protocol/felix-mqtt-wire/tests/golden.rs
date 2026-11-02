//! Packets written by other implementations and by hand from the specification.
//!
//! `vectors/rumqttc.txt` holds what rumqttc's encoder writes (see
//! `vectors/rumqttc-gen`). Each must decode, and encode back to the same bytes,
//! except where rumqttc orders a packet's properties differently from this
//! codec: properties are a set, so those are compared as decoded packets.

use bytes::{Bytes, BytesMut};
use felix_mqtt_wire::{
    Ack, Auth, ConnAck, Connect, Disconnect, Limits, Packet, PacketType, Properties, QoS,
    ReasonCode, RetainHandling, SubAck, Subscribe, SubscribeFilter, Utf8, Version, Will,
};

/// Vectors whose property order differs from ours.
const REORDERED: &[&str] = &[
    "v5 connect_full",
    "v5 connack_properties",
    "v5 publish_qos1_properties",
    "v5 disconnect_properties",
];

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn vectors() -> Vec<(String, Version, Vec<u8>)> {
    include_str!("vectors/rumqttc.txt")
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
        .map(|line| {
            let mut parts = line.split(' ');
            let version = match parts.next().unwrap() {
                "v4" => Version::V311,
                _ => Version::V5,
            };
            let name = parts.next().unwrap();
            let bytes = hex(parts.next().unwrap());
            (
                format!(
                    "{} {name}",
                    if version == Version::V5 { "v5" } else { "v4" }
                ),
                version,
                bytes,
            )
        })
        .collect()
}

/// Decodes `bytes` as the one packet they hold. `CONNECT` is read the way a
/// server reads a connection's first packet.
fn decode(bytes: &[u8], version: Version) -> Packet {
    let mut src = BytesMut::from(bytes);
    let known = (bytes[0] >> 4 != 1).then_some(version);
    let packet = Packet::decode(&mut src, known, &Limits::default())
        .expect("decodes")
        .expect("whole packet");
    assert!(src.is_empty(), "bytes left after the packet");
    packet
}

fn s(text: &str) -> Utf8 {
    Utf8::try_from(text).unwrap()
}

#[test]
fn rumqttc_vectors_round_trip() {
    let all = vectors();
    assert!(all.len() > 50, "vector file looks truncated");
    for (name, version, bytes) in all {
        let packet = decode(&bytes, version);
        let written = packet
            .to_bytes(version)
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        if REORDERED.contains(&name.as_str()) {
            assert_eq!(written.len(), bytes.len(), "{name}");
            assert_eq!(decode(&written, version), packet, "{name}");
        } else {
            assert_eq!(written.as_ref(), &bytes[..], "{name}");
        }
        assert_eq!(packet.encoded_len(version).unwrap(), bytes.len(), "{name}");
    }
}

fn vector(name: &str) -> (Version, Vec<u8>) {
    vectors()
        .into_iter()
        .find(|(n, _, _)| n == name)
        .map(|(_, v, b)| (v, b))
        .unwrap_or_else(|| panic!("no vector {name}"))
}

fn decoded(name: &str) -> Packet {
    let (version, bytes) = vector(name);
    decode(&bytes, version)
}

#[test]
fn v311_connect_with_will_and_login() {
    let Packet::Connect(c) = decoded("v4 connect_will_login") else {
        panic!("not a CONNECT");
    };
    assert_eq!(c.version, Version::V311);
    assert!(!c.clean_start);
    assert_eq!(c.keep_alive, 60);
    assert_eq!(c.client_id, "device-7");
    assert_eq!(c.username.unwrap(), "user");
    assert_eq!(c.password.unwrap(), Bytes::from_static(b"pass"));
    let will = c.will.unwrap();
    assert_eq!(will.topic, "will/topic");
    assert_eq!(will.payload, Bytes::from_static(b"bye"));
    assert_eq!(will.qos, QoS::AtLeastOnce);
    assert!(will.retain);
    assert!(will.properties.is_empty());
}

#[test]
fn v311_connack_return_codes_map_to_reason_codes() {
    let expected = [
        ("connack_accepted", false, ReasonCode::Success),
        ("connack_session_present", true, ReasonCode::Success),
        (
            "connack_bad_version",
            false,
            ReasonCode::UnsupportedProtocolVersion,
        ),
        (
            "connack_bad_client_id",
            false,
            ReasonCode::ClientIdentifierNotValid,
        ),
        ("connack_unavailable", false, ReasonCode::ServerUnavailable),
        (
            "connack_bad_login",
            false,
            ReasonCode::BadUserNameOrPassword,
        ),
        ("connack_not_authorized", false, ReasonCode::NotAuthorized),
    ];
    for (name, session_present, reason) in expected {
        let packet = decoded(&format!("v4 {name}"));
        assert_eq!(
            packet,
            Packet::ConnAck(ConnAck {
                session_present,
                reason,
                properties: Properties::default(),
            }),
            "{name}"
        );
    }
}

#[test]
fn v311_publish_flags_and_payload() {
    let Packet::Publish(p) = decoded("v4 publish_qos1_dup") else {
        panic!("not a PUBLISH");
    };
    assert!(p.dup && !p.retain);
    assert_eq!(p.qos, QoS::AtLeastOnce);
    assert_eq!(p.topic, "sensors/temp");
    assert_eq!(p.packet_id, Some(10));
    assert_eq!(p.payload.as_ref(), &[1, 2, 3, 0, 255]);

    let Packet::Publish(p) = decoded("v4 publish_qos2_empty") else {
        panic!("not a PUBLISH");
    };
    assert_eq!((p.qos, p.packet_id), (QoS::ExactlyOnce, Some(65535)));
    assert!(p.payload.is_empty());

    let Packet::Publish(p) = decoded("v4 publish_two_byte_length") else {
        panic!("not a PUBLISH");
    };
    assert_eq!(p.payload.len(), 200);
}

#[test]
fn v311_subscribe_and_suback() {
    let Packet::Subscribe(sub) = decoded("v4 subscribe_many") else {
        panic!("not a SUBSCRIBE");
    };
    let got: Vec<_> = sub
        .filters
        .iter()
        .map(|f| (f.filter.as_str(), f.qos))
        .collect();
    assert_eq!(
        got,
        [
            ("a/#", QoS::AtMostOnce),
            ("b/c", QoS::ExactlyOnce),
            ("+/+/d", QoS::AtLeastOnce)
        ]
    );
    assert_eq!(sub.packet_id, 4);

    assert_eq!(
        decoded("v4 suback"),
        Packet::SubAck(SubAck {
            packet_id: 4,
            reasons: vec![
                ReasonCode::Success,
                ReasonCode::GrantedQos1,
                ReasonCode::GrantedQos2,
                ReasonCode::UnspecifiedError
            ],
            properties: Properties::default(),
        })
    );
}

#[test]
fn v5_connect_with_every_connect_property_and_a_will() {
    let Packet::Connect(c) = decoded("v5 connect_full") else {
        panic!("not a CONNECT");
    };
    assert_eq!(c.version, Version::V5);
    let p = &c.properties;
    assert_eq!(p.session_expiry_interval, Some(3600));
    assert_eq!(p.receive_maximum, Some(20));
    assert_eq!(p.maximum_packet_size, Some(65536));
    assert_eq!(p.topic_alias_maximum, Some(10));
    assert_eq!(p.request_response_information, Some(true));
    assert_eq!(p.request_problem_information, Some(false));
    assert_eq!(p.authentication_method.as_ref().unwrap(), "PLAIN");
    assert_eq!(p.user_properties, [(s("k"), s("v")), (s("k"), s("w"))]);
    let will = c.will.unwrap();
    assert_eq!(will.qos, QoS::ExactlyOnce);
    assert_eq!(will.properties.will_delay_interval, Some(5));
    assert_eq!(will.properties.payload_format_indicator, Some(true));
    assert_eq!(will.properties.response_topic.as_ref().unwrap(), "resp/t");
    assert_eq!(c.username.unwrap(), "user");
}

#[test]
fn v5_publish_properties() {
    let Packet::Publish(p) = decoded("v5 publish_qos1_properties") else {
        panic!("not a PUBLISH");
    };
    let props = &p.properties;
    assert_eq!(props.topic_alias, Some(3));
    assert_eq!(props.message_expiry_interval, Some(30));
    assert_eq!(props.subscription_identifiers, [7, 300]);
    assert_eq!(props.content_type.as_ref().unwrap(), "application/json");
    assert_eq!(p.payload.as_ref(), b"{\"t\":21}");

    let Packet::Publish(p) = decoded("v5 publish_alias_empty_topic") else {
        panic!("not a PUBLISH");
    };
    assert!(p.topic.is_empty());
    assert_eq!(p.properties.topic_alias, Some(3));
}

#[test]
fn v5_subscribe_options() {
    let Packet::Subscribe(sub) = decoded("v5 subscribe_options") else {
        panic!("not a SUBSCRIBE");
    };
    assert_eq!(sub.properties.subscription_identifiers, [42]);
    let second = &sub.filters[1];
    assert_eq!(second.qos, QoS::ExactlyOnce);
    assert!(second.no_local && second.retain_as_published);
    assert_eq!(second.retain_handling, RetainHandling::SendIfNew);
    assert_eq!(sub.filters[2].retain_handling, RetainHandling::DoNotSend);
}

#[test]
fn v5_reason_codes() {
    let Packet::PubAck(ack) = decoded("v5 puback_not_authorized_props") else {
        panic!("not a PUBACK");
    };
    assert_eq!(ack.reason, ReasonCode::NotAuthorized);
    assert_eq!(ack.properties.reason_string.as_ref().unwrap(), "denied");

    let Packet::SubAck(ack) = decoded("v5 suback_reasons") else {
        panic!("not a SUBACK");
    };
    assert_eq!(ack.reasons[3], ReasonCode::NotAuthorized);
    assert_eq!(ack.reasons[6], ReasonCode::SharedSubscriptionsNotSupported);

    let Packet::Disconnect(d) = decoded("v5 disconnect_properties") else {
        panic!("not a DISCONNECT");
    };
    assert_eq!(d.reason, ReasonCode::SessionTakenOver);
    assert_eq!(
        d.properties.server_reference.as_ref().unwrap(),
        "other.example"
    );
}

/// Byte strings built from the specification that no encoder here writes: the
/// optional short forms, which a decoder has to accept.
#[test]
fn v5_short_forms_decode() {
    let bare = Ack::new(7);
    for form in [
        &[0x40, 0x02, 0x00, 0x07][..],
        &[0x40, 0x03, 0x00, 0x07, 0x00],
        &[0x40, 0x04, 0x00, 0x07, 0x00, 0x00],
    ] {
        assert_eq!(decode(form, Version::V5), Packet::PubAck(bare.clone()));
    }
    // Reason code without a property length.
    let mut with_reason = Ack::new(7);
    with_reason.reason = ReasonCode::NoMatchingSubscribers;
    assert_eq!(
        decode(&[0x40, 0x03, 0x00, 0x07, 0x10], Version::V5),
        Packet::PubAck(with_reason)
    );

    let normal = Packet::Disconnect(Disconnect::new(ReasonCode::Success));
    for form in [
        &[0xE0, 0x00][..],
        &[0xE0, 0x01, 0x00],
        &[0xE0, 0x02, 0x00, 0x00],
    ] {
        assert_eq!(decode(form, Version::V5), normal);
    }
    assert_eq!(
        decode(&[0xE0, 0x02, 0x95, 0x00], Version::V5),
        Packet::Disconnect(Disconnect::new(ReasonCode::PacketTooLarge))
    );
}

#[test]
fn v5_auth() {
    let bytes = [
        0xF0, 0x0E, 0x18, 0x0C, 0x15, 0x00, 0x05, b'S', b'C', b'R', b'A', b'M', 0x16, 0x00, 0x01,
        b'x',
    ];
    let Packet::Auth(Auth { reason, properties }) = decode(&bytes, Version::V5) else {
        panic!("not an AUTH");
    };
    assert_eq!(reason, ReasonCode::ContinueAuthentication);
    assert_eq!(properties.authentication_method.as_ref().unwrap(), "SCRAM");
    assert_eq!(
        properties.authentication_data.as_ref().unwrap().as_ref(),
        b"x"
    );
    let packet = Packet::Auth(Auth { reason, properties });
    assert_eq!(packet.to_bytes(Version::V5).unwrap().as_ref(), &bytes[..]);
}

#[test]
fn v311_connect_with_an_empty_client_id() {
    let bytes = [
        0x10, 0x0C, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x00,
    ];
    let expected = Connect {
        version: Version::V311,
        clean_start: true,
        keep_alive: 60,
        client_id: s(""),
        will: None::<Will>,
        username: None,
        password: None,
        properties: Properties::default(),
    };
    assert_eq!(decode(&bytes, Version::V311), Packet::Connect(expected));
}

/// The specification's table of variable byte integer examples, through the
/// remaining length of a `PUBLISH` whose payload sets it.
#[test]
fn remaining_length_boundaries() {
    let mut limits = Limits::default();
    limits.max_packet_size = 300_000_000;
    // Topic "t" takes 3 bytes of the body, so payload = length - 3.
    for (length, prefix) in [
        (127usize, &[0x7F][..]),
        (128, &[0x80, 0x01]),
        (16_383, &[0xFF, 0x7F]),
        (16_384, &[0x80, 0x80, 0x01]),
        (2_097_151, &[0xFF, 0xFF, 0x7F]),
        (2_097_152, &[0x80, 0x80, 0x80, 0x01]),
    ] {
        let mut bytes = vec![0x30];
        bytes.extend_from_slice(prefix);
        bytes.extend_from_slice(&[0x00, 0x01, b't']);
        bytes.resize(1 + prefix.len() + length, 0xAA);
        let mut src = BytesMut::from(&bytes[..]);
        let Some(Packet::Publish(p)) =
            Packet::decode(&mut src, Some(Version::V311), &limits).unwrap()
        else {
            panic!("no packet at length {length}");
        };
        assert_eq!(p.payload.len(), length - 3);
        assert_eq!(
            Packet::Publish(p).to_bytes(Version::V311).unwrap().as_ref(),
            &bytes[..]
        );
    }
}

#[test]
fn packet_types_have_the_specified_codes() {
    assert_eq!(PacketType::Connect.code(), 1);
    assert_eq!(PacketType::Auth.code(), 15);
    let sub = Packet::Subscribe(Subscribe {
        packet_id: 1,
        filters: vec![SubscribeFilter::new(s("a"), QoS::AtMostOnce)],
        properties: Properties::default(),
    });
    // SUBSCRIBE, PUBREL and UNSUBSCRIBE are the packets with flags 0b0010.
    assert_eq!(sub.to_bytes(Version::V311).unwrap()[0], 0x82);
    assert_eq!(
        Packet::PubRel(Ack::new(1)).to_bytes(Version::V311).unwrap()[0],
        0x62
    );
}
