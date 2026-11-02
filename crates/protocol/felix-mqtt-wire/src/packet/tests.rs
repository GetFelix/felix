//! The encoder's refusals, and the streaming behaviour of `decode`.

use super::*;
use crate::property::Properties;
use crate::protocol::QoS;
use crate::reason::ReasonCode;
use crate::text::Utf8;

fn s(text: &str) -> Utf8 {
    Utf8::try_from(text).unwrap()
}

fn publish(qos: QoS, packet_id: Option<u16>) -> Publish {
    Publish {
        dup: false,
        qos,
        retain: false,
        topic: s("a/b"),
        packet_id,
        properties: Properties::default(),
        payload: Bytes::from_static(b"payload"),
    }
}

fn connect(version: Version) -> Connect {
    Connect {
        version,
        clean_start: true,
        keep_alive: 30,
        client_id: s("c"),
        will: None,
        username: None,
        password: None,
        properties: Properties::default(),
    }
}

fn encode(packet: Packet, version: Version) -> Result<Bytes, EncodeError> {
    packet.to_bytes(version)
}

#[test]
fn connect_refusals() {
    let c = connect(Version::V5);
    assert_eq!(
        encode(Packet::Connect(c.clone()), Version::V311),
        Err(EncodeError::VersionMismatch)
    );

    let mut c311 = connect(Version::V311);
    c311.password = Some(Bytes::from_static(b"p"));
    assert_eq!(
        encode(Packet::Connect(c311.clone()), Version::V311),
        Err(EncodeError::PasswordWithoutUsername)
    );
    c311.password = None;
    c311.properties.session_expiry_interval = Some(1);
    assert_eq!(
        encode(Packet::Connect(c311), Version::V311),
        Err(EncodeError::NotInVersion("properties"))
    );

    let mut will = Will {
        topic: s("a/+"),
        payload: Bytes::new(),
        qos: QoS::AtMostOnce,
        retain: false,
        properties: Properties::default(),
    };
    let mut c = connect(Version::V5);
    c.will = Some(will.clone());
    assert!(matches!(
        encode(Packet::Connect(c.clone()), Version::V5),
        Err(EncodeError::InvalidTopicName(_))
    ));
    will.topic = s("a/b");
    will.payload = Bytes::from(vec![0u8; 65_536]);
    c.will = Some(will);
    assert_eq!(
        encode(Packet::Connect(c.clone()), Version::V5),
        Err(EncodeError::FieldTooLong)
    );
    c.will = None;
    c.password = Some(Bytes::from(vec![0u8; 65_536]));
    assert_eq!(
        encode(Packet::Connect(c), Version::V5),
        Err(EncodeError::FieldTooLong)
    );
}

#[test]
fn connack_refusals() {
    let ok = ConnAck {
        session_present: false,
        reason: ReasonCode::Success,
        properties: Properties::default(),
    };
    assert!(encode(Packet::ConnAck(ok.clone()), Version::V311).is_ok());

    let mut refused = ok.clone();
    refused.session_present = true;
    refused.reason = ReasonCode::NotAuthorized;
    assert!(encode(Packet::ConnAck(refused), Version::V5).is_err());

    // 3.1.1 can only say six things.
    let mut other = ok.clone();
    other.reason = ReasonCode::ServerBusy;
    assert!(matches!(
        encode(Packet::ConnAck(other.clone()), Version::V311),
        Err(EncodeError::InvalidReasonCode { .. })
    ));
    assert!(encode(Packet::ConnAck(other), Version::V5).is_ok());

    let mut wrong = ok;
    wrong.reason = ReasonCode::GrantedQos1;
    assert!(encode(Packet::ConnAck(wrong), Version::V5).is_err());
}

#[test]
fn publish_refusals() {
    assert!(
        encode(
            Packet::Publish(publish(QoS::AtMostOnce, None)),
            Version::V311
        )
        .is_ok()
    );
    for (qos, id) in [
        (QoS::AtMostOnce, Some(1)),
        (QoS::AtLeastOnce, None),
        (QoS::ExactlyOnce, Some(0)),
    ] {
        assert_eq!(
            encode(Packet::Publish(publish(qos, id)), Version::V311),
            Err(EncodeError::InvalidPacketId)
        );
    }
    let mut dup = publish(QoS::AtMostOnce, None);
    dup.dup = true;
    assert_eq!(
        encode(Packet::Publish(dup), Version::V311),
        Err(EncodeError::DupOnQos0)
    );

    let mut bad_topic = publish(QoS::AtMostOnce, None);
    bad_topic.topic = s("a/#");
    assert!(matches!(
        encode(Packet::Publish(bad_topic), Version::V311),
        Err(EncodeError::InvalidTopicName(_))
    ));

    let mut empty = publish(QoS::AtMostOnce, None);
    empty.topic = s("");
    assert!(encode(Packet::Publish(empty.clone()), Version::V311).is_err());
    assert!(encode(Packet::Publish(empty.clone()), Version::V5).is_err());
    empty.properties.topic_alias = Some(2);
    assert!(encode(Packet::Publish(empty.clone()), Version::V5).is_ok());
    assert!(encode(Packet::Publish(empty), Version::V311).is_err());
}

#[test]
fn publish_head_and_payload_are_the_whole_packet() {
    for version in [Version::V311, Version::V5] {
        let p = publish(QoS::AtLeastOnce, Some(9));
        let whole = Packet::Publish(p.clone()).to_bytes(version).unwrap();
        let mut head = BytesMut::new();
        p.encode_head(version, &mut head).unwrap();
        head.extend_from_slice(&p.payload);
        assert_eq!(head.as_ref(), whole.as_ref());
    }
}

#[test]
fn acknowledgement_refusals() {
    assert_eq!(
        encode(Packet::PubAck(Ack::new(0)), Version::V311),
        Err(EncodeError::InvalidPacketId)
    );
    let mut ack = Ack::new(1);
    ack.reason = ReasonCode::NoMatchingSubscribers;
    assert_eq!(
        encode(Packet::PubAck(ack.clone()), Version::V311),
        Err(EncodeError::NotInVersion("reason code"))
    );
    assert!(encode(Packet::PubAck(ack.clone()), Version::V5).is_ok());
    // The same reason does not belong in a PUBREL.
    assert!(matches!(
        encode(Packet::PubRel(ack), Version::V5),
        Err(EncodeError::InvalidReasonCode { .. })
    ));
}

#[test]
fn acknowledgements_use_the_shortest_form_that_says_it() {
    let bare = encode(Packet::PubAck(Ack::new(7)), Version::V5).unwrap();
    assert_eq!(bare.as_ref(), &[0x40, 0x02, 0x00, 0x07]);

    let mut denied = Ack::new(7);
    denied.reason = ReasonCode::NotAuthorized;
    let with_reason = encode(Packet::PubAck(denied), Version::V5).unwrap();
    assert_eq!(with_reason.as_ref(), &[0x40, 0x04, 0x00, 0x07, 0x87, 0x00]);
}

#[test]
fn subscribe_refusals() {
    let ok = Subscribe {
        packet_id: 1,
        filters: vec![SubscribeFilter::new(s("a/+"), QoS::AtLeastOnce)],
        properties: Properties::default(),
    };
    assert!(encode(Packet::Subscribe(ok.clone()), Version::V311).is_ok());

    let mut empty = ok.clone();
    empty.filters.clear();
    assert_eq!(
        encode(Packet::Subscribe(empty), Version::V5),
        Err(EncodeError::EmptyList(PacketType::Subscribe))
    );
    let mut zero = ok.clone();
    zero.packet_id = 0;
    assert_eq!(
        encode(Packet::Subscribe(zero), Version::V5),
        Err(EncodeError::InvalidPacketId)
    );

    let mut options = ok.clone();
    options.filters[0].no_local = true;
    assert_eq!(
        encode(Packet::Subscribe(options.clone()), Version::V311),
        Err(EncodeError::NotInVersion("subscription options"))
    );
    assert!(encode(Packet::Subscribe(options), Version::V5).is_ok());

    let mut shared = ok.clone();
    shared.filters[0] = SubscribeFilter::new(s("$share/g/a"), QoS::AtMostOnce);
    shared.filters[0].no_local = true;
    assert_eq!(
        encode(Packet::Subscribe(shared), Version::V5),
        Err(EncodeError::NoLocalOnSharedSubscription)
    );

    let mut two = ok.clone();
    two.properties.subscription_identifiers = vec![1, 2];
    assert!(encode(Packet::Subscribe(two), Version::V5).is_err());

    let mut bad = ok;
    bad.filters[0].filter = s("a/#/b");
    assert!(matches!(
        encode(Packet::Subscribe(bad), Version::V5),
        Err(EncodeError::InvalidTopicFilter(_))
    ));
}

#[test]
fn ack_list_refusals() {
    let suback = |reasons: Vec<ReasonCode>| SubAck {
        packet_id: 1,
        reasons,
        properties: Properties::default(),
    };
    assert_eq!(
        encode(Packet::SubAck(suback(vec![])), Version::V5),
        Err(EncodeError::EmptyList(PacketType::SubAck))
    );
    // 3.1.1 has no per-reason codes beyond QoS and failure.
    assert!(
        encode(
            Packet::SubAck(suback(vec![ReasonCode::NotAuthorized])),
            Version::V311
        )
        .is_err()
    );
    assert!(
        encode(
            Packet::SubAck(suback(vec![ReasonCode::NotAuthorized])),
            Version::V5
        )
        .is_ok()
    );

    let unsuback = UnsubAck {
        packet_id: 1,
        reasons: vec![ReasonCode::Success],
        properties: Properties::default(),
    };
    assert_eq!(
        encode(Packet::UnsubAck(unsuback.clone()), Version::V311),
        Err(EncodeError::NotInVersion("UNSUBACK reason codes"))
    );
    assert!(encode(Packet::UnsubAck(unsuback), Version::V5).is_ok());
    let bare = UnsubAck {
        packet_id: 1,
        reasons: vec![],
        properties: Properties::default(),
    };
    assert!(encode(Packet::UnsubAck(bare.clone()), Version::V311).is_ok());
    assert!(encode(Packet::UnsubAck(bare), Version::V5).is_err());

    let unsub = Unsubscribe {
        packet_id: 1,
        filters: vec![],
        properties: Properties::default(),
    };
    assert_eq!(
        encode(Packet::Unsubscribe(unsub), Version::V311),
        Err(EncodeError::EmptyList(PacketType::Unsubscribe))
    );
}

#[test]
fn disconnect_and_auth_refusals() {
    let normal = Disconnect::new(ReasonCode::Success);
    assert_eq!(
        encode(Packet::Disconnect(normal.clone()), Version::V311)
            .unwrap()
            .as_ref(),
        &[0xE0, 0]
    );
    assert_eq!(
        encode(Packet::Disconnect(normal), Version::V5)
            .unwrap()
            .as_ref(),
        &[0xE0, 0]
    );

    let shutting_down = Disconnect::new(ReasonCode::ServerShuttingDown);
    assert_eq!(
        encode(Packet::Disconnect(shutting_down.clone()), Version::V311),
        Err(EncodeError::NotInVersion("reason code"))
    );
    assert_eq!(
        encode(Packet::Disconnect(shutting_down), Version::V5)
            .unwrap()
            .as_ref(),
        &[0xE0, 0x02, 0x8B, 0x00]
    );
    assert!(
        encode(
            Packet::Disconnect(Disconnect::new(ReasonCode::GrantedQos1)),
            Version::V5
        )
        .is_err()
    );

    let mut auth = Auth {
        reason: ReasonCode::ContinueAuthentication,
        properties: Properties::default(),
    };
    assert_eq!(
        encode(Packet::Auth(auth.clone()), Version::V311),
        Err(EncodeError::NotInVersion("AUTH"))
    );
    assert_eq!(
        encode(Packet::Auth(auth.clone()), Version::V5),
        Err(EncodeError::MissingAuthenticationMethod)
    );
    auth.properties.authentication_method = Some(s("m"));
    assert!(encode(Packet::Auth(auth.clone()), Version::V5).is_ok());
    auth.reason = ReasonCode::NotAuthorized;
    assert!(encode(Packet::Auth(auth), Version::V5).is_err());
}

#[test]
fn a_packet_too_big_for_the_length_field_is_refused() {
    // Zero-filled allocations are not touched until written, so this is cheap.
    let mut p = publish(QoS::AtMostOnce, None);
    p.payload = Bytes::from(vec![0u8; MAX_VARINT as usize]);
    assert!(matches!(
        Packet::Publish(p).encoded_len(Version::V311),
        Err(EncodeError::PacketTooLarge(_))
    ));
}

#[test]
fn encode_appends_and_encoded_len_matches() {
    let mut dst = BytesMut::from(&b"xx"[..]);
    let packet = Packet::Publish(publish(QoS::AtLeastOnce, Some(5)));
    packet.encode(Version::V5, &mut dst).unwrap();
    packet.encode(Version::V5, &mut dst).unwrap();
    let one = packet.encoded_len(Version::V5).unwrap();
    assert_eq!(dst.len(), 2 + 2 * one);
    assert_eq!(&dst[..2], b"xx");
}

#[test]
fn payloads_share_the_buffer_they_were_read_from() {
    let mut buf = BytesMut::new();
    Packet::Publish(publish(QoS::AtMostOnce, None))
        .encode(Version::V311, &mut buf)
        .unwrap();
    let start = buf.as_ptr() as usize;
    let end = start + buf.len();
    let Some(Packet::Publish(p)) =
        Packet::decode(&mut buf, Some(Version::V311), &Limits::default()).unwrap()
    else {
        panic!("no PUBLISH");
    };
    let at = p.payload.as_ptr() as usize;
    assert!(
        start <= at && at + p.payload.len() <= end,
        "payload was copied"
    );
    let topic = p.topic.as_bytes().as_ptr() as usize;
    assert!(start <= topic && topic < end, "topic was copied");
}
