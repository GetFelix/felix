use bytes::Bytes;
use felix_wire::Message;

use super::*;

#[test]
fn parse_subscribe_response_variants() {
    assert_eq!(
        parse_subscribe_response(Some(Message::Subscribed {
            subscription_id: 7,
            start_offset: None,
            live_offset: None,
        }))
        .expect("ok"),
        7
    );
    assert!(parse_subscribe_response(Some(Message::error("nope"))).is_err());
    assert!(parse_subscribe_response(None).is_err());
}

#[test]
fn ensure_publish_ok_variants() {
    let ok = |request_id, offset| Some(Message::PublishOk { request_id, offset });
    ensure_publish_ok(ok(9, None), 9).expect("ok");
    assert!(ensure_publish_ok(ok(8, None), 9).is_err());
    // A client that did not offer the offset flag must not be sent one.
    assert!(ensure_publish_ok(ok(9, Some(3)), 9).is_err());
    assert!(ensure_publish_ok(Some(Message::error("no")), 9).is_err());
    assert!(ensure_publish_ok(None, 9).is_err());
}

#[test]
fn ensure_ok_response_variants() {
    ensure_ok_response(Some(Message::Ok), "cache put").expect("ok");
    assert!(ensure_ok_response(Some(Message::error("no")), "cache put").is_err());
    assert!(ensure_ok_response(None, "cache put").is_err());
}

#[test]
fn parse_cache_get_response_variants() {
    let value = Bytes::from_static(b"value");
    assert_eq!(
        parse_cache_get_response(Some(Message::CacheValue {
            tenant_id: "t1".into(),
            namespace: "default".into(),
            cache: "primary".into(),
            key: "k".into(),
            value: Some(value.clone()),
            request_id: None,
            version: None,
        }))
        .expect("ok"),
        Some(value)
    );
    assert!(parse_cache_get_response(Some(Message::Ok)).is_err());
    assert!(parse_cache_get_response(None).is_err());
}

#[test]
fn ensure_event_order_variants() {
    ensure_event_order(&[b"alpha".to_vec(), b"beta".to_vec()]).expect("ok");
    assert!(ensure_event_order(&[b"beta".to_vec(), b"alpha".to_vec()]).is_err());
}

#[test]
fn ensure_cache_value_variants() {
    let expected = Bytes::from_static(b"value");
    ensure_cache_value(Some(expected.clone()), expected.clone(), "cache get").expect("ok");
    assert!(ensure_cache_value(None, expected.clone(), "cache get").is_err());
    assert!(ensure_cache_value(Some(Bytes::from_static(b"nope")), expected, "cache get").is_err());
}

#[test]
fn ensure_cache_expired_variants() {
    ensure_cache_expired(None, "expired").expect("ok");
    assert!(ensure_cache_expired(Some(Bytes::from_static(b"value")), "expired").is_err());
}

#[test]
fn ensure_client_event_variants() {
    ensure_client_event(&Bytes::from_static(b"alpha"), Bytes::from_static(b"alpha")).expect("ok");
    assert!(
        ensure_client_event(&Bytes::from_static(b"alpha"), Bytes::from_static(b"beta")).is_err()
    );
}

#[test]
fn ensure_flag_advertised_variants() {
    let flag = felix_wire::FLAG_BINARY_PUBLISH_ACK_OFFSET;
    let auth_ok = |server_flags| {
        Some(Message::AuthOk {
            server_flags,
            server_features: None,
            server_features_hi: None,
            listener_ports: None,
            publish_window: None,
        })
    };
    assert_eq!(
        ensure_flag_advertised(auth_ok(flag | 1), flag).expect("ok"),
        flag | 1
    );
    assert!(ensure_flag_advertised(auth_ok(1), flag).is_err());
    assert!(ensure_flag_advertised(Some(Message::Ok), flag).is_err());
    assert!(ensure_flag_advertised(None, flag).is_err());
}

#[test]
fn publish_ack_offset_reads_both_encodings() {
    let binary = |bytes: bytes::Bytes| Frame::decode(bytes).expect("frame");
    let at = felix_wire::binary::encode_publish_ack_bytes_at(4, None, None, None, Some(17), None)
        .expect("encode");
    assert_eq!(
        publish_ack_offset(binary(at.clone()), 4).expect("ok"),
        Some(17)
    );
    assert!(publish_ack_offset(binary(at), 5).is_err());
    let plain = felix_wire::binary::encode_publish_ack_bytes(4, None).expect("encode");
    assert_eq!(publish_ack_offset(binary(plain), 4).expect("ok"), None);
    let failed = felix_wire::binary::encode_publish_ack_bytes(4, Some("no")).expect("encode");
    assert!(publish_ack_offset(binary(failed), 4).is_err());

    let json = |message: Message| message.encode().expect("encode");
    let ok = |offset| Message::PublishOk {
        request_id: 4,
        offset,
    };
    assert_eq!(
        publish_ack_offset(json(ok(Some(9))), 4).expect("ok"),
        Some(9)
    );
    assert_eq!(publish_ack_offset(json(ok(None)), 4).expect("ok"), None);
    assert!(publish_ack_offset(json(Message::error("no")), 4).is_err());
}

#[test]
fn ensure_offset_follows_variants() {
    assert_eq!(
        ensure_offset_follows("next", 10, 2, Some(12)).expect("ok"),
        12
    );
    assert!(ensure_offset_follows("gap", 10, 2, Some(13)).is_err());
    assert!(ensure_offset_follows("back", 10, 2, Some(10)).is_err());
    assert!(ensure_offset_follows("none", 10, 2, None).is_err());
}

#[test]
fn ensure_same_frame_compares_flags_and_payload() {
    let plain = felix_wire::binary::encode_publish_ack_bytes(4, None).expect("encode");
    let plain = Frame::decode(plain).expect("frame");
    ensure_same_frame(&plain, &plain.clone(), "same").expect("ok");
    let at = felix_wire::binary::encode_publish_ack_bytes_at(4, None, None, None, Some(1), None)
        .expect("encode");
    assert!(ensure_same_frame(&Frame::decode(at).expect("frame"), &plain, "offset").is_err());
    let json = Message::PublishOk {
        request_id: 4,
        offset: None,
    }
    .encode()
    .expect("encode");
    let with_offset = Message::PublishOk {
        request_id: 4,
        offset: Some(1),
    }
    .encode()
    .expect("encode");
    assert!(ensure_same_frame(&with_offset, &json, "json").is_err());
}
