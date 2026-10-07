//! The `unsupported` answer, the extension area, and decoding a `type` this
//! build does not know.

use super::super::*;

#[test]
fn an_unknown_type_decodes_as_unknown_rather_than_failing() {
    let frame = Frame::new(
        0,
        Bytes::from_static(br#"{"type":"frobnicate","request_id":7,"x":[1,2]}"#),
    )
    .unwrap();
    assert_eq!(Message::decode(frame.clone()).unwrap(), Message::Unknown);
    assert_eq!(
        Message::unknown_request(&frame),
        Some(UnknownRequest {
            request_type: "frobnicate".to_string(),
            request_id: Some(7),
        })
    );
}

#[test]
fn a_request_id_that_is_not_a_number_still_names_the_type() {
    let frame = Frame::new(
        0,
        Bytes::from_static(br#"{"type":"frobnicate","request_id":"seven"}"#),
    )
    .unwrap();
    assert_eq!(
        Message::unknown_request(&frame),
        Some(UnknownRequest {
            request_type: "frobnicate".to_string(),
            request_id: None,
        })
    );
}

/// A known type with a malformed body is still an error: only an unknown
/// `type` is a request from a newer peer.
#[test]
fn a_known_type_with_a_bad_body_is_still_an_error() {
    let frame = Frame::new(0, Bytes::from_static(br#"{"type":"publish_ok"}"#)).unwrap();
    assert!(Message::decode(frame).is_err());
    let garbage = Frame::new(0, Bytes::from_static(b"not json")).unwrap();
    assert!(Message::decode(garbage.clone()).is_err());
    assert_eq!(Message::unknown_request(&garbage), None);
}

#[test]
fn unknown_is_never_encoded() {
    assert!(Message::Unknown.encode().is_err());
}

#[test]
fn unsupported_round_trips_and_omits_what_it_does_not_have() {
    let message = Message::Unsupported {
        request_type: "frobnicate".to_string(),
        extension: None,
        request_id: None,
    };
    let frame = message.encode().unwrap();
    assert_eq!(
        frame.payload,
        Bytes::from_static(br#"{"type":"unsupported","request_type":"frobnicate"}"#)
    );
    assert_eq!(Message::decode(frame).unwrap(), message);
}

#[test]
fn an_extension_carries_any_body() {
    let message = Message::Extension {
        name: "acme.rewind".to_string(),
        request_id: Some(3),
        body: serde_json::json!({"to": 12, "why": ["because"]}),
    };
    let frame = message.encode().unwrap();
    assert_eq!(Message::decode(frame).unwrap(), message);

    let bare = Message::Extension {
        name: "acme.ping".to_string(),
        request_id: None,
        body: serde_json::Value::Null,
    };
    let frame = bare.encode().unwrap();
    assert_eq!(
        frame.payload,
        Bytes::from_static(br#"{"type":"extension","name":"acme.ping"}"#)
    );
    assert_eq!(Message::decode(frame).unwrap(), bare);
}

/// The routing field is new, so a view without it has to stay the frame an
/// old broker sent, and one with it has to decode on an old client (which
/// ignores unknown fields).
#[test]
fn stream_shards_view_only_names_a_routing_mode_that_is_not_modulo() {
    let plain = Message::StreamShardsView {
        shards: 4,
        request_id: 1,
        routing: None,
    };
    assert_eq!(
        plain.encode().unwrap().payload,
        Bytes::from_static(br#"{"type":"stream_shards_view","shards":4,"request_id":1}"#)
    );
    let jump = Message::StreamShardsView {
        shards: 4,
        request_id: 1,
        routing: Some(crate::routing::ShardRouting::JumpHash),
    };
    let frame = jump.encode().unwrap();
    assert_eq!(Message::decode(frame).unwrap(), jump);
}

#[test]
fn an_extension_body_keeps_its_floats_exact() {
    // The default serde_json float parser can land one ulp off on long
    // literals, so a body passed on after decoding would carry another number.
    let frame = Frame::new(
        0,
        Bytes::from_static(
            br#"{"type":"extension","name":"acme.x","body":[44200000000000000000000000000000000000000042444444444]}"#,
        ),
    )
    .unwrap();
    let message = Message::decode(frame).unwrap();
    let again = Message::decode(message.encode().unwrap()).unwrap();
    assert_eq!(again, message);
}
