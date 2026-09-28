use bytes::Bytes;

use crate::{Frame, Message};

/// **An `AuthOk` from a broker that predates features decodes.** Absent is not
/// zero-by-accident: it has to mean "implements none", because a client that
/// read silence as support would send a message the broker's control loop
/// treats as a fatal protocol error, costing the connection.
#[test]
fn an_auth_ok_without_features_reads_as_supporting_none() {
    let frame = Frame::new(
        0,
        Bytes::from_static(br#"{"type":"auth_ok","server_flags":7}"#),
    )
    .expect("frame");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(
        decoded,
        Message::AuthOk {
            server_flags: 7,
            server_features: None,
            listener_ports: None,
            publish_window: None,
        }
    );
}

/// **A broker advertising no features encodes the same bytes it always did.**
/// An old client parses this, and a new one reads it as supporting nothing.
#[test]
fn an_auth_ok_advertising_nothing_omits_the_field() {
    let encoded = Message::AuthOk {
        server_flags: 7,
        server_features: None,
        listener_ports: None,
        publish_window: None,
    }
    .encode()
    .expect("encode");
    let json = std::str::from_utf8(&encoded.payload).expect("utf8");
    assert!(
        !json.contains("server_features"),
        "an absent feature set must not appear on the wire: {json}"
    );
    assert!(
        !json.contains("listener_ports"),
        "a single-listener broker must not mention listener_ports: {json}"
    );
}

/// **A broker with one listener is byte-identical to one that predates the
/// field.** The default is a single listener, so this is the common case: an
/// old client must see exactly the frame it has always seen.
#[test]
fn a_single_listener_auth_ok_is_unchanged_on_the_wire() {
    let before = Message::AuthOk {
        server_flags: 7,
        server_features: Some(crate::FEATURE_TOPOLOGY),
        listener_ports: None,
        publish_window: None,
    }
    .encode()
    .expect("encode");
    let json = std::str::from_utf8(&before.payload).expect("utf8");
    assert!(!json.contains("listener_ports"), "{json}");
}

#[test]
fn an_auth_ok_carries_the_listener_ports_it_binds() {
    let message = Message::AuthOk {
        server_flags: felix_wire_flags(),
        server_features: Some(crate::FEATURE_TOPOLOGY),
        listener_ports: Some(vec![5000, 5001, 5002, 5003]),
        publish_window: None,
    };
    let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
    assert_eq!(decoded, message);
}

#[test]
fn an_auth_ok_carries_the_features_it_advertises() {
    let message = Message::AuthOk {
        server_flags: felix_wire_flags(),
        server_features: Some(crate::FEATURE_TOPOLOGY),
        listener_ports: None,
        publish_window: None,
    };
    let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
    assert_eq!(decoded, message);
}

fn felix_wire_flags() -> u16 {
    crate::KNOWN_FLAGS
}

/// **An `Auth` from a client that predates features decodes.** The mirror of
/// the broker-side case: absent has to mean "implements none", or a broker
/// would send a message the client cannot decode and cost the connection.
#[test]
fn an_auth_without_features_reads_as_supporting_none() {
    let frame = Frame::new(
        0,
        Bytes::from_static(br#"{"type":"auth","tenant_id":"t1","token":"x"}"#),
    )
    .expect("frame");
    let decoded = Message::decode(frame).expect("decode");
    assert_eq!(
        decoded,
        Message::Auth {
            tenant_id: "t1".to_string(),
            token: "x".to_string(),
            client_flags: None,
            client_features: None,
        }
    );
}

/// A client advertising nothing sends the bytes it always did, so a broker that
/// predates features parses it unchanged.
#[test]
fn an_auth_advertising_nothing_omits_the_field() {
    let encoded = Message::Auth {
        tenant_id: "t1".to_string(),
        token: "x".to_string(),
        client_flags: None,
        client_features: None,
    }
    .encode()
    .expect("encode");
    let json = std::str::from_utf8(&encoded.payload).expect("utf8");
    assert!(
        !json.contains("client_features"),
        "an absent feature set must not appear on the wire: {json}"
    );
}

#[test]
fn an_auth_ok_without_a_publish_window_is_the_frame_old_clients_read() {
    let json = String::from_utf8(
        Message::AuthOk {
            server_flags: 7,
            server_features: Some(crate::FEATURE_TOPOLOGY),
            listener_ports: None,
            publish_window: None,
        }
        .encode()
        .expect("encode")
        .payload
        .to_vec(),
    )
    .expect("utf8");
    assert!(!json.contains("publish_window"), "{json}");
}

#[test]
fn an_auth_ok_carries_the_publish_window_it_grants() {
    let message = Message::AuthOk {
        server_flags: felix_wire_flags(),
        server_features: Some(crate::FEATURE_PUBLISH_PIPELINE),
        listener_ports: None,
        publish_window: Some(128),
    };
    let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
    assert_eq!(decoded, message);
}
