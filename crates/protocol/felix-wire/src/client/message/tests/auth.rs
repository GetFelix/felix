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
            server_features_hi: None,
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
        server_features_hi: None,
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
        server_features_hi: None,
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
        server_features_hi: None,
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
        server_features_hi: None,
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
            client_features_hi: None,
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
        client_features_hi: None,
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
            server_features_hi: None,
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
        server_features_hi: None,
        listener_ports: None,
        publish_window: Some(128),
    };
    let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
    assert_eq!(decoded, message);
}

/// The `Auth` and `AuthOk` of a peer that predates the extended feature word:
/// the feature set is one `u32`, and serde ignores fields it does not know.
#[derive(Debug, PartialEq, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum OldPeer {
    Auth {
        client_flags: Option<u16>,
        client_features: Option<u32>,
    },
    AuthOk {
        server_flags: u16,
        server_features: Option<u32>,
    },
}

fn json(message: Message) -> String {
    let frame = message.encode().expect("encode");
    String::from_utf8(frame.payload.to_vec()).expect("utf8")
}

fn auth(features: u32, features_hi: u32) -> Message {
    let (client_features, client_features_hi) = crate::offer_features(features, features_hi);
    Message::Auth {
        tenant_id: "t1".to_string(),
        token: "demo-token".to_string(),
        client_flags: Some(25),
        client_features: Some(client_features),
        client_features_hi,
    }
}

fn auth_ok(features: u32, features_hi: u32, peer_features: u32) -> Message {
    let (server_features, server_features_hi) =
        crate::answer_features(features, features_hi, peer_features);
    Message::AuthOk {
        server_flags: 25,
        server_features: Some(server_features),
        server_features_hi,
        listener_ports: None,
        publish_window: None,
    }
}

/// **A peer that knows no extended feature sends the frames it always did.**
/// Neither the marker nor the second word appears, so an old peer on the
/// other side sees exactly what it saw before the word existed.
#[test]
fn without_extended_features_auth_and_auth_ok_are_unchanged() {
    let known = crate::KNOWN_FEATURES;
    assert_eq!(
        json(auth(known, 0)),
        format!(
            r#"{{"type":"auth","tenant_id":"t1","token":"demo-token","client_flags":25,"client_features":{known}}}"#
        )
    );
    // A new client offering nothing extended, or an old one, gets the old
    // `auth_ok` from a new broker, even one that serves extended features.
    for peer in [known, 0] {
        assert_eq!(
            json(auth_ok(known, 1, peer)),
            format!(r#"{{"type":"auth_ok","server_flags":25,"server_features":{known}}}"#)
        );
    }
}

/// **A new client's extended offer still decodes at an old broker.** The
/// first word stays a `u32` and the second is a field it ignores, so the
/// `auth` is read as it always was, marker bit aside.
#[test]
fn an_old_broker_reads_a_new_clients_auth() {
    let old: OldPeer =
        serde_json::from_str(&json(auth(crate::FEATURE_TOPOLOGY, 1))).expect("old decode");
    assert_eq!(
        old,
        OldPeer::Auth {
            client_flags: Some(25),
            client_features: Some(crate::FEATURE_TOPOLOGY | crate::FEATURE_EXTENDED),
        }
    );
}

/// **A new client reads an old broker's `auth_ok` as no extended features**,
/// and an old client reads a new broker's extended `auth_ok` with the second
/// word ignored.
#[test]
fn old_and_new_auth_ok_decode_on_both_sides() {
    let old_frame = r#"{"type":"auth_ok","server_flags":25,"server_features":1}"#;
    let Message::AuthOk {
        server_features,
        server_features_hi,
        ..
    } = serde_json::from_str(old_frame).expect("new decode")
    else {
        panic!("expected auth_ok");
    };
    assert_eq!(server_features_hi, None);
    assert_eq!(
        crate::peer_features_hi(server_features.unwrap_or(0), server_features_hi),
        0
    );

    let extended = json(auth_ok(crate::FEATURE_TOPOLOGY, 1, crate::FEATURE_EXTENDED));
    let old: OldPeer = serde_json::from_str(&extended).expect("old decode");
    assert_eq!(
        old,
        OldPeer::AuthOk {
            server_flags: 25,
            server_features: Some(crate::FEATURE_TOPOLOGY | crate::FEATURE_EXTENDED),
        }
    );
}

/// **A bit in the extended word is negotiated end to end.** The client offers
/// it, the broker reads it, answers with its own extended word, and the client
/// reads that. Bit 32 stands in for the first real extended feature.
#[test]
fn an_extended_feature_is_negotiated_end_to_end() {
    const FIRST_EXTENDED: u32 = 0x0000_0001;

    let offered = Message::decode(
        auth(crate::FEATURE_TOPOLOGY, FIRST_EXTENDED)
            .encode()
            .unwrap(),
    )
    .expect("broker decode");
    let Message::Auth {
        client_features: Some(client_features),
        client_features_hi,
        ..
    } = offered
    else {
        panic!("expected auth");
    };
    let client_hi = crate::peer_features_hi(client_features, client_features_hi);
    assert!(crate::supports_feature(client_hi, FIRST_EXTENDED));
    assert!(crate::supports_feature(
        client_features,
        crate::FEATURE_TOPOLOGY
    ));

    let answered = Message::decode(
        auth_ok(crate::FEATURE_TOPOLOGY, FIRST_EXTENDED, client_features)
            .encode()
            .unwrap(),
    )
    .expect("client decode");
    let Message::AuthOk {
        server_features: Some(server_features),
        server_features_hi,
        ..
    } = answered
    else {
        panic!("expected auth_ok");
    };
    let server_hi = crate::peer_features_hi(server_features, server_features_hi);
    assert!(crate::supports_feature(server_hi, FIRST_EXTENDED));
}

/// **The second word counts only under the marker.** A peer that sent a
/// `_hi` word without `FEATURE_EXTENDED` broke the rule, and its word is not
/// trusted.
#[test]
fn an_extended_word_without_the_marker_is_ignored() {
    assert_eq!(crate::peer_features_hi(crate::FEATURE_TOPOLOGY, Some(1)), 0);
    assert_eq!(crate::peer_features_hi(crate::FEATURE_EXTENDED, Some(1)), 1);
    assert_eq!(crate::peer_features_hi(crate::FEATURE_EXTENDED, None), 0);
}
