use bytes::{Bytes, BytesMut};

use super::*;

fn s(text: &str) -> Utf8 {
    Utf8::try_from(text).unwrap()
}

/// Every property that fits in `scope`, set to something.
fn full(scope: Scope) -> Properties {
    let mut p = Properties::default();
    let mut set = |id: u8, f: &mut dyn FnMut(&mut Properties)| {
        if allowed(id, scope) {
            f(&mut p);
        }
    };
    set(0x01, &mut |p| p.payload_format_indicator = Some(true));
    set(0x02, &mut |p| p.message_expiry_interval = Some(60));
    set(0x03, &mut |p| p.content_type = Some(s("text/plain")));
    set(0x08, &mut |p| p.response_topic = Some(s("reply/to")));
    set(0x09, &mut |p| {
        p.correlation_data = Some(Bytes::from_static(b"\x00\x01"))
    });
    set(0x0B, &mut |p| {
        p.subscription_identifiers = vec![1, 268_435_455]
    });
    set(0x11, &mut |p| p.session_expiry_interval = Some(0xFFFF_FFFF));
    set(0x12, &mut |p| {
        p.assigned_client_identifier = Some(s("auto"))
    });
    set(0x13, &mut |p| p.server_keep_alive = Some(30));
    set(0x15, &mut |p| p.authentication_method = Some(s("SCRAM")));
    set(0x16, &mut |p| {
        p.authentication_data = Some(Bytes::from_static(b"d"))
    });
    set(0x17, &mut |p| p.request_problem_information = Some(false));
    set(0x18, &mut |p| p.will_delay_interval = Some(10));
    set(0x19, &mut |p| p.request_response_information = Some(true));
    set(0x1A, &mut |p| p.response_information = Some(s("info")));
    set(0x1C, &mut |p| p.server_reference = Some(s("host:1883")));
    set(0x1F, &mut |p| p.reason_string = Some(s("because")));
    set(0x21, &mut |p| p.receive_maximum = Some(65_535));
    set(0x22, &mut |p| p.topic_alias_maximum = Some(0));
    set(0x23, &mut |p| p.topic_alias = Some(1));
    set(0x24, &mut |p| p.maximum_qos = Some(QoS::AtLeastOnce));
    set(0x25, &mut |p| p.retain_available = Some(true));
    set(0x26, &mut |p| {
        p.user_properties = vec![(s("k"), s("v")), (s("k"), s("w"))]
    });
    set(0x27, &mut |p| p.maximum_packet_size = Some(1_048_576));
    set(0x28, &mut |p| {
        p.wildcard_subscription_available = Some(true)
    });
    set(0x29, &mut |p| {
        p.subscription_identifiers_available = Some(false)
    });
    set(0x2A, &mut |p| p.shared_subscription_available = Some(false));
    p
}

const SCOPES: [Scope; 11] = [
    Scope::Connect,
    Scope::Will,
    Scope::ConnAck,
    Scope::Publish,
    Scope::Ack,
    Scope::Subscribe,
    Scope::SubAck,
    Scope::Unsubscribe,
    Scope::UnsubAck,
    Scope::Disconnect,
    Scope::Auth,
];

fn encode(p: &Properties, scope: Scope) -> Result<BytesMut, EncodeError> {
    let mut dst = BytesMut::new();
    p.write_section(scope, &mut dst)?;
    Ok(dst)
}

fn decode(bytes: &[u8], scope: Scope) -> Result<Properties, DecodeError> {
    let mut r = Reader::new(Bytes::copy_from_slice(bytes));
    let p = Properties::decode(&mut r, scope, &Limits::default())?;
    r.finish()?;
    Ok(p)
}

#[test]
fn every_property_round_trips_in_every_scope_that_allows_it() {
    for scope in SCOPES {
        let mut p = full(scope);
        // Only AUTH and CONNECT may pair data with a method; the table allows the pair there.
        if p.authentication_data.is_some() && p.authentication_method.is_none() {
            p.authentication_data = None;
        }
        let bytes = encode(&p, scope).unwrap();
        assert_eq!(p.section_len(scope).unwrap(), bytes.len(), "{scope:?}");
        assert_eq!(decode(&bytes, scope).unwrap(), p, "{scope:?}");
    }
}

#[test]
fn the_allowed_table_matches_the_specification() {
    // Table 2-4: the packets each identifier may appear in, by scope index into SCOPES.
    use Scope::*;
    let table: &[(u8, &[Scope])] = &[
        (0x01, &[Publish, Will]),
        (0x02, &[Publish, Will]),
        (0x03, &[Publish, Will]),
        (0x08, &[Publish, Will]),
        (0x09, &[Publish, Will]),
        (0x0B, &[Publish, Subscribe]),
        (0x11, &[Connect, ConnAck, Disconnect]),
        (0x12, &[ConnAck]),
        (0x13, &[ConnAck]),
        (0x15, &[Connect, ConnAck, Auth]),
        (0x16, &[Connect, ConnAck, Auth]),
        (0x17, &[Connect]),
        (0x18, &[Will]),
        (0x19, &[Connect]),
        (0x1A, &[ConnAck]),
        (0x1C, &[ConnAck, Disconnect]),
        (0x1F, &[ConnAck, Ack, SubAck, UnsubAck, Disconnect, Auth]),
        (0x21, &[Connect, ConnAck]),
        (0x22, &[Connect, ConnAck]),
        (0x23, &[Publish]),
        (0x24, &[ConnAck]),
        (0x25, &[ConnAck]),
        (0x26, &SCOPES),
        (0x27, &[Connect, ConnAck]),
        (0x28, &[ConnAck]),
        (0x29, &[ConnAck]),
        (0x2A, &[ConnAck]),
    ];
    for (id, scopes) in table {
        for scope in SCOPES {
            assert_eq!(
                allowed(*id, scope),
                scopes.contains(&scope),
                "{id:#04x} in {scope:?}"
            );
        }
        assert!(is_known(*id));
    }
    for id in [0x00, 0x04, 0x0A, 0x1B, 0x1D, 0x1E, 0x20, 0x2B, 0xFF] {
        assert!(!is_known(id), "{id:#04x}");
    }
}

#[test]
fn an_empty_block_is_one_zero_byte() {
    let p = Properties::default();
    assert!(p.is_empty());
    for scope in SCOPES {
        assert_eq!(encode(&p, scope).unwrap().as_ref(), &[0]);
        assert_eq!(decode(&[0], scope).unwrap(), p);
    }
}

#[test]
fn the_encoder_refuses_values_the_decoder_refuses() {
    type Set = fn(&mut Properties);
    let bad: &[(u8, Scope, Set)] = &[
        (0x21, Scope::Connect, |p| p.receive_maximum = Some(0)),
        (0x23, Scope::Publish, |p| p.topic_alias = Some(0)),
        (0x27, Scope::Connect, |p| p.maximum_packet_size = Some(0)),
        (0x0B, Scope::Publish, |p| {
            p.subscription_identifiers = vec![0]
        }),
        (0x0B, Scope::Publish, |p| {
            p.subscription_identifiers = vec![268_435_456]
        }),
        (0x08, Scope::Publish, |p| p.response_topic = Some(s("a/+"))),
        (0x08, Scope::Publish, |p| p.response_topic = Some(s(""))),
        // Authentication data needs a method beside it.
        (0x16, Scope::Connect, |p| {
            p.authentication_data = Some(Bytes::new())
        }),
    ];
    for (id, scope, set) in bad {
        let mut p = Properties::default();
        set(&mut p);
        assert_eq!(
            p.encoded_len(*scope),
            Err(EncodeError::InvalidProperty(*id)),
            "{id:#04x}"
        );
    }
    let p = Properties {
        maximum_qos: Some(QoS::ExactlyOnce),
        ..Properties::default()
    };
    assert_eq!(
        p.encoded_len(Scope::ConnAck),
        Err(EncodeError::InvalidMaximumQos)
    );
    let p = Properties {
        correlation_data: Some(Bytes::from(vec![0u8; 65_536])),
        ..Properties::default()
    };
    assert_eq!(
        p.encoded_len(Scope::Publish),
        Err(EncodeError::FieldTooLong)
    );
    // A property that does not belong.
    let p = Properties {
        topic_alias: Some(1),
        ..Properties::default()
    };
    assert_eq!(
        p.encoded_len(Scope::ConnAck),
        Err(EncodeError::InvalidProperty(0x23))
    );
}

#[test]
fn the_decoder_stops_at_the_declared_length() {
    // Length 2 holds a one byte flag property; a further byte is outside the block.
    let mut r = Reader::new(Bytes::from_static(&[0x02, 0x25, 0x01, 0x99]));
    let p = Properties::decode(&mut r, Scope::ConnAck, &Limits::default()).unwrap();
    assert_eq!(p.retain_available, Some(true));
    assert_eq!(r.remaining(), 1);
    // Length 1 cuts the property short.
    assert_eq!(
        decode(&[0x01, 0x25], Scope::ConnAck),
        Err(DecodeError::Truncated)
    );
    // Length beyond the packet.
    assert_eq!(
        decode(&[0x05, 0x25], Scope::ConnAck),
        Err(DecodeError::Truncated)
    );
}

#[test]
fn identifiers_are_variable_byte_integers() {
    // 0x25 as a two byte varint is not minimal.
    assert_eq!(
        decode(&[0x03, 0xA5, 0x00, 0x01], Scope::ConnAck),
        Err(DecodeError::MalformedLength)
    );
    // An identifier too large for a byte is unknown, not a panic.
    assert_eq!(
        decode(&[0x03, 0x80, 0x02, 0x00], Scope::ConnAck),
        Err(DecodeError::UnknownProperty(256))
    );
}
