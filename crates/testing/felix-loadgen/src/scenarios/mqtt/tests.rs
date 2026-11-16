use super::{connect_packet, publish_packet, remaining_length};

#[test]
fn remaining_length_uses_the_continuation_bit() {
    for (n, bytes) in [
        (0, vec![0]),
        (127, vec![127]),
        (128, vec![0x80, 1]),
        (16_384, vec![0x80, 0x80, 1]),
    ] {
        let mut out = Vec::new();
        remaining_length(n, &mut out);
        assert_eq!(out, bytes, "{n}");
    }
}

#[test]
fn connect_is_a_clean_311_session() {
    let p = connect_packet("a", None);
    assert_eq!(p[0], 0x10);
    assert_eq!(&p[2..10], b"\0\x04MQTT\x04\x02");
    assert_eq!(p[1] as usize, p.len() - 2);
}

#[test]
fn publish_is_qos1_with_the_packet_id_after_the_topic() {
    let mut out = Vec::new();
    publish_packet("t/1", 7, b"xy", &mut out);
    assert_eq!(out, [0x32, 9, 0, 3, b't', b'/', b'1', 0, 7, b'x', b'y']);
}

#[test]
fn connect_with_credentials_sends_username_then_password() {
    let p = connect_packet("a", Some(("t", "jwt")));
    assert_eq!(p[9], 0xc2);
    assert_eq!(p[1] as usize, p.len() - 2);
    assert!(p.ends_with(b"\0\x01a\0\x01t\0\x03jwt"));
}
