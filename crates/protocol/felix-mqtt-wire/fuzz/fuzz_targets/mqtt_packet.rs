//! MQTT packets against arbitrary bytes.
//!
//! The first byte picks the connection state the rest is decoded in: before
//! `CONNECT`, or after it at 3.1.1 or 5.0. The rest is a stream, so pipelined
//! packets and a packet that has not fully arrived are covered by the same
//! input.

#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

use felix_mqtt_wire::{Limits, Packet, Version};

fuzz_target!(|data: &[u8]| {
    let Some((&selector, stream)) = data.split_first() else {
        return;
    };
    let mut version = match selector % 3 {
        0 => None,
        1 => Some(Version::V311),
        _ => Some(Version::V5),
    };
    let limits = Limits::default();
    let mut src = BytesMut::from(stream);

    // Property 1: any bytes decode, wait or error, never panic.
    for _ in 0..16 {
        let before = src.len();
        let packet = match Packet::decode(&mut src, version, &limits) {
            Ok(Some(packet)) => packet,
            // Property 2: waiting for more input takes nothing from the buffer.
            Ok(None) => {
                assert_eq!(src.len(), before);
                return;
            }
            Err(_) => return,
        };
        assert!(src.len() < before);

        // A CONNECT sets the connection's version for what follows.
        if let Packet::Connect(c) = &packet {
            version = Some(c.version);
        }
        let v = version.unwrap_or(Version::V311);

        // Property 3: what the decoder accepts, the encoder can write, and it
        // reads back as the same packet.
        let bytes = packet.to_bytes(v).expect("a decoded packet encodes");
        assert_eq!(packet.encoded_len(v).expect("sized"), bytes.len());
        let mut again = BytesMut::from(&bytes[..]);
        let known = (!matches!(packet, Packet::Connect(_))).then_some(v);
        let back = Packet::decode(&mut again, known, &limits)
            .expect("an encoded packet decodes")
            .expect("whole packet");
        assert_eq!(back, packet);
        assert!(again.is_empty());

        // Property 4: the encoding is a fixed point.
        assert_eq!(back.to_bytes(v).expect("encodes again"), bytes);
    }
});
