//! The broker-to-broker decoder against arbitrary bytes.
//!
//! A peer connection is authenticated, but "authenticated" is not "trusted with
//! a length field": a compromised or simply mismatched broker reaches this
//! decoder with whatever it likes, and an undecodable frame here ends a
//! long-lived lane carrying every in-flight request.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use felix_wire::internal::{
    FrameEnvelope, InternalHeader, InternalMessage, Kind, correlation_id_in,
};

fuzz_target!(|data: &[u8]| {
    // Property 1: an unknown kind is rejected, never skipped. The kind selects
    // how to read the body, so ignoring one means misparsing it.
    if data.len() >= 2 {
        let raw = u16::from_be_bytes([data[0], data[1]]);
        if let Ok(kind) = Kind::from_u16(raw) {
            assert_eq!(kind as u16, raw);
        }
    }

    // The peer reader steps over a kind it does not know using only the
    // envelope, so the envelope must agree with the full header whenever both
    // read the same bytes.
    let input = Bytes::copy_from_slice(data);
    if let Ok(header) = InternalHeader::decode(&input) {
        let envelope = FrameEnvelope::decode(&input).expect("header decoded");
        assert_eq!(envelope.kind, header.kind as u16);
        assert_eq!(envelope.length, header.length);
    }
    let _ = correlation_id_in(input.get(InternalHeader::LEN..).unwrap_or_default());

    // Property 2: arbitrary bytes decode or error, never panic, and never
    // allocate on the strength of an unvalidated length.
    let Ok(message) = InternalMessage::decode(Bytes::copy_from_slice(data)) else {
        return;
    };

    // Property 3: what decoded re-encodes to the same bytes. The decoder
    // rejects trailing bytes rather than ignoring them, and this is what holds
    // that true — a body with slack would re-encode shorter than it arrived.
    let Ok(re_encoded) = message.encode() else {
        panic!("a message that decoded could not be re-encoded");
    };
    assert_eq!(re_encoded.as_ref(), data);

    // Property 4: the round trip is stable, so decoding is a function of the
    // bytes rather than of anything left over from the first pass.
    let again = InternalMessage::decode(re_encoded).expect("re-decode");
    assert_eq!(again, message);
});
