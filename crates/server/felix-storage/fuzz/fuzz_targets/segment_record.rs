//! Record decoding against arbitrary bytes.
//!
//! Segment bytes also arrive over the network from a replication peer, so the
//! decoder is an attack surface: it must never panic, never allocate on the
//! strength of an unvalidated length, and never report a record it did not
//! actually verify. Both record layouts are tried: v8, and v9 with the
//! generation in the header.

#![no_main]

use libfuzzer_sys::fuzz_target;

use felix_storage::segment::format::{
    GENERATION_VERSION, HEADERS_VERSION, body_len, decode_record_in, encode_record_at,
    record_len_in,
};

fuzz_target!(|data: &[u8]| {
    for version in [HEADERS_VERSION, GENERATION_VERSION] {
        // Property 1: arbitrary bytes decode or error, but never panic or hang.
        let Ok((decoded, consumed)) = decode_record_in(data, version) else {
            continue;
        };
        // Property 2: a successful decode reports exactly the bytes it used, so
        // a caller walking a segment cannot be walked off a cliff.
        assert_eq!(
            consumed,
            record_len_in(
                body_len(
                    decoded.payload.len(),
                    decoded.headers.as_deref(),
                    decoded.publisher.as_deref(),
                ),
                &decoded.mark,
                version,
            )
        );
        assert!(consumed as usize <= data.len());
        assert_eq!(
            decoded.header.generation.is_some(),
            version >= GENERATION_VERSION
        );

        // Property 3: re-encoding what was decoded reproduces the same bytes.
        // A decoder that accepted a record it could not have written would mean
        // two byte sequences map to one record, which breaks checksum-based
        // replication comparisons.
        let mut re_encoded = Vec::new();
        encode_record_at(
            &mut re_encoded,
            decoded.header.generation,
            decoded.header.offset,
            decoded.header.timestamp_micros,
            &decoded.payload,
            decoded.headers.as_deref(),
            decoded.publisher.as_deref(),
            &decoded.mark,
        );
        assert_eq!(re_encoded.as_slice(), &data[..consumed as usize]);
    }
});
