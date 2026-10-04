//! Record decoding against arbitrary bytes.
//!
//! Segment bytes also arrive over the network from a replication peer, so the
//! decoder is an attack surface: it must never panic, never allocate on the
//! strength of an unvalidated length, and never report a record it did not
//! actually verify.

#![no_main]

use libfuzzer_sys::fuzz_target;

use felix_storage::segment::format::{body_len, decode_record, encode_record, record_len};

fuzz_target!(|data: &[u8]| {
    // Property 1: arbitrary bytes decode or error, but never panic or hang.
    if let Ok((decoded, consumed)) = decode_record(data) {
        // Property 2: a successful decode reports exactly the bytes it used, so
        // a caller walking a segment cannot be walked off a cliff.
        assert_eq!(
            consumed,
            record_len(
                body_len(decoded.payload.len(), decoded.publisher.as_deref()),
                &decoded.mark
            )
        );
        assert!(consumed as usize <= data.len());

        // Property 3: re-encoding what was decoded reproduces the same bytes.
        // A decoder that accepted a record it could not have written would mean
        // two byte sequences map to one record, which breaks checksum-based
        // replication comparisons.
        let mut re_encoded = Vec::new();
        encode_record(
            &mut re_encoded,
            decoded.header.offset,
            decoded.header.timestamp_micros,
            &decoded.payload,
            decoded.publisher.as_deref(),
            &decoded.mark,
        );
        assert_eq!(re_encoded.as_slice(), &data[..consumed as usize]);
    }
});
