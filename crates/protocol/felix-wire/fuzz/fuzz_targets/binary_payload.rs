//! The binary payload decoders against arbitrary bodies.
//!
//! These are where the length and count fields live: a publish batch is a
//! tenant, a namespace, a stream, a count, and then that many length-prefixed
//! payloads. Every one of those numbers comes from the client, and every one of
//! them is a chance to size a buffer from something nobody checked.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use felix_wire::binary;
use felix_wire::{
    FLAG_BINARY_EVENT_BATCH, FLAG_BINARY_EVENT_BATCH_SHARED, FLAG_BINARY_PUBLISH_ACK,
    FLAG_BINARY_PUBLISH_ACK_OFFSET, FLAG_BINARY_PUBLISH_ACK_OWNER, FLAG_BINARY_PUBLISH_ACKED, FLAG_BINARY_PUBLISH_BATCH, FLAG_BINARY_PUBLISH_KEYED,
    FLAG_EVENT_BATCH_OFFSETS, Frame,
};

/// Every flag combination a decoder is reachable through, so the fuzzer is not
/// left to guess the two bytes that select a layout.
const LAYOUTS: &[u16] = &[
    FLAG_BINARY_PUBLISH_BATCH,
    FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_KEYED,
    FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED,
    FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED | FLAG_BINARY_PUBLISH_KEYED,
    FLAG_BINARY_PUBLISH_ACK,
    FLAG_BINARY_PUBLISH_ACK | FLAG_BINARY_PUBLISH_ACK_OFFSET,
    FLAG_BINARY_PUBLISH_ACK | FLAG_BINARY_PUBLISH_ACK_OWNER | FLAG_BINARY_PUBLISH_ACK_OFFSET,
    FLAG_BINARY_EVENT_BATCH,
    FLAG_BINARY_EVENT_BATCH | FLAG_EVENT_BATCH_OFFSETS,
    FLAG_BINARY_EVENT_BATCH_SHARED,
    FLAG_BINARY_EVENT_BATCH_SHARED | FLAG_EVENT_BATCH_OFFSETS,
];

fuzz_target!(|data: &[u8]| {
    for &flags in LAYOUTS {
        let Ok(frame) = Frame::new(flags, Bytes::copy_from_slice(data)) else {
            continue;
        };

        // Property 1: every decoder either parses the body or errors. None of
        // them may panic, and none may allocate from a count it has not
        // checked against the bytes actually present.
        if flags & FLAG_BINARY_PUBLISH_ACKED != 0 {
            if let Ok(batch) = binary::decode_acked_publish_batch(&frame) {
                // Property 2: a decoded batch holds no more payloads than the
                // body had bytes to describe them with.
                assert!(batch.batch.payloads.len() <= data.len());
            }
        } else if flags & FLAG_BINARY_PUBLISH_BATCH != 0 {
            if let Ok(batch) = binary::decode_publish_batch(&frame) {
                assert!(batch.payloads.len() <= data.len());
            }
        } else if flags & FLAG_BINARY_PUBLISH_ACK != 0 {
            let _ = binary::decode_publish_ack(&frame);
        } else if flags & FLAG_BINARY_EVENT_BATCH_SHARED != 0 {
            if let Ok(batch) = binary::decode_shared_event_batch(&frame) {
                assert!(batch.payloads.len() <= data.len());
            }
        } else if let Ok(batch) = binary::decode_event_batch(&frame) {
            assert!(batch.payloads.len() <= data.len());
        }
    }
});
