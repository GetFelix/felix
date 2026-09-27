//! The JSON control message against arbitrary bodies.
//!
//! Serde does the parsing, so the interesting property is not "does it parse"
//! but what happens to a message the broker will then dispatch on: an unknown
//! type lands only in `Message::Unknown` (answered, never re-encoded), and any
//! other variant that decodes must survive a round trip so the broker acts on
//! what the client sent.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use felix_wire::{Frame, Message};

fuzz_target!(|data: &[u8]| {
    let Ok(frame) = Frame::new(0, Bytes::copy_from_slice(data)) else {
        return;
    };

    // Property 1: arbitrary bodies decode or error, never panic.
    let Ok(message) = Message::decode(frame.clone()) else {
        return;
    };

    // An unknown type is receive-only: the broker reads its type and request
    // id for the `unsupported` answer and never re-encodes it.
    if message == Message::Unknown {
        let _ = Message::unknown_request(&frame);
        return;
    }

    // Property 2: a decoded message re-encodes and decodes back to itself.
    // Optional fields default to the pre-existing behaviour, so a field that
    // did not survive the trip is one a broker would act on differently from
    // what the client asked for.
    let encoded = message.encode().expect("a decoded message re-encodes");
    let again = Message::decode(encoded).expect("re-decode");
    assert_eq!(again, message);
});
