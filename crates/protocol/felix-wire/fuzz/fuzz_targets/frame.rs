//! Frame decoding against arbitrary bytes.
//!
//! This is the first thing an unauthenticated connection reaches, so the bar is
//! that nothing gets past it by accident: a frame either decodes into exactly
//! what was on the wire, or it is refused.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use felix_wire::{Frame, FrameHeader, KNOWN_FLAGS, has_unknown_flags};

fuzz_target!(|data: &[u8]| {
    let input = Bytes::copy_from_slice(data);

    // Property 1: arbitrary bytes decode or error, never panic.
    let Ok(frame) = Frame::decode(input.clone()) else {
        return;
    };

    // Property 2: the payload is exactly as long as the header said. A frame
    // reader uses this length to find the next frame, so a decode that
    // disagreed with it would desynchronise the stream.
    assert_eq!(frame.payload.len(), frame.header.length as usize);
    assert!(FrameHeader::LEN + frame.payload.len() <= data.len());

    // Property 3: re-encoding reproduces the bytes it came from. Two byte
    // sequences mapping to one frame would mean the length prefix is not the
    // only thing deciding where a frame ends.
    assert_eq!(
        frame.encode().as_ref(),
        &data[..FrameHeader::LEN + frame.payload.len()],
    );

    // Property 4: nothing with an unknown flag bit decodes. Flags select the
    // payload *layout*, so a bit the dispatch does not know means confidently
    // misparsing the body rather than refusing it.
    assert!(!has_unknown_flags(frame.header.flags));
    assert_eq!(frame.header.flags & !KNOWN_FLAGS, 0);
});
