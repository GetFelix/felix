//! The record headers block against arbitrary bytes.
//!
//! The block's counts and lengths come from whoever published the record, and
//! the broker stores what passes `validate` without parsing it again.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use felix_wire::RecordHeaders;

fuzz_target!(|data: &[u8]| {
    // Property 1: `validate` and `decode` never panic and always agree, so a
    // reader that only validates accepts exactly what a parsing reader does.
    let validated = RecordHeaders::validate(data);
    let decoded = RecordHeaders::decode(Bytes::copy_from_slice(data));
    assert_eq!(validated.is_ok(), decoded.is_ok());

    // Property 2: a block that parses is written back as the bytes it was.
    // Two encodings of one block would let a record change under a re-write.
    if let Ok(block) = decoded {
        assert_eq!(block.encode().expect("re-encode").as_ref(), data);
    }
});
