//! A produce partition's record batches, straight into the batch decoder.
//!
//! Mutation spends its budget on the batch format and the codecs rather than
//! on reaching them through a request. A batch must decode or be refused, and
//! must not inflate past its bound.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    felix_kafka::fuzzing::record_batches(data);
});
