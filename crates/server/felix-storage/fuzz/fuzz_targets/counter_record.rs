//! A counter record, and a forwarded counter sum.
//!
//! A counter is a fold over these records, so one misread record makes the
//! sum wrong for good. They must decode exactly or be refused.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    felix_storage::fuzzing::counter_record(data);
});
