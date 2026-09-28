//! A request frame, as the listener reads it after the length prefix.
//!
//! This is the first thing an unauthenticated Kafka client reaches: the
//! request header, every body the listener answers, and the refusals for the
//! group and transaction APIs. It must decode, answer or refuse, never panic.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    felix_kafka::fuzzing::request(data);
});
