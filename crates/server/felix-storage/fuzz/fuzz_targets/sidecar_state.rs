//! The small per-shard state files read at open: the durable mark, the
//! replica state, the generation history and the producer snapshot.
//!
//! Each is checksummed, and an unreadable one is either ignored or refused
//! depending on what it guards. None of them may panic on the way.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    felix_storage::fuzzing::sidecar_state(data);
});
