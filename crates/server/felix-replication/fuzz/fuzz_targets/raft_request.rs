//! The body of a `Raft` frame, as a listener reads it from a peer broker.
//!
//! It must decode or be refused, never panic, and what decoded must encode
//! the same way twice. See `felix_replication::raft::fuzzing::raft_request`.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    felix_replication::raft::fuzzing::raft_request(data);
});
