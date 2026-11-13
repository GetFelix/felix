//! The body of a `RaftAnswer` frame, as a sender reads it from a peer broker.
//!
//! It must decode or be refused, never panic, and what decoded must encode
//! the same way twice. See `felix_replication::raft::fuzzing::raft_answer`.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    felix_replication::raft::fuzzing::raft_answer(data);
});
