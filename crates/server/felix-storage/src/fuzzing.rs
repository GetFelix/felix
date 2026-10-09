//! Entry points for the libFuzzer targets in `crates/server/felix-storage/fuzz`.
//!
//! Behind the `fuzzing` feature and not an API: they reach decoders the crate
//! does not export. Each one asserts the properties its target checks, so a
//! violation is a panic libFuzzer records.

use bytes::Bytes;

use crate::counter_log::record::CounterOp;

/// Decode a counter record. Anything that decodes re-encodes to exactly the
/// bytes it came from: the decoder rejects slack, so two byte strings cannot
/// mean one record.
pub fn counter_record(bytes: &[u8]) {
    let _ = crate::counter_log::decode_sum(bytes);
    if let Ok(op) = CounterOp::decode(&Bytes::copy_from_slice(bytes)) {
        assert_eq!(op.encode().as_ref(), bytes);
    }
}

/// Decode `bytes` as each small per-shard state file: the durable mark, the
/// replica state, the generation history and the producer snapshot.
pub fn sidecar_state(bytes: &[u8]) {
    crate::disk_log::fuzzing::sidecars(bytes);
}

/// Plan the recovery of the shard in `dir`, then recover it: startup must
/// reach the verdict the plan reached, and planning must have written nothing.
pub fn recovery_plan_agrees(dir: &std::path::Path, repair_checksum_tail: bool) {
    crate::disk_log::fuzzing::recovery_plan_agrees(dir, repair_checksum_tail);
}
