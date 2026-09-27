//! Entry points for the libFuzzer targets in `crates/server/felix-kafka/fuzz`.
//!
//! Behind the `fuzzing` feature and not an API: they exist so the targets can
//! reach the decoders without the crate exporting them. Each one asserts the
//! properties a target checks, so a violation is a panic libFuzzer records.

use bytes::Bytes;

use crate::records::decode::{self, MAX_BATCH_BYTES};

/// Decode a partition's record batches the way a produce does.
///
/// Arbitrary bytes must decode or be refused, and no batch may inflate past
/// [`MAX_BATCH_BYTES`]: that bound is what stands between a few megabytes of
/// zstd and the broker's memory.
pub fn record_batches(bytes: &[u8]) {
    record_batches_bytes(Bytes::copy_from_slice(bytes));
}

fn record_batches_bytes(bytes: Bytes) {
    let Ok(batches) = decode::decode(bytes) else {
        return;
    };
    for batch in batches {
        let inflated: usize = batch.values.iter().map(Bytes::len).sum();
        assert!(
            inflated <= MAX_BATCH_BYTES,
            "a batch decoded to {inflated} bytes, past the {MAX_BATCH_BYTES} bound"
        );
    }
}
