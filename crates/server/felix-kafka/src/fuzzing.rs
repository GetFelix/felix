//! Entry points for the libFuzzer targets in `crates/server/felix-kafka/fuzz`.
//!
//! Behind the `fuzzing` feature and not an API: they exist so the targets can
//! reach the decoders without the crate exporting them. Each one asserts the
//! properties a target checks, so a violation is a panic libFuzzer records.

use bytes::Bytes;

use crate::api::{self, Body, Parsed};
use crate::records::decode::{self, MAX_BATCH_BYTES};

/// Decode a request frame the way the connection does after reading its
/// length prefix, and a SASL/PLAIN message out of any authenticate body.
///
/// Every byte here comes from a client before it has authenticated, so the
/// only acceptable outcomes are a decoded request, an answer, or an error.
pub fn request(bytes: &[u8]) {
    if let Ok(Parsed::Request(request)) = api::parse(Bytes::copy_from_slice(bytes))
        && let Body::SaslAuthenticate(auth) = &request.body
    {
        let _ = api::sasl::parse_plain(&auth.auth_bytes);
    }
    // The PLAIN parser also sees raw bytes directly, so mutation is not left
    // to rebuild a whole request header to reach it.
    let _ = api::sasl::parse_plain(bytes);
}

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
