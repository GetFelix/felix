//! A cache record, as replay reads it back out of the cache's log.
//!
//! The cache is rebuilt from these bytes on every start, and they also arrive
//! from a replication peer, so a record must decode or be refused as
//! corruption, never panic.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use felix_storage::cache::CacheOp;

fuzz_target!(|data: &[u8]| {
    let Ok(op) = CacheOp::decode(&Bytes::copy_from_slice(data)) else {
        return;
    };
    // A put's value is the rest of the record, so a put re-encodes to exactly
    // what it came from. A delete ignores its expiry and anything after the
    // key, so it only has to survive a round trip.
    let encoded = op.encode();
    if matches!(op, CacheOp::Put { .. }) {
        assert_eq!(encoded.as_ref(), data);
    }
    assert_eq!(CacheOp::decode(&encoded).expect("re-decode"), op);
});
