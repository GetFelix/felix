//! Binary batch codec for publish and event batches.
//!
//! These frames carry untrusted, attacker-controllable payload counts, so every
//! decode path bounds the count against the remaining buffer before allocating.

mod acked_publish;
mod event_batch;
mod publish;
mod publish_ack;

pub use acked_publish::{
    AckedPublishBatch, ProducerSequence, decode_acked_publish_batch,
    encode_acked_publish_batch_bytes, encode_acked_publish_batch_bytes_keyed,
    encode_idempotent_publish_batch_bytes, peek_acked_publish_prefix,
};
pub use event_batch::{
    EncodedEventBatchParts, EventBatch, EventBatchMeta, MAX_PUBLISHER_BYTES, SharedEventBatch,
    decode_event_batch, decode_shared_event_batch, encode_event_batch_bytes,
    encode_event_batch_bytes_with_meta, encode_event_batch_bytes_with_offset,
    encode_event_batch_bytes_with_skip, encode_event_batch_parts, encode_shared_event_batch_bytes,
    encode_shared_event_batch_bytes_with_meta, encode_shared_event_batch_bytes_with_offset,
    encode_shared_event_batch_bytes_with_skip, peek_event_batch_base_offset,
};
pub use publish::{
    EncodeStats, PublishBatch, decode_publish_batch, encode_publish_batch,
    encode_publish_batch_bytes, encode_publish_batch_bytes_from_bytes,
    encode_publish_batch_bytes_with_stats, encode_publish_batch_bytes_with_stats_from_bytes,
    encode_publish_batch_bytes_with_stats_keyed,
    encode_publish_batch_bytes_with_stats_keyed_from_bytes, encode_publish_batch_keyed,
};
pub use publish_ack::{
    PublishAck, PublishOwner, decode_publish_ack, encode_publish_ack_bytes,
    encode_publish_ack_bytes_at, encode_publish_ack_bytes_coded, encode_publish_ack_bytes_detailed,
    encode_publish_ack_bytes_owned,
};

use crate::error::{Error, Result};

// Every payload in a batch is encoded as a 4-byte length prefix followed by its
// bytes, so a frame can never carry more payloads than it has 4-byte groups left.
const PAYLOAD_LEN_PREFIX: usize = 4;

// Bound an attacker-declared payload count against what the frame can actually
// hold, before it ever reaches `Vec::with_capacity`. The count is read straight
// off the wire, so without this a ~20-byte frame declaring `u32::MAX` payloads
// reserves 95-127 GiB of address space. Overcommit means one such frame usually
// succeeds, but roughly a thousand concurrent ones exhaust the address space and
// the failing allocation calls `handle_alloc_error`, which aborts the process
// instead of unwinding — so it is not contained by per-task panic recovery. A
// memory cgroup or strict overcommit, as in a typical container deployment,
// brings that threshold far lower. The loop below still validates each payload
// individually; this only stops the count itself from being trusted.
fn checked_payload_count(count: usize, remaining: usize) -> Result<usize> {
    if count > remaining / PAYLOAD_LEN_PREFIX {
        return Err(Error::Incomplete);
    }
    Ok(count)
}
