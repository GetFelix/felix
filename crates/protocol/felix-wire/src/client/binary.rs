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
    encode_idempotent_publish_batch_bytes, encode_publish_batch_bytes_with_headers,
    peek_acked_publish_prefix,
};
pub use event_batch::{
    EncodedEventBatchParts, EventBatch, EventBatchMeta, MAX_PUBLISHER_BYTES, SharedEventBatch,
    decode_event_batch, decode_shared_event_batch, encode_event_batch_bytes,
    encode_event_batch_bytes_with_meta, encode_event_batch_bytes_with_offset,
    encode_event_batch_bytes_with_skip, encode_event_batch_parts,
    encode_event_batch_parts_with_meta, encode_shared_event_batch_bytes,
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

use bytes::{Buf, Bytes};

use crate::client::flags::{FLAG_EXTENDED, KNOWN_EXT_FLAGS};
use crate::client::record_headers::RecordHeaders;
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

const EXTENSION_LEN: usize = 4;

// The extension word a `FLAG_EXTENDED` frame starts with, consumed from `buf`.
// Zero when the frame is not extended. A word of zero or with a bit this
// version does not know is refused: it would be read as a layout it is not.
fn take_extension(flags: u16, buf: &mut Bytes) -> Result<u32> {
    if flags & FLAG_EXTENDED == 0 {
        return Ok(0);
    }
    if buf.remaining() < EXTENSION_LEN {
        return Err(Error::Incomplete);
    }
    let ext = buf.get_u32();
    if ext == 0 || ext & !KNOWN_EXT_FLAGS != 0 {
        return Err(Error::UnknownExtension(ext));
    }
    Ok(ext)
}

// For a frame kind with no extension word: reading on would take the word for
// the start of its own layout.
pub(super) fn reject_extension(flags: u16) -> Result<()> {
    if flags & FLAG_EXTENDED != 0 {
        return Err(Error::UnknownFlags(flags));
    }
    Ok(())
}

// A record's block as the frame carries it: `u32 len`, then the block. A
// length of zero is a record with none.
fn take_record_block(buf: &mut Bytes) -> Result<Option<Bytes>> {
    if buf.remaining() < PAYLOAD_LEN_PREFIX {
        return Err(Error::Incomplete);
    }
    let len = buf.get_u32() as usize;
    if buf.remaining() < len {
        return Err(Error::Incomplete);
    }
    if len == 0 {
        return Ok(None);
    }
    let block = buf.split_to(len);
    RecordHeaders::validate(&block)?;
    Ok(Some(block))
}

// The blocks to write for `count` records, or `None` when no record has one,
// in which case the frame is the plain one.
fn present_blocks(
    blocks: Option<&[Option<Bytes>]>,
    count: usize,
) -> Result<Option<&[Option<Bytes>]>> {
    let Some(blocks) = blocks else {
        return Ok(None);
    };
    if blocks.len() != count {
        return Err(Error::Incomplete);
    }
    if blocks.iter().flatten().any(Bytes::is_empty) {
        return Err(Error::InvalidRecordHeaders("empty block"));
    }
    Ok(blocks.iter().any(Option::is_some).then_some(blocks))
}
