//! Acked binary publish: a publish batch the broker owes an answer for.
//!
//! The unacked binary publish frame has no room for a request id, which is why
//! acked publishes were stuck on the JSON control encoding. An acked frame sets
//! FLAG_BINARY_PUBLISH_ACKED alongside FLAG_BINARY_PUBLISH_BATCH and prefixes the
//! ordinary publish-batch body with:
//!
//! ```text
//! u64 request_id
//! u8  ack_mode      (1 = PerMessage, 2 = PerBatch)
//! ```
//!
//! The prefix goes first so a decoder can read the correlation id without parsing
//! the rest of the frame — that is what lets the broker answer with an error
//! carrying the right request_id even when the body turns out to be malformed.
//!
//! AckMode::None is deliberately not representable here: an unacked publish uses
//! the plain FLAG_BINARY_PUBLISH_BATCH encoding with no prefix at all, so there is
//! exactly one encoding per mode rather than two ways to say "no ack".

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::de::Error as SerdeError;

use super::publish::{
    PublishBatch, decode_publish_body, encode_publish_batch_keyed, key_prefix_len, publish_flags,
    put_key_prefix,
};
use super::{EXTENSION_LEN, present_blocks, take_extension};
use crate::client::flags::{
    EXT_RECORD_HEADERS, FLAG_BINARY_PUBLISH_ACKED, FLAG_BINARY_PUBLISH_IDEMPOTENT,
    FLAG_BINARY_PUBLISH_KEYED, FLAG_EXTENDED,
};
use crate::client::frame::{Frame, FrameHeader};
use crate::client::message::AckMode;
use crate::error::{Error, Result};

const ACK_MODE_PER_MESSAGE: u8 = 1;
const ACK_MODE_PER_BATCH: u8 = 2;
// u64 request_id + u8 ack_mode.
const ACKED_PREFIX_LEN: usize = 9;

// u64 producer_id + u64 sequence, after the acked prefix.
const PRODUCER_PREFIX_LEN: usize = 16;

/// A binary publish batch that asked to be acknowledged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckedPublishBatch {
    pub request_id: u64,
    pub ack: AckMode,
    /// Present when the frame carried `FLAG_BINARY_PUBLISH_IDEMPOTENT`.
    pub producer: Option<ProducerSequence>,
    pub batch: PublishBatch,
}

/// The producer id and sequence an idempotent batch is appended under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerSequence {
    pub producer_id: u64,
    pub sequence: u64,
}

/// Encode an acked publish batch into a full framed buffer (header included).
pub fn encode_acked_publish_batch_bytes(
    request_id: u64,
    ack: AckMode,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<Bytes> {
    encode_acked_publish_batch_bytes_keyed(
        request_id, ack, None, tenant_id, namespace, stream, payloads,
    )
}

/// The keyed form of [`encode_acked_publish_batch_bytes`].
///
/// The key prefix sits *after* the correlation prefix, so
/// [`peek_acked_publish_prefix`] still reads the request id at offset 0 whether
/// or not a key follows.
pub fn encode_acked_publish_batch_bytes_keyed(
    request_id: u64,
    ack: AckMode,
    key: Option<&[u8]>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<Bytes> {
    encode_acked_inner(
        request_id, ack, None, key, tenant_id, namespace, stream, payloads,
    )
}

/// Encode a batch under an idempotent producer's sequence. Always acked per
/// batch; the producer prefix follows the acked prefix and precedes any key.
#[allow(clippy::too_many_arguments)]
pub fn encode_idempotent_publish_batch_bytes(
    request_id: u64,
    producer: ProducerSequence,
    key: Option<&[u8]>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<Bytes> {
    encode_acked_inner(
        request_id,
        AckMode::PerBatch,
        Some(producer),
        key,
        tenant_id,
        namespace,
        stream,
        payloads,
    )
}

/// Read only the correlation prefix, without decoding the batch body.
///
/// The broker needs this to answer a malformed acked publish with a
/// `PublishError` the client can actually match to its pending request. Without
/// it a body-level decode failure would leave the client blocked until timeout.
/// On an extended frame it steps over the extension word without checking it,
/// for the same reason.
pub fn peek_acked_publish_prefix(frame: &Frame) -> Result<(u64, AckMode)> {
    let mut buf = frame.payload.clone();
    if frame.header.flags & FLAG_EXTENDED != 0 {
        if buf.remaining() < EXTENSION_LEN {
            return Err(Error::Incomplete);
        }
        buf.advance(EXTENSION_LEN);
    }
    take_acked_prefix(&mut buf)
}

fn take_acked_prefix(buf: &mut Bytes) -> Result<(u64, AckMode)> {
    if buf.remaining() < ACKED_PREFIX_LEN {
        return Err(Error::Incomplete);
    }
    let request_id = buf.get_u64();
    let ack = ack_mode_from_wire(buf.get_u8())?;
    Ok((request_id, ack))
}

/// Decode an acked binary publish batch frame.
pub fn decode_acked_publish_batch(frame: &Frame) -> Result<AckedPublishBatch> {
    let flags = frame.header.flags;
    let mut buf = frame.payload.clone();
    let ext = take_extension(flags, &mut buf)?;
    let (request_id, ack) = take_acked_prefix(&mut buf)?;
    let producer = if flags & FLAG_BINARY_PUBLISH_IDEMPOTENT != 0 {
        if buf.remaining() < PRODUCER_PREFIX_LEN {
            return Err(Error::Incomplete);
        }
        Some(ProducerSequence {
            producer_id: buf.get_u64(),
            sequence: buf.get_u64(),
        })
    } else {
        None
    };
    // What is left is the plain batch body, so both encodings share one parser
    // and one set of bounds checks.
    Ok(AckedPublishBatch {
        request_id,
        ack,
        producer,
        batch: decode_publish_body(
            buf,
            flags & FLAG_BINARY_PUBLISH_KEYED != 0,
            ext & EXT_RECORD_HEADERS != 0,
        )?,
    })
}

/// Encode a publish batch whose records may carry headers, with whichever of
/// the ack prefix, producer sequence and routing key apply.
///
/// `record_headers` holds one block per payload (`None` for a record with
/// none); a block must be a valid [`RecordHeaders`](crate::RecordHeaders)
/// encoding. When every entry is `None` the frame is exactly the one
/// [`encode_publish_batch_bytes_with_stats_keyed`](super::encode_publish_batch_bytes_with_stats_keyed),
/// [`encode_acked_publish_batch_bytes_keyed`] or
/// [`encode_idempotent_publish_batch_bytes`] would give, so a caller can use
/// this for every publish. Otherwise the frame is `FLAG_EXTENDED` with
/// `EXT_RECORD_HEADERS`, which only a broker that offered it in
/// `AuthOk.server_flags_ext` accepts.
///
/// `ack` is the request id and mode of an acked publish. A `producer` needs
/// `ack` with [`AckMode::PerBatch`], as an idempotent publish does.
#[allow(clippy::too_many_arguments)]
pub fn encode_publish_batch_bytes_with_headers<P: AsRef<[u8]>>(
    ack: Option<(u64, AckMode)>,
    producer: Option<ProducerSequence>,
    key: Option<&[u8]>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[P],
    record_headers: &[Option<Bytes>],
) -> Result<Bytes> {
    let ack = ack
        .map(|(request_id, mode)| Ok::<_, Error>((request_id, ack_mode_to_wire(mode)?)))
        .transpose()?;
    if producer.is_some() && !matches!(ack, Some((_, ACK_MODE_PER_BATCH))) {
        return Err(Error::Deserialize(SerdeError::custom(
            "an idempotent publish must be acked per batch",
        )));
    }
    let blocks = present_blocks(Some(record_headers), payloads.len())?;
    let names = [tenant_id, namespace, stream];
    for name in names {
        u16::try_from(name.len()).map_err(|_| Error::FrameTooLarge)?;
    }
    let mut payload_len = key_prefix_len(key)?
        + names.iter().map(|name| 2 + name.len()).sum::<usize>()
        + 4
        + if blocks.is_some() { EXTENSION_LEN } else { 0 }
        + if ack.is_some() { ACKED_PREFIX_LEN } else { 0 }
        + if producer.is_some() {
            PRODUCER_PREFIX_LEN
        } else {
            0
        };
    for (index, payload) in payloads.iter().enumerate() {
        u32::try_from(payload.as_ref().len()).map_err(|_| Error::FrameTooLarge)?;
        let block = blocks.map_or(0, |blocks| 4 + blocks[index].as_ref().map_or(0, Bytes::len));
        payload_len = payload_len
            .checked_add(4 + payload.as_ref().len() + block)
            .ok_or(Error::FrameTooLarge)?;
    }
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }

    let mut flags = publish_flags(key);
    if ack.is_some() {
        flags |= FLAG_BINARY_PUBLISH_ACKED;
    }
    if producer.is_some() {
        flags |= FLAG_BINARY_PUBLISH_IDEMPOTENT;
    }
    if blocks.is_some() {
        flags |= FLAG_EXTENDED;
    }
    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + payload_len);
    FrameHeader::new(flags, payload_len as u32).encode(&mut buf);
    if blocks.is_some() {
        buf.put_u32(EXT_RECORD_HEADERS);
    }
    if let Some((request_id, mode)) = ack {
        buf.put_u64(request_id);
        buf.put_u8(mode);
    }
    if let Some(producer) = producer {
        buf.put_u64(producer.producer_id);
        buf.put_u64(producer.sequence);
    }
    put_key_prefix(&mut buf, key);
    for name in names {
        buf.put_u16(name.len() as u16);
        buf.extend_from_slice(name.as_bytes());
    }
    buf.put_u32(payloads.len() as u32);
    for (index, payload) in payloads.iter().enumerate() {
        if let Some(blocks) = blocks {
            let block = blocks[index].as_deref().unwrap_or_default();
            buf.put_u32(block.len() as u32);
            buf.extend_from_slice(block);
        }
        buf.put_u32(payload.as_ref().len() as u32);
        buf.extend_from_slice(payload.as_ref());
    }
    Ok(buf.freeze())
}

#[allow(clippy::too_many_arguments)]
fn encode_acked_inner(
    request_id: u64,
    ack: AckMode,
    producer: Option<ProducerSequence>,
    key: Option<&[u8]>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<Bytes> {
    let ack_byte = ack_mode_to_wire(ack)?;
    // Reuse the unacked body encoder rather than duplicating its bounds checks,
    // then splice the prefixes in front and restate the header.
    let body = encode_publish_batch_keyed(key, tenant_id, namespace, stream, payloads)?.payload;
    let prefix_len = ACKED_PREFIX_LEN + producer.map_or(0, |_| PRODUCER_PREFIX_LEN);
    let payload_len = prefix_len
        .checked_add(body.len())
        .ok_or(Error::FrameTooLarge)?;
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }
    let mut flags = publish_flags(key) | FLAG_BINARY_PUBLISH_ACKED;
    if producer.is_some() {
        flags |= FLAG_BINARY_PUBLISH_IDEMPOTENT;
    }
    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + payload_len);
    FrameHeader::new(flags, payload_len as u32).encode(&mut buf);
    buf.put_u64(request_id);
    buf.put_u8(ack_byte);
    if let Some(producer) = producer {
        buf.put_u64(producer.producer_id);
        buf.put_u64(producer.sequence);
    }
    buf.extend_from_slice(&body);
    Ok(buf.freeze())
}

fn ack_mode_to_wire(ack: AckMode) -> Result<u8> {
    match ack {
        AckMode::PerMessage => Ok(ACK_MODE_PER_MESSAGE),
        AckMode::PerBatch => Ok(ACK_MODE_PER_BATCH),
        // Callers must route AckMode::None to `encode_publish_batch`.
        AckMode::None => Err(Error::Deserialize(SerdeError::custom(
            "AckMode::None has no acked binary encoding",
        ))),
    }
}

fn ack_mode_from_wire(byte: u8) -> Result<AckMode> {
    match byte {
        ACK_MODE_PER_MESSAGE => Ok(AckMode::PerMessage),
        ACK_MODE_PER_BATCH => Ok(AckMode::PerBatch),
        _ => Err(Error::Deserialize(SerdeError::custom("invalid ack mode"))),
    }
}

#[cfg(test)]
mod tests;
