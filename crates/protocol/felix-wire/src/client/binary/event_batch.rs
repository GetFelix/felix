//! Binary event batches, the broker's delivery frames: one per subscriber, or
//! shared, encoded once for every subscriber of a stream.

use bytes::{Buf, Bytes, BytesMut};

use super::checked_payload_count;
use crate::client::flags::{
    FLAG_BINARY_EVENT_BATCH, FLAG_BINARY_EVENT_BATCH_SHARED, FLAG_EVENT_BATCH_OFFSETS,
    FLAG_EVENT_BATCH_PUBLISHER, FLAG_EVENT_BATCH_SKIPPED,
};
use crate::client::frame::{Frame, FrameHeader};
use crate::error::{Error, Result};

/// Parsed representation of a binary event batch frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBatch {
    pub subscription_id: u64,
    pub payloads: Vec<Bytes>,
    /// Offset of `payloads[0]`, when the sender negotiated
    /// `FLAG_EVENT_BATCH_OFFSETS`. Payload `i` is at `base_offset + i`.
    pub base_offset: Option<u64>,
    /// How many offsets immediately before `base_offset` hold no event, from
    /// `FLAG_EVENT_BATCH_SKIPPED`. Zero when the frame does not carry it.
    pub skipped_before: u64,
    /// The principal that published every event in the batch, from
    /// `FLAG_EVENT_BATCH_PUBLISHER`. `None` when the frame does not carry one.
    pub publisher: Option<Bytes>,
}

/// Parsed representation of a shared event batch frame. It names no
/// subscription: one encoding serves every subscriber of the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedEventBatch {
    pub payloads: Vec<Bytes>,
    /// Offset of `payloads[0]`, when negotiated. See [`EventBatch::base_offset`].
    pub base_offset: Option<u64>,
    /// See [`EventBatch::skipped_before`].
    pub skipped_before: u64,
    /// See [`EventBatch::publisher`].
    pub publisher: Option<Bytes>,
}

/// What an event batch says about its events besides their payloads. Each
/// field goes on the frame only when set, under its own flag, so a batch
/// with none of them is the plain frame.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EventBatchMeta<'a> {
    /// Offset of the first payload (`FLAG_EVENT_BATCH_OFFSETS`).
    pub base_offset: Option<u64>,
    /// Offsets before `base_offset` that hold no event
    /// (`FLAG_EVENT_BATCH_SKIPPED`). Written only when non-zero and with an
    /// offset.
    pub skipped_before: u64,
    /// The principal that published every event in the batch
    /// (`FLAG_EVENT_BATCH_PUBLISHER`), at most 255 bytes.
    pub publisher: Option<&'a [u8]>,
}

/// An event batch as segments, so a writer can send each payload's existing
/// [`Bytes`] without first copying them into one buffer.
#[derive(Debug, Clone)]
pub struct EncodedEventBatchParts {
    frame_len: usize,
    segments: Vec<Bytes>,
}

impl EncodedEventBatchParts {
    /// Total length of every segment, header included.
    pub fn frame_len(&self) -> usize {
        self.frame_len
    }

    /// The segments, in write order.
    pub fn segments(&self) -> &[Bytes] {
        &self.segments
    }

    /// The segments, in write order.
    pub fn into_segments(self) -> Vec<Bytes> {
        self.segments
    }
}

/// Encode binary event batch into a full framed payload.
pub fn encode_event_batch_bytes(subscription_id: u64, payloads: &[Bytes]) -> Result<Bytes> {
    let parts = encode_event_batch_parts(subscription_id, payloads)?;
    let mut buf = BytesMut::with_capacity(parts.frame_len());
    for segment in parts.segments() {
        buf.extend_from_slice(segment.as_ref());
    }
    Ok(buf.freeze())
}

/// Encode an event batch carrying the offset of its first payload.
///
/// Layout is the plain batch with a `u64 base_offset` inserted after the
/// subscription id, and the frame flagged `FLAG_EVENT_BATCH_OFFSETS` so a
/// decoder knows to expect it. Only sent to a peer that negotiated the bit; a
/// peer that did not gets [`encode_event_batch_bytes`] and is unable to tell
/// the difference.
pub fn encode_event_batch_bytes_with_offset(
    subscription_id: u64,
    payloads: &[Bytes],
    base_offset: u64,
) -> Result<Bytes> {
    encode_event_batch_bytes_with_meta(
        subscription_id,
        payloads,
        EventBatchMeta {
            base_offset: Some(base_offset),
            ..EventBatchMeta::default()
        },
    )
}

/// [`encode_event_batch_bytes_with_offset`], also saying how many offsets
/// immediately before `base_offset` hold no event.
///
/// With `skipped_before` zero the frame is exactly the offsets-only one. Only
/// for a peer that negotiated `FLAG_EVENT_BATCH_SKIPPED`.
pub fn encode_event_batch_bytes_with_skip(
    subscription_id: u64,
    payloads: &[Bytes],
    base_offset: u64,
    skipped_before: u64,
) -> Result<Bytes> {
    encode_event_batch_bytes_with_meta(
        subscription_id,
        payloads,
        EventBatchMeta {
            base_offset: Some(base_offset),
            skipped_before,
            publisher: None,
        },
    )
}

/// An event batch carrying whichever of [`EventBatchMeta`]'s fields are set.
/// Only for a peer that negotiated the flag of each one that is.
pub fn encode_event_batch_bytes_with_meta(
    subscription_id: u64,
    payloads: &[Bytes],
    meta: EventBatchMeta<'_>,
) -> Result<Bytes> {
    encode_meta_batch(Some(subscription_id), payloads, meta)
}

/// [`encode_event_batch_bytes`] as segments; see [`EncodedEventBatchParts`].
pub fn encode_event_batch_parts(
    subscription_id: u64,
    payloads: &[Bytes],
) -> Result<EncodedEventBatchParts> {
    let mut payload_len = 8usize + 4;
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        payload_len = payload_len
            .checked_add(4 + len as usize)
            .ok_or(Error::FrameTooLarge)?;
    }
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }

    // Segment 0 is the frame header + fixed event-batch prefix. Remaining segments
    // alternate between [payload_len_prefix, payload_bytes] so writers can stream
    // payload Bytes directly without copying into a contiguous buffer.
    let mut frame_prefix = [0u8; FrameHeader::LEN + 12];
    let header = FrameHeader::new(FLAG_BINARY_EVENT_BATCH, payload_len as u32);
    let mut header_bytes = [0u8; FrameHeader::LEN];
    header.encode_into(&mut header_bytes);
    frame_prefix[..FrameHeader::LEN].copy_from_slice(&header_bytes);
    frame_prefix[FrameHeader::LEN..FrameHeader::LEN + 8]
        .copy_from_slice(&subscription_id.to_be_bytes());
    frame_prefix[FrameHeader::LEN + 8..FrameHeader::LEN + 12]
        .copy_from_slice(&(payloads.len() as u32).to_be_bytes());

    let mut segments = Vec::with_capacity(1 + (payloads.len() * 2));
    segments.push(Bytes::copy_from_slice(&frame_prefix));
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        segments.push(Bytes::copy_from_slice(&len.to_be_bytes()));
        segments.push(payload.clone());
    }

    Ok(EncodedEventBatchParts {
        frame_len: FrameHeader::LEN + payload_len,
        segments,
    })
}

/// Encode a shared (encode-once, fan-out-to-many) batch.
pub fn encode_shared_event_batch_bytes(payloads: &[Bytes]) -> Result<Bytes> {
    let mut payload_len = 4usize;
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        payload_len = payload_len
            .checked_add(4 + len as usize)
            .ok_or(Error::FrameTooLarge)?;
    }
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }

    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + payload_len);
    FrameHeader::new(FLAG_BINARY_EVENT_BATCH_SHARED, payload_len as u32).encode(&mut buf);
    buf.extend_from_slice(&(payloads.len() as u32).to_be_bytes());
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(payload);
    }
    Ok(buf.freeze())
}

/// Encode a shared (encode-once, fan-out-to-many) batch carrying its base offset.
///
/// Offsets belong to the stream rather than the subscriber, so one encoding
/// still serves every subscriber that negotiated the bit -- which is what keeps
/// this off the per-subscriber cost model.
pub fn encode_shared_event_batch_bytes_with_offset(
    payloads: &[Bytes],
    base_offset: u64,
) -> Result<Bytes> {
    encode_shared_event_batch_bytes_with_meta(
        payloads,
        EventBatchMeta {
            base_offset: Some(base_offset),
            ..EventBatchMeta::default()
        },
    )
}

/// [`encode_shared_event_batch_bytes_with_offset`] with a skip count; see
/// [`encode_event_batch_bytes_with_skip`].
pub fn encode_shared_event_batch_bytes_with_skip(
    payloads: &[Bytes],
    base_offset: u64,
    skipped_before: u64,
) -> Result<Bytes> {
    encode_shared_event_batch_bytes_with_meta(
        payloads,
        EventBatchMeta {
            base_offset: Some(base_offset),
            skipped_before,
            publisher: None,
        },
    )
}

/// [`encode_event_batch_bytes_with_meta`] for the shared frame.
pub fn encode_shared_event_batch_bytes_with_meta(
    payloads: &[Bytes],
    meta: EventBatchMeta<'_>,
) -> Result<Bytes> {
    encode_meta_batch(None, payloads, meta)
}

/// An event batch: per-subscriber when `subscription_id` is set, shared
/// otherwise. The optional fields follow the subscription id in a fixed
/// order: offset, skip count, publisher.
fn encode_meta_batch(
    subscription_id: Option<u64>,
    payloads: &[Bytes],
    meta: EventBatchMeta<'_>,
) -> Result<Bytes> {
    let (mut flags, mut payload_len) = match subscription_id {
        Some(_) => (FLAG_BINARY_EVENT_BATCH, 8usize + 4),
        None => (FLAG_BINARY_EVENT_BATCH_SHARED, 4usize),
    };
    let skipped_before = meta.base_offset.map_or(0, |_| meta.skipped_before);
    if meta.base_offset.is_some() {
        flags |= FLAG_EVENT_BATCH_OFFSETS;
        payload_len += 8;
    }
    if skipped_before > 0 {
        flags |= FLAG_EVENT_BATCH_SKIPPED;
        payload_len += 8;
    }
    if let Some(publisher) = meta.publisher {
        if publisher.len() > MAX_PUBLISHER_BYTES {
            return Err(Error::FrameTooLarge);
        }
        flags |= FLAG_EVENT_BATCH_PUBLISHER;
        payload_len += 1 + publisher.len();
    }
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        payload_len = payload_len
            .checked_add(4 + len as usize)
            .ok_or(Error::FrameTooLarge)?;
    }
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }

    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + payload_len);
    FrameHeader::new(flags, payload_len as u32).encode(&mut buf);
    if let Some(subscription_id) = subscription_id {
        buf.extend_from_slice(&subscription_id.to_be_bytes());
    }
    if let Some(base_offset) = meta.base_offset {
        buf.extend_from_slice(&base_offset.to_be_bytes());
    }
    if skipped_before > 0 {
        buf.extend_from_slice(&skipped_before.to_be_bytes());
    }
    if let Some(publisher) = meta.publisher {
        buf.extend_from_slice(&[publisher.len() as u8]);
        buf.extend_from_slice(publisher);
    }
    buf.extend_from_slice(&(payloads.len() as u32).to_be_bytes());
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(payload);
    }
    Ok(buf.freeze())
}

/// The longest publisher an event batch carries: its length is one byte.
pub const MAX_PUBLISHER_BYTES: usize = u8::MAX as usize;

/// Which optional fields an event batch's flags say it carries. A skip count
/// means nothing without the offset it counts back from.
fn meta_fields(flags: u16) -> Result<(bool, bool, bool)> {
    let has_offsets = flags & FLAG_EVENT_BATCH_OFFSETS != 0;
    let has_skip = flags & FLAG_EVENT_BATCH_SKIPPED != 0;
    if has_skip && !has_offsets {
        return Err(Error::UnknownFlags(flags));
    }
    Ok((
        has_offsets,
        has_skip,
        flags & FLAG_EVENT_BATCH_PUBLISHER != 0,
    ))
}

/// Read the publisher a batch carries when `present`, after its fixed fields.
fn take_publisher(buf: &mut Bytes, present: bool) -> Result<Option<Bytes>> {
    if !present {
        return Ok(None);
    }
    if !buf.has_remaining() {
        return Err(Error::Incomplete);
    }
    let len = usize::from(buf.get_u8());
    if buf.remaining() < len {
        return Err(Error::Incomplete);
    }
    Ok(Some(buf.split_to(len)))
}

/// Decode binary event batch frame into its structured form.
pub fn decode_event_batch(frame: &Frame) -> Result<EventBatch> {
    let mut buf = frame.payload.clone();
    let (has_offsets, has_skip, has_publisher) = meta_fields(frame.header.flags)?;
    let fixed = 8 + if has_offsets { 8 } else { 0 } + if has_skip { 8 } else { 0 };
    if buf.remaining() < fixed {
        return Err(Error::Incomplete);
    }
    let subscription_id = buf.get_u64();
    let base_offset = has_offsets.then(|| buf.get_u64());
    let skipped_before = if has_skip { buf.get_u64() } else { 0 };
    let publisher = take_publisher(&mut buf, has_publisher)?;
    if buf.remaining() < 4 {
        return Err(Error::Incomplete);
    }
    let count = checked_payload_count(buf.get_u32() as usize, buf.remaining())?;
    let mut payloads = Vec::with_capacity(count);
    for _ in 0..count {
        if buf.remaining() < 4 {
            return Err(Error::Incomplete);
        }
        let len = buf.get_u32() as usize;
        if buf.remaining() < len {
            return Err(Error::Incomplete);
        }
        let bytes = buf.copy_to_bytes(len);
        payloads.push(bytes);
    }
    Ok(EventBatch {
        subscription_id,
        payloads,
        base_offset,
        skipped_before,
        publisher,
    })
}

/// Decode a shared event batch frame.
pub fn decode_shared_event_batch(frame: &Frame) -> Result<SharedEventBatch> {
    let mut buf = frame.payload.clone();
    let (has_offsets, has_skip, has_publisher) = meta_fields(frame.header.flags)?;
    let fixed = if has_offsets { 8 } else { 0 } + if has_skip { 8 } else { 0 };
    if buf.remaining() < fixed {
        return Err(Error::Incomplete);
    }
    let base_offset = has_offsets.then(|| buf.get_u64());
    let skipped_before = if has_skip { buf.get_u64() } else { 0 };
    let publisher = take_publisher(&mut buf, has_publisher)?;
    if buf.remaining() < 4 {
        return Err(Error::Incomplete);
    }
    let count = checked_payload_count(buf.get_u32() as usize, buf.remaining())?;
    let mut payloads = Vec::with_capacity(count);
    for _ in 0..count {
        if buf.remaining() < 4 {
            return Err(Error::Incomplete);
        }
        let len = buf.get_u32() as usize;
        if buf.remaining() < len {
            return Err(Error::Incomplete);
        }
        payloads.push(buf.copy_to_bytes(len));
    }
    Ok(SharedEventBatch {
        payloads,
        base_offset,
        skipped_before,
        publisher,
    })
}

/// The offset of an event batch's first record, read without decoding its
/// payloads. `None` for a frame that is not an event batch or carries no
/// offsets, or one too short to hold the field.
pub fn peek_event_batch_base_offset(frame: &Frame) -> Option<u64> {
    let flags = frame.header.flags;
    if flags & FLAG_EVENT_BATCH_OFFSETS == 0 {
        return None;
    }
    // A per-subscriber batch leads with its subscription id; a shared one
    // names no subscription.
    let at = if flags & FLAG_BINARY_EVENT_BATCH_SHARED != 0 {
        0
    } else if flags & FLAG_BINARY_EVENT_BATCH != 0 {
        8
    } else {
        return None;
    };
    let field = frame.payload.get(at..at + 8)?;
    Some(u64::from_be_bytes(field.try_into().ok()?))
}

#[cfg(test)]
mod tests;
