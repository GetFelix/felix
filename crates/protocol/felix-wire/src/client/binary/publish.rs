//! The binary publish batch: a client's records for one stream, optionally keyed.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::de::Error as SerdeError;

use super::checked_payload_count;
use crate::client::flags::{
    FLAG_BINARY_PUBLISH_ACKED, FLAG_BINARY_PUBLISH_BATCH, FLAG_BINARY_PUBLISH_IDEMPOTENT,
    FLAG_BINARY_PUBLISH_KEYED,
};
use crate::client::frame::{Frame, FrameHeader};
use crate::error::{Error, Result};

// A keyed frame prefixes the body with a u16 length and the key bytes.
const KEY_LEN_PREFIX: usize = 2;

/// Parsed representation of a binary publish batch frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBatch {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    /// The routing key, present only when the frame carried
    /// `FLAG_BINARY_PUBLISH_KEYED`. `None` means unkeyed, which is not the same
    /// as an empty key: an empty key is a key, and hashes like any other.
    pub key: Option<Bytes>,
    /// Record bodies, each a slice of the decoded frame's payload rather than
    /// a copy, so a retained record keeps its whole frame alive.
    pub payloads: Vec<Bytes>,
}

/// How often the output buffer had to grow while encoding.
#[derive(Debug, Clone, Copy, Default)]
pub struct EncodeStats {
    pub reallocs: u64,
}

/// Encode a publish batch into a binary frame.
pub fn encode_publish_batch(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<Frame> {
    encode_publish_batch_keyed(None, tenant_id, namespace, stream, payloads)
}

/// Encode a publish batch into a binary frame, optionally keyed.
pub fn encode_publish_batch_keyed(
    key: Option<&[u8]>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<Frame> {
    let tenant_bytes = tenant_id.as_bytes();
    let tenant_len = u16::try_from(tenant_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let namespace_bytes = namespace.as_bytes();
    let namespace_len = u16::try_from(namespace_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let stream_bytes = stream.as_bytes();
    let stream_len = u16::try_from(stream_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let mut payload_len = key_prefix_len(key)?
        + 2usize
        + tenant_bytes.len()
        + 2
        + namespace_bytes.len()
        + 2
        + stream_bytes.len()
        + 4;
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        payload_len = payload_len
            .checked_add(4 + len as usize)
            .ok_or(Error::FrameTooLarge)?;
    }
    if payload_len > u32::MAX as usize {
        return Err(Error::FrameTooLarge);
    }
    let mut buf = BytesMut::with_capacity(payload_len);
    put_key_prefix(&mut buf, key);
    buf.put_u16(tenant_len);
    buf.extend_from_slice(tenant_bytes);
    buf.put_u16(namespace_len);
    buf.extend_from_slice(namespace_bytes);
    buf.put_u16(stream_len);
    buf.extend_from_slice(stream_bytes);
    buf.put_u32(payloads.len() as u32);
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        buf.put_u32(len);
        buf.extend_from_slice(payload);
    }
    Frame::new(publish_flags(key), buf.freeze())
}

/// Encode a full binary publish frame, including header and payload.
pub fn encode_publish_batch_bytes(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<Bytes> {
    let (bytes, _stats) =
        encode_publish_batch_bytes_with_stats(tenant_id, namespace, stream, payloads)?;
    Ok(bytes)
}

/// [`encode_publish_batch_bytes`] for payloads already held as [`Bytes`].
pub fn encode_publish_batch_bytes_from_bytes(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Bytes],
) -> Result<Bytes> {
    let (bytes, _stats) =
        encode_publish_batch_bytes_with_stats_from_bytes(tenant_id, namespace, stream, payloads)?;
    Ok(bytes)
}

/// Encode a full binary publish frame, including header and payload, and return stats.
pub fn encode_publish_batch_bytes_with_stats(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<(Bytes, EncodeStats)> {
    encode_publish_batch_bytes_with_stats_keyed(None, tenant_id, namespace, stream, payloads)
}

/// The keyed form of [`encode_publish_batch_bytes_with_stats`].
///
/// The key rides in the frame rather than forcing the caller onto the JSON
/// encoding, which is what a keyed publish used to cost.
pub fn encode_publish_batch_bytes_with_stats_keyed(
    key: Option<&[u8]>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Vec<u8>],
) -> Result<(Bytes, EncodeStats)> {
    let tenant_bytes = tenant_id.as_bytes();
    let tenant_len = u16::try_from(tenant_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let namespace_bytes = namespace.as_bytes();
    let namespace_len = u16::try_from(namespace_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let stream_bytes = stream.as_bytes();
    let stream_len = u16::try_from(stream_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let mut payload_len = key_prefix_len(key)?
        + 2usize
        + tenant_bytes.len()
        + 2
        + namespace_bytes.len()
        + 2
        + stream_bytes.len()
        + 4;
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
    let mut reallocs = 0u64;
    let mut cap = buf.capacity();
    let header = FrameHeader::new(publish_flags(key), payload_len as u32);
    header.encode(&mut buf);
    put_key_prefix(&mut buf, key);
    buf.put_u16(tenant_len);
    buf.extend_from_slice(tenant_bytes);
    buf.put_u16(namespace_len);
    buf.extend_from_slice(namespace_bytes);
    buf.put_u16(stream_len);
    buf.extend_from_slice(stream_bytes);
    buf.put_u32(payloads.len() as u32);
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        buf.put_u32(len);
        buf.extend_from_slice(payload);
        let next_cap = buf.capacity();
        if next_cap != cap {
            reallocs += 1;
            cap = next_cap;
        }
    }
    Ok((buf.freeze(), EncodeStats { reallocs }))
}

/// [`encode_publish_batch_bytes_with_stats`] for payloads already held as
/// [`Bytes`].
pub fn encode_publish_batch_bytes_with_stats_from_bytes(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Bytes],
) -> Result<(Bytes, EncodeStats)> {
    encode_publish_batch_bytes_with_stats_keyed_from_bytes(
        None, tenant_id, namespace, stream, payloads,
    )
}

/// The keyed form of [`encode_publish_batch_bytes_with_stats_from_bytes`].
pub fn encode_publish_batch_bytes_with_stats_keyed_from_bytes(
    key: Option<&[u8]>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Bytes],
) -> Result<(Bytes, EncodeStats)> {
    let tenant_bytes = tenant_id.as_bytes();
    let tenant_len = u16::try_from(tenant_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let namespace_bytes = namespace.as_bytes();
    let namespace_len = u16::try_from(namespace_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let stream_bytes = stream.as_bytes();
    let stream_len = u16::try_from(stream_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let mut payload_len = key_prefix_len(key)?
        + 2usize
        + tenant_bytes.len()
        + 2
        + namespace_bytes.len()
        + 2
        + stream_bytes.len()
        + 4;
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
    let mut reallocs = 0u64;
    let mut cap = buf.capacity();
    let header = FrameHeader::new(publish_flags(key), payload_len as u32);
    header.encode(&mut buf);
    put_key_prefix(&mut buf, key);
    buf.put_u16(tenant_len);
    buf.extend_from_slice(tenant_bytes);
    buf.put_u16(namespace_len);
    buf.extend_from_slice(namespace_bytes);
    buf.put_u16(stream_len);
    buf.extend_from_slice(stream_bytes);
    buf.put_u32(payloads.len() as u32);
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?;
        buf.put_u32(len);
        let start = buf.len();
        buf.resize(start + len as usize, 0);
        buf[start..].copy_from_slice(payload);
        if buf.capacity() != cap {
            reallocs = reallocs.saturating_add(1);
            cap = buf.capacity();
        }
    }
    Ok((buf.freeze(), EncodeStats { reallocs }))
}

/// Decode a binary publish batch frame into its structured form.
pub fn decode_publish_batch(frame: &Frame) -> Result<PublishBatch> {
    // The producer prefix only exists after an acked prefix, which this does
    // not parse. Reading on would take the producer id for the tenant length.
    if frame.header.flags & FLAG_BINARY_PUBLISH_IDEMPOTENT != 0 {
        return Err(Error::Deserialize(SerdeError::custom(
            "an idempotent publish must be acked",
        )));
    }
    // Likewise the acked prefix: read as a plain batch, its request id would
    // become the tenant length. `decode_acked_publish_batch` is the parser.
    if frame.header.flags & FLAG_BINARY_PUBLISH_ACKED != 0 {
        return Err(Error::Deserialize(SerdeError::custom(
            "an acked publish needs the acked decoder",
        )));
    }
    let mut buf = frame.payload.clone();
    // The key prefix comes first, so everything after it is the ordinary body
    // at a shifted offset rather than a second layout to parse.
    let key = if frame.header.flags & FLAG_BINARY_PUBLISH_KEYED != 0 {
        if buf.remaining() < KEY_LEN_PREFIX {
            return Err(Error::Incomplete);
        }
        let key_len = buf.get_u16() as usize;
        if buf.remaining() < key_len {
            return Err(Error::Incomplete);
        }
        Some(buf.copy_to_bytes(key_len))
    } else {
        None
    };
    if buf.remaining() < 2 {
        return Err(Error::Incomplete);
    }
    let tenant_len = buf.get_u16() as usize;
    if buf.remaining() < tenant_len + 2 {
        return Err(Error::Incomplete);
    }
    let tenant_bytes = buf.copy_to_bytes(tenant_len);
    let tenant_id = String::from_utf8(tenant_bytes.to_vec())
        .map_err(|_| Error::Deserialize(SerdeError::custom("invalid tenant id")))?;
    let namespace_len = buf.get_u16() as usize;
    if buf.remaining() < namespace_len + 2 {
        return Err(Error::Incomplete);
    }
    let namespace_bytes = buf.copy_to_bytes(namespace_len);
    let namespace = String::from_utf8(namespace_bytes.to_vec())
        .map_err(|_| Error::Deserialize(SerdeError::custom("invalid namespace")))?;
    let stream_len = buf.get_u16() as usize;
    if buf.remaining() < stream_len + 4 {
        return Err(Error::Incomplete);
    }
    let stream_bytes = buf.copy_to_bytes(stream_len);
    let stream = String::from_utf8(stream_bytes.to_vec())
        .map_err(|_| Error::Deserialize(SerdeError::custom("invalid stream name")))?;
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
    Ok(PublishBatch {
        tenant_id,
        namespace,
        stream,
        key,
        payloads,
    })
}

// Flags for a publish batch with or without a key.
pub(super) fn publish_flags(key: Option<&[u8]>) -> u16 {
    if key.is_some() {
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_KEYED
    } else {
        FLAG_BINARY_PUBLISH_BATCH
    }
}

// Bytes the key prefix adds to a payload, and a `FrameTooLarge` for a key that
// cannot state its own length.
fn key_prefix_len(key: Option<&[u8]>) -> Result<usize> {
    match key {
        None => Ok(0),
        Some(key) => {
            u16::try_from(key.len()).map_err(|_| Error::FrameTooLarge)?;
            Ok(KEY_LEN_PREFIX + key.len())
        }
    }
}

fn put_key_prefix(buf: &mut BytesMut, key: Option<&[u8]>) {
    if let Some(key) = key {
        // Length already validated by `key_prefix_len`.
        buf.put_u16(key.len() as u16);
        buf.extend_from_slice(key);
    }
}

#[cfg(test)]
mod tests;
