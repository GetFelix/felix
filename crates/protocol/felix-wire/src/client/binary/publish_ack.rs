//! The broker's binary acknowledgement of an acked publish.
//!
//! ```text
//! u8  status        (0 = ok, 1 = error)
//! u64 request_id
//! u16 message_len   (0 when status = ok)
//! u8[message_len] message
//! ```
//!
//! With FLAG_BINARY_PUBLISH_ACK_CODE (failed acks only), the error's code
//! follows the message:
//!
//! ```text
//! u16 code          (ErrorCode::to_u16)
//! u8  retry         (RetryClass::to_u8)
//! ```
//!
//! With FLAG_BINARY_PUBLISH_ACK_DETAIL as well, the error's detail follows the
//! code:
//!
//! ```text
//! u16 reason_len      (0 when there is no reason)
//! u8[reason_len] reason
//! u64 retry_after_ms  (0 when the broker suggests no wait)
//! ```
//!
//! With FLAG_BINARY_PUBLISH_ACK_OWNER, the batch was forwarded and the owner
//! follows:
//!
//! ```text
//! u16 node_id_len
//! u8[node_id_len] node_id
//! u16 addr_len      (0 when the owner's client address is not published)
//! u8[addr_len] addr
//! u64 generation
//! ```
//!
//! With FLAG_BINARY_PUBLISH_ACK_OFFSET (successful acks only), the offset of
//! the batch's first record comes last:
//!
//! ```text
//! u64 offset
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::de::Error as SerdeError;

use crate::client::error_code::{ErrorCode, ErrorDetail, RetryClass};
use crate::client::flags::{
    FLAG_BINARY_PUBLISH_ACK, FLAG_BINARY_PUBLISH_ACK_CODE, FLAG_BINARY_PUBLISH_ACK_DETAIL,
    FLAG_BINARY_PUBLISH_ACK_OFFSET, FLAG_BINARY_PUBLISH_ACK_OWNER,
};
use crate::client::frame::{Frame, FrameHeader};
use crate::error::{Error, Result};

const ACK_STATUS_OK: u8 = 0;
const ACK_STATUS_ERROR: u8 = 1;

/// Broker → client acknowledgement for an acked binary publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishAck {
    pub request_id: u64,
    /// `None` on success, `Some(message)` when the publish failed.
    pub error: Option<String>,
    /// The failure's code and retry class, when the broker sent them. Only a
    /// client that advertised `FLAG_BINARY_PUBLISH_ACK_CODE` gets them.
    pub code: Option<(ErrorCode, RetryClass)>,
    /// Why the failure happened and how long to wait, when the broker said.
    /// Only a client that advertised `FLAG_BINARY_PUBLISH_ACK_DETAIL` gets it.
    pub detail: Option<ErrorDetail>,
    /// Set when this batch was forwarded, naming the shard's owner.
    ///
    /// `None` means the broker handled it itself -- or predates the hint, or
    /// was talking to a client that did not advertise it. All three are the
    /// same thing to a caller: no better place to send the next batch is known.
    pub forwarded_to: Option<PublishOwner>,
    /// The log offset of the batch's first record. The rest follow it
    /// contiguously.
    ///
    /// `None` when the broker did not say: it acknowledged before writing, the
    /// stream has no log, or the client did not advertise
    /// `FLAG_BINARY_PUBLISH_ACK_OFFSET`. Always `None` on a failure.
    pub offset: Option<u64>,
}

/// The broker that owns the shard a forwarded batch went to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishOwner {
    /// Who owns it, so a client recognises an owner it already knows.
    pub node_id: String,
    /// `host:port` the owner serves *clients* on. `None` when the cluster has
    /// not been told where clients reach it -- the same gap `NotLeader` has,
    /// and the same consequence: there is nowhere to route to, so a client
    /// keeps publishing where it is.
    pub addr: Option<String>,
    /// The ownership generation this answer was true for. A client holding a
    /// cached owner can tell a newer answer from an older one rather than
    /// letting two brokers mid-rebalance overwrite each other.
    pub generation: u64,
}

/// Encode a publish ack into a full framed buffer (header included).
pub fn encode_publish_ack_bytes(request_id: u64, error: Option<&str>) -> Result<Bytes> {
    encode_publish_ack_bytes_owned(request_id, error, None)
}

/// Encode a publish ack, optionally naming the owner a forwarded batch went to.
///
/// `forwarded_to` sets `FLAG_BINARY_PUBLISH_ACK_OWNER`, so the caller must only
/// pass it for a client that advertised the bit -- one that did not will reject
/// the frame, and the frame it rejects acknowledges a publish that succeeded.
pub fn encode_publish_ack_bytes_owned(
    request_id: u64,
    error: Option<&str>,
    forwarded_to: Option<&PublishOwner>,
) -> Result<Bytes> {
    encode_publish_ack_bytes_coded(request_id, error, None, forwarded_to)
}

/// Encode a publish ack, optionally with a failure's code and the owner a
/// forwarded batch went to.
///
/// `code` sets `FLAG_BINARY_PUBLISH_ACK_CODE` and is ignored on success, so the
/// caller must only pass it for a client that advertised that bit -- the same
/// rule as `forwarded_to`.
pub fn encode_publish_ack_bytes_coded(
    request_id: u64,
    error: Option<&str>,
    code: Option<(&ErrorCode, RetryClass)>,
    forwarded_to: Option<&PublishOwner>,
) -> Result<Bytes> {
    encode_publish_ack_bytes_detailed(request_id, error, code, None, forwarded_to)
}

/// Encode a publish ack with a failure's code and detail.
///
/// `detail` sets `FLAG_BINARY_PUBLISH_ACK_DETAIL` and is ignored without a
/// `code`, so the caller must only pass it for a client that advertised that
/// bit.
pub fn encode_publish_ack_bytes_detailed(
    request_id: u64,
    error: Option<&str>,
    code: Option<(&ErrorCode, RetryClass)>,
    detail: Option<&ErrorDetail>,
    forwarded_to: Option<&PublishOwner>,
) -> Result<Bytes> {
    encode_publish_ack_bytes_at(request_id, error, code, detail, None, forwarded_to)
}

/// Encode a publish ack with everything it can carry.
///
/// `offset` sets `FLAG_BINARY_PUBLISH_ACK_OFFSET` and is ignored on a failure,
/// so the caller must only pass it for a client that advertised that bit.
pub fn encode_publish_ack_bytes_at(
    request_id: u64,
    error: Option<&str>,
    code: Option<(&ErrorCode, RetryClass)>,
    detail: Option<&ErrorDetail>,
    offset: Option<u64>,
    forwarded_to: Option<&PublishOwner>,
) -> Result<Bytes> {
    let message = error.unwrap_or("");
    let message_bytes = message.as_bytes();
    let message_len = u16::try_from(message_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let code = code.filter(|_| error.is_some());
    let detail = detail.filter(|_| code.is_some());
    let offset = offset.filter(|_| error.is_none());
    let mut payload_len = 1 + 8 + 2 + message_bytes.len();
    if offset.is_some() {
        payload_len += 8;
    }
    if code.is_some() {
        payload_len += 2 + 1;
    }
    let detail = match detail {
        Some(detail) => {
            let reason = detail.reason.as_deref().unwrap_or("").as_bytes();
            let reason_len = u16::try_from(reason.len()).map_err(|_| Error::FrameTooLarge)?;
            payload_len += 2 + reason.len() + 8;
            Some((reason, reason_len, detail.retry_after_ms.unwrap_or(0)))
        }
        None => None,
    };

    let owner = match forwarded_to {
        Some(owner) => {
            let node_id = owner.node_id.as_bytes();
            let addr = owner.addr.as_deref().unwrap_or("").as_bytes();
            let node_id_len = u16::try_from(node_id.len()).map_err(|_| Error::FrameTooLarge)?;
            let addr_len = u16::try_from(addr.len()).map_err(|_| Error::FrameTooLarge)?;
            payload_len += 2 + node_id.len() + 2 + addr.len() + 8;
            Some((node_id, node_id_len, addr, addr_len, owner.generation))
        }
        None => None,
    };

    let mut flags = FLAG_BINARY_PUBLISH_ACK;
    if owner.is_some() {
        flags |= FLAG_BINARY_PUBLISH_ACK_OWNER;
    }
    if code.is_some() {
        flags |= FLAG_BINARY_PUBLISH_ACK_CODE;
    }
    if detail.is_some() {
        flags |= FLAG_BINARY_PUBLISH_ACK_DETAIL;
    }
    if offset.is_some() {
        flags |= FLAG_BINARY_PUBLISH_ACK_OFFSET;
    }

    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + payload_len);
    FrameHeader::new(flags, payload_len as u32).encode(&mut buf);
    buf.put_u8(if error.is_some() {
        ACK_STATUS_ERROR
    } else {
        ACK_STATUS_OK
    });
    buf.put_u64(request_id);
    buf.put_u16(message_len);
    buf.extend_from_slice(message_bytes);
    if let Some((code, retry)) = code {
        buf.put_u16(code.to_u16());
        buf.put_u8(retry.to_u8());
    }
    if let Some((reason, reason_len, retry_after_ms)) = detail {
        buf.put_u16(reason_len);
        buf.extend_from_slice(reason);
        buf.put_u64(retry_after_ms);
    }
    if let Some((node_id, node_id_len, addr, addr_len, generation)) = owner {
        buf.put_u16(node_id_len);
        buf.extend_from_slice(node_id);
        buf.put_u16(addr_len);
        buf.extend_from_slice(addr);
        buf.put_u64(generation);
    }
    if let Some(offset) = offset {
        buf.put_u64(offset);
    }
    Ok(buf.freeze())
}

/// Decode a publish ack frame.
pub fn decode_publish_ack(frame: &Frame) -> Result<PublishAck> {
    let mut buf = frame.payload.clone();
    if buf.remaining() < 11 {
        return Err(Error::Incomplete);
    }
    let status = buf.get_u8();
    let request_id = buf.get_u64();
    let message_len = buf.get_u16() as usize;
    if buf.remaining() < message_len {
        return Err(Error::Incomplete);
    }
    let message_bytes = buf.copy_to_bytes(message_len);
    let error = match status {
        ACK_STATUS_OK => None,
        ACK_STATUS_ERROR => Some(
            String::from_utf8(message_bytes.to_vec())
                .map_err(|_| Error::Deserialize(SerdeError::custom("invalid ack message")))?,
        ),
        _ => return Err(Error::Deserialize(SerdeError::custom("invalid ack status"))),
    };
    let code = if frame.header.flags & FLAG_BINARY_PUBLISH_ACK_CODE != 0 {
        if buf.remaining() < 3 {
            return Err(Error::Incomplete);
        }
        let code = ErrorCode::from_u16(buf.get_u16());
        let retry = RetryClass::from_u8(buf.get_u8());
        Some((code, retry))
    } else {
        None
    };
    let detail = if frame.header.flags & FLAG_BINARY_PUBLISH_ACK_DETAIL != 0 {
        if buf.remaining() < 2 {
            return Err(Error::Incomplete);
        }
        let reason_len = buf.get_u16() as usize;
        if buf.remaining() < reason_len + 8 {
            return Err(Error::Incomplete);
        }
        let reason = String::from_utf8(buf.copy_to_bytes(reason_len).to_vec())
            .map_err(|_| Error::Deserialize(SerdeError::custom("invalid error reason")))?;
        let retry_after_ms = buf.get_u64();
        // Zero on the wire is "not said", the same as an absent JSON field.
        Some(ErrorDetail {
            reason: (!reason.is_empty()).then_some(reason),
            retry_after_ms: (retry_after_ms != 0).then_some(retry_after_ms),
        })
    } else {
        None
    };
    let forwarded_to = if frame.header.flags & FLAG_BINARY_PUBLISH_ACK_OWNER != 0 {
        if buf.remaining() < 2 {
            return Err(Error::Incomplete);
        }
        let node_id_len = buf.get_u16() as usize;
        if buf.remaining() < node_id_len + 2 {
            return Err(Error::Incomplete);
        }
        let node_id = String::from_utf8(buf.copy_to_bytes(node_id_len).to_vec())
            .map_err(|_| Error::Deserialize(SerdeError::custom("invalid owner node id")))?;
        let addr_len = buf.get_u16() as usize;
        if buf.remaining() < addr_len + 8 {
            return Err(Error::Incomplete);
        }
        let addr = String::from_utf8(buf.copy_to_bytes(addr_len).to_vec())
            .map_err(|_| Error::Deserialize(SerdeError::custom("invalid owner address")))?;
        let generation = buf.get_u64();
        Some(PublishOwner {
            node_id,
            // Empty means "not published", which is different from an address
            // that happens to be empty -- there is no such address.
            addr: (!addr.is_empty()).then_some(addr),
            generation,
        })
    } else {
        None
    };
    let offset = if frame.header.flags & FLAG_BINARY_PUBLISH_ACK_OFFSET != 0 {
        // Only a success has an offset; a failure claiming one is malformed.
        if error.is_some() {
            return Err(Error::Deserialize(SerdeError::custom(
                "offset on a failed ack",
            )));
        }
        if buf.remaining() < 8 {
            return Err(Error::Incomplete);
        }
        Some(buf.get_u64())
    } else {
        None
    };
    Ok(PublishAck {
        request_id,
        error,
        code,
        detail,
        forwarded_to,
        offset,
    })
}

#[cfg(test)]
mod tests;
