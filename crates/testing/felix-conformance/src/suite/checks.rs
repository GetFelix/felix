//! Small assertions on broker responses, each an error naming what differed.

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use felix_wire::{Frame, Message};

pub(crate) fn parse_subscribe_response(response: Option<Message>) -> Result<u64> {
    match response {
        Some(Message::Subscribed {
            subscription_id, ..
        }) => Ok(subscription_id),
        other => Err(anyhow!("subscribe failed: {other:?}")),
    }
}

pub(crate) fn ensure_publish_ok(response: Option<Message>, request_id: u64) -> Result<()> {
    // A client that did not offer FLAG_BINARY_PUBLISH_ACK_OFFSET must get the
    // old frame, with no `offset`.
    if response
        != Some(Message::PublishOk {
            request_id,
            offset: None,
        })
    {
        return Err(anyhow!("publish failed: {response:?}"));
    }
    Ok(())
}

pub(crate) fn ensure_ok_response(response: Option<Message>, context: &str) -> Result<()> {
    if response != Some(Message::Ok) {
        return Err(anyhow!("{context} failed: {response:?}"));
    }
    Ok(())
}

pub(crate) fn parse_cache_get_response(response: Option<Message>) -> Result<Option<Bytes>> {
    match response {
        Some(Message::CacheValue { value, .. }) => Ok(value),
        other => Err(anyhow!("unexpected cache response: {other:?}")),
    }
}

pub(crate) fn ensure_event_order(received: &[Vec<u8>]) -> Result<()> {
    if received != [b"alpha".to_vec(), b"beta".to_vec()] {
        return Err(anyhow!("unexpected event order: {received:?}"));
    }
    Ok(())
}

pub(crate) fn ensure_cache_value(
    value: Option<Bytes>,
    expected: Bytes,
    context: &str,
) -> Result<()> {
    if value != Some(expected) {
        return Err(anyhow!("{context} mismatch: {value:?}"));
    }
    Ok(())
}

pub(crate) fn ensure_cache_expired(value: Option<Bytes>, context: &str) -> Result<()> {
    if value.is_some() {
        return Err(anyhow!("{context}"));
    }
    Ok(())
}

pub(crate) fn ensure_client_event(payload: &Bytes, expected: Bytes) -> Result<()> {
    if payload != &expected {
        return Err(anyhow!("client event mismatch: {:?}", payload));
    }
    Ok(())
}

/// The server flags an `AuthOk` advertised, failing unless they include `flag`.
pub(crate) fn ensure_flag_advertised(response: Option<Message>, flag: u16) -> Result<u16> {
    match response {
        Some(Message::AuthOk { server_flags, .. }) if server_flags & flag == flag => {
            Ok(server_flags)
        }
        other => Err(anyhow!(
            "auth_ok did not advertise flag {flag:#06x}: {other:?}"
        )),
    }
}

/// The offset a successful publish ack for `request_id` reported, from either
/// a binary ack or a JSON `publish_ok`.
pub(crate) fn publish_ack_offset(frame: Frame, request_id: u64) -> Result<Option<u64>> {
    let (answered, offset) = if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_ACK != 0 {
        let ack = felix_wire::binary::decode_publish_ack(&frame).context("decode publish ack")?;
        if let Some(error) = ack.error {
            return Err(anyhow!("publish {request_id} failed: {error}"));
        }
        (ack.request_id, ack.offset)
    } else {
        match Message::decode(frame).context("decode publish answer")? {
            Message::PublishOk { request_id, offset } => (request_id, offset),
            other => return Err(anyhow!("publish {request_id} failed: {other:?}")),
        }
    };
    if answered != request_id {
        return Err(anyhow!("ack for request {answered}, expected {request_id}"));
    }
    Ok(offset)
}

/// The offset of a batch published right after one of `records` records at
/// `previous`: offsets are contiguous, so it must be `previous + records`.
pub(crate) fn ensure_offset_follows(
    context: &str,
    previous: u64,
    records: u64,
    offset: Option<u64>,
) -> Result<u64> {
    let expected = previous + records;
    match offset {
        Some(offset) if offset == expected => Ok(offset),
        other => Err(anyhow!("{context}: offset {other:?}, expected {expected}")),
    }
}

/// Fail unless `frame` is exactly `expected`, header and payload.
pub(crate) fn ensure_same_frame(frame: &Frame, expected: &Frame, context: &str) -> Result<()> {
    if frame.header.flags != expected.header.flags || frame.payload != expected.payload {
        return Err(anyhow!(
            "{context}: got flags {:#06x} {:?}, expected flags {:#06x} {:?}",
            frame.header.flags,
            frame.payload,
            expected.header.flags,
            expected.payload,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
