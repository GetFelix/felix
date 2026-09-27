//! QUIC frame/message encoding and decoding helpers with size limits.

use anyhow::{Context, Result, anyhow};
use bytes::{Bytes, BytesMut};
use felix_wire::{Frame, FrameHeader, Message};
use quinn::{ReadExactError, RecvStream, SendStream};

#[cfg(feature = "telemetry")]
use super::telemetry;

// Helper for tests and small control flows with an explicit frame cap.
pub async fn read_message_limited(
    recv: &mut RecvStream,
    max_frame_bytes: usize,
    frame_scratch: &mut BytesMut,
) -> Result<Option<Message>> {
    let frame = match read_frame_limited_into(recv, max_frame_bytes, frame_scratch).await? {
        Some(frame) => frame,
        None => return Ok(None),
    };
    if felix_wire::has_unknown_flags(frame.header.flags) {
        return Err(anyhow!(
            "unsupported frame flags {:#06x}",
            frame.header.flags
        ));
    }
    Message::decode(frame).map(Some).context("decode message")
}

// Helper to encode + write a single message.
pub async fn write_message(send: &mut SendStream, message: Message) -> Result<()> {
    let frame = message.encode().context("encode message")?;
    write_frame(send, &frame).await
}

/// Low-level frame reader with a max payload cap.
///
/// A frame with unknown flag bits is returned rather than refused, with its
/// body consumed, so the control stream can answer it and stay on a frame
/// boundary. Every caller must check `felix_wire::has_unknown_flags` before
/// reading the body.
pub async fn read_frame_limited_into(
    recv: &mut RecvStream,
    max_payload_bytes: usize,
    scratch: &mut BytesMut,
) -> Result<Option<Frame>> {
    let mut header_bytes = [0u8; FrameHeader::LEN];
    match recv.read_exact(&mut header_bytes).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(ReadExactError::ReadError(err)) => return Err(err.into()),
    }

    let header = FrameHeader::decode_allowing_unknown_flags(Bytes::copy_from_slice(&header_bytes))
        .context("decode frame header")?;
    let length = usize::try_from(header.length).context("frame length")?;
    if length > max_payload_bytes {
        return Err(anyhow!(
            "frame length {length} exceeds max_payload_bytes {max_payload_bytes}"
        ));
    }
    read_payload_into(recv, length, scratch).await?;
    let frame = Frame {
        header,
        payload: scratch.split().freeze(),
    };
    #[cfg(feature = "telemetry")]
    {
        let counters = telemetry::frame_counters();
        counters
            .frames_in_ok
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes = (FrameHeader::LEN + frame.payload.len()) as u64;
        counters
            .bytes_in
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(Some(frame))
}

/// Most of a payload reserved before any of it has arrived.
const PAYLOAD_RESERVE_BYTES: usize = 64 * 1024;

// Grow `scratch` with the bytes that actually arrive, not with the length the
// header declares. The header costs the peer twelve bytes; reserving from it let
// one packet per stream commit the broker to a full-size frame.
async fn read_payload_into(
    recv: &mut RecvStream,
    length: usize,
    scratch: &mut BytesMut,
) -> Result<()> {
    scratch.clear();
    scratch.reserve(length.min(PAYLOAD_RESERVE_BYTES));
    while scratch.len() < length {
        let remaining = length - scratch.len();
        match recv
            .read_chunk(remaining, true)
            .await
            .context("read frame payload")?
        {
            Some(chunk) => scratch.extend_from_slice(&chunk.bytes),
            None => {
                return Err(anyhow!(
                    "read frame payload: stream finished after {} of {length} bytes",
                    scratch.len()
                ));
            }
        }
    }
    Ok(())
}

// Low-level frame writer for QUIC streams.
pub(super) async fn write_frame(send: &mut SendStream, frame: &Frame) -> Result<()> {
    let mut header_bytes = [0u8; FrameHeader::LEN];
    frame.header.encode_into(&mut header_bytes);
    send.write_all(&header_bytes)
        .await
        .context("write frame header")?;
    send.write_all(&frame.payload)
        .await
        .context("write frame payload")?;
    #[cfg(feature = "telemetry")]
    {
        let counters = telemetry::frame_counters();
        counters
            .frames_out_ok
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes = (FrameHeader::LEN + frame.payload.len()) as u64;
        counters
            .bytes_out
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
