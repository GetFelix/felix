//! QUIC frame/message encoding and decoding helpers with size limits.

use std::collections::VecDeque;

use anyhow::{Context, Result, anyhow};
use bytes::{Bytes, BytesMut};
use felix_wire::{Frame, FrameHeader, Message};
use quinn::{RecvStream, SendStream, StreamId};

#[cfg(feature = "telemetry")]
use super::telemetry;

// Helper for tests and small control flows with an explicit frame cap.
pub async fn read_message_limited(
    recv: &mut RecvStream,
    max_frame_bytes: usize,
    frame_scratch: &mut FrameScratch,
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

/// Chunks taken off one receive stream and not yet returned as a frame.
///
/// A batched chunk read takes whatever the stream has buffered, which often
/// runs past the end of the current frame. The rest waits here for the next
/// read, so use one `FrameScratch` per stream for every frame read from it.
#[derive(Debug, Default)]
pub struct FrameScratch {
    chunks: VecDeque<Bytes>,
    len: usize,
    stream: Option<StreamId>,
}

impl FrameScratch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bytes read off the stream and not yet returned in a frame.
    pub fn buffered(&self) -> usize {
        self.len
    }

    // Bytes left over from one stream would be read as the next frame of
    // another, so refuse that rather than misparse.
    fn bind(&mut self, stream: StreamId) -> Result<()> {
        if self.stream != Some(stream) {
            if self.len > 0 {
                return Err(anyhow!(
                    "frame scratch holds {} bytes from another stream",
                    self.len
                ));
            }
            self.chunks.clear();
            self.stream = Some(stream);
        }
        Ok(())
    }

    // Read until at least `want` bytes are buffered. `false` means the stream
    // finished first. Only appends after `read_chunks` returns, so this is
    // cancel-safe.
    async fn fill(&mut self, recv: &mut RecvStream, want: usize) -> Result<bool> {
        while self.len < want {
            let mut bufs: [Bytes; READ_CHUNKS] = Default::default();
            match recv.read_chunks(&mut bufs).await.context("read frame")? {
                Some(read) => {
                    for chunk in bufs.into_iter().take(read) {
                        self.len += chunk.len();
                        self.chunks.push_back(chunk);
                    }
                }
                None => return Ok(false),
            }
        }
        Ok(true)
    }

    fn peek_header(&self) -> [u8; FrameHeader::LEN] {
        let mut out = [0u8; FrameHeader::LEN];
        let mut filled = 0;
        for chunk in &self.chunks {
            let n = chunk.len().min(out.len() - filled);
            out[filled..filled + n].copy_from_slice(&chunk[..n]);
            filled += n;
            if filled == out.len() {
                break;
            }
        }
        out
    }

    // Remove the first `n` bytes, copying them into `out` when given. The
    // caller has checked that `n` bytes are buffered.
    fn drain(&mut self, mut n: usize, mut out: Option<&mut BytesMut>) {
        self.len -= n;
        while n > 0 {
            let Some(front) = self.chunks.front_mut() else {
                break;
            };
            if front.len() <= n {
                n -= front.len();
                if let Some(out) = out.as_deref_mut() {
                    out.extend_from_slice(front);
                }
                self.chunks.pop_front();
            } else {
                let head = front.split_to(n);
                if let Some(out) = out.as_deref_mut() {
                    out.extend_from_slice(&head);
                }
                n = 0;
            }
        }
    }
}

/// Chunks taken per `read_chunks` call. Each call takes the connection lock
/// once, where `read_chunk` took it once per packet.
const READ_CHUNKS: usize = 32;

/// Low-level frame reader with a max payload cap.
///
/// A frame with unknown flag bits is returned rather than refused, with its
/// body consumed, so the control stream can answer it and stay on a frame
/// boundary. Every caller must check `felix_wire::has_unknown_flags` before
/// reading the body.
///
/// Cancel-safe: nothing is consumed from `scratch` until the whole frame has
/// arrived.
pub async fn read_frame_limited_into(
    recv: &mut RecvStream,
    max_payload_bytes: usize,
    scratch: &mut FrameScratch,
) -> Result<Option<Frame>> {
    scratch.bind(recv.id())?;
    if !scratch.fill(recv, FrameHeader::LEN).await? {
        return Ok(None);
    }
    let header_bytes = scratch.peek_header();
    let header = FrameHeader::decode_allowing_unknown_flags(Bytes::copy_from_slice(&header_bytes))
        .context("decode frame header")?;
    let length = usize::try_from(header.length).context("frame length")?;
    if length > max_payload_bytes {
        return Err(anyhow!(
            "frame length {length} exceeds max_payload_bytes {max_payload_bytes}"
        ));
    }
    // Buffering the chunks as they come means memory follows the bytes that
    // actually arrived, not the length the header claims.
    if !scratch.fill(recv, FrameHeader::LEN + length).await? {
        return Err(anyhow!(
            "read frame payload: stream finished after {} of {length} bytes",
            scratch.buffered() - FrameHeader::LEN
        ));
    }
    scratch.drain(FrameHeader::LEN, None);
    // A fresh buffer per frame: decoded records are slices of it and may
    // outlive this read by a long way (the stream's ring keeps them).
    let mut payload = BytesMut::with_capacity(length);
    scratch.drain(length, Some(&mut payload));
    let frame = Frame {
        header,
        payload: payload.freeze(),
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
