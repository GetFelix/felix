//! A WebSocket connection as a byte stream.
//!
//! MQTT over WebSockets puts the same bytes in binary messages. A message can
//! hold part of a packet or several, so the MQTT decoder sees one stream and
//! applies its own limits to it; the frames are only the carrier.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::frame::{self, Event, FrameDecoder};

/// Bytes read from the socket at a time.
const READ_CHUNK: usize = 8 * 1024;
/// Largest binary frame written. Bigger writes go out as several frames.
const MAX_FRAME_PAYLOAD: usize = 16 * 1024;
/// A pong is not queued while this much is waiting to be written, which keeps
/// a peer that pings without reading from growing the buffer.
const PONG_HIGH_WATER: usize = 64 * 1024;

pub struct WsStream<S> {
    io: S,
    decoder: FrameDecoder,
    /// Read from the socket, not yet decoded.
    raw: BytesMut,
    /// Decoded payload not yet handed to the reader.
    data: BytesMut,
    /// Frames waiting for the socket.
    out: BytesMut,
    /// The peer sent a close frame, or the socket ended.
    eof: bool,
    /// Our close frame is queued; nothing more is written.
    close_queued: bool,
}

impl<S: AsyncRead + AsyncWrite + Unpin> WsStream<S> {
    /// `rest` is what arrived after the upgrade request.
    pub(super) fn new(io: S, rest: BytesMut) -> Self {
        Self {
            io,
            decoder: FrameDecoder::new(),
            raw: rest,
            data: BytesMut::new(),
            out: BytesMut::new(),
            eof: false,
            close_queued: false,
        }
    }

    /// Write queued frames to the socket.
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while !self.out.is_empty() {
            let n = ready!(Pin::new(&mut self.io).poll_write(cx, &self.out))?;
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.out.advance(n);
        }
        Poll::Ready(Ok(()))
    }

    fn queue_close(&mut self, code: u16) {
        if !self.close_queued {
            self.close_queued = true;
            frame::put_close(code, &mut self.out);
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for WsStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            // Replies to control frames go out from here, since a connection
            // that only listens never calls write.
            if let Poll::Ready(Err(err)) = this.poll_drain(cx) {
                return Poll::Ready(Err(err));
            }
            if !this.data.is_empty() {
                let n = this.data.len().min(buf.remaining());
                buf.put_slice(&this.data.split_to(n));
                return Poll::Ready(Ok(()));
            }
            if this.eof {
                return Poll::Ready(Ok(()));
            }
            match this.decoder.next(&mut this.raw, &mut this.data) {
                Ok(Some(Event::Data | Event::Pong)) => {}
                Ok(Some(Event::Ping(payload))) => {
                    if !this.close_queued && this.out.len() < PONG_HIGH_WATER {
                        frame::put_pong(&payload, &mut this.out);
                    }
                }
                Ok(Some(Event::Close(code))) => {
                    let echo = code.filter(|&c| frame::valid_close_code(c));
                    let reply = match (code, echo) {
                        (None, _) => frame::CLOSE_NORMAL,
                        (Some(_), Some(code)) => code,
                        (Some(_), None) => frame::CLOSE_PROTOCOL_ERROR,
                    };
                    this.queue_close(reply);
                    this.eof = true;
                }
                Ok(None) => {
                    let mut chunk = [0u8; READ_CHUNK];
                    let mut read = ReadBuf::new(&mut chunk);
                    ready!(Pin::new(&mut this.io).poll_read(cx, &mut read))?;
                    if read.filled().is_empty() {
                        this.eof = true;
                    } else {
                        this.raw.extend_from_slice(read.filled());
                    }
                }
                Err(violation) => {
                    this.queue_close(violation.close_code());
                    let _ = this.poll_drain(cx);
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        match violation {
                            frame::Violation::Protocol(why) => why,
                            frame::Violation::Text => "text frame",
                        },
                    )));
                }
            }
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for WsStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.close_queued {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }
        // The previous frame goes out first, so a peer that stopped reading
        // stalls the writer here and the connection's write timeout applies.
        ready!(this.poll_drain(cx))?;
        let n = data.len().min(MAX_FRAME_PAYLOAD);
        frame::put_binary(&data[..n], &mut this.out);
        let _ = this.poll_drain(cx)?;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_drain(cx))?;
        Pin::new(&mut this.io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.queue_close(frame::CLOSE_NORMAL);
        ready!(this.poll_drain(cx))?;
        Pin::new(&mut this.io).poll_shutdown(cx)
    }
}
