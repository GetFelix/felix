//! WebSocket frames (RFC 6455), server side.
//!
//! The decoder streams: payload bytes are handed over as they arrive, so no
//! frame size needs a limit here. What a client can make the broker buffer is
//! bounded by the MQTT decoder behind it, which refuses an oversized packet
//! from its header.

use bytes::{Buf, BufMut, BytesMut};

const OP_CONTINUATION: u8 = 0x0;
const OP_TEXT: u8 = 0x1;
const OP_BINARY: u8 = 0x2;
const OP_CLOSE: u8 = 0x8;
const OP_PING: u8 = 0x9;
const OP_PONG: u8 = 0xA;

/// Longest control frame payload the format allows.
const MAX_CONTROL_PAYLOAD: usize = 125;
/// Longest frame header a client sends: two bytes, an eight byte length, and
/// the mask.
const MAX_HEADER: usize = 2 + 8 + 4;

/// Close codes the broker sends.
pub(super) const CLOSE_NORMAL: u16 = 1000;
pub(super) const CLOSE_PROTOCOL_ERROR: u16 = 1002;
pub(super) const CLOSE_UNSUPPORTED: u16 = 1003;

/// What a frame decoder found.
#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    /// Payload bytes of a binary message were appended to the output.
    Data,
    Ping(Vec<u8>),
    Pong,
    /// The peer is closing, with the code it gave, if any.
    Close(Option<u16>),
}

/// Why a stream of frames cannot continue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Violation {
    /// A reserved bit, an unknown opcode, an unmasked frame, a bad control
    /// frame, or messages that interleave.
    Protocol(&'static str),
    /// A text frame. MQTT is binary.
    Text,
}

impl Violation {
    /// The close code to answer with.
    pub(super) fn close_code(self) -> u16 {
        match self {
            Self::Protocol(_) => CLOSE_PROTOCOL_ERROR,
            Self::Text => CLOSE_UNSUPPORTED,
        }
    }
}

enum State {
    Header,
    /// The payload of a data frame.
    Data {
        remaining: u64,
        mask: Mask,
    },
    /// A control frame, collected whole because it is at most 125 bytes.
    Control {
        opcode: u8,
        remaining: usize,
        mask: Mask,
        payload: Vec<u8>,
    },
}

#[derive(Clone, Copy)]
struct Mask {
    key: [u8; 4],
    /// Bytes of the payload already unmasked, mod 4.
    offset: usize,
}

impl Mask {
    fn apply(&mut self, bytes: &mut [u8]) {
        for byte in bytes {
            *byte ^= self.key[self.offset];
            self.offset = (self.offset + 1) % 4;
        }
    }
}

/// Reads the frames a client sends.
pub struct FrameDecoder {
    state: State,
    /// A data message has started and its final frame has not arrived.
    in_message: bool,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self {
            state: State::Header,
            in_message: false,
        }
    }

    /// Take what `src` holds. Binary payload is appended to `data`; a control
    /// frame is returned. `Ok(None)` means more input is needed, and nothing
    /// was appended.
    pub fn next(
        &mut self,
        src: &mut BytesMut,
        data: &mut BytesMut,
    ) -> Result<Option<Event>, Violation> {
        loop {
            match &mut self.state {
                State::Header => {
                    let Some(next) = self.header(src)? else {
                        return Ok(None);
                    };
                    self.state = next;
                }
                State::Data { remaining, mask } => {
                    if *remaining == 0 {
                        self.state = State::Header;
                        continue;
                    }
                    if src.is_empty() {
                        return Ok(None);
                    }
                    let take = src
                        .len()
                        .min(usize::try_from(*remaining).unwrap_or(usize::MAX));
                    let start = data.len();
                    data.extend_from_slice(&src[..take]);
                    src.advance(take);
                    mask.apply(&mut data[start..]);
                    *remaining -= take as u64;
                    return Ok(Some(Event::Data));
                }
                State::Control {
                    opcode,
                    remaining,
                    mask,
                    payload,
                } => {
                    let take = src.len().min(*remaining);
                    let start = payload.len();
                    payload.extend_from_slice(&src[..take]);
                    src.advance(take);
                    mask.apply(&mut payload[start..]);
                    *remaining -= take;
                    if *remaining > 0 {
                        return Ok(None);
                    }
                    let (opcode, payload) = (*opcode, std::mem::take(payload));
                    self.state = State::Header;
                    return control(opcode, payload).map(Some);
                }
            }
        }
    }

    /// Parse a frame header if it is all here.
    fn header(&mut self, src: &mut BytesMut) -> Result<Option<State>, Violation> {
        if src.len() < 2 {
            return Ok(None);
        }
        let (first, second) = (src[0], src[1]);
        let fin = first & 0x80 != 0;
        // No extension is negotiated, so no reserved bit is meaningful.
        if first & 0x70 != 0 {
            return Err(Violation::Protocol("reserved bits set"));
        }
        let opcode = first & 0x0F;
        if second & 0x80 == 0 {
            return Err(Violation::Protocol("client frame not masked"));
        }
        let (len_bytes, len) = match second & 0x7F {
            126 => (2, 0),
            127 => (8, 0),
            short => (0, u64::from(short)),
        };
        let header_len = 2 + len_bytes + 4;
        debug_assert!(header_len <= MAX_HEADER);
        if src.len() < header_len {
            return Ok(None);
        }
        let len = match len_bytes {
            2 => u64::from(u16::from_be_bytes([src[2], src[3]])),
            8 => {
                let mut raw = [0u8; 8];
                raw.copy_from_slice(&src[2..10]);
                let len = u64::from_be_bytes(raw);
                if len >> 63 != 0 {
                    return Err(Violation::Protocol("length has the high bit set"));
                }
                len
            }
            _ => len,
        };
        let mut key = [0u8; 4];
        key.copy_from_slice(&src[2 + len_bytes..header_len]);
        let mask = Mask { key, offset: 0 };

        match opcode {
            OP_TEXT => Err(Violation::Text),
            OP_BINARY | OP_CONTINUATION => {
                if (opcode == OP_CONTINUATION) != self.in_message {
                    return Err(Violation::Protocol(if self.in_message {
                        "new message inside a fragmented one"
                    } else {
                        "continuation without a message"
                    }));
                }
                self.in_message = !fin;
                src.advance(header_len);
                Ok(Some(State::Data {
                    remaining: len,
                    mask,
                }))
            }
            OP_CLOSE | OP_PING | OP_PONG => {
                if !fin || len > MAX_CONTROL_PAYLOAD as u64 {
                    return Err(Violation::Protocol("fragmented or oversized control frame"));
                }
                src.advance(header_len);
                Ok(Some(State::Control {
                    opcode,
                    remaining: len as usize,
                    mask,
                    payload: Vec::with_capacity(len as usize),
                }))
            }
            _ => Err(Violation::Protocol("unknown opcode")),
        }
    }
}

fn control(opcode: u8, payload: Vec<u8>) -> Result<Event, Violation> {
    match opcode {
        OP_PING => Ok(Event::Ping(payload)),
        OP_PONG => Ok(Event::Pong),
        _ => match payload.as_slice() {
            [] => Ok(Event::Close(None)),
            [_] => Err(Violation::Protocol("close payload of one byte")),
            [hi, lo, ..] => Ok(Event::Close(Some(u16::from_be_bytes([*hi, *lo])))),
        },
    }
}

/// Whether a peer may send `code` in a close frame.
pub(super) fn valid_close_code(code: u16) -> bool {
    matches!(code, 1000..=1003 | 1007..=1011 | 3000..=4999)
}

/// Write a frame header. Server frames are never masked.
fn put_header(opcode: u8, len: usize, out: &mut BytesMut) {
    out.put_u8(0x80 | opcode);
    match len {
        0..=125 => out.put_u8(len as u8),
        126..=0xFFFF => {
            out.put_u8(126);
            out.put_u16(len as u16);
        }
        _ => {
            out.put_u8(127);
            out.put_u64(len as u64);
        }
    }
}

/// Append a binary frame carrying `payload`.
pub(super) fn put_binary(payload: &[u8], out: &mut BytesMut) {
    put_header(OP_BINARY, payload.len(), out);
    out.extend_from_slice(payload);
}

pub(super) fn put_pong(payload: &[u8], out: &mut BytesMut) {
    put_header(OP_PONG, payload.len(), out);
    out.extend_from_slice(payload);
}

pub(super) fn put_close(code: u16, out: &mut BytesMut) {
    put_header(OP_CLOSE, 2, out);
    out.put_u16(code);
}
