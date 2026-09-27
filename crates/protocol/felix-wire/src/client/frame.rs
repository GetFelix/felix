//! Frame header layout and framing primitives for the v1 wire protocol.

use bytes::{Buf, Bytes, BytesMut};

use crate::error::{Error, Result};

/// `FLX1`, the first four bytes of every client frame.
pub const MAGIC: u32 = 0x464C5831;
/// The client protocol's version. Capabilities are negotiated with flags
/// rather than by bumping this.
pub const VERSION: u16 = 1;

/// Frame containing a header and payload.
///
/// ```
/// use bytes::Bytes;
/// use felix_wire::Frame;
///
/// let frame = Frame::new(0x1, Bytes::from_static(b"hello")).expect("frame");
/// let encoded = frame.encode();
/// let decoded = Frame::decode(encoded).expect("decode");
/// assert_eq!(decoded.payload, Bytes::from_static(b"hello"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub header: FrameHeader,
    pub payload: Bytes,
}

impl Frame {
    /// Frame a payload; fails if it is too long for the header's `u32` length.
    pub fn new(flags: u16, payload: Bytes) -> Result<Self> {
        if payload.len() > u32::MAX as usize {
            return Err(Error::FrameTooLarge);
        }
        Ok(Self {
            header: FrameHeader::new(flags, payload.len() as u32),
            payload,
        })
    }

    /// The header and payload as one buffer.
    pub fn encode(&self) -> Bytes {
        // Pre-allocate the exact size to avoid reallocation.
        let mut buf = BytesMut::with_capacity(FrameHeader::LEN + self.payload.len());
        self.header.encode(&mut buf);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    /// Split a buffer into header and payload. Bytes past the declared length
    /// are not part of this frame and are ignored.
    pub fn decode(input: Bytes) -> Result<Self> {
        if input.len() < FrameHeader::LEN {
            return Err(Error::Incomplete);
        }
        let header = FrameHeader::decode(input.slice(0..FrameHeader::LEN))?;
        let length = header.length as usize;
        if input.len() < FrameHeader::LEN + length {
            return Err(Error::Incomplete);
        }
        let payload = input.slice(FrameHeader::LEN..FrameHeader::LEN + length);
        Ok(Self { header, payload })
    }
}

/// The fixed 12-byte header in front of every frame, in network byte order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub magic: u32,
    pub version: u16,
    pub flags: u16,
    pub length: u32,
}

impl FrameHeader {
    /// Encoded length in bytes.
    pub const LEN: usize = 12;

    /// Create a header with the current protocol constants.
    pub fn new(flags: u16, length: u32) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            flags,
            length,
        }
    }

    /// Append the header to `buf`.
    pub fn encode(&self, buf: &mut BytesMut) {
        // Always encode in network byte order for portability.
        buf.extend_from_slice(&self.magic.to_be_bytes());
        buf.extend_from_slice(&self.version.to_be_bytes());
        buf.extend_from_slice(&self.flags.to_be_bytes());
        buf.extend_from_slice(&self.length.to_be_bytes());
    }

    /// Write the header into a fixed buffer, for a caller assembling a frame
    /// from segments rather than one `BytesMut`.
    pub fn encode_into(&self, buf: &mut [u8; Self::LEN]) {
        // Always encode in network byte order for portability.
        buf[0..4].copy_from_slice(&self.magic.to_be_bytes());
        buf[4..6].copy_from_slice(&self.version.to_be_bytes());
        buf[6..8].copy_from_slice(&self.flags.to_be_bytes());
        buf[8..12].copy_from_slice(&self.length.to_be_bytes());
    }

    /// Parse a header, refusing a foreign magic, an unsupported version, or a
    /// flag bit outside [`crate::KNOWN_FLAGS`].
    ///
    /// Flags select the payload layout, so a frame with an unknown bit cannot
    /// be parsed correctly by this version, and refusing it here covers every
    /// reader rather than only the ones that remember to check.
    pub fn decode(buf: Bytes) -> Result<Self> {
        let header = Self::decode_allowing_unknown_flags(buf)?;
        if crate::has_unknown_flags(header.flags) {
            return Err(Error::UnknownFlags(header.flags));
        }
        Ok(header)
    }

    /// [`Self::decode`] without the flag check, for a reader that consumes the
    /// body anyway and refuses the frame itself, so the stream stays on a frame
    /// boundary and the peer can be told why. The caller must check
    /// [`crate::has_unknown_flags`] before looking at the body.
    pub fn decode_allowing_unknown_flags(mut buf: Bytes) -> Result<Self> {
        // Validate header before we trust the length.
        if buf.remaining() < Self::LEN {
            return Err(Error::Incomplete);
        }
        let magic = buf.get_u32();
        if magic != MAGIC {
            return Err(Error::InvalidMagic);
        }
        let version = buf.get_u16();
        if version != VERSION {
            return Err(Error::UnsupportedVersion(version));
        }
        let flags = buf.get_u16();
        let length = buf.get_u32();
        Ok(Self {
            magic,
            version,
            flags,
            length,
        })
    }
}

#[cfg(test)]
mod tests;
