//! The block a record carries when it has a key or headers.
//!
//! ```text
//! u8  flags                       bit 0 = a key follows; every other bit is 0
//! [u32 key_len, key bytes]        only when bit 0 is set
//! u32 count
//! count x { u16 name_len, name (UTF-8), u32 value_len, value bytes }
//! ```
//!
//! The key is the record's own and does not route: a batch's shard still comes
//! from its batch key (`FLAG_BINARY_PUBLISH_KEYED`).
//!
//! Big-endian like the rest of the protocol. Names may repeat and keep their
//! order; a name may not be empty. A record with neither a key nor headers has
//! no block at all, so a block with no key and no headers is refused: "none" has
//! one encoding.
//!
//! The same bytes ride a frame ([`EXT_RECORD_HEADERS`](crate::EXT_RECORD_HEADERS))
//! and sit in the log, where storage keeps them opaque. [`RecordHeaders::validate`]
//! checks a raw block without allocating, for a reader that only forwards it.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

const FLAG_KEY: u8 = 0x01;
const KNOWN_BLOCK_FLAGS: u8 = FLAG_KEY;

/// Header names that start with this belong to Felix's own listeners (the MQTT
/// listener uses `felix.mqtt.*`), so a client publish that carries one is
/// refused and a reader can trust it came from the broker.
pub const RESERVED_HEADER_PREFIX: &str = "felix.";

// u16 name length + u32 value length: the least a header can take, since a
// name may not be empty but a value may.
const MIN_HEADER_LEN: usize = 6;

/// A record's key and headers, as one block.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecordHeaders {
    pub key: Option<Bytes>,
    /// In send order; a name may repeat.
    pub headers: Vec<(String, Bytes)>,
}

impl RecordHeaders {
    /// True if there is no key and no header. Such a record has no block, so
    /// [`encode`](Self::encode) refuses it.
    pub fn is_empty(&self) -> bool {
        self.key.is_none() && self.headers.is_empty()
    }

    /// The block's bytes. Fails on a block with no key and no headers, an empty
    /// header name, or a length that does not fit its field.
    pub fn encode(&self) -> Result<Bytes> {
        if self.is_empty() {
            return Err(Error::InvalidRecordHeaders("empty block"));
        }
        let mut len = 1 + 4;
        if let Some(key) = &self.key {
            u32::try_from(key.len()).map_err(|_| Error::FrameTooLarge)?;
            len += 4 + key.len();
        }
        for (name, value) in &self.headers {
            if name.is_empty() {
                return Err(Error::InvalidRecordHeaders("empty header name"));
            }
            u16::try_from(name.len()).map_err(|_| Error::FrameTooLarge)?;
            u32::try_from(value.len()).map_err(|_| Error::FrameTooLarge)?;
            len += 2 + name.len() + 4 + value.len();
        }
        let count = u32::try_from(self.headers.len()).map_err(|_| Error::FrameTooLarge)?;
        let mut buf = BytesMut::with_capacity(len);
        match &self.key {
            Some(key) => {
                buf.put_u8(FLAG_KEY);
                buf.put_u32(key.len() as u32);
                buf.extend_from_slice(key);
            }
            None => buf.put_u8(0),
        }
        buf.put_u32(count);
        for (name, value) in &self.headers {
            buf.put_u16(name.len() as u16);
            buf.extend_from_slice(name.as_bytes());
            buf.put_u32(value.len() as u32);
            buf.extend_from_slice(value);
        }
        Ok(buf.freeze())
    }

    /// Parse a block. The key and values are slices of `block`, not copies.
    pub fn decode(block: Bytes) -> Result<Self> {
        let mut cursor = Cursor::new(&block);
        let flags = cursor.flags()?;
        let key = if flags & FLAG_KEY != 0 {
            let len = cursor.u32()? as usize;
            let at = cursor.take(len)?;
            Some(block.slice(at))
        } else {
            None
        };
        let count = cursor.count()?;
        if flags == 0 && count == 0 {
            return Err(Error::InvalidRecordHeaders("empty block"));
        }
        let mut headers = Vec::with_capacity(count);
        for _ in 0..count {
            let name_len = cursor.u16()? as usize;
            let name_at = cursor.take(name_len)?;
            let name = std::str::from_utf8(&block[name_at]).map_err(|_| Error::InvalidUtf8)?;
            if name.is_empty() {
                return Err(Error::InvalidRecordHeaders("empty header name"));
            }
            let value_len = cursor.u32()? as usize;
            let value_at = cursor.take(value_len)?;
            headers.push((name.to_owned(), block.slice(value_at)));
        }
        cursor.finish()?;
        Ok(Self { key, headers })
    }

    /// Whether any header name in `block` starts with `prefix`, without
    /// allocating. Names are compared as bytes, so the match is case-sensitive.
    /// A block that does not parse is an error, as for [`validate`](Self::validate).
    pub fn any_name_starts_with(block: &[u8], prefix: &str) -> Result<bool> {
        let mut cursor = Cursor::new(block);
        let flags = cursor.flags()?;
        if flags & FLAG_KEY != 0 {
            let len = cursor.u32()? as usize;
            cursor.take(len)?;
        }
        let count = cursor.count()?;
        let mut found = false;
        for _ in 0..count {
            let name_len = cursor.u16()? as usize;
            let name_at = cursor.take(name_len)?;
            found |= block[name_at].starts_with(prefix.as_bytes());
            let value_len = cursor.u32()? as usize;
            cursor.take(value_len)?;
        }
        cursor.finish()?;
        Ok(found)
    }

    /// Check that `block` parses, without keeping any of it.
    pub fn validate(block: &[u8]) -> Result<()> {
        let mut cursor = Cursor::new(block);
        let flags = cursor.flags()?;
        if flags & FLAG_KEY != 0 {
            let len = cursor.u32()? as usize;
            cursor.take(len)?;
        }
        let count = cursor.count()?;
        if flags == 0 && count == 0 {
            return Err(Error::InvalidRecordHeaders("empty block"));
        }
        for _ in 0..count {
            let name_len = cursor.u16()? as usize;
            let name_at = cursor.take(name_len)?;
            let name = std::str::from_utf8(&block[name_at]).map_err(|_| Error::InvalidUtf8)?;
            if name.is_empty() {
                return Err(Error::InvalidRecordHeaders("empty header name"));
            }
            let value_len = cursor.u32()? as usize;
            cursor.take(value_len)?;
        }
        cursor.finish()
    }
}

/// A read position over a block, so `decode` and `validate` share one set of
/// bounds checks.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.at
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let range = self.take(N)?;
        Ok(self.bytes[range].try_into().expect("N bytes"))
    }

    fn u16(&mut self) -> Result<u16> {
        self.array().map(u16::from_be_bytes)
    }

    fn u32(&mut self) -> Result<u32> {
        self.array().map(u32::from_be_bytes)
    }

    /// The range of the next `len` bytes, which are consumed.
    fn take(&mut self, len: usize) -> Result<std::ops::Range<usize>> {
        if self.remaining() < len {
            return Err(Error::Incomplete);
        }
        let range = self.at..self.at + len;
        self.at += len;
        Ok(range)
    }

    fn flags(&mut self) -> Result<u8> {
        let [flags] = self.array()?;
        if flags & !KNOWN_BLOCK_FLAGS != 0 {
            return Err(Error::InvalidRecordHeaders("reserved block flag set"));
        }
        Ok(flags)
    }

    /// The header count, bounded by what the rest of the block could hold
    /// before anything is reserved for it.
    fn count(&mut self) -> Result<usize> {
        let count = self.u32()? as usize;
        if count > self.remaining() / MIN_HEADER_LEN {
            return Err(Error::Incomplete);
        }
        Ok(count)
    }

    fn finish(&self) -> Result<()> {
        if self.remaining() != 0 {
            return Err(Error::InvalidRecordHeaders("trailing bytes"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
