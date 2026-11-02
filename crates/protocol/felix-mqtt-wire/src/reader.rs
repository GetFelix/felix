//! Checked reads over a packet body. Fields come out as slices of the buffer
//! the packet was read into, so nothing is copied.

use bytes::{Buf, Bytes};

use crate::error::DecodeError;
use crate::text::{self, Utf8};

pub(crate) struct Reader {
    buf: Bytes,
}

impl Reader {
    pub(crate) fn new(buf: Bytes) -> Self {
        Reader { buf }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Refuses anything left over.
    pub(crate) fn finish(&self) -> Result<(), DecodeError> {
        match self.buf.len() {
            0 => Ok(()),
            n => Err(DecodeError::TrailingBytes(n)),
        }
    }

    pub(crate) fn u8(&mut self) -> Result<u8, DecodeError> {
        if self.buf.is_empty() {
            return Err(DecodeError::Truncated);
        }
        Ok(self.buf.get_u8())
    }

    pub(crate) fn u16(&mut self) -> Result<u16, DecodeError> {
        if self.buf.len() < 2 {
            return Err(DecodeError::Truncated);
        }
        Ok(self.buf.get_u16())
    }

    pub(crate) fn u32(&mut self) -> Result<u32, DecodeError> {
        if self.buf.len() < 4 {
            return Err(DecodeError::Truncated);
        }
        Ok(self.buf.get_u32())
    }

    /// A variable byte integer of at most four bytes, in its shortest form.
    pub(crate) fn varint(&mut self) -> Result<u32, DecodeError> {
        let mut value = 0u32;
        for i in 0..4 {
            let byte = self.u8()?;
            value |= u32::from(byte & 0x7F) << (7 * i);
            if byte & 0x80 == 0 {
                if i > 0 && byte == 0 {
                    return Err(DecodeError::MalformedLength);
                }
                return Ok(value);
            }
        }
        Err(DecodeError::MalformedLength)
    }

    pub(crate) fn take(&mut self, len: usize) -> Result<Bytes, DecodeError> {
        if self.buf.len() < len {
            return Err(DecodeError::Truncated);
        }
        Ok(self.buf.split_to(len))
    }

    /// Binary data: a two byte length, then that many bytes.
    pub(crate) fn binary(&mut self) -> Result<Bytes, DecodeError> {
        let len = usize::from(self.u16()?);
        self.take(len)
    }

    pub(crate) fn string(&mut self) -> Result<Utf8, DecodeError> {
        let bytes = self.binary()?;
        text::validate(&bytes)?;
        Ok(Utf8::from_validated(bytes))
    }

    pub(crate) fn rest(&mut self) -> Bytes {
        self.buf.split_off(0)
    }

    /// The next `len` bytes as a reader of their own.
    pub(crate) fn sub(&mut self, len: usize) -> Result<Reader, DecodeError> {
        Ok(Reader::new(self.take(len)?))
    }
}

#[cfg(test)]
mod tests;
