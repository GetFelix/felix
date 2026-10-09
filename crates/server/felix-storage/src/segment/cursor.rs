//! A read-ahead window over one segment file.
//!
//! Scans and range reads both go through [`SegmentCursor`], which reads ahead
//! in fixed chunks using positioned reads. That keeps peak memory at one chunk
//! plus one record — never the segment size — and lets a single descriptor
//! serve concurrent readers, because `pread` does not touch the file cursor.

use std::fs::File;
use std::io;

use crate::io::read_at;

/// Read-ahead window. Large enough that a scan of small records is dominated by
/// memcpy rather than syscalls, small enough to stay comfortably in L2.
pub(super) const READ_CHUNK_BYTES: usize = 64 * 1024;

/// A sliding read-ahead window over a segment file.
///
/// Callers ask for "at least `want` bytes at file position `pos`" and get back
/// however many are available. The window only ever moves forward in practice,
/// so a sequential walk refills once per chunk.
pub(super) struct SegmentCursor<'a> {
    file: &'a File,
    buf: Vec<u8>,
    /// File position of `buf[0]`.
    buf_start: u64,
    /// Valid bytes in `buf`.
    buf_len: usize,
    /// Bytes at or past this position read as absent, as if the file ended
    /// here.
    end: u64,
}

impl<'a> SegmentCursor<'a> {
    pub(super) fn new(file: &'a File) -> Self {
        Self {
            file,
            buf: vec![0u8; READ_CHUNK_BYTES],
            buf_start: 0,
            buf_len: 0,
            end: u64::MAX,
        }
    }

    /// A cursor that sees the file as if it were cut at `end`.
    pub(super) fn ending_at(file: &'a File, end: u64) -> Self {
        Self {
            end,
            ..Self::new(file)
        }
    }

    /// Bytes available at `pos`, up to at least `want` where the file allows.
    ///
    /// A returned slice shorter than `want` means end of file, which callers
    /// interpret as truncation.
    pub(super) fn slice_at(&mut self, pos: u64, want: usize) -> io::Result<&[u8]> {
        let cached = pos >= self.buf_start
            && pos
                .checked_sub(self.buf_start)
                .is_some_and(|delta| delta as usize + want <= self.buf_len);
        if !cached {
            // A record bigger than the window gets a one-off larger read rather
            // than a permanently inflated buffer.
            let capacity = want.max(READ_CHUNK_BYTES);
            if self.buf.len() < capacity {
                self.buf.resize(capacity, 0);
            }
            let read = read_at(self.file, &mut self.buf[..capacity], pos)?;
            self.buf_len =
                read.min(usize::try_from(self.end.saturating_sub(pos)).unwrap_or(usize::MAX));
            self.buf_start = pos;
        }
        let from = (pos - self.buf_start) as usize;
        let to = self.buf_len.min(from + want);
        Ok(&self.buf[from..to])
    }
}

#[cfg(test)]
mod tests;
