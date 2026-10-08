//! Blocks reserved ahead of a segment's writes, grown as the segment fills.
//!
//! A segment starts with a small reservation and doubles it each time its
//! records pass half of what is reserved, up to the segment size. A busy
//! segment still writes into reserved blocks almost all the time, and an idle
//! one holds a megabyte instead of a whole segment.
//!
//! Reserving never moves `st_size` (see `crate::io::preallocate`), so nothing
//! here changes what recovery sees. An extension is best effort and runs off
//! the append path; one that fails only means later writes allocate as they go.

use std::fs::File;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::io::preallocate;

/// The most a new segment reserves before it has records.
pub(crate) const INITIAL_RESERVATION_BYTES: u64 = 1024 * 1024;

/// What a segment has reserved, and the next extension it is due.
#[derive(Debug)]
pub(crate) struct Reservation {
    /// The most this segment reserves; zero when preallocation is off.
    limit: u64,
    /// Bytes reserved, or handed out to an [`Extension`] that will reserve
    /// them. Handing out moves it, so each extension is due once.
    claimed: u64,
    file: Arc<File>,
    /// Set by [`Self::close`]. Taken by an extension for its whole call, so one
    /// that runs late cannot reserve past a file its seal has trimmed.
    closed: Arc<Mutex<bool>>,
}

impl Reservation {
    /// A reservation for `file`, which already has `claimed` bytes reserved
    /// or written.
    pub(crate) fn new(file: Arc<File>, limit: u64, claimed: u64) -> Self {
        Self {
            limit,
            claimed,
            file,
            closed: Arc::new(Mutex::new(false)),
        }
    }

    /// The first reservation of a segment that reserves at most `limit`.
    /// Never more than a sixteenth of it, so a small segment still grows
    /// its reservation as it fills rather than taking it all at once.
    pub(crate) fn initial_bytes(limit: u64) -> u64 {
        INITIAL_RESERVATION_BYTES.min(limit / 16)
    }

    /// The extension due now that `written` bytes are in the segment, if one
    /// is: once the writes pass half of what is claimed, double it.
    pub(crate) fn due(&mut self, written: u64) -> Option<Extension> {
        if self.claimed >= self.limit || written <= self.claimed / 2 {
            return None;
        }
        let to = (self.claimed * 2)
            .max(written * 2)
            .max(Self::initial_bytes(self.limit))
            .min(self.limit);
        let from = std::mem::replace(&mut self.claimed, to);
        Some(Extension {
            file: Arc::clone(&self.file),
            closed: Arc::clone(&self.closed),
            from,
            to,
        })
    }

    /// Refuse every extension from now on. Called before a seal trims the
    /// file to its records.
    pub(crate) fn close(&self) {
        *self.closed.lock() = true;
    }
}

impl Drop for Reservation {
    /// A writer replaced without a seal, as a truncation does, stops its own
    /// extensions too: the next writer on the file keeps its own count.
    fn drop(&mut self) {
        self.close();
    }
}

/// One step of a segment's reservation, to run off the append path.
#[derive(Debug)]
pub(crate) struct Extension {
    file: Arc<File>,
    closed: Arc<Mutex<bool>>,
    from: u64,
    to: u64,
}

impl Extension {
    /// Reserve the blocks, unless the segment has been sealed since this was
    /// handed out.
    pub(crate) fn apply(&self) -> std::io::Result<()> {
        let closed = self.closed.lock();
        if *closed {
            return Ok(());
        }
        preallocate(&self.file, self.from, self.to - self.from)
    }

    /// Where the reservation ends once this is applied.
    pub(crate) fn to(&self) -> u64 {
        self.to
    }
}

#[cfg(test)]
mod tests;
