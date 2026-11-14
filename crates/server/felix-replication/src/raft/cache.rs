//! The state machine of a cache shard's group: one log holds cache puts,
//! deletes and counter adds, and every member folds it into the same
//! [`CacheState`].
//!
//! Nothing here reads a clock, a random source or a hash order. The leader
//! stamps each entry, and apply, the snapshot and expiry in a write all go
//! by that stamp, so two members that applied the same entries hold the same
//! state. `docs/formal/FelixRaftCache.tla` is the model, and
//! `docs/per-shard-raft.md`, "Cache and counters", the design.

mod layers;
#[cfg(test)]
mod measure;
mod record;
mod snapshot;
mod state;
#[cfg(test)]
mod tests;

use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub(crate) use record::{CacheCommand, Request};
#[cfg(test)]
pub(crate) use snapshot::{ENCODED, restores_of};
#[cfg(test)]
pub(crate) use state::floor;
pub(crate) use state::{Answer, CacheState, Cut, Read, stamp};

use super::codec::invalid;

/// What a put requires of the key. A delete can only name a version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Condition {
    None,
    /// The key has no unexpired value.
    Absent,
    /// The key holds this version, unexpired.
    Version(u64),
}

fn put_key(buf: &mut BytesMut, key: &str) {
    buf.put_u32_le(key.len() as u32);
    buf.put_slice(key.as_bytes());
}

/// Reads the fixed-width fields of a record or a snapshot. Every read is
/// bounds checked: the bytes come from disk or from a peer.
struct Fields<'a> {
    what: &'static str,
    rest: &'a mut Bytes,
}

impl Fields<'_> {
    fn need(&self, len: usize) -> io::Result<()> {
        match self.rest.len() >= len {
            true => Ok(()),
            false => Err(invalid(format!("{} is cut short", self.what))),
        }
    }

    fn u8(&mut self) -> io::Result<u8> {
        self.need(1)?;
        Ok(self.rest.get_u8())
    }

    fn u64(&mut self) -> io::Result<u64> {
        self.need(8)?;
        Ok(self.rest.get_u64_le())
    }

    fn i64(&mut self) -> io::Result<i64> {
        self.need(8)?;
        Ok(self.rest.get_i64_le())
    }

    fn bytes(&mut self) -> io::Result<Bytes> {
        self.need(4)?;
        let len = self.rest.get_u32_le() as usize;
        self.need(len)?;
        Ok(self.rest.split_to(len))
    }

    fn key(&mut self) -> io::Result<String> {
        String::from_utf8(self.bytes()?.to_vec())
            .map_err(|_| invalid(format!("a key in {} is not UTF-8", self.what)))
    }

    fn end(&self) -> io::Result<()> {
        match self.rest.is_empty() {
            true => Ok(()),
            false => Err(invalid(format!("{} has bytes after its end", self.what))),
        }
    }
}
