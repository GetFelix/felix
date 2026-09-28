//! Where each leadership generation began in a shard's log.
//!
//! Offsets alone cannot say where two logs part company. Both have an offset
//! 100, and "they differ at 100" says nothing about how far back they agree.
//! A generation does say, because it belongs to exactly one leader: the highest
//! generation two brokers share is the last one they cannot disagree within.
//!
//! So each shard keeps a short list of `(generation, start offset)`. It is
//! small — one entry per leadership change, not per record — and it is written
//! rather than derived, because nothing in the log itself records which leader
//! wrote a record. See `docs/replication-design.md`, "Divergence and
//! truncation".

use std::path::{Path, PathBuf};

use crate::io::sync_dir;
use crate::log::{Epoch, Offset};
use crate::{Result, StorageError};

/// `"FLEP"`, so a stray file in a shard directory is not mistaken for one.
const EPOCH_MAGIC: u32 = 0x464C_4550;
const EPOCH_VERSION: u16 = 1;
/// magic(4) + version(2) + count(2) + crc(4)
pub(super) const EPOCH_HEADER_LEN: usize = 12;
/// generation(8) + start_offset(8)
pub(super) const EPOCH_ENTRY_LEN: usize = 16;

/// A cap on how much history is kept.
///
/// Only the newest entries can matter: a generation older than the oldest
/// retained record cannot be the one two brokers diverge within, because
/// neither still holds it. Bounding the file also bounds the read on every
/// open.
const MAX_ENTRIES: usize = 512;

/// A shard's generation history, newest last.
///
/// Absent on disk is a valid state, not an error: every shard written before
/// this existed has no history, and a broker that cannot say where a generation
/// began simply cannot offer a truncation point — which is a refusal, not a
/// corruption.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct EpochMap {
    entries: Vec<Epoch>,
}

impl EpochMap {
    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn entries(&self) -> &[Epoch] {
        &self.entries
    }

    /// The newest generation recorded, if any.
    pub(super) fn newest(&self) -> Option<Epoch> {
        self.entries.last().copied()
    }

    /// Note that `generation` begins at `start_offset`.
    ///
    /// Ignores a generation at or below the newest already held: a leader
    /// re-reporting its own generation is ordinary, and one *older* than the
    /// newest is a stale message that must not rewrite history.
    pub(super) fn record(&mut self, generation: u64, start_offset: Offset) -> bool {
        if let Some(newest) = self.newest()
            && generation <= newest.generation
        {
            return false;
        }
        self.entries.push(Epoch {
            generation,
            start_offset,
        });
        if self.entries.len() > MAX_ENTRIES {
            let excess = self.entries.len() - MAX_ENTRIES;
            self.entries.drain(..excess);
        }
        true
    }

    /// Where `generation` stops, given the log's tail.
    ///
    /// The start of the next generation recorded, or the tail when this is the
    /// newest. `None` when the generation is not in the history at all — which
    /// is the case that must refuse rather than guess, since a guess here is a
    /// truncation point.
    pub(super) fn end_of(&self, generation: u64, tail: Offset) -> Option<Offset> {
        let index = self
            .entries
            .iter()
            .position(|epoch| epoch.generation == generation)?;
        Some(match self.entries.get(index + 1) {
            Some(next) => next.start_offset,
            None => tail,
        })
    }

    /// The newest generation held by both, and where it began here.
    ///
    /// The point of comparing at all: this is the last generation within which
    /// the two cannot disagree, so it is where a repair starts from.
    #[cfg(test)]
    pub(super) fn newest_shared(&self, other: &[Epoch]) -> Option<Epoch> {
        self.entries
            .iter()
            .rev()
            .find(|mine| {
                other
                    .iter()
                    .any(|theirs| theirs.generation == mine.generation)
            })
            .copied()
    }

    /// Forget generations that begin at or after `offset`.
    ///
    /// Called with a truncation, so the history does not outlive the records it
    /// describes and point at offsets the log no longer has.
    pub(super) fn truncate_from(&mut self, offset: Offset) {
        self.entries.retain(|epoch| epoch.start_offset < offset);
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut body = Vec::with_capacity(self.entries.len() * EPOCH_ENTRY_LEN);
        for epoch in &self.entries {
            body.extend_from_slice(&epoch.generation.to_be_bytes());
            body.extend_from_slice(&epoch.start_offset.to_be_bytes());
        }
        let mut out = Vec::with_capacity(EPOCH_HEADER_LEN + body.len());
        out.extend_from_slice(&EPOCH_MAGIC.to_be_bytes());
        out.extend_from_slice(&EPOCH_VERSION.to_be_bytes());
        out.extend_from_slice(&(self.entries.len() as u16).to_be_bytes());
        out.extend_from_slice(&crate::segment::format::crc32(&[&body]).to_be_bytes());
        out.extend_from_slice(&body);
        out
    }

    pub(super) fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < EPOCH_HEADER_LEN {
            return None;
        }
        let magic = u32::from_be_bytes(bytes[0..4].try_into().ok()?);
        let version = u16::from_be_bytes(bytes[4..6].try_into().ok()?);
        if magic != EPOCH_MAGIC || version != EPOCH_VERSION {
            return None;
        }
        let count = u16::from_be_bytes(bytes[6..8].try_into().ok()?) as usize;
        let expected_crc = u32::from_be_bytes(bytes[8..12].try_into().ok()?);
        let body = bytes.get(EPOCH_HEADER_LEN..EPOCH_HEADER_LEN + count * EPOCH_ENTRY_LEN)?;
        if crate::segment::format::crc32(&[body]) != expected_crc {
            return None;
        }
        let entries = body
            .chunks_exact(EPOCH_ENTRY_LEN)
            .map(|chunk| Epoch {
                generation: u64::from_be_bytes(chunk[0..8].try_into().expect("8 bytes")),
                start_offset: u64::from_be_bytes(chunk[8..16].try_into().expect("8 bytes")),
            })
            .collect();
        Some(Self { entries })
    }
}

/// Read a shard's generation history.
///
/// An absent, short, or corrupt file reads as empty rather than failing the
/// open. This is derived state used to *offer* a truncation point, so losing it
/// costs the ability to repair a divergence automatically — it never costs a
/// record, and refusing to start a broker over it would trade an outage for a
/// convenience.
pub(super) fn load(dir: &Path) -> EpochMap {
    match std::fs::read(path_in(dir)) {
        Ok(bytes) => EpochMap::decode(&bytes).unwrap_or_else(|| {
            tracing::warn!(
                dir = %dir.display(),
                "the generation history is unreadable and was ignored; a \
                 divergence on this shard will need an operator until it is \
                 rewritten",
            );
            EpochMap::default()
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => EpochMap::default(),
        Err(err) => {
            tracing::warn!(
                dir = %dir.display(),
                error = %err,
                "could not read the generation history; continuing without it",
            );
            EpochMap::default()
        }
    }
}

/// Write a shard's generation history, atomically.
///
/// Through a temporary and a rename, for the same reason compaction's directory
/// swap is: a half-written history is worse than none, because it would be read
/// back as a confident answer about where a generation began.
pub(super) fn store(dir: &Path, map: &EpochMap) -> Result<()> {
    use std::io::Write;

    let path = path_in(dir);
    let temporary = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&temporary).map_err(StorageError::Io)?;
        file.write_all(&map.encode()).map_err(StorageError::Io)?;
        crate::io::sync_all(&file).map_err(StorageError::Io)?;
    }
    std::fs::rename(&temporary, &path).map_err(StorageError::Io)?;
    sync_dir(dir).map_err(StorageError::Io)?;
    Ok(())
}

pub(super) fn epochs_file_name() -> &'static str {
    "epochs"
}

fn path_in(dir: &Path) -> PathBuf {
    dir.join(epochs_file_name())
}

#[cfg(test)]
mod tests;
