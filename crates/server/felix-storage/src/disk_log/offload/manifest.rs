//! The offload manifest: which sealed segments have a verified copy in the
//! object store.
//!
//! Unlike an index it is not derived. Once a segment's local file is gone this
//! is the only record that its records still exist, so a manifest that is
//! present but does not decode fails the open instead of reading as empty.
//! It is rewritten whole (temporary, fsync, rename, directory fsync) so a
//! crash leaves the old manifest or the new one and never a mix.

use std::path::{Path, PathBuf};

use crate::io::sync_dir;
use crate::log::{Offset, SegmentDescriptor, SegmentId};
use crate::{Result, StorageError};

/// `"FLOF"`.
const MAGIC: u32 = 0x464C_4F46;
const VERSION: u16 = 1;
/// magic(4) + version(2) + reserved(2) + count(4)
const HEADER_LEN: usize = 12;
/// id, base, last, size, oldest and newest timestamp (8 each), checksum(4),
/// key length(2), then the key.
const ENTRY_FIXED_LEN: usize = 6 * 8 + 4 + 2;
const CRC_LEN: usize = 4;
pub(crate) const FILE_NAME: &str = "offload.manifest";

/// One segment with a verified copy in the object store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManifestEntry {
    pub(crate) segment_id: SegmentId,
    pub(crate) base_offset: Offset,
    pub(crate) last_offset: Offset,
    /// Length of the object, which is the segment's valid bytes.
    pub(crate) size_bytes: u64,
    /// CRC-32 of the object's bytes.
    pub(crate) checksum: u32,
    pub(crate) oldest_timestamp_micros: u64,
    pub(crate) newest_timestamp_micros: u64,
    pub(crate) key: String,
}

impl ManifestEntry {
    /// Whether this entry is a copy of the local segment `descriptor`.
    pub(crate) fn describes(&self, descriptor: &SegmentDescriptor) -> bool {
        self.segment_id == descriptor.id
            && self.base_offset == descriptor.base_offset
            && self.last_offset == descriptor.last_offset
            && self.size_bytes == descriptor.size_bytes
    }
}

/// A shard's offloaded segments, ordered by base offset and never
/// overlapping. Gaps are allowed: a segment retention deleted while offload was
/// off was never copied.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Manifest {
    entries: Vec<ManifestEntry>,
}

impl Manifest {
    #[cfg(test)]
    pub(crate) fn entries(&self) -> &[ManifestEntry] {
        &self.entries
    }

    /// Whether `descriptor` has a recorded copy.
    pub(crate) fn records(&self, descriptor: &SegmentDescriptor) -> bool {
        self.entries.iter().any(|entry| entry.describes(descriptor))
    }

    /// Whether some entry overlaps `[base, last]`.
    pub(crate) fn overlaps(&self, base: Offset, last: Offset) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.base_offset <= last && base <= entry.last_offset)
    }

    /// Whether entries cover every offset in `[from, to)` with no gap.
    pub(crate) fn covers(&self, from: Offset, to: Offset) -> bool {
        let mut next = from;
        for entry in &self.entries {
            if next >= to {
                break;
            }
            if entry.last_offset < next {
                continue;
            }
            if entry.base_offset > next {
                return false;
            }
            next = entry.last_offset + 1;
        }
        next >= to
    }

    /// Add `entry`, keeping the order. Refuses one that overlaps an entry
    /// already here: two copies claiming the same offsets cannot both be right.
    pub(crate) fn insert(&mut self, entry: ManifestEntry) -> Result<()> {
        if self.overlaps(entry.base_offset, entry.last_offset) {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "offload manifest already covers part of offsets {}..={}",
                    entry.base_offset, entry.last_offset
                ),
            )));
        }
        let at = self
            .entries
            .partition_point(|existing| existing.base_offset < entry.base_offset);
        self.entries.insert(at, entry);
        Ok(())
    }

    /// Drop every entry holding an offset at or after `offset`, returning
    /// whether any went. For a truncation: those copies hold records the log
    /// no longer has, and new records will take their offsets.
    pub(crate) fn forget_from(&mut self, offset: Offset) -> bool {
        let before = self.entries.len();
        self.entries.retain(|entry| entry.last_offset < offset);
        self.entries.len() != before
    }

    /// Drop every entry. For a reset, which discards the whole history.
    pub(crate) fn clear(&mut self) -> bool {
        let had = !self.entries.is_empty();
        self.entries.clear();
        had
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            HEADER_LEN
                + self
                    .entries
                    .iter()
                    .map(|entry| ENTRY_FIXED_LEN + entry.key.len())
                    .sum::<usize>()
                + CRC_LEN,
        );
        out.extend_from_slice(&MAGIC.to_be_bytes());
        out.extend_from_slice(&VERSION.to_be_bytes());
        out.extend_from_slice(&[0, 0]);
        out.extend_from_slice(&(self.entries.len() as u32).to_be_bytes());
        for entry in &self.entries {
            for value in [
                entry.segment_id,
                entry.base_offset,
                entry.last_offset,
                entry.size_bytes,
                entry.oldest_timestamp_micros,
                entry.newest_timestamp_micros,
            ] {
                out.extend_from_slice(&value.to_be_bytes());
            }
            out.extend_from_slice(&entry.checksum.to_be_bytes());
            out.extend_from_slice(&(entry.key.len() as u16).to_be_bytes());
            out.extend_from_slice(entry.key.as_bytes());
        }
        let crc = crate::segment::format::crc32(&[&out]);
        out.extend_from_slice(&crc.to_be_bytes());
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_LEN + CRC_LEN {
            return None;
        }
        let (body, crc) = bytes.split_at(bytes.len() - CRC_LEN);
        if crate::segment::format::crc32(&[body]) != u32::from_be_bytes(crc.try_into().ok()?) {
            return None;
        }
        let mut reader = Reader { bytes: body };
        if reader.u32()? != MAGIC || reader.u16()? != VERSION {
            return None;
        }
        reader.take(2)?;
        let count = reader.u32()? as usize;
        let mut manifest = Manifest::default();
        for _ in 0..count {
            let segment_id = reader.u64()?;
            let base_offset = reader.u64()?;
            let last_offset = reader.u64()?;
            let size_bytes = reader.u64()?;
            let oldest_timestamp_micros = reader.u64()?;
            let newest_timestamp_micros = reader.u64()?;
            let checksum = reader.u32()?;
            let key_len = reader.u16()? as usize;
            let key = String::from_utf8(reader.take(key_len)?.to_vec()).ok()?;
            if last_offset < base_offset {
                return None;
            }
            manifest
                .insert(ManifestEntry {
                    segment_id,
                    base_offset,
                    last_offset,
                    size_bytes,
                    checksum,
                    oldest_timestamp_micros,
                    newest_timestamp_micros,
                    key,
                })
                .ok()?;
        }
        reader.bytes.is_empty().then_some(manifest)
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.bytes.len() < len {
            return None;
        }
        let (head, rest) = self.bytes.split_at(len);
        self.bytes = rest;
        Some(head)
    }

    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_be_bytes(self.take(2)?.try_into().ok()?))
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }
}

/// Read a shard's manifest. Absent reads as empty: nothing was offloaded.
pub(crate) fn load(dir: &Path) -> Result<Manifest> {
    let path = path_in(dir);
    match std::fs::read(&path) {
        Ok(bytes) => Manifest::decode(&bytes).ok_or_else(|| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{} does not decode; it is the only record of which segments \
                     exist only in the object store and cannot be rebuilt",
                    path.display()
                ),
            ))
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Manifest::default()),
        Err(err) => Err(StorageError::Io(err)),
    }
}

/// Write a shard's manifest durably. Returns once the new manifest and its
/// directory entry are flushed, which is what makes an unlink after it safe.
pub(crate) fn store(dir: &Path, manifest: &Manifest) -> Result<()> {
    let path = path_in(dir);
    let temporary = path.with_extension("tmp");
    {
        let file = std::fs::File::create(&temporary).map_err(StorageError::Io)?;
        crate::io::write_all(&file, &manifest.encode()).map_err(StorageError::Io)?;
        crate::io::sync_all(&file).map_err(StorageError::Io)?;
    }
    std::fs::rename(&temporary, &path).map_err(StorageError::Io)?;
    sync_dir(dir).map_err(StorageError::Io)?;
    Ok(())
}

fn path_in(dir: &Path) -> PathBuf {
    dir.join(FILE_NAME)
}

#[cfg(test)]
mod tests;
