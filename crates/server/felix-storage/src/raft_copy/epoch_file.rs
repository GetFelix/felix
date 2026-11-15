//! A copy's group epoch, kept in `raft-epoch`: the stream incarnation and,
//! for every re-form the copy's log went through, the base it started from
//! and the epoch's first index.
//!
//! An entry's epoch is not stored in its record; it is derived from these
//! bases (see [`EpochFile::epoch_at`]). A copy without the file is at epoch
//! 0. Nothing can rebuild it, so a file that does not decode is an error.
//! See `docs/storage-format.md`, "`raft-epoch`".

use std::io;
use std::path::Path;

use crate::raft_log::state_file;

/// `"FLEP"`.
const MAGIC: u32 = 0x464C_4550;
const VERSION: u16 = 1;
/// magic(4) + version(2) + epochs(2) + incarnation(8)
const HEADER_LEN: usize = 16;
/// flags(1) + reserved(7) + base index(8) + base epoch(8) + base term(8) + first index(8)
const BASE_LEN: usize = 40;
const CRC_LEN: usize = 4;
const HAS_BASE: u8 = 1;
pub(crate) const FILE_NAME: &str = "raft-epoch";

/// A log id with its group epoch: the entry at `index`, written in `term`
/// of epoch `epoch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EpochLogId {
    pub index: u64,
    pub epoch: u64,
    pub term: u64,
}

/// How one re-form started its epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochBase {
    /// The base's last log id, `None` for an empty base.
    pub base_last: Option<EpochLogId>,
    /// The epoch's first index. Indexes from the base's end up to it are a
    /// gap: offsets no record will ever take.
    pub first_index: u64,
}

impl EpochBase {
    /// One past the base's last index.
    pub fn base_end(&self) -> u64 {
        self.base_last.map_or(0, |last| last.index + 1)
    }
}

/// Where an index sits in a log of some epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochAt {
    /// An entry written in this epoch, or copied into a later base from it.
    Entry(u64),
    /// In the gap this epoch's re-form left: never a record.
    Gap(u64),
}

/// The contents of `raft-epoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochFile {
    pub incarnation: u64,
    /// The base of epoch `n + 1` at position `n`, so the copy's epoch is the
    /// length.
    pub bases: Vec<EpochBase>,
}

impl EpochFile {
    pub fn epoch(&self) -> u64 {
        self.bases.len() as u64
    }

    /// The epoch of index `index` in a log at epoch `epoch` whose re-forms
    /// started from `bases`: the highest epoch at or below `epoch` whose
    /// first index is at or below it, a gap when it sits between that
    /// epoch's base end and first index, else epoch 0.
    pub fn epoch_at(bases: &[EpochBase], epoch: u64, index: u64) -> EpochAt {
        let upto = usize::try_from(epoch)
            .unwrap_or(usize::MAX)
            .min(bases.len());
        for (n, base) in bases[..upto].iter().enumerate().rev() {
            let e = n as u64 + 1;
            if index >= base.first_index {
                return EpochAt::Entry(e);
            }
            if index >= base.base_end() {
                return EpochAt::Gap(e);
            }
        }
        EpochAt::Entry(0)
    }

    fn encode(&self) -> io::Result<Vec<u8>> {
        let count = u16::try_from(self.bases.len())
            .map_err(|_| io::Error::other("too many epochs for raft-epoch"))?;
        let mut out = Vec::with_capacity(HEADER_LEN + self.bases.len() * BASE_LEN + CRC_LEN);
        out.extend_from_slice(&MAGIC.to_be_bytes());
        out.extend_from_slice(&VERSION.to_be_bytes());
        out.extend_from_slice(&count.to_be_bytes());
        out.extend_from_slice(&self.incarnation.to_be_bytes());
        for base in &self.bases {
            let last = base.base_last.unwrap_or(EpochLogId {
                index: 0,
                epoch: 0,
                term: 0,
            });
            out.push(if base.base_last.is_some() {
                HAS_BASE
            } else {
                0
            });
            out.extend_from_slice(&[0; 7]);
            for value in [last.index, last.epoch, last.term, base.first_index] {
                out.extend_from_slice(&value.to_be_bytes());
            }
        }
        let crc = crate::segment::format::crc32(&[&out]);
        out.extend_from_slice(&crc.to_be_bytes());
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let u64_at = |at: usize| Some(u64::from_be_bytes(bytes.get(at..at + 8)?.try_into().ok()?));
        let header = bytes.get(..HEADER_LEN)?;
        let count = usize::from(u16::from_be_bytes(header[6..8].try_into().ok()?));
        if u32::from_be_bytes(header[0..4].try_into().ok()?) != MAGIC
            || u16::from_be_bytes(header[4..6].try_into().ok()?) != VERSION
            || bytes.len() != HEADER_LEN + count * BASE_LEN + CRC_LEN
        {
            return None;
        }
        let body = bytes.len() - CRC_LEN;
        let crc = u32::from_be_bytes(bytes[body..].try_into().ok()?);
        if crate::segment::format::crc32(&[&bytes[..body]]) != crc {
            return None;
        }
        let mut bases = Vec::with_capacity(count);
        for n in 0..count {
            let at = HEADER_LEN + n * BASE_LEN;
            let base_last = match bytes[at] {
                0 => None,
                HAS_BASE => Some(EpochLogId {
                    index: u64_at(at + 8)?,
                    epoch: u64_at(at + 16)?,
                    term: u64_at(at + 24)?,
                }),
                _ => return None,
            };
            bases.push(EpochBase {
                base_last,
                first_index: u64_at(at + 32)?,
            });
        }
        Some(Self {
            incarnation: u64_at(8)?,
            bases,
        })
    }
}

/// The epoch file in `dir`, or `None` when the copy is at epoch 0.
pub fn load(dir: &Path) -> io::Result<Option<EpochFile>> {
    let Some(bytes) = state_file::load(dir, FILE_NAME)? else {
        return Ok(None);
    };
    EpochFile::decode(&bytes).map(Some).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} does not decode; it holds the copy's group epoch and cannot be rebuilt",
                dir.join(FILE_NAME).display()
            ),
        )
    })
}

/// Replace the epoch file in `dir` with `file`, synced.
pub fn store(dir: &Path, file: &EpochFile) -> io::Result<()> {
    state_file::store(dir, FILE_NAME, &[&file.encode()?])
}
