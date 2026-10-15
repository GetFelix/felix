//! A key index written to disk, so a restart replays only the log past it.
//!
//! The file is derived, never trusted: a reader checks its checksum and its
//! length, and the store that loads it checks that the log it describes is
//! still the log on disk. Anything that fails falls back to rebuilding the
//! index from the whole log, which is always correct, only slower. The byte
//! layout is in `docs/storage-format.md`.
//!
//! Entries are keyed by a composite key (kind, key, then slot and field for the
//! collection kinds) and written sorted by it, so the same file can hold the
//! per-field state hashes, sets and lists keep, and a sorted on-disk index
//! could later be built from it.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// The snapshot's name inside a shard directory.
pub(crate) const FILE_NAME: &str = "keys.idx";
const TEMPORARY_NAME: &str = "keys.idx.tmp";

/// `"FXIX"`.
const MAGIC: [u8; 4] = *b"FXIX";
const FORMAT: u16 = 1;
/// magic(4) format(2) store(1) covered_through(8) entry_count(8) log_bytes(8)
/// last_checksum(4).
const HEADER_LEN: u64 = 35;
/// offset(8) version(8) expires_at_millis(8) bytes(4).
const ENTRY_FIXED_LEN: u64 = 28;
const TRAILER_LEN: u64 = 4;

/// The `store` byte of a cache shard's snapshot. Collections will be 1.
pub(crate) const STORE_CACHE: u8 = 0;
/// The composite-key kind of a cache value. Hashes, sets and lists take 1, 2
/// and 3, and add a slot byte and a field after the key.
const KIND_CACHE_VALUE: u8 = 0;

const CRC32C: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

/// Where one key's (or one field's) current record lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    pub(crate) offset: u64,
    /// What a conditional write compares against: the offset of the put that
    /// wrote this value, kept unchanged when compaction moves it.
    pub(crate) version: u64,
    /// Absolute Unix milliseconds; zero means it never expires.
    pub(crate) expires_at_millis: u64,
    /// The record's payload length, for deciding when to compact. Record
    /// bodies are capped at 2^26 bytes, so it always fits.
    pub(crate) bytes: u32,
}

impl IndexEntry {
    pub(crate) fn is_expired(&self, now_millis: u64) -> bool {
        self.expires_at_millis != 0 && self.expires_at_millis <= now_millis
    }
}

/// What a snapshot says about the log it was taken from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) store: u8,
    /// Every record below this offset is reflected in the entries, and none at
    /// or past it is.
    pub(crate) covered_through: u64,
    /// Payload bytes of every record in the log below `covered_through`, live
    /// or not, so compaction's garbage ratio survives the restart.
    pub(crate) log_bytes: u64,
    /// The checksum of the record at `covered_through - 1`, which is how a
    /// loader tells the log it was taken from from one cut back and rewritten
    /// to the same length. Zero when nothing is covered.
    pub(crate) last_checksum: u32,
}

/// Why a snapshot was not used. Never an error the caller returns: the index
/// is rebuilt from the log instead.
#[derive(Debug)]
pub(crate) struct Rejected(pub(crate) String);

impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The composite key of a cache value: kind, then the key with its length.
pub(crate) fn cache_key(key: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + key.len());
    out.push(KIND_CACHE_VALUE);
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key.as_bytes());
    out
}

/// The key a cache value's composite key names, or `None` when it is not one.
pub(crate) fn parse_cache_key(composite: &[u8]) -> Option<&str> {
    let (&kind, rest) = composite.split_first()?;
    if kind != KIND_CACHE_VALUE || rest.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes(rest[..4].try_into().ok()?) as usize;
    let key = &rest[4..];
    if key.len() != len {
        return None;
    }
    std::str::from_utf8(key).ok()
}

/// Write a snapshot to the temporary file beside `dir`'s snapshot and flush
/// it. Nothing a loader reads changes until [`install`].
///
/// Sorts `entries` by composite key first.
pub(crate) fn write_temporary(
    dir: &Path,
    header: &Header,
    entries: &mut [(Vec<u8>, IndexEntry)],
) -> std::io::Result<()> {
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let file = File::create(temporary_path(dir))?;
    let mut out = Checked::new(BufWriter::new(&file));
    out.put(&MAGIC)?;
    out.put(&FORMAT.to_be_bytes())?;
    out.put(&[header.store])?;
    out.put(&header.covered_through.to_be_bytes())?;
    out.put(&(entries.len() as u64).to_be_bytes())?;
    out.put(&header.log_bytes.to_be_bytes())?;
    out.put(&header.last_checksum.to_be_bytes())?;
    for (key, entry) in entries.iter() {
        out.put(&(key.len() as u32).to_be_bytes())?;
        out.put(key)?;
        out.put(&entry.offset.to_be_bytes())?;
        out.put(&entry.version.to_be_bytes())?;
        out.put(&entry.expires_at_millis.to_be_bytes())?;
        out.put(&entry.bytes.to_be_bytes())?;
    }
    let crc = out.digest.finalize();
    let mut inner = out.inner;
    inner.write_all(&crc.to_be_bytes())?;
    inner.flush()?;
    drop(inner);
    crate::io::sync_all(&file)
}

/// Rename the temporary file over the snapshot. The caller flushes the
/// directory after.
pub(crate) fn install(dir: &Path) -> std::io::Result<()> {
    std::fs::rename(temporary_path(dir), dir.join(FILE_NAME))
}

/// Remove the snapshot and any temporary one. Missing files are fine.
pub(crate) fn remove(dir: &Path) -> std::io::Result<()> {
    for path in [dir.join(FILE_NAME), temporary_path(dir)] {
        match std::fs::remove_file(&path) {
            Err(err) if err.kind() != std::io::ErrorKind::NotFound => return Err(err),
            _ => {}
        }
    }
    Ok(())
}

fn temporary_path(dir: &Path) -> PathBuf {
    dir.join(TEMPORARY_NAME)
}

/// Reads one snapshot, entry by entry, checking the checksum at the end.
pub(crate) struct Reader {
    input: Checked<BufReader<File>>,
    header: Header,
    entry_count: u64,
    read: u64,
    /// Bytes left before the trailer, which bounds what a damaged length
    /// field can make this allocate.
    remaining: u64,
}

impl Reader {
    /// Open `dir`'s snapshot. `Ok(None)` when there is none.
    pub(crate) fn open(dir: &Path) -> Result<Option<Self>, Rejected> {
        let file = match File::open(dir.join(FILE_NAME)) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(Rejected(format!("could not open it: {err}"))),
        };
        let len = file
            .metadata()
            .map_err(|err| Rejected(format!("could not stat it: {err}")))?
            .len();
        if len < HEADER_LEN + TRAILER_LEN {
            return Err(Rejected(format!("{len} bytes is shorter than a header")));
        }
        let mut input = Checked::new(BufReader::new(file));
        let mut header = [0u8; HEADER_LEN as usize];
        input.take(&mut header)?;
        if header[0..4] != MAGIC {
            return Err(Rejected("bad magic".into()));
        }
        let format = u16::from_be_bytes([header[4], header[5]]);
        if format != FORMAT {
            return Err(Rejected(format!("unknown format {format}")));
        }
        let be64 = |at: usize| u64::from_be_bytes(header[at..at + 8].try_into().expect("8"));
        let entry_count = be64(15);
        let remaining = len - HEADER_LEN - TRAILER_LEN;
        if entry_count > remaining / (4 + ENTRY_FIXED_LEN) {
            return Err(Rejected(format!(
                "{entry_count} entries cannot fit in {len} bytes"
            )));
        }
        Ok(Some(Self {
            input,
            header: Header {
                store: header[6],
                covered_through: be64(7),
                log_bytes: be64(23),
                last_checksum: u32::from_be_bytes(header[31..35].try_into().expect("4")),
            },
            entry_count,
            read: 0,
            remaining,
        }))
    }

    pub(crate) fn header(&self) -> Header {
        self.header
    }

    pub(crate) fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// The next entry, or `None` once all `entry_count` have been read.
    pub(crate) fn next_entry(&mut self) -> Result<Option<(Vec<u8>, IndexEntry)>, Rejected> {
        if self.read == self.entry_count {
            return Ok(None);
        }
        let mut len = [0u8; 4];
        self.consume(4)?;
        self.input.take(&mut len)?;
        let len = u64::from(u32::from_be_bytes(len));
        self.consume(len + ENTRY_FIXED_LEN)?;
        let mut key = vec![0u8; len as usize];
        self.input.take(&mut key)?;
        let mut fixed = [0u8; ENTRY_FIXED_LEN as usize];
        self.input.take(&mut fixed)?;
        let be64 = |at: usize| u64::from_be_bytes(fixed[at..at + 8].try_into().expect("8"));
        self.read += 1;
        Ok(Some((
            key,
            IndexEntry {
                offset: be64(0),
                version: be64(8),
                expires_at_millis: be64(16),
                bytes: u32::from_be_bytes(fixed[24..28].try_into().expect("4")),
            },
        )))
    }

    /// Check the trailer, once every entry has been read. Until this returns
    /// `Ok`, nothing read from the file may be used.
    pub(crate) fn finish(mut self) -> Result<(), Rejected> {
        if self.read != self.entry_count {
            return Err(Rejected("not every entry was read".into()));
        }
        if self.remaining != 0 {
            return Err(Rejected(format!(
                "{} bytes past the last entry",
                self.remaining
            )));
        }
        let computed = self.input.digest.finalize();
        let mut trailer = [0u8; 4];
        self.input
            .inner
            .read_exact(&mut trailer)
            .map_err(|err| Rejected(format!("could not read the checksum: {err}")))?;
        if u32::from_be_bytes(trailer) != computed {
            return Err(Rejected("checksum mismatch".into()));
        }
        Ok(())
    }

    fn consume(&mut self, len: u64) -> Result<(), Rejected> {
        self.remaining = self
            .remaining
            .checked_sub(len)
            .ok_or_else(|| Rejected("an entry runs past the end of the file".into()))?;
        Ok(())
    }
}

/// A reader or writer that checksums every byte through it.
struct Checked<T> {
    inner: T,
    digest: crc::Digest<'static, u32>,
}

impl<T> Checked<T> {
    fn new(inner: T) -> Self {
        Self {
            inner,
            digest: CRC32C.digest(),
        }
    }
}

impl<W: Write> Checked<W> {
    fn put(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.digest.update(bytes);
        self.inner.write_all(bytes)
    }
}

impl<R: Read> Checked<R> {
    fn take(&mut self, buf: &mut [u8]) -> Result<(), Rejected> {
        self.inner
            .read_exact(buf)
            .map_err(|err| Rejected(format!("could not read it: {err}")))?;
        self.digest.update(buf);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
