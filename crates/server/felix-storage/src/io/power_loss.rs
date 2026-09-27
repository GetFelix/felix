//! A simulated power loss, for recovery tests.
//!
//! A process crash keeps the page cache; a power loss does not. Every byte
//! written since the last flush that covered it may or may not be on the
//! device afterwards, in whole pages or torn within one, and the file size can
//! reach disk without the data it covers (which then reads back as zeros).
//! Directory entries follow the same rule: a create, rename or unlink is only
//! certain once its directory has been flushed.
//!
//! The model keeps the real files as the page cache and remembers, per inode,
//! what the device holds: the bytes each flush covered, captured when the flush
//! is issued through the `io` seam (`sync_data`, `sync_all`, the `io_uring`
//! submission, `sync_dir`). A crash then builds the post-reboot tree in another
//! directory, choosing with a seeded generator which unsynced pages made it,
//! which were torn, and which directory changes stuck. Recovery runs against
//! that copy while the original log keeps going, so one workload yields many
//! crashes.
//!
//! Capturing at the start of a flush is the adversarial choice: writes racing
//! the flush may be lost, and everything written before it may not be.
//!
//! Linux only: the capture reopens a descriptor through `/proc/self/fd`, which
//! is what lets it read a segment the log holds write-only.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsString;
use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use super::SyncKind;

/// The unit a page cache writes back.
pub(crate) const PAGE: usize = 4096;
/// The unit a device writes atomically; a torn page is torn on these.
pub(crate) const SECTOR: usize = 512;

/// Live observers. A flush consults this only while one is installed.
static OBSERVERS: Mutex<Vec<Weak<PowerLoss>>> = Mutex::new(Vec::new());

/// How unsynced pages reach the device before the power goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Writeback {
    /// Any subset of unsynced pages, in any order, each whole, torn, or not at
    /// all. What the kernel is allowed to do.
    AnySubset,
    /// A prefix of each file's unsynced pages, the last one possibly torn.
    /// What an append-only writer usually sees in practice.
    InOrder,
}

/// Counts of the flushes the observer saw, so a test can prove the path it
/// meant to cover was the one taken.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SyncCounts {
    pub(crate) data: u64,
    pub(crate) all: u64,
    pub(crate) uring: u64,
    pub(crate) dir: u64,
}

/// One tree under simulated power loss.
pub(crate) struct PowerLoss {
    root: PathBuf,
    state: Mutex<Durable>,
}

/// What the device holds, as of the last flush of each thing.
#[derive(Default)]
struct Durable {
    files: HashMap<Inode, Vec<u8>>,
    dirs: HashMap<PathBuf, BTreeMap<OsString, Entry>>,
    counts: SyncCounts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Inode {
    dev: u64,
    ino: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Entry {
    File(Inode),
    Dir(Inode),
}

impl PowerLoss {
    /// Watch `root`, taking everything already under it as durable.
    pub(crate) fn install(root: &Path) -> std::io::Result<Arc<Self>> {
        let root = root.canonicalize()?;
        let mut durable = Durable::default();
        snapshot_tree(&root, &mut durable)?;
        let this = Arc::new(Self {
            root,
            state: Mutex::new(durable),
        });
        let mut observers = OBSERVERS.lock();
        observers.retain(|observer| observer.strong_count() > 0);
        observers.push(Arc::downgrade(&this));
        Ok(this)
    }

    pub(crate) fn counts(&self) -> SyncCounts {
        self.state.lock().counts
    }

    /// Build the tree a reboot after a power loss *now* would find, into
    /// `into` (which must exist and be empty).
    pub(crate) fn crash(
        &self,
        seed: u64,
        writeback: Writeback,
        into: &Path,
    ) -> std::io::Result<()> {
        let mut rng = SplitMix64::new(seed);
        // Held for the whole build: a flush captured halfway would mix two
        // moments of the device.
        let durable = self.state.lock();
        crash_dir(&self.root, into, &durable, &mut rng, writeback)
    }

    fn owns(&self, path: &Path) -> bool {
        path.starts_with(&self.root)
    }

    fn capture_file(&self, file: &File, kind: SyncKind) {
        let Ok(meta) = file.metadata() else { return };
        let inode = Inode {
            dev: meta.dev(),
            ino: meta.ino(),
        };
        // Through the descriptor rather than the path, so a file renamed or
        // unlinked since it was opened is still the one captured.
        let Ok(bytes) = std::fs::read(format!("/proc/self/fd/{}", file.as_raw_fd())) else {
            return;
        };
        let mut state = self.state.lock();
        state.files.insert(inode, bytes);
        match kind {
            SyncKind::Data => state.counts.data += 1,
            SyncKind::All => state.counts.all += 1,
            SyncKind::Uring => state.counts.uring += 1,
        }
    }

    fn capture_dir(&self, dir: &Path) {
        let Ok(listing) = list(dir) else { return };
        let mut state = self.state.lock();
        state.dirs.insert(dir.to_path_buf(), listing);
        state.counts.dir += 1;
    }
}

impl Drop for PowerLoss {
    fn drop(&mut self) {
        OBSERVERS
            .lock()
            .retain(|observer| observer.strong_count() > 0);
    }
}

/// Called by `io` before a file flush is issued.
pub(crate) fn observe_file(file: &File, kind: SyncKind) {
    let observers = live_observers();
    if observers.is_empty() {
        return;
    }
    let Ok(path) = std::fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd())) else {
        return;
    };
    for observer in observers.iter().filter(|o| o.owns(&path)) {
        observer.capture_file(file, kind);
    }
}

/// Called by `io` before a directory flush is issued.
pub(crate) fn observe_dir(path: &Path) {
    let observers = live_observers();
    if observers.is_empty() {
        return;
    }
    let Ok(path) = path.canonicalize() else {
        return;
    };
    for observer in observers.iter().filter(|o| o.owns(&path)) {
        observer.capture_dir(&path);
    }
}

fn live_observers() -> Vec<Arc<PowerLoss>> {
    OBSERVERS.lock().iter().filter_map(Weak::upgrade).collect()
}

/// Everything under `root` as it stands, recorded as durable.
fn snapshot_tree(dir: &Path, durable: &mut Durable) -> std::io::Result<()> {
    let listing = list(dir)?;
    for (name, entry) in &listing {
        let path = dir.join(name);
        match entry {
            Entry::File(inode) => {
                durable.files.insert(*inode, std::fs::read(&path)?);
            }
            Entry::Dir(_) => snapshot_tree(&path, durable)?,
        }
    }
    durable.dirs.insert(dir.to_path_buf(), listing);
    Ok(())
}

fn list(dir: &Path) -> std::io::Result<BTreeMap<OsString, Entry>> {
    let mut listing = BTreeMap::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        // A file removed between the listing and the stat is simply not there.
        let Ok(meta) = std::fs::symlink_metadata(entry.path()) else {
            continue;
        };
        let inode = Inode {
            dev: meta.dev(),
            ino: meta.ino(),
        };
        if meta.is_dir() {
            listing.insert(entry.file_name(), Entry::Dir(inode));
        } else if meta.is_file() {
            listing.insert(entry.file_name(), Entry::File(inode));
        }
    }
    Ok(listing)
}

/// Rebuild one directory after the crash.
///
/// A name whose entry has not changed since the directory was last flushed is
/// kept. One created, removed or replaced since then comes back either way.
fn crash_dir(
    dir: &Path,
    into: &Path,
    durable: &Durable,
    rng: &mut SplitMix64,
    writeback: Writeback,
) -> std::io::Result<()> {
    // A directory that did not exist at the last flush of anything has no
    // durable entries: everything in it is new.
    let synced = durable.dirs.get(dir).cloned().unwrap_or_default();
    let current = list(dir).unwrap_or_default();
    let names: BTreeSet<&OsString> = synced.keys().chain(current.keys()).collect();

    for name in names {
        let chosen = match (current.get(name), synced.get(name)) {
            (Some(now), Some(then)) if now == then => Some(*now),
            (Some(now), Some(then)) => Some(if rng.coin() { *now } else { *then }),
            (Some(now), None) => rng.coin().then_some(*now),
            (None, Some(then)) => rng.coin().then_some(*then),
            (None, None) => None,
        };
        let from = dir.join(name);
        let to = into.join(name);
        match chosen {
            None => {}
            Some(Entry::Dir(_)) => {
                std::fs::create_dir(&to)?;
                // A resurrected directory has nothing current to walk; its
                // durable listing (if it was ever flushed) is keyed by its old
                // path, which is this one.
                crash_dir(&from, &to, durable, rng, writeback)?;
            }
            Some(Entry::File(inode)) => {
                let synced = durable.files.get(&inode).map(Vec::as_slice).unwrap_or(&[]);
                // Only the inode the name holds now has current contents; one
                // that was replaced or unlinked comes back as it was flushed.
                let now = match current.get(name) {
                    Some(Entry::File(live)) if *live == inode => std::fs::read(&from).ok(),
                    _ => None,
                };
                let bytes = match now {
                    Some(now) => crash_contents(synced, &now, rng, writeback),
                    None => synced.to_vec(),
                };
                std::fs::write(&to, bytes)?;
            }
        }
    }
    Ok(())
}

/// What a file reads back as after the crash, given what its last flush
/// covered and what the page cache held.
pub(crate) fn crash_contents(
    synced: &[u8],
    now: &[u8],
    rng: &mut SplitMix64,
    writeback: Writeback,
) -> Vec<u8> {
    // The size is metadata and travels on its own: it may have reached the
    // device ahead of the data it covers, or not at all.
    let len = if synced.len() == now.len() {
        now.len()
    } else {
        let (low, high) = (synced.len().min(now.len()), synced.len().max(now.len()));
        match rng.below(3) {
            0 => synced.len(),
            1 => now.len(),
            _ => (low + rng.below((high - low) as u64 + 1) as usize).min(high),
        }
    };

    let pages = len.div_ceil(PAGE);
    let dirty: Vec<usize> = (0..pages)
        .filter(|page| page_of(synced, *page) != page_of(now, *page))
        .collect();
    // Under in-order writeback every dirty page before the cut made it, the
    // cut page may be torn, and none after it did.
    let cut = rng.below(dirty.len() as u64 + 1) as usize;

    let mut out = vec![0u8; pages * PAGE];
    for page in 0..pages {
        let old = page_of(synced, page);
        let new = page_of(now, page);
        let target = &mut out[page * PAGE..(page + 1) * PAGE];
        if old == new {
            target.copy_from_slice(&new);
            continue;
        }
        let rank = dirty.iter().position(|p| *p == page).unwrap_or(0);
        let fate = match writeback {
            Writeback::AnySubset => match rng.below(3) {
                0 => Fate::Lost,
                1 => Fate::Written,
                _ => Fate::Torn,
            },
            Writeback::InOrder => match rank.cmp(&cut) {
                std::cmp::Ordering::Less => Fate::Written,
                std::cmp::Ordering::Equal => {
                    if rng.coin() {
                        Fate::Torn
                    } else {
                        Fate::Lost
                    }
                }
                std::cmp::Ordering::Greater => Fate::Lost,
            },
        };
        match fate {
            Fate::Lost => target.copy_from_slice(&old),
            Fate::Written => target.copy_from_slice(&new),
            Fate::Torn => {
                // The leading sectors of the page made it and the rest did not.
                let sectors = 1 + rng.below((PAGE / SECTOR - 1) as u64) as usize;
                let split = sectors * SECTOR;
                target[..split].copy_from_slice(&new[..split]);
                target[split..].copy_from_slice(&old[split..]);
            }
        }
    }
    out.truncate(len);
    out
}

enum Fate {
    Lost,
    Written,
    Torn,
}

/// One page of `bytes`, zero-padded past its end: an unwritten block inside
/// the file size reads as zeros.
fn page_of(bytes: &[u8], page: usize) -> [u8; PAGE] {
    let mut out = [0u8; PAGE];
    let start = page * PAGE;
    if start < bytes.len() {
        let end = bytes.len().min(start + PAGE);
        out[..end - start].copy_from_slice(&bytes[start..end]);
    }
    out
}

/// A small, seedable generator, so a failing crash can be replayed from the
/// seed in its message.
pub(crate) struct SplitMix64(u64);

impl SplitMix64 {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..bound`; zero when `bound` is zero.
    pub(crate) fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            0
        } else {
            self.next_u64() % bound
        }
    }

    pub(crate) fn coin(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }
}

#[cfg(test)]
mod tests;
