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
//!
//! Besides the unit tests, a debug broker can run under it for the cluster
//! harness's whole-cluster power loss: `trigger` arms it from the environment
//! and turns the power off when the fault file says so.

pub(crate) mod trigger;

use felix_common::random::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs::File;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use super::SyncKind;
use super::writeback::crash_contents;
pub(crate) use super::writeback::{PAGE, SplitMix64, Writeback};

/// Files up to this size are re-read whole at every flush, since they may be
/// rewritten in place. Larger ones are taken to only grow (segments), so a
/// flush reads from the page holding the previously captured length on.
const WHOLE_FILE_UP_TO: u64 = 64 * 1024;

/// Live observers. A flush consults this only while one is installed.
static OBSERVERS: Mutex<Vec<Weak<PowerLoss>>> = Mutex::new(Vec::new());

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
    /// Parks the next file capture between its read and its record.
    #[cfg(test)]
    park_next_capture: Mutex<Option<ParkedCapture>>,
}

#[cfg(test)]
struct ParkedCapture {
    parked: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

/// What the device holds, as of the last flush of each thing.
#[derive(Default)]
struct Durable {
    files: HashMap<Inode, Vec<u8>>,
    dirs: HashMap<PathBuf, BTreeMap<OsString, Entry>>,
    /// The ticket of the capture each entry above was read by. Two flushes of
    /// one file can overlap, and the one that started reading later saw more:
    /// its read must not be replaced by the other's.
    file_tickets: HashMap<Inode, u64>,
    dir_tickets: HashMap<PathBuf, u64>,
    next_ticket: u64,
    counts: SyncCounts,
}

impl Durable {
    /// A ticket for a capture about to read. Taken before the read, so a
    /// higher ticket's read saw everything a lower one's could have.
    fn ticket(&mut self) -> u64 {
        self.next_ticket += 1;
        self.next_ticket
    }
}

/// One file, for as long as it exists. The inode number alone is not enough:
/// a filesystem hands a freed number to the next file it creates, and the
/// model still needs the old file's bytes for a name whose unlink was not
/// flushed. The birth time tells the two apart wherever the filesystem
/// records one (ext4, xfs, btrfs), but it comes from a coarse clock, so an
/// unlink and create within a few milliseconds can share it. The inode
/// generation, which ext4 and others bump on reuse, covers that case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Inode {
    dev: u64,
    ino: u64,
    born: Option<std::time::SystemTime>,
    /// `None` where the filesystem does not answer `FS_IOC_GETVERSION`.
    generation: Option<u32>,
}

impl Inode {
    fn of(meta: &std::fs::Metadata, file: &File) -> Self {
        Self {
            dev: meta.dev(),
            ino: meta.ino(),
            born: meta.created().ok(),
            generation: generation(file),
        }
    }

    /// For a directory listing: the key of the file or directory at `path`,
    /// built from one descriptor so the stat and the generation describe the
    /// same file. `None` for anything else or if it cannot be opened.
    fn at(path: &Path) -> Option<(Self, bool)> {
        // O_NONBLOCK so a racing swap to a fifo cannot stall the open.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW)
            .open(path)
            .ok()?;
        let meta = file.metadata().ok()?;
        (meta.is_dir() || meta.is_file()).then(|| (Self::of(&meta, &file), meta.is_dir()))
    }
}

/// `FS_IOC_GETVERSION`, i.e. `_IOR('v', 1, long)`. The kernel writes an `int`.
const FS_IOC_GETVERSION: u64 = 0x8008_7601;

fn generation(file: &File) -> Option<u32> {
    let mut value: libc::c_int = 0;
    // SAFETY: the request writes one `int` through the pointer.
    let rc = unsafe {
        libc::ioctl(
            file.as_raw_fd(),
            FS_IOC_GETVERSION as _,
            &mut value as *mut libc::c_int,
        )
    };
    (rc == 0).then_some(value as u32)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
            #[cfg(test)]
            park_next_capture: Mutex::new(None),
        });
        let mut observers = OBSERVERS.lock();
        observers.retain(|observer| observer.strong_count() > 0);
        observers.push(Arc::downgrade(&this));
        Ok(this)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn counts(&self) -> SyncCounts {
        self.state.lock().counts
    }

    /// Build the tree a reboot after a power loss *now* would find, into
    /// `into` (which must exist and be empty).
    #[cfg_attr(not(test), allow(dead_code))]
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

    /// Build the post-reboot tree into `into`, then kill this process without
    /// releasing the device state. A flush that has not been captured yet
    /// blocks on that lock, so nothing acknowledged after the image was taken
    /// can be missing from it.
    pub(crate) fn power_off(&self, seed: u64, into: &Path) -> ! {
        let writeback = if seed & 1 == 0 {
            Writeback::AnySubset
        } else {
            Writeback::InOrder
        };
        // Built beside `into` and renamed, so `into` existing means complete.
        let staging = into.with_extension("partial");
        let _ = std::fs::remove_dir_all(&staging);
        let durable = self.state.lock();
        tracing::warn!(seed, ?writeback, into = %into.display(), "simulated power loss");
        let built = std::fs::create_dir_all(&staging)
            .and_then(|()| {
                crash_dir(
                    &self.root,
                    &staging,
                    &durable,
                    &mut SplitMix64::new(seed),
                    writeback,
                )
            })
            .and_then(|()| std::fs::rename(&staging, into));
        if let Err(err) = built {
            tracing::error!(%err, "could not build the power-loss image");
        }
        // SAFETY: signalling this process takes no pointers.
        unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
        std::process::abort()
    }

    fn owns(&self, path: &Path) -> bool {
        path.starts_with(&self.root)
    }

    fn capture_file(&self, file: &File, kind: SyncKind) {
        let Ok(meta) = file.metadata() else { return };
        let inode = Inode::of(&meta, file);
        // Through the descriptor rather than the path, so a file renamed or
        // unlinked since it was opened is still the one captured.
        let path = format!("/proc/self/fd/{}", file.as_raw_fd());
        let (ticket, captured) = {
            let mut state = self.state.lock();
            (state.ticket(), state.files.get(&inode).map(Vec::len))
        };
        // Only a file that has not shrunk is read from where the last capture
        // ended. The segment writer truncates only to rewind a failed append,
        // which the harness heals with a restart and a fresh observer.
        let from = match captured {
            Some(len) if meta.len() > WHOLE_FILE_UP_TO && meta.len() >= len as u64 => {
                len / PAGE * PAGE
            }
            _ => 0,
        };
        let Ok(tail) = read_from(&path, from) else {
            return;
        };
        #[cfg(test)]
        self.park_if_asked();
        let mut state = self.state.lock();
        match kind {
            SyncKind::Data => state.counts.data += 1,
            SyncKind::All => state.counts.all += 1,
            SyncKind::Uring => state.counts.uring += 1,
        }
        if state
            .file_tickets
            .get(&inode)
            .is_some_and(|newer| *newer > ticket)
        {
            return;
        }
        state.file_tickets.insert(inode, ticket);
        let spliced = match state.files.get_mut(&inode) {
            Some(bytes) if from > 0 && bytes.len() >= from => {
                bytes.truncate(from);
                bytes.extend_from_slice(&tail);
                true
            }
            _ => false,
        };
        if !spliced {
            // A concurrent capture left less than the tail starts from.
            let whole = if from == 0 {
                tail
            } else {
                let Ok(whole) = std::fs::read(&path) else {
                    return;
                };
                whole
            };
            state.files.insert(inode, whole);
        }
    }

    fn capture_dir(&self, dir: &Path) {
        let ticket = self.state.lock().ticket();
        let Ok(listing) = list(dir) else { return };
        let mut state = self.state.lock();
        state.counts.dir += 1;
        if state
            .dir_tickets
            .get(dir)
            .is_some_and(|newer| *newer > ticket)
        {
            return;
        }
        state.dir_tickets.insert(dir.to_path_buf(), ticket);
        state.dirs.insert(dir.to_path_buf(), listing);
    }

    /// Park the next file capture after it has read the file and before it
    /// records what it read, until the returned sender sends (or is dropped).
    /// The receiver hears when it has parked.
    #[cfg(test)]
    pub(crate) fn park_next_capture(
        &self,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (parked, on_parked) = std::sync::mpsc::channel();
        let (release, on_release) = std::sync::mpsc::channel();
        *self.park_next_capture.lock() = Some(ParkedCapture {
            parked,
            release: on_release,
        });
        (on_parked, release)
    }

    #[cfg(test)]
    fn park_if_asked(&self) {
        let Some(park) = self.park_next_capture.lock().take() else {
            return;
        };
        let _ = park.parked.send(());
        // Bounded, so a test that never releases fails rather than hangs.
        let _ = park
            .release
            .recv_timeout(std::time::Duration::from_secs(10));
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

/// The bytes of the file at `path` from `offset` to its end.
fn read_from(path: &str, offset: usize) -> std::io::Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset as u64))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
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
        // Typed before opening, so devices and fifos are never opened.
        let Ok(meta) = std::fs::symlink_metadata(entry.path()) else {
            continue;
        };
        if !(meta.is_dir() || meta.is_file()) {
            continue;
        }
        let Some((inode, is_dir)) = Inode::at(&entry.path()) else {
            continue;
        };
        listing.insert(
            entry.file_name(),
            if is_dir {
                Entry::Dir(inode)
            } else {
                Entry::File(inode)
            },
        );
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

    // A rename is one change, so its two names come back together. One seen
    // within a directory is a name that now holds what another name held at
    // the last flush; a chain of renames shows up as its two ends.
    let left: HashMap<Entry, &OsString> = synced
        .iter()
        .filter(|(name, entry)| current.get(*name) != Some(*entry))
        .map(|(name, entry)| (*entry, name))
        .collect();
    let mut renamed: BTreeMap<&OsString, bool> = BTreeMap::new();
    for (to, now) in &current {
        if synced.get(to) == Some(now) {
            continue;
        }
        let Some(from) = left.get(now).copied() else {
            continue;
        };
        if from == to || renamed.contains_key(to) || renamed.contains_key(from) {
            continue;
        }
        let applied = rng.coin();
        renamed.insert(to, applied);
        renamed.insert(from, applied);
    }

    for name in names {
        let chosen = match renamed.get(name) {
            Some(true) => current.get(name).copied(),
            Some(false) => synced.get(name).copied(),
            None => match (current.get(name), synced.get(name)) {
                (Some(now), Some(then)) if now == then => Some(*now),
                (Some(now), Some(then)) => Some(if rng.coin() { *now } else { *then }),
                (Some(now), None) => rng.coin().then_some(*now),
                (None, Some(then)) => rng.coin().then_some(*then),
                (None, None) => None,
            },
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

#[cfg(test)]
mod tests;
