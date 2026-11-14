//! What the copies on a data directory hold of what their seat acknowledged,
//! kept across restarts and apart from any copy's ballot.
//!
//! A start that rotated the storage id may have lost acknowledged records on
//! any copy, and a later clean restart cannot tell. So [`super::begin`]
//! records in `<root>/copy-trust`, before it rotates, that no copy is
//! trusted. From then on a copy is trusted only once [`CopyTrust::vouch`]
//! names it, which the broker calls when a leader ships the copy level
//! through what that leader acknowledged. The next rotation distrusts every
//! copy again.
//!
//! The file also names the copies this broker led before such a crash
//! ([`CopyTrust::doubt`]) until a newer leader ships to them: the copy's
//! ballot says so only until it takes a newer leader's, and a clean restart
//! has nothing else. And it names the copies that took a ballot while their
//! promises counted for nothing ([`CopyTrust::unkeep`]) until they are
//! readmitted, since that ballot looks like any other after a restart.
//!
//! A file that does not decode marks every copy all three ways. A missing
//! one on a directory that ran before marks every copy untrusted and unkept
//! ([`ensure`]). Each copy then clears by the usual rules. See
//! `docs/storage-format.md`, "`copy-trust`".

use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use crate::storage_identity::{self, StorageId};
use crate::{Result, StorageError};
use felix_common::random::{HashMap, HashSet};

/// The file's name under the durable root. Not a directory, so shard
/// discovery passes over it.
pub const FILE_NAME: &str = "copy-trust";

/// Says a release that keeps [`FILE_NAME`] ran on the directory: written,
/// synced, before the record is first created, and never removed. Without it
/// a missing record is an older release's directory; with it, a lost one.
/// Its own file, since the run state and the storage id decode strictly and
/// an older build would take a changed one for a crash.
pub const KEPT_FILE_NAME: &str = "copy-trust-kept";

const HEADER: &str = "copy_trust\n";
const UNTRUSTED: &str = "untrusted";
const DOUBTED: &str = "doubted";
const UNKEPT: &str = "unkept";
const STORAGE_ID: &str = "storage_id";
const UNKEPT_SINCE: &str = "unkept_since";
const READMITTED: &str = "readmitted";

/// Which copies carry one mark: every copy but `except`, or only `except`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Marks {
    all: bool,
    except: HashSet<Vec<u8>>,
}

impl Marks {
    fn everyone() -> Self {
        Self {
            all: true,
            except: HashSet::default(),
        }
    }

    fn marked(&self, key: &[u8]) -> bool {
        self.all != self.except.contains(key)
    }

    /// Whether this changed anything.
    fn set(&mut self, key: &[u8], marked: bool) -> bool {
        if self.marked(key) == marked {
            return false;
        }
        if !self.except.remove(key) {
            self.except.insert(key.to_vec());
        }
        true
    }
}

/// What the file records.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct State {
    untrusted: Marks,
    doubted: Marks,
    unkept: Marks,
    /// For an unkept copy, the newest generation it took a ballot at while
    /// unkept: only a set newer than that readmits it. 0 when not known.
    unkept_since: HashMap<Vec<u8>, u64>,
    /// For a copy on a directory that lost its ballots, the set generation
    /// that readmitted it and the storage id it was readmitted under.
    readmitted: HashMap<Vec<u8>, (u64, StorageId)>,
}

impl State {
    /// What a file that does not decode stands for: nothing is known, so
    /// every mark is on.
    fn unknown() -> Self {
        Self {
            untrusted: Marks::everyone(),
            doubted: Marks::everyone(),
            unkept: Marks::everyone(),
            unkept_since: HashMap::default(),
            readmitted: HashMap::default(),
        }
    }
}

/// Make sure `root` has the file before anything reads it: a fresh one on a
/// new directory (`created`), and on a directory that ran before without it
/// (an older release's, or one whose file was removed) one that marks every
/// copy untrusted. Not doubted: nothing says which copy this broker led
/// before a crash, doubting every one would refuse every older caller's
/// fence on an idle shard, and the ballots still say what they said before
/// the file existed. Not unkept: an older release never gave a learner a
/// ballot, so every ballot it left is one its refusals allowed.
///
/// A file written under another storage id than the directory's now reads as
/// unknown, and is rewritten so under the current id, so its marks stay on
/// until each copy clears them: an older release may have rotated the id
/// after a crash without touching this file.
///
/// A missing record where [`KEPT_FILE_NAME`] says one was kept reads as
/// unknown too: it was lost, and its marks with it.
pub(crate) fn ensure(root: &Path, created: bool) -> Result<()> {
    let id = storage_identity::load(root)?;
    let exists = |name| crate::fs::try_exists(root.join(name)).map_err(StorageError::Io);
    let state = if exists(FILE_NAME)? {
        read(root)?
    } else if exists(KEPT_FILE_NAME)? {
        tracing::error!(
            root = %root.display(),
            "this data directory's copy-trust record is missing though one was kept here; \
             taking every copy as untrusted, doubted and unkept until a leader vouches for it"
        );
        State::unknown()
    } else if created {
        State::default()
    } else {
        tracing::warn!(
            root = %root.display(),
            "this data directory has no copy-trust record, as one an older release ran on \
             has not; taking every copy as untrusted until a leader vouches for it"
        );
        State {
            untrusted: Marks::everyone(),
            ..State::default()
        }
    };
    if !exists(KEPT_FILE_NAME)? {
        super::write_atomically(root, KEPT_FILE_NAME, b"copy_trust\n", true)?;
    }
    super::write_atomically(root, FILE_NAME, &encode(&state, id), true)
}

/// Record, durably, that every copy on `root` may have lost acknowledged
/// records: no copy is trusted until [`CopyTrust::vouch`] names it again.
/// Doubted and unkept copies stay so. Written under `next`, the id the
/// directory is about to take: a crash before it does leaves a file that
/// names another id, which reads as unknown.
pub(crate) fn distrust_all(root: &Path, next: StorageId) -> Result<()> {
    let mut state = read(root)?;
    state.untrusted = Marks::everyone();
    super::write_atomically(root, FILE_NAME, &encode(&state, Some(next)), true)
}

/// The file under `root`. Missing reads as a new directory's: [`ensure`],
/// at the start of every run, decides otherwise.
///
/// A file that names another storage id than the directory's, or none on a
/// directory that has one, reads as unknown too.
fn read(root: &Path) -> Result<State> {
    let path = root.join(FILE_NAME);
    match crate::fs::read(&path) {
        Ok(bytes) => {
            let current = storage_identity::load(root)?;
            Ok(match decode(&bytes) {
                Some((state, written)) if written == current => state,
                Some(_) => {
                    tracing::error!(
                        path = %path.display(),
                        "the copy-trust record was written under another storage id, as \
                         after a downgrade that rotated it; taking every copy as untrusted, \
                         doubted and unkept until a leader vouches for it"
                    );
                    State::unknown()
                }
                None => {
                    tracing::error!(
                        path = %path.display(),
                        "the copy-trust record does not decode; taking every copy as \
                         untrusted, doubted and unkept until a leader vouches for it"
                    );
                    State::unknown()
                }
            })
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(State::default()),
        Err(err) => Err(StorageError::Io(err)),
    }
}

/// The copies on one data directory, as [`FILE_NAME`] records them. A copy
/// is named by bytes the caller chooses.
///
/// Changes are group committed: changes made while a write is in flight
/// share the next write and its sync.
#[derive(Debug)]
pub struct CopyTrust {
    root: PathBuf,
    /// `root` resolved, which a rotation matches by.
    canonical: PathBuf,
    /// The directory's storage id, which every write names. Changes only
    /// through [`rotate_storage_id`].
    id: Mutex<Option<StorageId>>,
    /// What is on disk. Readers see only this.
    durable: Mutex<State>,
    /// Every change made, with the count of changes so far.
    wanted: Mutex<(State, u64)>,
    /// Held while writing: the count of changes on disk.
    writer: Mutex<u64>,
    #[cfg(test)]
    writes: std::sync::atomic::AtomicUsize,
}

impl CopyTrust {
    /// Read the record under `root` for the run on it: a rotation of its
    /// storage id while the run goes on ([`rotate_storage_id`]) distrusts
    /// every copy here too.
    pub fn open_live(root: &Path) -> Result<std::sync::Arc<Self>> {
        let trust = std::sync::Arc::new(Self::open(root)?);
        let mut live = LIVE.lock();
        live.retain(|weak| weak.strong_count() > 0);
        live.push(std::sync::Arc::downgrade(&trust));
        Ok(trust)
    }

    /// Read the record under `root`.
    pub fn open(root: &Path) -> Result<Self> {
        let state = read(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            canonical: canonical(root),
            id: Mutex::new(storage_identity::load(root)?),
            durable: Mutex::new(state.clone()),
            wanted: Mutex::new((state, 0)),
            writer: Mutex::new(0),
            #[cfg(test)]
            writes: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Whether the copy named `key` holds everything its seat acknowledged.
    pub fn trusted(&self, key: &[u8]) -> bool {
        !self.durable.lock().untrusted.marked(key)
    }

    /// Whether the copy named `key` is one this broker led before a crash
    /// that may have cost it acknowledged records, and no newer leader has
    /// shipped to it since.
    pub fn doubted(&self, key: &[u8]) -> bool {
        self.durable.lock().doubted.marked(key)
    }

    /// Whether the copy named `key` took a ballot while its promises counted
    /// for nothing and has not been readmitted since, and since when.
    pub fn unkept(&self, key: &[u8]) -> Option<Since> {
        let state = self.durable.lock();
        state
            .unkept
            .marked(key)
            .then(|| match state.unkept_since.get(key) {
                Some(&generation) => Since::At(generation),
                None => Since::Unknown,
            })
    }

    /// The set generation that readmitted the copy named `key` after its
    /// directory lost its ballots ([`Self::readmit`]), when that was under
    /// the directory's storage id now. A rotation since, a restore among
    /// them, ends it.
    pub fn readmitted(&self, key: &[u8]) -> Option<u64> {
        let id = (*self.id.lock())?;
        self.durable
            .lock()
            .readmitted
            .get(key)
            .filter(|(_, under)| *under == id)
            .map(|(generation, _)| *generation)
    }

    /// Record, durably, that the copy named `key`, on a directory that lost
    /// its ballots, was readmitted by a set introduced at `generation` that
    /// names it under `id`: its promises count for that set and later ones.
    /// The first readmission under an id stands, and a later one changes
    /// nothing. The copy is untrusted until
    /// a leader vouches for it again, so a vouch from before, which any
    /// leader could give, does not count. Blocks on the sync.
    pub fn readmit(&self, key: &[u8], generation: u64, id: StorageId) -> Result<()> {
        self.update(|state| {
            if state
                .readmitted
                .get(key)
                .is_some_and(|(_, under)| *under == id)
            {
                return false;
            }
            state.untrusted.set(key, true);
            state.readmitted.insert(key.to_vec(), (generation, id));
            true
        })
    }

    /// Record, durably, that a newer leader shipped the copy named `key`
    /// level through its generation's start, which ends any doubt, and, when
    /// `trusts`, through the mark it acknowledged too, which trusts it. The
    /// readers say so only once it is synced. Blocks on the sync.
    pub fn vouch(&self, key: &[u8], trusts: bool) -> Result<()> {
        self.update(|state| {
            let cleared = state.doubted.set(key, false);
            (trusts && state.untrusted.set(key, false)) || cleared
        })
    }

    /// Record, durably, that the copy named `key` is one this broker led
    /// before a crash that may have cost it acknowledged records. Call before
    /// the copy takes a newer leader's ballot, and take none on an error.
    /// Blocks on the sync.
    pub fn doubt(&self, key: &[u8]) -> Result<()> {
        self.update(|state| state.doubted.set(key, true))
    }

    /// Record, durably, that the copy named `key` is about to take a ballot
    /// while its promises count for nothing, as a learner does. That ballot
    /// is no kept promise, even after a crash that keeps it. Call before the
    /// ballot is written, and take none on an error. `generation` is the
    /// ballot's. Blocks on the sync.
    pub fn unkeep(&self, key: &[u8], generation: u64) -> Result<()> {
        self.update(|state| {
            let marked = state.unkept.set(key, true);
            let known = state.unkept_since.contains_key(key);
            let since = state.unkept_since.entry(key.to_vec()).or_insert(0);
            let raised = generation > *since;
            *since = (*since).max(generation);
            marked || raised || !known
        })
    }

    /// Record, durably, that the copy named `key` was readmitted, into a set
    /// newer than [`Self::unkept`] says: its promises count again. Blocks on
    /// the sync.
    pub fn clear_unkept(&self, key: &[u8]) -> Result<()> {
        self.update(|state| {
            let removed = state.unkept_since.remove(key).is_some();
            state.unkept.set(key, false) || removed
        })
    }

    /// Apply `change`, then return once it is on disk, written by this call
    /// or by one that wrote after it was made. A call that changes nothing
    /// still waits for earlier changes, which may have made its change.
    fn update(&self, change: impl FnOnce(&mut State) -> bool) -> Result<()> {
        let mine = {
            let mut wanted = self.wanted.lock();
            if change(&mut wanted.0) {
                wanted.1 += 1;
            }
            wanted.1
        };
        let mut written = self.writer.lock();
        if *written >= mine {
            return Ok(());
        }
        let (state, count) = self.wanted.lock().clone();
        let id = *self.id.lock();
        super::write_atomically(&self.root, FILE_NAME, &encode(&state, id), true)?;
        #[cfg(test)]
        self.writes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        *self.durable.lock() = state;
        *written = count;
        Ok(())
    }
}

/// Since when a copy is unkept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Since {
    /// It took an unkept ballot at this generation, the newest such.
    At(u64),
    /// The record does not say: it did not decode, or named another id.
    Unknown,
}

/// The records open for a running broker, so a rotation reaches them.
static LIVE: Mutex<Vec<std::sync::Weak<CopyTrust>>> = Mutex::new(Vec::new());

/// Give the directory at `root` the storage id `fresh`, the one way every
/// rotation takes: first every copy is distrusted, synced, under the new id
/// (in the record a running broker holds too, so its readers see it at once),
/// then the id is written. A crash between the two leaves a record naming
/// another id than the directory's, which reads as unknown.
///
/// The root's Raft mark log (`raft-marks`) is removed first. Every shard
/// directory minted under the old id rotates when it opens, so its marks
/// protect nothing any more, and a log kept under the new id would hold a
/// root sequence nothing outside the root has a copy of: the Raft restore
/// gate would hold the root after a reboot. A crash after the removal leaves
/// the old id with no marks, which rotates every directory too. Nothing may
/// hold the mark log open: the gate's own rotation reopens it after.
pub fn rotate_storage_id(root: &Path, fresh: [u8; 16]) -> Result<StorageId> {
    forget_raft_marks(root)?;
    let next = StorageId::from_bytes(fresh);
    let canonical = canonical(root);
    let live = LIVE
        .lock()
        .iter()
        .filter_map(std::sync::Weak::upgrade)
        .find(|trust| trust.canonical == canonical);
    match live {
        Some(trust) => trust.distrust_all(next)?,
        None => distrust_all(root, next)?,
    }
    storage_identity::replace(root, fresh)
}

/// The root's per-shard Raft mark log; see `raft_copy`.
pub(crate) const RAFT_MARKS: &str = "raft-marks";

/// Remove the root's Raft mark log, durably, if it has one.
pub(crate) fn forget_raft_marks(root: &Path) -> Result<()> {
    match crate::fs::remove_file(root.join(RAFT_MARKS)) {
        Ok(()) => crate::io::sync_dir(root).map_err(StorageError::from),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

impl CopyTrust {
    /// [`distrust_all`] on the live record, under the writer's lock so no
    /// write in flight puts an older trust back.
    fn distrust_all(&self, next: StorageId) -> Result<()> {
        let mut written = self.writer.lock();
        let (state, count) = {
            let mut wanted = self.wanted.lock();
            wanted.0.untrusted = Marks::everyone();
            wanted.1 += 1;
            wanted.clone()
        };
        super::write_atomically(&self.root, FILE_NAME, &encode(&state, Some(next)), true)?;
        *self.id.lock() = Some(next);
        *self.durable.lock() = state;
        *written = count;
        Ok(())
    }
}

/// The header, `storage_id <hex>` naming the directory's id, then for each
/// mark either `<mark> all` and one
/// `<mark> except <key>` line per copy without it, or one `<mark> <key>`
/// line per copy with it. Keys are lowercase hex, sorted.
fn encode(state: &State, id: Option<StorageId>) -> Vec<u8> {
    use std::fmt::Write;

    let mut out = String::from(HEADER);
    if let Some(id) = id {
        let _ = writeln!(out, "{STORAGE_ID} {}", id.to_hex());
    }
    for (name, marks) in [
        (UNTRUSTED, &state.untrusted),
        (DOUBTED, &state.doubted),
        (UNKEPT, &state.unkept),
    ] {
        if marks.all {
            let _ = writeln!(out, "{name} all");
        }
        let mut keys: Vec<&Vec<u8>> = marks.except.iter().collect();
        keys.sort_unstable();
        for key in keys {
            out.push_str(name);
            out.push_str(if marks.all { " except " } else { " " });
            for byte in key {
                let _ = write!(out, "{byte:02x}");
            }
            out.push('\n');
        }
    }
    let mut since: Vec<_> = state.unkept_since.iter().collect();
    since.sort_unstable();
    for (key, generation) in since {
        out.push_str(UNKEPT_SINCE);
        out.push(' ');
        for byte in key {
            let _ = write!(out, "{byte:02x}");
        }
        let _ = writeln!(out, " {generation}");
    }
    let mut readmitted: Vec<_> = state.readmitted.iter().collect();
    readmitted.sort_unstable_by(|a, b| a.0.cmp(b.0));
    for (key, (generation, id)) in readmitted {
        out.push_str(READMITTED);
        out.push(' ');
        for byte in key {
            let _ = write!(out, "{byte:02x}");
        }
        let _ = writeln!(out, " {generation} {}", id.to_hex());
    }
    out.into_bytes()
}

fn decode(bytes: &[u8]) -> Option<(State, Option<StorageId>)> {
    let body = std::str::from_utf8(bytes).ok()?.strip_prefix(HEADER)?;
    let mut state = State::default();
    let mut id = None;
    let mut named = Vec::new();
    for line in body.lines() {
        let (name, rest) = line.split_once(' ')?;
        if name == STORAGE_ID && id.is_none() {
            id = Some(StorageId::from_bytes(unhex(rest)?.try_into().ok()?));
            continue;
        }
        if name == UNKEPT_SINCE {
            let (hex, generation) = rest.split_once(' ')?;
            state
                .unkept_since
                .insert(unhex(hex)?, generation.parse().ok()?);
            continue;
        }
        if name == READMITTED {
            let mut parts = rest.split(' ');
            let key = unhex(parts.next()?)?;
            let generation = parts.next()?.parse().ok()?;
            let id = StorageId::from_bytes(unhex(parts.next()?)?.try_into().ok()?);
            if parts.next().is_some() {
                return None;
            }
            state.readmitted.insert(key, (generation, id));
            continue;
        }
        let marks = marks_of(&mut state, name)?;
        if rest == "all" {
            marks.all = true;
        } else if let Some(hex) = rest.strip_prefix("except ") {
            named.push((name, true, unhex(hex)?));
        } else {
            named.push((name, false, unhex(rest)?));
        }
    }
    for (name, except, key) in named {
        let marks = marks_of(&mut state, name)?;
        // An `except` line only goes with `all`, a plain one only without.
        if except != marks.all {
            return None;
        }
        marks.except.insert(key);
    }
    Some((state, id))
}

fn marks_of<'a>(state: &'a mut State, name: &str) -> Option<&'a mut Marks> {
    match name {
        UNTRUSTED => Some(&mut state.untrusted),
        DOUBTED => Some(&mut state.doubted),
        UNKEPT => Some(&mut state.unkept),
        _ => None,
    }
}

/// `root` with links and relative parts resolved, or as given when it
/// cannot be.
fn canonical(root: &Path) -> PathBuf {
    std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf())
}

fn unhex(hex: &str) -> Option<Vec<u8>> {
    if hex.is_empty() || !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(hex.get(at..at + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests;
