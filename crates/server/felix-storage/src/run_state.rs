//! Whether the last broker on a data directory stopped cleanly.
//!
//! `<durable root>/run-state` says either that a broker is running there, and
//! whether its fsync mode acknowledges records before they are synced, or that
//! the last one stopped after syncing everything it acknowledged. A crash
//! leaves the first. Under `FsyncMode::Periodic` or `None` a crash of the
//! machine can have lost acknowledged records while the storage id stayed the
//! same, so [`begin`] gives the directory a new id, and peers start their copy
//! of it over. A crash of the process alone loses nothing the kernel accepted,
//! so the record also names the boot it was written in, and a crash in the
//! boot still running keeps the id. See `docs/durable-storage.md`, "Storage
//! identity".
//!
//! `<durable root>/registration` keeps the node incarnation the control plane
//! last gave this broker. A directory restored from a snapshot or copied in
//! carries an older one than the control plane holds, which a crash cannot
//! show, so [`begin`] rotates the id for that too.

mod boot;

use std::path::{Path, PathBuf};

use parking_lot::Mutex;

pub use self::boot::{BootId, this_boot};
use crate::io::sync_dir;
use crate::storage_identity::{self, Origin, StorageId};
use crate::{Result, StorageError};

/// `"FLRN"`: Felix RuN.
const MAGIC: u32 = 0x464C_524E;
const VERSION: u16 = 1;
/// A running record that names its boot. Older builds do not decode it, which
/// they read as a relaxed crash, as they would read the record they wrote for
/// the same run. A clean stop is always written at [`VERSION`], so a
/// downgrade after one keeps the id.
const VERSION_BOOT: u16 = 2;
/// magic(4) + version(2) + state(1) + flags(1) + id(16) + crc(4) over
/// everything before it.
pub(crate) const ENCODED_LEN: usize = 28;
/// [`ENCODED_LEN`] with the boot(16) before the crc.
pub(crate) const ENCODED_LEN_BOOT: usize = 44;
/// The file's name under the durable root. Not a directory, so shard
/// discovery passes over it.
pub const FILE_NAME: &str = "run-state";

/// `"FLRG"`: Felix ReGistration.
const REGISTRATION_MAGIC: u32 = 0x464C_5247;
const REGISTRATION_VERSION: u16 = 1;
/// magic(4) + version(2) + incarnation(8) + crc(4) over everything before it.
pub(crate) const REGISTRATION_LEN: usize = 18;
/// The registration record's name under the durable root.
pub const REGISTRATION_FILE_NAME: &str = "registration";

const RUNNING: u8 = 1;
const STOPPED: u8 = 2;
/// The run acknowledges records before they are synced.
const RELAXED: u8 = 1;

/// Directories whose record [`forget_boot`] rewrites once any log in this
/// process is poisoned. See [`watch_poison`].
static WATCHED: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// What the record says about the run before this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LastRun {
    /// No record: a new directory, or one only older builds have run on.
    Unrecorded,
    /// It stopped after syncing everything it acknowledged.
    Clean,
    /// It did not stop cleanly. `relaxed` when its fsync mode acknowledged
    /// records before syncing them, so the crash may have lost some. `boot`
    /// is the boot it ran in, when it recorded one. A record that does not
    /// decode, or that names another storage id, reads as a relaxed crash in
    /// an unknown boot.
    Crashed { relaxed: bool, boot: Option<BootId> },
}

/// The node incarnation this directory last recorded, from
/// [`record_registration`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recorded {
    /// No record: a new directory, or one only older builds have run on.
    Unrecorded,
    /// The control plane registered this node at `incarnation` while this
    /// directory was in use.
    At(u64),
    /// A record that does not decode.
    Unreadable,
}

impl Recorded {
    /// Whether a control plane that has this node at `registered` has seen a
    /// registration this directory missed, so the directory is older than the
    /// node's last one. Only a control plane ahead counts: one behind was
    /// restored or lost the node, which says nothing about this directory.
    pub fn behind(self, registered: Option<u64>) -> bool {
        match (self, registered) {
            (_, None) | (Self::Unrecorded, _) => false,
            (Self::At(recorded), Some(registered)) => registered > recorded,
            (Self::Unreadable, Some(_)) => true,
        }
    }
}

/// The identity a run starts under, from [`begin`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Begun {
    pub id: StorageId,
    /// Where the id on disk came from, before any rotation.
    pub origin: Origin,
    pub last_run: LastRun,
    /// The node incarnation the directory last recorded.
    pub recorded: Recorded,
    /// Whether `id` is new because the last run may have lost records it
    /// acknowledged, or because the directory is older than the node's last
    /// registration.
    pub rotated: bool,
}

/// Start a run on the directory at `root`: read its storage id, give it a new
/// one when the last run may have lost records it acknowledged or the
/// directory is older than the node's last registration, then record that a
/// run under `relaxed` is under way in `boot`. Call before any store opens,
/// and announce only the id this returns. Any reason to rotate wins.
///
/// `registered` is the incarnation the control plane holds for this node, or
/// `None` when it was not asked or has never registered the node. Ahead of
/// what the directory recorded, the directory was restored or copied from
/// before that registration and may hold less than peers counted.
///
/// A relaxed crash keeps the id only when it was recorded in `boot` and `boot`
/// is known: the machine has not restarted since, so its page cache still
/// holds every write the crashed run made. An unknown boot on either side
/// rotates.
///
/// `repair_pending` is asked only when there is no record but an id was
/// loaded, which is the first start after an upgrade: then a torn tail or
/// another repair startup would make is the only sign of a crash. Every
/// rotation and the new record are synced, with the directory, before this
/// returns.
pub fn begin(
    root: &Path,
    relaxed: bool,
    registered: Option<u64>,
    boot: Option<BootId>,
    repair_pending: impl FnOnce() -> bool,
    mut fresh: impl FnMut() -> [u8; 16],
) -> Result<Begun> {
    let (loaded, origin) = storage_identity::load_or_create(root, &mut fresh)?;
    let last_run = read(root, loaded)?;
    let recorded = read_registration(root)?;
    let restored = origin == Origin::Loaded && recorded.behind(registered);
    // A new id already tells peers to start over. `||` order matters: the
    // filesystem is synced only when nothing else would rotate the id.
    let crashed = origin == Origin::Loaded
        && !restored
        && match last_run {
            LastRun::Clean | LastRun::Crashed { relaxed: false, .. } => false,
            LastRun::Crashed {
                relaxed: true,
                boot: crashed_in,
            } => crashed_in.is_none() || crashed_in != boot || !synced_after_crash(root),
            LastRun::Unrecorded => repair_pending(),
        };
    let rotate = restored || crashed;
    let id = if rotate {
        let id = storage_identity::replace(root, fresh())?;
        if restored {
            tracing::warn!(
                previous = %loaded,
                storage_id = %id,
                recorded = ?recorded,
                registered = ?registered,
                "this data directory is older than the node's last registration, as a restored \
                 snapshot or a copy is; gave it a new storage id, so peers copy it again"
            );
        } else {
            tracing::warn!(
                previous = %loaded,
                storage_id = %id,
                last_run = ?last_run,
                "the last run on this data directory did not stop cleanly and may have lost \
                 records it acknowledged; gave it a new storage id, so peers copy it again"
            );
        }
        id
    } else {
        if origin == Origin::Loaded && matches!(last_run, LastRun::Crashed { relaxed: true, .. }) {
            tracing::info!(
                storage_id = %loaded,
                "the last run on this data directory crashed in this boot of the machine, \
                 so the page cache kept what it wrote; keeping the storage id"
            );
        }
        loaded
    };
    // The boot matters only to a relaxed run, and only that record is one an
    // older build cannot read.
    let record = match (relaxed, boot) {
        (true, Some(boot)) => encode_boot(id, boot).to_vec(),
        (true, None) => encode(RUNNING, RELAXED, id).to_vec(),
        (false, _) => encode(RUNNING, 0, id).to_vec(),
    };
    write(root, &record, true)?;
    Ok(Begun {
        id,
        origin,
        last_run,
        recorded,
        rotated: rotate,
    })
}

/// Push everything the crashed run left in the page cache to the device,
/// before the kept id is announced or a clean stop could be recorded under
/// it. Without this a later clean stop would vouch for pages only the kernel
/// held, such as a retired segment whose seal never ran, and a power loss
/// after it would lose them under the same id. A failure, which includes a
/// writeback error the crashed run never saw, or a platform that cannot do
/// it (anything but Linux), means the id cannot be kept.
fn synced_after_crash(root: &Path) -> bool {
    #[cfg(test)]
    {
        // Unit tests judge the decision on every platform; the real call is
        // tested on its own.
        let _ = root;
        tests::SYNCS.with(|syncs| syncs.set(syncs.get() + 1));
        !tests::SYNC_FAILS.with(std::cell::Cell::get)
    }
    #[cfg(not(test))]
    match crate::io::sync_filesystems(root) {
        Ok(()) => true,
        Err(err) => {
            tracing::warn!(
                root = %root.display(),
                error = %err,
                "could not sync the data directory's filesystems after a process crash; \
                 taking a new storage id"
            );
            false
        }
    }
}

/// Record that the control plane registered this node at `incarnation`. Call
/// on every registration, before the incarnation is acted on.
///
/// A crash between a registration and this record makes the next start read
/// the directory as older than it is and rotate its id, which costs a copy
/// and loses nothing.
pub fn record_registration(root: &Path, incarnation: u64) -> Result<()> {
    write_atomically(
        root,
        REGISTRATION_FILE_NAME,
        &encode_registration(incarnation),
        true,
    )
}

/// The node incarnation recorded under `root`.
pub fn read_registration(root: &Path) -> Result<Recorded> {
    let path = root.join(REGISTRATION_FILE_NAME);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Recorded::Unrecorded),
        Err(err) => return Err(StorageError::Io(err)),
    };
    Ok(match decode_registration(&bytes) {
        Some(incarnation) => Recorded::At(incarnation),
        None => {
            tracing::warn!(
                path = %path.display(),
                "the registration record does not decode; taking this directory as older \
                 than any registration"
            );
            Recorded::Unreadable
        }
    })
}

pub(crate) fn encode_registration(incarnation: u64) -> [u8; REGISTRATION_LEN] {
    let mut out = [0u8; REGISTRATION_LEN];
    out[0..4].copy_from_slice(&REGISTRATION_MAGIC.to_be_bytes());
    out[4..6].copy_from_slice(&REGISTRATION_VERSION.to_be_bytes());
    out[6..14].copy_from_slice(&incarnation.to_be_bytes());
    let crc = crate::segment::format::crc32(&[&out[..14]]);
    out[14..].copy_from_slice(&crc.to_be_bytes());
    out
}

/// The incarnation, or `None` for anything this build did not write.
pub(crate) fn decode_registration(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != REGISTRATION_LEN {
        return None;
    }
    let magic = u32::from_be_bytes(bytes[0..4].try_into().ok()?);
    let version = u16::from_be_bytes(bytes[4..6].try_into().ok()?);
    let crc = u32::from_be_bytes(bytes[14..].try_into().ok()?);
    if magic != REGISTRATION_MAGIC
        || version != REGISTRATION_VERSION
        || crate::segment::format::crc32(&[&bytes[..14]]) != crc
    {
        return None;
    }
    Some(u64::from_be_bytes(bytes[6..14].try_into().ok()?))
}

/// Record that the run under `id` stopped with everything it acknowledged
/// synced. Only once nothing will write to the directory again.
pub fn mark_clean(root: &Path, id: StorageId) -> Result<()> {
    write(root, &encode(STOPPED, 0, id), true)
}

/// Record that the run under way on `root` may have lost records it
/// acknowledged without a machine crash: a failed fsync may have dropped the
/// pages it could not write, or a stop could not flush them. A crash after
/// this rotates the id whatever the boot. Only a relaxed run's record names a
/// boot, so any other is left as it is.
///
/// Not synced. Only a crash of the process has to find it, and the page cache
/// keeps it for that; a crash of the machine rotates anyway. Syncing could
/// also fail on the very device that just failed a sync.
pub fn forget_boot(root: &Path) -> Result<()> {
    let bytes = match std::fs::read(path_in(root)) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(StorageError::Io(err)),
    };
    match decode(&bytes) {
        Some(Record {
            id, boot: Some(_), ..
        }) => write(root, &encode(RUNNING, RELAXED, id), false),
        _ => Ok(()),
    }
}

/// Call [`forget_boot`] for `root` as soon as any log in this process is
/// poisoned, or now if one already is.
pub fn watch_poison(root: &Path) {
    WATCHED.lock().push(root.to_path_buf());
    if crate::disk_log::any_log_poisoned() {
        forget_watched();
    }
}

/// [`forget_boot`] on every directory [`watch_poison`] was given.
pub(crate) fn forget_watched() {
    for root in WATCHED.lock().iter() {
        if let Err(err) = forget_boot(root) {
            tracing::error!(
                root = %root.display(),
                error = %err,
                "could not record that a poisoned log may have lost acknowledged records; \
                 a process crash now would keep the storage id"
            );
        }
    }
}

/// What the record under `root` says, judged against the id now there.
pub fn read(root: &Path, id: StorageId) -> Result<LastRun> {
    let path = path_in(root);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(LastRun::Unrecorded),
        Err(err) => return Err(StorageError::Io(err)),
    };
    Ok(match decode(&bytes) {
        Some(Record {
            state: STOPPED,
            id: recorded,
            ..
        }) if recorded == id => LastRun::Clean,
        Some(Record {
            state: RUNNING,
            flags,
            id: recorded,
            boot,
        }) if recorded == id => LastRun::Crashed {
            relaxed: flags & RELAXED != 0,
            boot,
        },
        _ => {
            tracing::warn!(
                path = %path.display(),
                "the run state does not decode or names another storage id; \
                 taking the last run as a crash"
            );
            LastRun::Crashed {
                relaxed: true,
                boot: None,
            }
        }
    })
}

/// A decoded record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Record {
    pub(crate) state: u8,
    pub(crate) flags: u8,
    pub(crate) id: StorageId,
    pub(crate) boot: Option<BootId>,
}

pub(crate) fn encode(state: u8, flags: u8, id: StorageId) -> [u8; ENCODED_LEN] {
    let mut out = [0u8; ENCODED_LEN];
    header(&mut out, VERSION, state, flags, id);
    let crc = crate::segment::format::crc32(&[&out[..24]]);
    out[24..].copy_from_slice(&crc.to_be_bytes());
    out
}

/// A relaxed run under way in `boot`.
pub(crate) fn encode_boot(id: StorageId, boot: BootId) -> [u8; ENCODED_LEN_BOOT] {
    let mut out = [0u8; ENCODED_LEN_BOOT];
    header(&mut out, VERSION_BOOT, RUNNING, RELAXED, id);
    out[24..40].copy_from_slice(&boot.to_bytes());
    let crc = crate::segment::format::crc32(&[&out[..40]]);
    out[40..].copy_from_slice(&crc.to_be_bytes());
    out
}

fn header(out: &mut [u8], version: u16, state: u8, flags: u8, id: StorageId) {
    out[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    out[4..6].copy_from_slice(&version.to_be_bytes());
    out[6] = state;
    out[7] = flags;
    out[8..24].copy_from_slice(&id.to_bytes());
}

/// The record, or `None` for anything this build did not write.
pub(crate) fn decode(bytes: &[u8]) -> Option<Record> {
    let (version, body_len) = match bytes.len() {
        ENCODED_LEN => (VERSION, 24),
        ENCODED_LEN_BOOT => (VERSION_BOOT, 40),
        _ => return None,
    };
    let magic = u32::from_be_bytes(bytes[0..4].try_into().ok()?);
    let crc = u32::from_be_bytes(bytes[body_len..].try_into().ok()?);
    let (state, flags) = (bytes[6], bytes[7]);
    if magic != MAGIC
        || u16::from_be_bytes(bytes[4..6].try_into().ok()?) != version
        || !matches!(state, RUNNING | STOPPED)
        || flags & !RELAXED != 0
        || crate::segment::format::crc32(&[&bytes[..body_len]]) != crc
    {
        return None;
    }
    // Only a relaxed run under way names its boot.
    if version == VERSION_BOOT && (state, flags) != (RUNNING, RELAXED) {
        return None;
    }
    let boot = if version == VERSION_BOOT {
        Some(BootId::from_bytes(bytes[24..40].try_into().ok()?))
    } else {
        None
    };
    Some(Record {
        state,
        flags,
        id: StorageId::from_bytes(bytes[8..24].try_into().ok()?),
        boot,
    })
}

fn write(root: &Path, record: &[u8], durable: bool) -> Result<()> {
    write_atomically(root, FILE_NAME, record, durable)
}

/// A crash leaves the old record or the new one, never a mix. With `durable`
/// the record and the rename are synced.
fn write_atomically(root: &Path, name: &str, bytes: &[u8], durable: bool) -> Result<()> {
    use std::io::Write;

    let path = root.join(name);
    let temporary = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&temporary).map_err(StorageError::Io)?;
        file.write_all(bytes).map_err(StorageError::Io)?;
        if durable {
            crate::io::sync_all(&file).map_err(StorageError::Io)?;
        }
    }
    std::fs::rename(&temporary, &path).map_err(StorageError::Io)?;
    if durable {
        sync_dir(root).map_err(StorageError::Io)?;
    }
    Ok(())
}

fn path_in(root: &Path) -> PathBuf {
    root.join(FILE_NAME)
}

#[cfg(test)]
mod tests;
