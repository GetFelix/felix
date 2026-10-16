//! Whether the last broker on a data directory stopped cleanly.
//!
//! `<durable root>/run-state` says either that a broker is running there, and
//! whether its fsync mode acknowledges records before they are synced, or that
//! the last one stopped after syncing everything it acknowledged. A crash
//! leaves the first. Under `FsyncMode::Periodic` or `None` that crash can have
//! lost acknowledged records while the storage id stayed the same, so
//! [`begin`] gives the directory a new id, and peers start their copy of it
//! over. See `docs/durable-storage.md`, "Storage identity".

use std::path::{Path, PathBuf};

use crate::io::sync_dir;
use crate::storage_identity::{self, Origin, StorageId};
use crate::{Result, StorageError};

/// `"FLRN"`: Felix RuN.
const MAGIC: u32 = 0x464C_524E;
const VERSION: u16 = 1;
/// magic(4) + version(2) + state(1) + flags(1) + id(16) + crc(4) over
/// everything before it.
pub(crate) const ENCODED_LEN: usize = 28;
/// The file's name under the durable root. Not a directory, so shard
/// discovery passes over it.
pub const FILE_NAME: &str = "run-state";

const RUNNING: u8 = 1;
const STOPPED: u8 = 2;
/// The run acknowledges records before they are synced.
const RELAXED: u8 = 1;

/// What the record says about the run before this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LastRun {
    /// No record: a new directory, or one only older builds have run on.
    Unrecorded,
    /// It stopped after syncing everything it acknowledged.
    Clean,
    /// It did not stop cleanly. `relaxed` when its fsync mode acknowledged
    /// records before syncing them, so the crash may have lost some. A record
    /// that does not decode, or that names another storage id, reads as a
    /// relaxed crash.
    Crashed { relaxed: bool },
}

/// The identity a run starts under, from [`begin`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Begun {
    pub id: StorageId,
    /// Where the id on disk came from, before any rotation.
    pub origin: Origin,
    pub last_run: LastRun,
    /// Whether `id` is new because the last run may have lost records it
    /// acknowledged.
    pub rotated: bool,
}

/// Start a run on the directory at `root`: read its storage id, give it a new
/// one when the last run may have lost acknowledged records, then record that
/// a run under `relaxed` is under way. Call before any store opens, and
/// announce only the id this returns.
///
/// `repair_pending` is asked only when there is no record but an id was
/// loaded, which is the first start after an upgrade: then a torn tail or
/// another repair startup would make is the only sign of a crash. Every
/// rotation and the new record are synced, with the directory, before this
/// returns.
pub fn begin(
    root: &Path,
    relaxed: bool,
    repair_pending: impl FnOnce() -> bool,
    mut fresh: impl FnMut() -> [u8; 16],
) -> Result<Begun> {
    let (loaded, origin) = storage_identity::load_or_create(root, &mut fresh)?;
    let last_run = read(root, loaded)?;
    // A new id already tells peers to start over.
    let rotate = origin == Origin::Loaded
        && match last_run {
            LastRun::Clean | LastRun::Crashed { relaxed: false } => false,
            LastRun::Crashed { relaxed: true } => true,
            LastRun::Unrecorded => repair_pending(),
        };
    let id = if rotate {
        let id = storage_identity::replace(root, fresh())?;
        tracing::warn!(
            previous = %loaded,
            storage_id = %id,
            last_run = ?last_run,
            "the last run on this data directory did not stop cleanly and may have lost \
             records it acknowledged; gave it a new storage id, so peers copy it again"
        );
        id
    } else {
        loaded
    };
    write(root, RUNNING, if relaxed { RELAXED } else { 0 }, id)?;
    Ok(Begun {
        id,
        origin,
        last_run,
        rotated: rotate,
    })
}

/// Record that the run under `id` stopped with everything it acknowledged
/// synced. Only once nothing will write to the directory again.
pub fn mark_clean(root: &Path, id: StorageId) -> Result<()> {
    write(root, STOPPED, 0, id)
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
        Some((STOPPED, _, recorded)) if recorded == id => LastRun::Clean,
        Some((RUNNING, flags, recorded)) if recorded == id => LastRun::Crashed {
            relaxed: flags & RELAXED != 0,
        },
        _ => {
            tracing::warn!(
                path = %path.display(),
                "the run state does not decode or names another storage id; \
                 taking the last run as a crash"
            );
            LastRun::Crashed { relaxed: true }
        }
    })
}

pub(crate) fn encode(state: u8, flags: u8, id: StorageId) -> [u8; ENCODED_LEN] {
    let mut out = [0u8; ENCODED_LEN];
    out[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    out[4..6].copy_from_slice(&VERSION.to_be_bytes());
    out[6] = state;
    out[7] = flags;
    out[8..24].copy_from_slice(&id.to_bytes());
    let crc = crate::segment::format::crc32(&[&out[..24]]);
    out[24..].copy_from_slice(&crc.to_be_bytes());
    out
}

/// The state, flags and id, or `None` for anything this build did not write.
pub(crate) fn decode(bytes: &[u8]) -> Option<(u8, u8, StorageId)> {
    if bytes.len() != ENCODED_LEN {
        return None;
    }
    let magic = u32::from_be_bytes(bytes[0..4].try_into().ok()?);
    let version = u16::from_be_bytes(bytes[4..6].try_into().ok()?);
    let crc = u32::from_be_bytes(bytes[24..].try_into().ok()?);
    let (state, flags) = (bytes[6], bytes[7]);
    if magic != MAGIC
        || version != VERSION
        || !matches!(state, RUNNING | STOPPED)
        || flags & !RELAXED != 0
        || crate::segment::format::crc32(&[&bytes[..24]]) != crc
    {
        return None;
    }
    let id = StorageId::from_bytes(bytes[8..24].try_into().ok()?);
    Some((state, flags, id))
}

/// A crash leaves the old record or the new one, never a mix.
fn write(root: &Path, state: u8, flags: u8, id: StorageId) -> Result<()> {
    use std::io::Write;

    let path = path_in(root);
    let temporary = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&temporary).map_err(StorageError::Io)?;
        file.write_all(&encode(state, flags, id))
            .map_err(StorageError::Io)?;
        crate::io::sync_all(&file).map_err(StorageError::Io)?;
    }
    std::fs::rename(&temporary, &path).map_err(StorageError::Io)?;
    sync_dir(root).map_err(StorageError::Io)?;
    Ok(())
}

fn path_in(root: &Path) -> PathBuf {
    root.join(FILE_NAME)
}

#[cfg(test)]
mod tests;
