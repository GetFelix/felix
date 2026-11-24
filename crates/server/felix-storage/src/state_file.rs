//! The small files kept beside a log's segments: a Raft group's purged id,
//! carried floor, snapshot, a log's pending rewind and the metadata store's
//! member file. Each is replaced whole, so a crash leaves the old file or the
//! new one.

use std::io::{self, Write};
use std::path::Path;

use crate::disk_log::crash_point;

/// Replace `name` in `dir` with `parts`, in order: a temporary, fsynced,
/// renamed over the old file, and the directory synced.
pub(crate) fn store(dir: &Path, name: &str, parts: &[&[u8]]) -> io::Result<()> {
    let path = dir.join(name);
    let temporary = temporary(dir, name);
    let step = || crash_point::reached(dir);
    step()?;
    {
        let mut file = crate::fs::File::create(&temporary)?;
        for part in parts {
            file.write_all(part)?;
        }
        step()?;
        crate::io::sync_all(&file)?;
    }
    step()?;
    crate::fs::rename(&temporary, &path)?;
    step()?;
    crate::io::sync_dir(dir)
}

/// As [`store`], with the writes going through the segment writes' path, so
/// a disk that refuses writes refuses this too. For a write that stands as a
/// probe of the disk.
#[cfg(feature = "raft")]
pub(crate) fn store_through_faults(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let path = dir.join(name);
    let temporary = temporary(dir, name);
    {
        let file = crate::fs::File::create(&temporary)?;
        crate::io::write_all(&file, bytes)?;
        crate::io::sync_all(&file)?;
    }
    crate::fs::rename(&temporary, &path)?;
    crate::io::sync_dir(dir)
}

/// The bytes of `name` in `dir`, or `None` when it was never written. A
/// temporary a crash left behind is not the file and is never read.
pub(crate) fn load(dir: &Path, name: &str) -> io::Result<Option<Vec<u8>>> {
    match crate::fs::read(dir.join(name)) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

/// Delete `name` in `dir`, if it is there, and sync the directory.
pub(crate) fn remove(dir: &Path, name: &str) -> io::Result<()> {
    match crate::fs::remove_file(dir.join(name)) {
        Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err),
        _ => {}
    }
    crate::io::sync_dir(dir)
}

/// Delete the temporary a crash left while replacing `name` in `dir`, if
/// there is one. It can be as large as the file and hold the same secrets.
#[cfg(feature = "raft")]
pub(crate) fn remove_temporary(dir: &Path, name: &str) -> io::Result<()> {
    match crate::fs::remove_file(temporary(dir, name)) {
        Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err),
        _ => Ok(()),
    }
}

pub(crate) fn temporary(dir: &Path, name: &str) -> std::path::PathBuf {
    dir.join(format!("{name}.tmp"))
}
