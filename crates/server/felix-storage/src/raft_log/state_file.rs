//! The small files a Raft group keeps beside its segments: the purged id,
//! the snapshot and the metadata store's member file. Each is replaced whole, so a crash leaves the old file or
//! the new one.

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

/// The bytes of `name` in `dir`, or `None` when it was never written. A
/// temporary a crash left behind is not the file and is never read.
pub(crate) fn load(dir: &Path, name: &str) -> io::Result<Option<Vec<u8>>> {
    match crate::fs::read(dir.join(name)) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

/// Delete the temporary a crash left while replacing `name` in `dir`, if
/// there is one. It can be as large as the file and hold the same secrets.
pub(crate) fn remove_temporary(dir: &Path, name: &str) -> io::Result<()> {
    match crate::fs::remove_file(temporary(dir, name)) {
        Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err),
        _ => Ok(()),
    }
}

pub(crate) fn temporary(dir: &Path, name: &str) -> std::path::PathBuf {
    dir.join(format!("{name}.tmp"))
}
