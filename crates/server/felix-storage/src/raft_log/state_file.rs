//! The small files a Raft log keeps beside its segments: the purged id and
//! the snapshot. Each is replaced whole, so a crash leaves the old file or
//! the new one.

use std::io::{self, Write};
use std::path::Path;

use crate::disk_log::crash_point;

/// Replace `name` in `dir` with `parts`, in order: a temporary, fsynced,
/// renamed over the old file, and the directory synced.
pub(super) fn store(dir: &Path, name: &str, parts: &[&[u8]]) -> io::Result<()> {
    let path = dir.join(name);
    let temporary = dir.join(format!("{name}.tmp"));
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
pub(super) fn load(dir: &Path, name: &str) -> io::Result<Option<Vec<u8>>> {
    match crate::fs::read(dir.join(name)) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}
