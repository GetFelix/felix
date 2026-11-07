//! Evidence, kept outside a data directory, that it is the one the last run
//! on this machine left.
//!
//! A restore can bring back every byte of a data directory, so nothing in it
//! can show it was not restored. Each run writes a fresh token to
//! `<root>/run-token` and the same token, with the boot and the directory's
//! path, to a witness file in memory (`/dev/shm` on Linux), which neither a
//! restore of the directory nor a rollback of the disk brings back, and a
//! reboot clears. A directory whose token the witness names is the one the
//! last run on it left in this boot, or a copy taken during that run. A start
//! that relies on that must treat anything else as possibly restored. See
//! `docs/durable-storage.md`, "The run witness".

use std::path::{Path, PathBuf};

use super::BootId;
use crate::{Result, StorageError};

/// The token's name under the durable root.
pub const TOKEN_FILE_NAME: &str = "run-token";

/// Where witnesses are kept: `/dev/shm` on Linux when it is a directory,
/// otherwise nowhere, and no directory is ever witnessed.
pub fn default_dir() -> Option<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        let shm = PathBuf::from("/dev/shm");
        if shm.is_dir() {
            return Some(shm);
        }
    }
    None
}

/// Start the witness of a run on `root`: remove the last run's witness, write
/// `token` to the directory, synced, then witness it in `dir` under `boot`.
/// Call once per run, before any store opens.
///
/// Fails if the old witness cannot be removed or the token cannot be written,
/// since an older copy of the directory would still match it. Returns whether
/// the new witness was written; without one the directory is simply not
/// witnessed.
pub fn witness(
    root: &Path,
    dir: Option<&Path>,
    boot: Option<BootId>,
    token: [u8; 16],
) -> Result<bool> {
    let path = match dir {
        Some(dir) => Some(witness_path(root, dir)?),
        None => None,
    };
    if let Some(path) = &path {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(StorageError::Io(err)),
        }
    }
    super::write_atomically(root, TOKEN_FILE_NAME, hex(&token).as_bytes(), true)?;
    let (Some(path), Some(boot)) = (path, boot) else {
        return Ok(false);
    };
    let body = render(root, boot, &hex(&token))?;
    // Memory only: a crash of the machine clears it anyway.
    let temporary = path.with_extension("tmp");
    let written =
        std::fs::write(&temporary, body).and_then(|()| std::fs::rename(&temporary, &path));
    if let Err(err) = written {
        tracing::warn!(
            path = %path.display(),
            error = %err,
            "could not write the run witness; this data directory cannot start from its \
             control-plane view until a later run writes one"
        );
        let _ = std::fs::remove_file(&temporary);
        return Ok(false);
    }
    Ok(true)
}

/// Whether the witness in `dir` names `root`, `boot`, and the token on disk.
/// Anything missing or unreadable, or an unknown boot, is no evidence.
pub fn witnessed(root: &Path, dir: Option<&Path>, boot: Option<BootId>) -> bool {
    let (Some(dir), Some(boot)) = (dir, boot) else {
        return false;
    };
    let token = match crate::fs::read(root.join(TOKEN_FILE_NAME)) {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };
    let Ok(token) = std::str::from_utf8(&token) else {
        return false;
    };
    let (Ok(path), Ok(expected)) = (witness_path(root, dir), render(root, boot, token)) else {
        return false;
    };
    std::fs::read(path).is_ok_and(|found| found == expected.as_bytes())
}

/// The witness for `root` under `dir`. The name only spreads directories over
/// files; the body names the directory in full.
fn witness_path(root: &Path, dir: &Path) -> Result<PathBuf> {
    let root = std::path::absolute(root).map_err(StorageError::Io)?;
    let hash = crc32fast::hash(root.as_os_str().as_encoded_bytes());
    Ok(dir.join(format!("felix-run-{hash:08x}")))
}

fn render(root: &Path, boot: BootId, token: &str) -> Result<String> {
    let root = std::path::absolute(root).map_err(StorageError::Io)?;
    Ok(format!(
        "felix-run\nboot={}\ntoken={token}\nroot={}\n",
        hex(&boot.to_bytes()),
        root.display(),
    ))
}

fn hex(bytes: &[u8; 16]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests;
