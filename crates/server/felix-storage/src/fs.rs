//! Where storage reaches the filesystem.
//!
//! Every file and directory call storage makes goes through here, so the
//! simulator can put a broker's data directory in memory (see
//! `docs/simulation.md`). Without the `sim` feature this is `std::fs` and
//! nothing else, and compiles to the same code as calling it directly. With
//! it, a call goes to the in-memory tree when its path is under a root the
//! simulator registered, and to the disk otherwise, so a process that is not
//! simulating keeps real files.
//!
//! A new call into `std::fs` on a broker's data directory should come through
//! here, or the simulator silently reads the disk instead of its own tree.
//! The exceptions are `fault.rs`, `run_state/boot.rs`, the witness half of
//! `run_state/witness.rs` and `io/power_loss.rs`, which read the host itself,
//! and the fuzzing entry points.

#[cfg(feature = "sim")]
mod mem;
#[cfg(feature = "sim")]
mod routed;

#[cfg(not(feature = "sim"))]
pub(crate) use std::fs::{
    File, OpenOptions, create_dir_all, metadata, read, read_dir, read_to_string, remove_dir_all,
    remove_file, rename, write,
};

#[cfg(feature = "sim")]
pub(crate) use mem::{Tree, claim, register, tree_at, unobserved, unregister};
#[cfg(feature = "sim")]
pub(crate) use routed::{
    File, OpenOptions, create_dir_all, metadata, note_torn_tail, read, read_dir, read_to_string,
    remove_dir_all, remove_file, rename, sync_simulated_dir, sync_simulated_filesystem, write,
};

use std::path::Path;

/// A file the test opened with `std::fs`, as the seam's type.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn from_os(file: std::fs::File) -> File {
    #[cfg(feature = "sim")]
    {
        File::Real(file)
    }
    #[cfg(not(feature = "sim"))]
    {
        file
    }
}

/// Record that recovery repaired the torn tail of `segment` or removed it,
/// so a simulation can see that a power cut it injected was dealt with.
/// Nothing without the `sim` feature.
#[cfg(not(feature = "sim"))]
#[inline]
pub(crate) fn note_torn_tail(_segment: &Path) {}

/// Whether `path` is on the simulator's filesystem, where a file has no
/// descriptor to lock. Never, without the `sim` feature.
#[cfg(feature = "raft")]
pub(crate) fn in_memory(path: &Path) -> bool {
    #[cfg(feature = "sim")]
    {
        mem::in_memory(path)
    }
    #[cfg(not(feature = "sim"))]
    {
        let _ = path;
        false
    }
}

/// Whether `path` exists. Any error counts as no, as with `Path::exists`.
pub(crate) fn exists(path: impl AsRef<Path>) -> bool {
    metadata(path).is_ok()
}

/// Whether `path` is a directory.
pub(crate) fn is_dir(path: impl AsRef<Path>) -> bool {
    metadata(path).is_ok_and(|meta| meta.is_dir())
}

/// `Path::try_exists`: only an error other than "not found" is reported.
pub(crate) fn try_exists(path: impl AsRef<Path>) -> std::io::Result<bool> {
    match metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}
