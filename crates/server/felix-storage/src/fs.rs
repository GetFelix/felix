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
//! The exceptions are `fault.rs`, `run_state/boot.rs` and `io/power_loss.rs`,
//! which read the host itself, and the fuzzing entry points.

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
pub(crate) use mem::{register, tree_at, unregister};
#[cfg(feature = "sim")]
pub(crate) use routed::{
    File, OpenOptions, create_dir_all, metadata, read, read_dir, read_to_string, remove_dir_all,
    remove_file, rename, sync_simulated_dir, sync_simulated_filesystem, write,
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
