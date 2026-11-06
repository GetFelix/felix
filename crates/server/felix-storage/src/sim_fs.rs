//! The in-memory filesystem a simulation keeps broker data on.
//!
//! A simulation registers the directory it gives each broker. Everything
//! storage does under that path then happens in memory, instantly and without
//! a file descriptor, and [`crash`] says what a power loss at that moment
//! would have left. See `docs/simulation.md` for what is modelled.

use std::io;
use std::path::Path;

use crate::io::writeback::Writeback;

/// Put `root` and everything below it in memory, empty. The path need not
/// exist on disk, but it must be absolute and unique among live simulations.
pub fn register_root(root: &Path) {
    crate::fs::register(root);
}

/// [`register_root`], unless a live simulation holds `root` already. Returns
/// whether it was free. A simulation that wants the same path every run (so
/// that maps keyed by path lay out the same) tries a fixed name first.
pub fn claim_root(root: &Path) -> bool {
    crate::fs::claim(root)
}

/// Forget `root` and drop every file under it.
pub fn unregister_root(root: &Path) {
    crate::fs::unregister(root);
}

/// Replace what is under `root` with what a power loss now would leave.
///
/// Every file keeps the bytes of its last flush plus a choice, drawn from
/// `seed`, of each unflushed 4 KiB page: whole, torn on a 512 B sector, or
/// lost. The length can reach the disk without the data. A create, rename or
/// unlink survives only if its directory was flushed, or if the seed lets it.
/// Files opened before the call are cut off from the new tree, as they are
/// from a rebooted disk.
///
/// A tree where every write was flushed comes back unchanged.
///
/// # Errors
/// `NotFound` when `root` was not registered.
pub fn crash(root: &Path, seed: u64) -> io::Result<()> {
    let tree = crate::fs::tree_at(root).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "no simulated filesystem at this root",
        )
    })?;
    tree.crash(seed, Writeback::AnySubset);
    Ok(())
}
