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
    tree(root)?.crash(seed, Writeback::AnySubset);
    Ok(())
}

/// Cut the power under `root` during the next append to a log segment, then
/// call `on_cut` with that segment's path below `root` and the offset where
/// its whole records end.
///
/// Every file and directory keeps what [`crash`] with `seed` would leave,
/// and the append in flight is then forced to come back short or torn: its
/// size reached the disk in full or in part, and each sector of its data
/// arrived or did not, at least one not. Recovery has to repair that tail,
/// unless the append held several records and the size stopped between two
/// of them with nothing missing before it. The offset is where the append
/// began, moved past each of its records that came back whole, so a segment
/// no longer than it has no tail to repair.
/// The device's contents are fixed at the cut; whatever the process writes
/// after it is lost at [`reboot`].
///
/// `on_cut` runs on the writing thread, inside the write, so it must not
/// touch the filesystem. It is the moment to stop the process.
///
/// # Errors
/// `NotFound` when `root` was not registered.
pub fn arm_power_cut(
    root: &Path,
    seed: u64,
    on_cut: impl FnOnce(&Path, u64) + Send + 'static,
) -> io::Result<()> {
    tree(root)?.arm(seed, Box::new(on_cut));
    Ok(())
}

/// Cut the power under `root` now, unless an armed cut already went off.
/// The device keeps what [`crash`] with `seed` would leave at this instant,
/// and [`reboot`] puts it in place.
///
/// # Errors
/// `NotFound` when `root` was not registered.
pub fn cut_power(root: &Path, seed: u64) -> io::Result<()> {
    tree(root)?.cut(seed);
    Ok(())
}

/// Replace the tree under `root` with what the device held when the power
/// was cut. Returns whether it had been: `false` changes nothing.
///
/// # Errors
/// `NotFound` when `root` was not registered.
pub fn reboot(root: &Path) -> io::Result<bool> {
    Ok(tree(root)?.reboot())
}

/// The segments, as paths below `root`, whose torn tails recovery has
/// repaired or which it removed as unfinished, in order and across reboots.
///
/// # Errors
/// `NotFound` when `root` was not registered.
pub fn torn_tails_repaired(root: &Path) -> io::Result<Vec<std::path::PathBuf>> {
    Ok(tree(root)?.torn_tails())
}

/// The segments, as paths below `root`, that were opened, in order. A
/// segment appears once for each stretch between reboots in which it was
/// opened. Opens made inside [`unobserved`] are left out.
///
/// # Errors
/// `NotFound` when `root` was not registered.
pub fn segments_opened(root: &Path) -> io::Result<Vec<std::path::PathBuf>> {
    Ok(tree(root)?.opened_segments())
}

/// Run `read`, a look at the devices from outside any broker, without
/// counting the segments it opens in [`segments_opened`]. It covers opens made
/// on the calling thread.
pub fn unobserved<R>(read: impl FnOnce() -> R) -> R {
    crate::fs::unobserved(read)
}

/// Fail one flush under `root` (of a file or a directory) with an I/O
/// error, after `skip` more have succeeded. The failed flush makes nothing
/// durable, so a [`crash`] after it may drop what it was asked to keep, as
/// a kernel may after a failed writeback.
///
/// # Errors
/// `NotFound` when `root` was not registered.
pub fn fail_flush(root: &Path, skip: u64) -> io::Result<()> {
    tree(root)?.fail_flush(skip);
    Ok(())
}

/// Disarm [`fail_flush`], and say whether its flush had failed.
///
/// # Errors
/// `NotFound` when `root` was not registered.
pub fn flush_failed(root: &Path) -> io::Result<bool> {
    Ok(tree(root)?.flush_failed())
}

fn tree(root: &Path) -> io::Result<std::sync::Arc<crate::fs::Tree>> {
    crate::fs::tree_at(root).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "no simulated filesystem at this root",
        )
    })
}

/// The length of the file at `path` below `root`, or `None` when there is
/// none.
///
/// # Errors
/// `NotFound` when `root` was not registered.
pub fn file_len(root: &Path, path: &Path) -> io::Result<Option<u64>> {
    let tree = tree(root)?;
    match tree.len_of(path) {
        Ok(len) => Ok(Some(len)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

/// The boot of the machine under `root`. It is the same until a power cut
/// is rebooted, then a new one, so a relaxed run's crash record names a boot
/// the way it does on a real machine.
///
/// # Errors
/// `NotFound` when `root` was not registered.
pub fn boot_id(root: &Path) -> io::Result<crate::run_state::BootId> {
    let boots = tree(root)?.boots();
    let mut bytes = [0xB0; 16];
    bytes[8..].copy_from_slice(&boots.to_be_bytes());
    Ok(crate::run_state::BootId::from_bytes(bytes))
}

/// Delete `below` (a path below `root`, a file or a directory) from the
/// device, and make that durable, as an operator's `rm -rf` on a stopped
/// broker would. An empty `below` empties `root`. Nothing is an error if it
/// is already gone.
///
/// # Errors
/// `NotFound` when `root` was not registered.
pub fn wipe(root: &Path, below: &Path) -> io::Result<()> {
    tree(root)?.wipe(below)
}
