//! Platform I/O primitives the storage layer is built on.
//!
//! Three techniques, each borrowed from log-structured storage engines that have
//! already paid for the lesson:
//!
//! 1. **Positioned reads** (`pread`). A `seek` + `read` pair mutates the file
//!    cursor, so a shared descriptor cannot serve two readers at once and every
//!    read costs an extra syscall. `pread` takes the offset as an argument, which
//!    means one descriptor per segment serves any number of concurrent readers
//!    with no lock and no seek. This is why Kafka, RocksDB and LevelDB all use
//!    positioned reads on their immutable files.
//!
//! 2. **Preallocation**. Appending past the end of a file forces the filesystem
//!    to allocate blocks and update the inode's block map on the write path, and
//!    it invites fragmentation as segments from different shards interleave.
//!    Reserving the whole segment up front moves that work off the append path.
//!    Both `fallocate` and `F_PREALLOCATE` reserve blocks *without* changing the
//!    file's logical length, so recovery's "valid bytes end at EOF" reasoning
//!    still holds.
//!
//! 3. **`fdatasync` over `fsync`**. An append changes file data and the file
//!    size, but not the owner, mode, or times that a full `fsync` also flushes.
//!    `fdatasync` skips that second metadata round trip. On a spinning disk or a
//!    network volume that is a whole extra I/O per commit.
//!
//! Every platform-specific call degrades to a correct no-op or to the portable
//! equivalent, so an unsupported target loses performance and never correctness.
//!
//! A log's appends and flushes each run on a thread of its own
//! (`log_thread`), and on Linux the flushes can be submitted to `io_uring`
//! instead (`uring_fsync`).

pub(crate) mod log_thread;
#[cfg(all(
    target_os = "linux",
    any(test, debug_assertions, feature = "fault-injection")
))]
pub(crate) mod power_loss;
#[cfg(target_os = "linux")]
pub(crate) mod uring_fsync;

use std::fs::File;
use std::io::{self, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

/// Read into `buf` starting at `offset` without touching the file cursor.
///
/// Returns the number of bytes read, which is short only at end of file.
pub(crate) fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        let mut filled = 0;
        while filled < buf.len() {
            match file.read_at(&mut buf[filled..], offset + filled as u64) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(filled)
    }
    #[cfg(windows)]
    {
        let mut filled = 0;
        while filled < buf.len() {
            match file.seek_read(&mut buf[filled..], offset + filled as u64) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(filled)
    }
    #[cfg(not(any(unix, windows)))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = file;
        file.seek(SeekFrom::Start(offset))?;
        let mut filled = 0;
        while filled < buf.len() {
            match file.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(filled)
    }
}

/// Write all of `buf` at `offset` without touching the file cursor.
pub(crate) fn write_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        #[cfg(unix)]
        let written = file.write_at(buf, offset);
        #[cfg(windows)]
        let written = file.seek_write(buf, offset);
        match written {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// Reserve `len` bytes of blocks for `file` without changing its logical length.
///
/// Best effort: an unsupported filesystem leaves the file untouched and appends
/// simply allocate as they go, which is correct but slower.
pub(crate) fn preallocate(file: &File, len: u64) -> io::Result<()> {
    if len == 0 {
        return Ok(());
    }
    #[cfg(any(debug_assertions, test, feature = "fault-injection"))]
    {
        crate::fault::refresh();
        if let Some(err) = crate::fault::injected_no_space() {
            return Err(err);
        }
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        // FALLOC_FL_KEEP_SIZE: reserve blocks, leave st_size alone.
        const FALLOC_FL_KEEP_SIZE: libc::c_int = 0x01;
        // SAFETY: `fd` is a live descriptor owned by `file` for the duration of
        // the call, and the remaining arguments are plain integers.
        let rc = unsafe {
            libc::fallocate(file.as_raw_fd(), FALLOC_FL_KEEP_SIZE, 0, len as libc::off_t)
        };
        if rc != 0 {
            let err = io::Error::last_os_error();
            // The filesystem cannot preallocate (tmpfs, some network mounts) or
            // the kernel lacks the call. Neither is fatal: appends fall back to
            // allocating as they go, which is slower but correct.
            //
            // `ENOTSUP` is deliberately absent: on Linux it is the same value as
            // `EOPNOTSUPP`, so listing both is an unreachable arm rather than
            // extra coverage. They differ on macOS, which is why the `sync_data`
            // fallback below does list both.
            if !matches!(
                err.raw_os_error(),
                Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS)
            ) {
                return Err(err);
            }
        }
        Ok(())
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        let mut store = libc::fstore_t {
            fst_flags: libc::F_ALLOCATECONTIG,
            fst_posmode: libc::F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: len as libc::off_t,
            fst_bytesalloc: 0,
        };
        // SAFETY: `fd` is live for the call and `store` is a correctly
        // initialised `fstore_t` that outlives it.
        let mut rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) };
        if rc == -1 {
            // Contiguous allocation failed; retry allowing fragmentation.
            store.fst_flags = libc::F_ALLOCATEALL;
            // SAFETY: as above.
            rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) };
        }
        // Still unsupported (e.g. a network mount): fall through silently.
        let _ = rc;
        Ok(())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = file;
        Ok(())
    }
}

/// Flush file *data* (and the size needed to read it back) to stable storage.
///
/// Deliberately `sync_data` rather than `sync_all` on platforms where the
/// distinction is real: see the module comment.
///
/// macOS is the exception. There, POSIX `fsync`/`fdatasync` only push data out
/// of the kernel — the drive is free to keep it in a volatile write cache, so a
/// power loss can still lose an "fsynced" write. `F_FULLFSYNC` is the call that
/// orders the device itself to flush.
///
/// Rust's `sync_data` currently issues `F_FULLFSYNC` on macOS, so this call is
/// redundant against today's standard library. It is here anyway because that
/// behaviour is an implementation detail rather than a documented guarantee, and
/// `FsyncMode::OnCommit` is a promise this crate makes, not one it delegates.
/// The measured cost is the same either way (~4ms per flush on APFS), which is
/// itself the evidence that the flush is reaching the device.
pub(crate) fn sync_data(file: &File) -> io::Result<()> {
    before_sync(file, SyncKind::Data)?;
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        // SAFETY: `fd` is a live descriptor owned by `file` for the duration of
        // the call, and `F_FULLFSYNC` takes no argument.
        let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) };
        if rc != -1 {
            return Ok(());
        }
        // Some filesystems (notably network mounts) do not implement it. Fall
        // back rather than failing the write: a weaker flush is still better
        // than reporting a durability error we cannot fix.
        let err = io::Error::last_os_error();
        if !matches!(
            err.raw_os_error(),
            Some(libc::ENOTSUP) | Some(libc::EINVAL) | Some(libc::EOPNOTSUPP)
        ) {
            return Err(err);
        }
    }
    file.sync_data()
}

/// Flush a directory entry so a newly created or renamed file survives a crash.
///
/// Creating a file makes the *file* durable only once its parent directory
/// entry is durable too; without this a crash can leave a segment that exists in
/// the page cache but not in the directory after reboot.
pub(crate) fn sync_dir(path: &std::path::Path) -> io::Result<()> {
    before_sync_dir(path)?;
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        // Windows has no directory-fsync equivalent; file handles carry their
        // own durability.
        let _ = path;
        Ok(())
    }
}

/// Create `path` and any missing ancestors, and make each new directory entry
/// durable.
///
/// Flushes the parent of every directory it created, deepest first, ending with
/// the first ancestor that already existed. When `path` already exists it still
/// flushes `path`'s parent, which makes a directory that a crashed earlier run
/// created but never flushed durable now.
pub(crate) fn create_dir_all_durable(path: &std::path::Path) -> io::Result<()> {
    let mut created = Vec::new();
    let mut next = Some(path);
    while let Some(dir) = next.filter(|dir| !dir.as_os_str().is_empty() && !dir.is_dir()) {
        created.push(dir);
        next = dir.parent();
    }
    std::fs::create_dir_all(path)?;
    if created.is_empty() {
        created.push(path);
    }
    for dir in created {
        match dir.parent() {
            // A relative path's last component lives in the working directory.
            Some(parent) if parent.as_os_str().is_empty() => sync_dir(std::path::Path::new("."))?,
            Some(parent) => sync_dir(parent)?,
            // The filesystem root has no entry to flush.
            None => {}
        }
    }
    Ok(())
}

/// Flush file data *and* metadata. For the small files written whole and
/// renamed into place (indexes, generation history), where the extra metadata
/// round trip does not matter.
pub(crate) fn sync_all(file: &File) -> io::Result<()> {
    before_sync(file, SyncKind::All)?;
    file.sync_all()
}

/// Which flush a fault hook is looking at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncKind {
    /// `sync_data`, including the macOS `F_FULLFSYNC` path.
    Data,
    /// `sync_all`.
    All,
    /// An `fdatasync` submitted to `io_uring`.
    #[cfg_attr(
        not(all(
            target_os = "linux",
            any(test, debug_assertions, feature = "fault-injection")
        )),
        allow(dead_code)
    )]
    Uring,
}

/// The fault hooks every blocking file flush passes before it is issued. In a
/// release build without `fault-injection` this is empty.
///
/// An injected failure returns before the flush and before the power-loss
/// layer sees it: a flush that reported `EIO` made nothing durable.
fn before_sync(file: &File, kind: SyncKind) -> io::Result<()> {
    injected_fault()?;
    #[cfg(all(
        target_os = "linux",
        any(test, debug_assertions, feature = "fault-injection")
    ))]
    power_loss::observe_file(file, kind);
    let _ = (file, kind);
    Ok(())
}

/// [`before_sync`] for a directory flush.
fn before_sync_dir(path: &std::path::Path) -> io::Result<()> {
    injected_fault()?;
    #[cfg(all(
        target_os = "linux",
        any(test, debug_assertions, feature = "fault-injection")
    ))]
    power_loss::observe_dir(path);
    let _ = path;
    Ok(())
}

/// Write all of `buf` at the file's cursor, for a segment append.
///
/// Passes the write fault from `crate::fault` first, which in a release build
/// without `fault-injection` is compiled out. An injected failure writes half
/// of `buf` before it reports, like a disk that fills mid-batch.
pub(crate) fn write_all(mut file: &File, buf: &[u8]) -> io::Result<()> {
    #[cfg(any(debug_assertions, test, feature = "fault-injection"))]
    {
        crate::fault::refresh();
        if let Some(delay) = crate::fault::write_delay() {
            std::thread::sleep(delay);
        }
        if let Some(err) = crate::fault::injected_write_failure() {
            file.write_all(&buf[..buf.len() / 2])?;
            return Err(err);
        }
    }
    file.write_all(buf)
}

/// A slow or failing device, from `crate::fault`.
fn injected_fault() -> io::Result<()> {
    #[cfg(any(debug_assertions, test, feature = "fault-injection"))]
    {
        crate::fault::refresh();
        if let Some(delay) = crate::fault::fsync_delay() {
            std::thread::sleep(delay);
        }
        if let Some(err) = crate::fault::injected_failure() {
            return Err(err);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
