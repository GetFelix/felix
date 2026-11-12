//! An open file in the in-memory tree.

use std::io::{self, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use super::{FileNode, Tree};

/// How a file is opened, as `std::fs::OpenOptions` spells it.
#[derive(Clone, Copy, Default)]
pub(in crate::fs) struct Flags {
    pub(in crate::fs) read: bool,
    pub(in crate::fs) write: bool,
    pub(in crate::fs) append: bool,
    pub(in crate::fs) create: bool,
    pub(in crate::fs) create_new: bool,
    pub(in crate::fs) truncate: bool,
}

/// What a handle may do.
#[derive(Clone, Copy)]
pub(in crate::fs) struct Access {
    read: bool,
    write: bool,
    append: bool,
}

/// A handle on a file. Clones made with [`MemFile::try_clone`] share the cursor
/// as `dup` does.
pub(crate) struct MemFile {
    node: Arc<FileNode>,
    cursor: Arc<Mutex<u64>>,
    access: Access,
    tree: Weak<Tree>,
    /// The path below the root, when the file is a log segment: an append
    /// to one can take an armed power cut.
    segment: Option<Arc<Path>>,
}

impl std::fmt::Debug for MemFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemFile").finish_non_exhaustive()
    }
}

impl MemFile {
    pub(super) fn new(
        node: Arc<FileNode>,
        flags: Flags,
        tree: Weak<Tree>,
        segment: Option<Arc<Path>>,
    ) -> Self {
        Self {
            node,
            cursor: Arc::new(Mutex::new(0)),
            access: Access {
                read: flags.read,
                write: flags.write,
                append: flags.append,
            },
            tree,
            segment,
        }
    }

    #[cfg(test)]
    pub(super) fn node(&self) -> &FileNode {
        &self.node
    }

    pub(crate) fn len(&self) -> u64 {
        self.node.data.lock().current.len() as u64
    }

    pub(crate) fn try_clone(&self) -> Self {
        Self {
            node: Arc::clone(&self.node),
            cursor: Arc::clone(&self.cursor),
            access: self.access,
            tree: self.tree.clone(),
            segment: self.segment.clone(),
        }
    }

    /// Read at `at` without moving the cursor; short only at the end.
    pub(crate) fn read_at(&self, buf: &mut [u8], at: u64) -> io::Result<usize> {
        self.readable()?;
        Ok(read_from(&self.node.data.lock().current, buf, at))
    }

    /// Write all of `buf` at `at` without moving the cursor.
    pub(crate) fn write_at(&self, buf: &[u8], at: u64) -> io::Result<()> {
        self.writable()?;
        let at = position(at)?;
        let append = {
            let mut data = self.node.data.lock();
            let append = data.appends_at(at);
            data.write_at(at, buf);
            append
        };
        self.appended(append, at, buf.len());
        Ok(())
    }

    pub(crate) fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.readable()?;
        let mut cursor = self.cursor.lock();
        let read = read_from(&self.node.data.lock().current, buf, *cursor);
        *cursor += read as u64;
        Ok(read)
    }

    pub(crate) fn write(&self, buf: &[u8]) -> io::Result<usize> {
        self.writable()?;
        let mut cursor = self.cursor.lock();
        let mut data = self.node.data.lock();
        if self.access.append {
            *cursor = data.current.len() as u64;
        }
        let at = position(*cursor)?;
        let append = data.appends_at(at);
        data.write_at(at, buf);
        *cursor += buf.len() as u64;
        drop((data, cursor));
        self.appended(append, at, buf.len());
        Ok(buf.len())
    }

    /// Give an armed power cut its chance at an append to a segment. A
    /// segment's header is written at its start, so `at > 0` leaves header
    /// writes alone.
    fn appended(&self, append: bool, at: usize, len: usize) {
        if append
            && at > 0
            && len > 0
            && let Some(path) = &self.segment
            && let Some(tree) = self.tree.upgrade()
        {
            tree.appended(&self.node, path, at, at + len);
        }
    }

    pub(crate) fn seek(&self, to: SeekFrom) -> io::Result<u64> {
        let mut cursor = self.cursor.lock();
        let next = match to {
            SeekFrom::Start(at) => Some(at),
            SeekFrom::Current(by) => cursor.checked_add_signed(by),
            SeekFrom::End(by) => self.len().checked_add_signed(by),
        };
        *cursor = next.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before the start of the file",
            )
        })?;
        Ok(*cursor)
    }

    pub(crate) fn set_len(&self, len: u64) -> io::Result<()> {
        self.writable()?;
        self.node.data.lock().set_len(position(len)?);
        Ok(())
    }

    /// Reserve blocks for `len` bytes from `offset`. The length stays.
    pub(crate) fn preallocate(&self, offset: u64, len: u64) {
        let mut data = self.node.data.lock();
        data.reserved = data.reserved.max(offset.saturating_add(len));
    }

    /// Make the file's bytes and length durable, unless the tree has a
    /// flush failure armed for this one.
    pub(crate) fn sync(&self) -> io::Result<()> {
        if let Some(tree) = self.tree.upgrade() {
            tree.before_flush()?;
        }
        self.node.data.lock().sync();
        Ok(())
    }

    fn readable(&self) -> io::Result<()> {
        if self.access.read {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "file not opened for reading",
            ))
        }
    }

    fn writable(&self) -> io::Result<()> {
        if self.access.write || self.access.append {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "file not opened for writing",
            ))
        }
    }
}

fn read_from(bytes: &[u8], buf: &mut [u8], at: u64) -> usize {
    let Some(rest) = usize::try_from(at).ok().and_then(|at| bytes.get(at..)) else {
        return 0;
    };
    let n = rest.len().min(buf.len());
    buf[..n].copy_from_slice(&rest[..n]);
    n
}

fn position(at: u64) -> io::Result<usize> {
    usize::try_from(at).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset too large"))
}
