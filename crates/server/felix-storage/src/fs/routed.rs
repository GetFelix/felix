//! The filesystem as the simulator builds it: each call goes to the in-memory
//! tree when its path is under a registered root, and to the OS otherwise.
//!
//! The names and signatures follow `std::fs` for what storage uses, so the
//! build without the `sim` feature can re-export `std::fs` and call sites
//! read the same either way.

use std::ffi::OsString;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::mem::{self, MemFile, Stat};

/// An open file, in memory or on disk, chosen when it was opened.
#[derive(Debug)]
pub(crate) enum File {
    Real(std::fs::File),
    Mem(MemFile),
}

impl File {
    pub(crate) fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new().read(true).open(path)
    }

    pub(crate) fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
    }

    pub(crate) fn metadata(&self) -> io::Result<Metadata> {
        match self {
            Self::Real(file) => file.metadata().map(Metadata::Real),
            Self::Mem(file) => Ok(Metadata::Mem(Stat {
                len: file.len(),
                is_dir: false,
            })),
        }
    }

    pub(crate) fn set_len(&self, len: u64) -> io::Result<()> {
        match self {
            Self::Real(file) => file.set_len(len),
            Self::Mem(file) => file.set_len(len),
        }
    }

    pub(crate) fn try_clone(&self) -> io::Result<Self> {
        match self {
            Self::Real(file) => file.try_clone().map(Self::Real),
            Self::Mem(file) => Ok(Self::Mem(file.try_clone())),
        }
    }

    /// The OS file, for the calls that need a descriptor. `None` for a file in
    /// memory, which has none.
    pub(crate) fn os_file(&self) -> Option<&std::fs::File> {
        match self {
            Self::Real(file) => Some(file),
            Self::Mem(_) => None,
        }
    }

    pub(crate) fn as_mem(&self) -> Option<&MemFile> {
        match self {
            Self::Real(_) => None,
            Self::Mem(file) => Some(file),
        }
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&*self).read(buf)
    }
}

impl Read for &File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            File::Real(file) => (&*file).read(buf),
            File::Mem(file) => file.read(buf),
        }
    }
}

impl Write for File {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&*self).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&*self).flush()
    }
}

impl Write for &File {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            File::Real(file) => (&*file).write(buf),
            File::Mem(file) => file.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for File {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        (&*self).seek(to)
    }
}

impl Seek for &File {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        match self {
            File::Real(file) => (&*file).seek(to),
            File::Mem(file) => file.seek(to),
        }
    }
}

/// `std::fs::OpenOptions`, for either kind of file.
#[derive(Clone)]
pub(crate) struct OpenOptions {
    real: std::fs::OpenOptions,
    flags: mem::Flags,
}

impl OpenOptions {
    pub(crate) fn new() -> Self {
        Self {
            real: std::fs::OpenOptions::new(),
            flags: mem::Flags::default(),
        }
    }

    pub(crate) fn read(&mut self, on: bool) -> &mut Self {
        self.real.read(on);
        self.flags.read = on;
        self
    }

    pub(crate) fn write(&mut self, on: bool) -> &mut Self {
        self.real.write(on);
        self.flags.write = on;
        self
    }

    pub(crate) fn append(&mut self, on: bool) -> &mut Self {
        self.real.append(on);
        self.flags.append = on;
        self
    }

    pub(crate) fn create(&mut self, on: bool) -> &mut Self {
        self.real.create(on);
        self.flags.create = on;
        self
    }

    pub(crate) fn create_new(&mut self, on: bool) -> &mut Self {
        self.real.create_new(on);
        self.flags.create_new = on;
        self
    }

    pub(crate) fn truncate(&mut self, on: bool) -> &mut Self {
        self.real.truncate(on);
        self.flags.truncate = on;
        self
    }

    pub(crate) fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        let path = path.as_ref();
        match mem::route(path) {
            Some(routed) => routed.tree.open(&routed.rel, self.flags).map(File::Mem),
            None => self.real.open(path).map(File::Real),
        }
    }
}

/// What a stat reports.
pub(crate) enum Metadata {
    Real(std::fs::Metadata),
    Mem(Stat),
}

impl Metadata {
    pub(crate) fn len(&self) -> u64 {
        match self {
            Self::Real(meta) => meta.len(),
            Self::Mem(stat) => stat.len,
        }
    }

    pub(crate) fn is_dir(&self) -> bool {
        match self {
            Self::Real(meta) => meta.is_dir(),
            Self::Mem(stat) => stat.is_dir,
        }
    }
}

/// The kind of a directory entry.
pub(crate) enum FileType {
    Real(std::fs::FileType),
    Mem { is_dir: bool },
}

impl FileType {
    pub(crate) fn is_dir(&self) -> bool {
        match self {
            Self::Real(kind) => kind.is_dir(),
            Self::Mem { is_dir } => *is_dir,
        }
    }

    pub(crate) fn is_file(&self) -> bool {
        match self {
            Self::Real(kind) => kind.is_file(),
            Self::Mem { is_dir } => !is_dir,
        }
    }
}

/// One entry of a directory listing.
pub(crate) enum DirEntry {
    Real(std::fs::DirEntry),
    Mem {
        path: PathBuf,
        name: OsString,
        is_dir: bool,
    },
}

impl DirEntry {
    pub(crate) fn path(&self) -> PathBuf {
        match self {
            Self::Real(entry) => entry.path(),
            Self::Mem { path, .. } => path.clone(),
        }
    }

    pub(crate) fn file_name(&self) -> OsString {
        match self {
            Self::Real(entry) => entry.file_name(),
            Self::Mem { name, .. } => name.clone(),
        }
    }

    pub(crate) fn file_type(&self) -> io::Result<FileType> {
        match self {
            Self::Real(entry) => entry.file_type().map(FileType::Real),
            Self::Mem { is_dir, .. } => Ok(FileType::Mem { is_dir: *is_dir }),
        }
    }
}

/// The entries of a directory. A directory in memory lists in name order.
pub(crate) enum ReadDir {
    Real(std::fs::ReadDir),
    Mem {
        dir: PathBuf,
        entries: std::vec::IntoIter<(OsString, bool)>,
    },
}

impl Iterator for ReadDir {
    type Item = io::Result<DirEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Real(entries) => entries.next().map(|entry| entry.map(DirEntry::Real)),
            Self::Mem { dir, entries } => entries.next().map(|(name, is_dir)| {
                Ok(DirEntry::Mem {
                    path: dir.join(&name),
                    name,
                    is_dir,
                })
            }),
        }
    }
}

pub(crate) fn read_dir(path: impl AsRef<Path>) -> io::Result<ReadDir> {
    let path = path.as_ref();
    match mem::route(path) {
        Some(routed) => Ok(ReadDir::Mem {
            dir: path.to_path_buf(),
            entries: routed.tree.list(&routed.rel)?.into_iter(),
        }),
        None => std::fs::read_dir(path).map(ReadDir::Real),
    }
}

pub(crate) fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    let path = path.as_ref();
    match mem::route(path) {
        Some(routed) => routed.tree.stat(&routed.rel).map(Metadata::Mem),
        None => std::fs::metadata(path).map(Metadata::Real),
    }
}

pub(crate) fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    Ok(bytes)
}

pub(crate) fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
    String::from_utf8(read(path)?).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "stream did not contain valid UTF-8",
        )
    })
}

pub(crate) fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> io::Result<()> {
    File::create(path)?.write_all(contents.as_ref())
}

pub(crate) fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    match mem::route(path) {
        Some(routed) => routed.tree.create_dir_all(&routed.rel),
        None => std::fs::create_dir_all(path),
    }
}

pub(crate) fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    match mem::route(path) {
        Some(routed) => routed.tree.remove_file(&routed.rel),
        None => std::fs::remove_file(path),
    }
}

pub(crate) fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    match mem::route(path) {
        Some(routed) => routed.tree.remove_dir_all(&routed.rel),
        None => std::fs::remove_dir_all(path),
    }
}

/// A rename across the memory/disk boundary is refused: no real rename can
/// cross filesystems either, and storage never asks for one.
pub(crate) fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    let (from, to) = (from.as_ref(), to.as_ref());
    match (mem::route(from), mem::route(to)) {
        (Some(a), Some(b)) if std::sync::Arc::ptr_eq(&a.tree, &b.tree) => {
            a.tree.rename(&a.rel, &b.rel)
        }
        (None, None) => std::fs::rename(from, to),
        _ => Err(io::Error::new(
            io::ErrorKind::CrossesDevices,
            "rename between the simulated filesystem and the disk, or between two simulated roots",
        )),
    }
}

/// Flush the directory at `path` if it is simulated. `None` means it is on
/// disk and the caller flushes it with the OS.
pub(crate) fn sync_simulated_dir(path: &Path) -> Option<io::Result<()>> {
    if let Some(routed) = mem::route(path) {
        return Some(routed.tree.sync_dir(&routed.rel));
    }
    mem::is_above_a_root(path).then_some(Ok(()))
}

/// Flush everything under `root` if it is simulated; `None` if it is on disk.
pub(crate) fn sync_simulated_filesystem(root: &Path) -> Option<io::Result<()>> {
    let routed = mem::route(root)?;
    routed.tree.sync_all();
    Some(Ok(()))
}

/// Record that recovery repaired the torn tail of `segment`, if it is
/// simulated.
pub(crate) fn note_torn_tail(segment: &Path) {
    if let Some(routed) = mem::route(segment) {
        routed.tree.note_torn_tail(&routed.rel);
    }
}
