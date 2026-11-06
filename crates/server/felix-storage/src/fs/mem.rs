//! The in-memory tree a simulation keeps a broker's data directory in.
//!
//! Every file has two byte strings: what the page cache holds (`current`,
//! what reads see) and what the last flush of that file made durable. Every
//! directory has the same pair for its entries. Nothing here is ever slow, so
//! a run that creates and renames thousands of segments costs no real I/O,
//! and [`Tree::crash`] can then say what a power loss at this instant leaves.
//!
//! One lock (`Tree::structure`) covers every change to the directory
//! structure, so a rename or a crash never sees half of another. File data has
//! its own lock, taken after it, so reads and writes of different files do not
//! queue behind each other.

mod crash;
mod file;
mod registry;

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::io;
use std::path::{Component, Path};
use std::sync::Arc;

use parking_lot::Mutex;

pub(super) use file::Flags;
pub(crate) use file::MemFile;
pub(super) use registry::{is_above_a_root, route};
pub(crate) use registry::{register, tree_at, unregister};

/// One registered root and everything below it.
pub(crate) struct Tree {
    /// Held for the length of every structural operation.
    structure: Mutex<Structure>,
}

struct Structure {
    root: Arc<DirNode>,
    /// Renames no flush has covered yet, oldest first. A rename is one change
    /// to the device, so a crash keeps all of it or none of it.
    pending: Vec<crash::Rename>,
}

/// How many unflushed renames are remembered. Past it the oldest is taken as
/// flushed, which keeps a tree nobody syncs from holding on to every file it
/// ever replaced.
const MAX_PENDING_RENAMES: usize = 1024;

/// Something with a name in a directory.
#[derive(Clone)]
pub(super) enum Node {
    File(Arc<FileNode>),
    Dir(Arc<DirNode>),
}

/// A file: its bytes now and its bytes as of the last flush.
pub(super) struct FileNode {
    data: Mutex<FileData>,
}

struct FileData {
    current: Vec<u8>,
    durable: Vec<u8>,
    /// Bytes before this offset are the same in `current` and `durable`, so a
    /// flush copies only what is past it instead of the whole file.
    dirty_from: usize,
    /// The furthest end of any preallocation. Reserving never changes the
    /// length: recovery reads "valid bytes end at EOF".
    reserved: u64,
}

/// A directory: its entries now and as of the last flush of the directory.
pub(super) struct DirNode {
    data: Mutex<DirData>,
}

#[derive(Default)]
struct DirData {
    current: BTreeMap<OsString, Node>,
    durable: BTreeMap<OsString, Node>,
}

/// What a stat reports.
#[derive(Clone, Copy)]
pub(crate) struct Stat {
    pub(super) len: u64,
    pub(super) is_dir: bool,
}

impl Tree {
    pub(super) fn new() -> Self {
        Self {
            structure: Mutex::new(Structure {
                root: Arc::new(DirNode::default()),
                pending: Vec::new(),
            }),
        }
    }

    pub(super) fn stat(&self, path: &Path) -> io::Result<Stat> {
        let state = self.structure.lock();
        Ok(match walk(&state.root, &names(path)?)? {
            Node::File(file) => Stat {
                len: file.data.lock().current.len() as u64,
                is_dir: false,
            },
            Node::Dir(_) => Stat {
                len: 0,
                is_dir: true,
            },
        })
    }

    /// The names in a directory, in name order, with whether each is a directory.
    pub(super) fn list(&self, path: &Path) -> io::Result<Vec<(OsString, bool)>> {
        let state = self.structure.lock();
        let Node::Dir(dir) = walk(&state.root, &names(path)?)? else {
            return Err(kind(io::ErrorKind::NotADirectory, "not a directory"));
        };
        let data = dir.data.lock();
        Ok(data
            .current
            .iter()
            .map(|(name, node)| (name.clone(), matches!(node, Node::Dir(_))))
            .collect())
    }

    pub(super) fn open(&self, path: &Path, flags: file::Flags) -> io::Result<MemFile> {
        if (flags.create || flags.create_new || flags.truncate) && !(flags.write || flags.append) {
            return Err(kind(io::ErrorKind::InvalidInput, "invalid open flags"));
        }
        let state = self.structure.lock();
        let names = names(path)?;
        let (parent, name) = parent_of(&state.root, &names)?;
        let mut entries = parent.data.lock();
        let node = match entries.current.get(name) {
            Some(Node::File(file)) => {
                if flags.create_new {
                    return Err(kind(io::ErrorKind::AlreadyExists, "file exists"));
                }
                if flags.truncate {
                    let mut data = file.data.lock();
                    data.current.clear();
                    data.dirty_from = 0;
                }
                Arc::clone(file)
            }
            Some(Node::Dir(_)) => return Err(kind(io::ErrorKind::IsADirectory, "is a directory")),
            None if flags.create || flags.create_new => {
                let file = Arc::new(FileNode::default());
                entries
                    .current
                    .insert(name.to_owned(), Node::File(Arc::clone(&file)));
                file
            }
            None => return Err(not_found()),
        };
        Ok(MemFile::new(node, flags))
    }

    pub(super) fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        let state = self.structure.lock();
        let mut dir = Arc::clone(&state.root);
        for name in names(path)? {
            let next = {
                let mut entries = dir.data.lock();
                match entries.current.get(name) {
                    Some(Node::Dir(next)) => Arc::clone(next),
                    Some(Node::File(_)) => {
                        return Err(kind(io::ErrorKind::NotADirectory, "not a directory"));
                    }
                    None => {
                        let next = Arc::new(DirNode::default());
                        entries
                            .current
                            .insert(name.to_owned(), Node::Dir(Arc::clone(&next)));
                        next
                    }
                }
            };
            dir = next;
        }
        Ok(())
    }

    pub(super) fn remove_file(&self, path: &Path) -> io::Result<()> {
        let state = self.structure.lock();
        let names = names(path)?;
        let (parent, name) = parent_of(&state.root, &names)?;
        let mut entries = parent.data.lock();
        match entries.current.get(name) {
            Some(Node::File(_)) => {
                entries.current.remove(name);
                Ok(())
            }
            Some(Node::Dir(_)) => Err(kind(io::ErrorKind::IsADirectory, "is a directory")),
            None => Err(not_found()),
        }
    }

    /// Unlinks every entry below `path`, deepest first, and then `path`
    /// itself. Each unlink is its own directory change, so a crash can keep
    /// any subset of them.
    pub(super) fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        let state = self.structure.lock();
        let names = names(path)?;
        let (parent, name) = parent_of(&state.root, &names)?;
        let mut entries = parent.data.lock();
        match entries.current.get(name) {
            Some(Node::Dir(dir)) => unlink_all(dir),
            Some(Node::File(_)) => {
                return Err(kind(io::ErrorKind::NotADirectory, "not a directory"));
            }
            None => return Err(not_found()),
        }
        entries.current.remove(name);
        Ok(())
    }

    pub(super) fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut state = self.structure.lock();
        let (from_names, to_names) = (names(from)?, names(to)?);
        if from_names == to_names {
            walk(&state.root, &from_names)?;
            return Ok(());
        }
        if to_names.starts_with(&from_names) {
            return Err(kind(
                io::ErrorKind::InvalidInput,
                "a directory cannot move into itself",
            ));
        }
        let (from_parent, from_name) = parent_of(&state.root, &from_names)?;
        let (to_parent, to_name) = parent_of(&state.root, &to_names)?;
        let same = Arc::ptr_eq(&from_parent, &to_parent);
        let mut source = from_parent.data.lock();
        let mut other = (!same).then(|| to_parent.data.lock());
        let moving = source
            .current
            .get(from_name)
            .cloned()
            .ok_or_else(not_found)?;
        let target = other.as_deref().unwrap_or(&source).current.get(to_name);
        match (&moving, target) {
            (Node::File(_), Some(Node::Dir(_))) => {
                return Err(kind(io::ErrorKind::IsADirectory, "is a directory"));
            }
            (Node::Dir(_), Some(Node::File(_))) => {
                return Err(kind(io::ErrorKind::NotADirectory, "not a directory"));
            }
            (Node::Dir(_), Some(Node::Dir(existing)))
                if !existing.data.lock().current.is_empty() =>
            {
                return Err(kind(
                    io::ErrorKind::DirectoryNotEmpty,
                    "directory not empty",
                ));
            }
            _ => {}
        }
        source.current.remove(from_name);
        let destination = other.as_deref_mut().unwrap_or(&mut source);
        destination
            .current
            .insert(to_name.to_owned(), moving.clone());
        drop((source, other));
        state.pending.push(crash::Rename {
            from_dir: from_parent,
            from_name: from_name.to_owned(),
            to_dir: to_parent,
            to_name: to_name.to_owned(),
            node: moving,
        });
        if state.pending.len() > MAX_PENDING_RENAMES {
            crash::commit(&mut state.pending, 1);
        }
        Ok(())
    }

    /// Make the entries of the directory at `path` durable, as `fsync` on the
    /// directory does.
    pub(super) fn sync_dir(&self, path: &Path) -> io::Result<()> {
        let mut state = self.structure.lock();
        let Node::Dir(dir) = walk(&state.root, &names(path)?)? else {
            return Err(kind(io::ErrorKind::NotADirectory, "not a directory"));
        };
        // A flush of either directory commits the rename, and every change
        // before it, as a journal does.
        if let Some(last) = state
            .pending
            .iter()
            .rposition(|rename| rename.touches(&dir))
        {
            crash::commit(&mut state.pending, last + 1);
        }
        let mut data = dir.data.lock();
        data.durable = data.current.clone();
        Ok(())
    }

    /// Make everything durable, as `syncfs` does for a filesystem.
    pub(super) fn sync_all(&self) {
        let mut state = self.structure.lock();
        state.pending.clear();
        sync_tree(&state.root);
    }

    /// Replace the tree with what a power loss now would leave. Handles opened
    /// before it keep the old files, which nothing reaches any more.
    pub(crate) fn crash(&self, seed: u64, writeback: crate::io::writeback::Writeback) {
        let mut state = self.structure.lock();
        let mut rng = crate::io::writeback::SplitMix64::new(seed);
        let plan = crash::Plan::new(&state.pending, &mut rng);
        state.root = crash::dir(&state.root, &plan, &mut rng, writeback);
        state.pending.clear();
    }
}

impl Default for DirNode {
    fn default() -> Self {
        Self {
            data: Mutex::new(DirData::default()),
        }
    }
}

impl Default for FileNode {
    fn default() -> Self {
        Self::with_bytes(Vec::new())
    }
}

impl FileNode {
    /// A file whose bytes are all on the device.
    fn with_bytes(bytes: Vec<u8>) -> Self {
        Self {
            data: Mutex::new(FileData {
                current: bytes.clone(),
                durable: bytes,
                dirty_from: usize::MAX,
                reserved: 0,
            }),
        }
    }
}

impl FileData {
    /// Make the file's bytes and length durable.
    fn sync(&mut self) {
        let from = self
            .dirty_from
            .min(self.durable.len())
            .min(self.current.len());
        self.durable.truncate(from);
        self.durable.extend_from_slice(&self.current[from..]);
        self.dirty_from = usize::MAX;
    }

    /// Write `buf` at `at`, zero-filling any gap, like `pwrite` past the end.
    fn write_at(&mut self, at: usize, buf: &[u8]) {
        let old = self.current.len();
        if at > old {
            self.current.resize(at, 0);
        }
        let end = at + buf.len();
        if end > self.current.len() {
            self.current.resize(end, 0);
        }
        self.current[at..end].copy_from_slice(buf);
        self.dirty_from = self.dirty_from.min(at.min(old));
    }

    fn set_len(&mut self, len: usize) {
        let old = self.current.len();
        self.current.resize(len, 0);
        self.dirty_from = self.dirty_from.min(len.min(old));
    }
}

/// Unlink everything below `dir` from the leaves up.
fn unlink_all(dir: &Arc<DirNode>) {
    let children: Vec<(OsString, Node)> = dir
        .data
        .lock()
        .current
        .iter()
        .map(|(name, node)| (name.clone(), node.clone()))
        .collect();
    for (name, node) in children {
        if let Node::Dir(child) = &node {
            unlink_all(child);
        }
        dir.data.lock().current.remove(&name);
    }
}

fn sync_tree(dir: &Arc<DirNode>) {
    let mut data = dir.data.lock();
    data.durable = data.current.clone();
    for node in data.current.values() {
        match node {
            Node::File(file) => file.data.lock().sync(),
            Node::Dir(child) => sync_tree(child),
        }
    }
}

/// The names `path` is made of, with `.` and `..` resolved.
fn names(path: &Path) -> io::Result<Vec<&OsStr>> {
    let mut out = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(name) => out.push(name),
            Component::CurDir | Component::RootDir => {}
            Component::ParentDir => {
                if out.pop().is_none() {
                    return Err(kind(
                        io::ErrorKind::InvalidInput,
                        "path climbs out of the simulated root",
                    ));
                }
            }
            Component::Prefix(_) => {
                return Err(kind(io::ErrorKind::InvalidInput, "unsupported path"));
            }
        }
    }
    Ok(out)
}

fn walk(root: &Arc<DirNode>, names: &[&OsStr]) -> io::Result<Node> {
    let mut node = Node::Dir(Arc::clone(root));
    for name in names {
        let next = match &node {
            Node::Dir(dir) => dir.data.lock().current.get(*name).cloned(),
            Node::File(_) => return Err(kind(io::ErrorKind::NotADirectory, "not a directory")),
        };
        node = next.ok_or_else(not_found)?;
    }
    Ok(node)
}

/// The directory holding the last name, and that name.
fn parent_of<'a>(
    root: &Arc<DirNode>,
    names: &[&'a OsStr],
) -> io::Result<(Arc<DirNode>, &'a OsStr)> {
    let Some((name, parents)) = names.split_last() else {
        return Err(kind(io::ErrorKind::InvalidInput, "the root has no parent"));
    };
    match walk(root, parents)? {
        Node::Dir(dir) => Ok((dir, name)),
        Node::File(_) => Err(kind(io::ErrorKind::NotADirectory, "not a directory")),
    }
}

fn not_found() -> io::Error {
    kind(io::ErrorKind::NotFound, "no such file or directory")
}

fn kind(kind: io::ErrorKind, message: &'static str) -> io::Error {
    io::Error::new(kind, message)
}

#[cfg(test)]
mod tests;
