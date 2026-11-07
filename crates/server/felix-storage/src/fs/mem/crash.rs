//! What a power loss leaves of a tree.
//!
//! The same rules as the Linux observer in `io::power_loss`: a name whose
//! entry changed since its directory was last flushed comes back either way,
//! and a file keeps its flushed bytes plus whichever unflushed pages the
//! seeded generator lets through, whole or torn on sector boundaries.
//!
//! A rename is the exception to "per name". It is one change, so its two
//! names come back together: the source and target of an unflushed rename
//! are either both as before it or both as after it, and renames that were
//! not flushed are kept as a prefix, oldest first. A change to one of those
//! names after the rename, or an earlier unflushed create of it, still gets
//! its own choice.
//!
//! Creates and unlinks are not ordered against each other here, which POSIX
//! allows. ext4 and xfs journal all metadata in order, so they can leave
//! fewer states than this model does.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::sync::Arc;

use super::{DirData, DirNode, FileNode, Node};
use crate::io::writeback::{SplitMix64, Writeback, crash_contents};

/// A rename no flush has covered yet.
pub(super) struct Rename {
    pub(super) from_dir: Arc<DirNode>,
    pub(super) from_name: OsString,
    pub(super) to_dir: Arc<DirNode>,
    pub(super) to_name: OsString,
    pub(super) node: Node,
    /// What the target name held before, if anything.
    pub(super) replaced: Option<Node>,
    /// Each directory on the way to both parents with its entry in its own
    /// parent. Committing the rename commits those too, since the journal
    /// made the directories before it moved a file into them.
    pub(super) chains: Vec<(Arc<DirNode>, OsString, Node)>,
}

impl Rename {
    pub(super) fn touches(&self, dir: &Arc<DirNode>) -> bool {
        Arc::ptr_eq(&self.from_dir, dir) || Arc::ptr_eq(&self.to_dir, dir)
    }
}

/// Make the first `count` renames durable: the entries they changed are
/// updated in the durable listing of both directories.
pub(super) fn commit(pending: &mut Vec<Rename>, count: usize) {
    for rename in pending.drain(..count) {
        rename
            .from_dir
            .data
            .lock()
            .durable
            .remove(&rename.from_name);
        rename
            .to_dir
            .data
            .lock()
            .durable
            .insert(rename.to_name, rename.node);
        for (parent, name, child) in rename.chains {
            // A directory that was removed since stays out, even when its
            // parent was synced after the rename. One that was only renamed
            // on is still where the rename left it.
            if matches!(&child, Node::Dir(dir) if dir.data.lock().removed) {
                continue;
            }
            parent.data.lock().durable.insert(name, child);
        }
    }
}

/// What the unflushed renames decide, before any name is looked at.
pub(super) struct Plan {
    touched: BTreeMap<(usize, OsString), Touch>,
}

/// What the renames that touched one name say about it.
#[derive(Default)]
struct Touch {
    /// The entry after the last rename that was kept. `None`: no rename that
    /// touched the name was kept, so the durable listing stands.
    kept: Option<Option<Node>>,
    /// The entry just before the first rename that was dropped. `None`: every
    /// rename that touched the name was kept.
    before_dropped: Option<Option<Node>>,
}

impl Plan {
    pub(super) fn new(pending: &[Rename], rng: &mut SplitMix64) -> Self {
        // A prefix of the renames made it. The rest left their names as the
        // last flush had them.
        let kept = rng.below(pending.len() as u64 + 1) as usize;
        let mut touched: BTreeMap<(usize, OsString), Touch> = BTreeMap::new();
        for (index, rename) in pending.iter().enumerate() {
            let applied = index < kept;
            for (dir, name, after, before) in [
                (
                    &rename.from_dir,
                    &rename.from_name,
                    None,
                    Some(rename.node.clone()),
                ),
                (
                    &rename.to_dir,
                    &rename.to_name,
                    Some(rename.node.clone()),
                    rename.replaced.clone(),
                ),
            ] {
                let touch = touched
                    .entry((Arc::as_ptr(dir) as usize, name.clone()))
                    .or_default();
                if applied {
                    touch.kept = Some(after);
                } else if touch.before_dropped.is_none() {
                    touch.before_dropped = Some(before);
                }
            }
        }
        Self { touched }
    }

    /// The names a rename touched in `dir`, which may be in neither listing
    /// (the middle name of a chain).
    fn names_in<'a>(&'a self, dir: &Arc<DirNode>) -> impl Iterator<Item = &'a OsString> {
        let at = Arc::as_ptr(dir) as usize;
        self.touched
            .range((at, OsString::new())..)
            .take_while(move |((key, _), _)| *key == at)
            .map(|((_, name), _)| name)
    }

    fn get(&self, dir: &Arc<DirNode>, name: &OsString) -> Option<&Touch> {
        self.touched.get(&(Arc::as_ptr(dir) as usize, name.clone()))
    }
}

/// The post-reboot copy of `dir`: everything in it is durable.
/// `torn` names one file whose contents are already decided, with them.
pub(super) fn dir(
    old: &Arc<DirNode>,
    plan: &Plan,
    rng: &mut SplitMix64,
    writeback: Writeback,
    torn: Option<(&FileNode, &[u8])>,
) -> Arc<DirNode> {
    let data = old.data.lock();
    let names: BTreeSet<&OsString> = data
        .durable
        .keys()
        .chain(data.current.keys())
        .chain(plan.names_in(old))
        .collect();
    let mut kept = DirData::default();
    for name in names {
        let chosen = match plan.get(old, name) {
            Some(touch) => {
                let base = match &touch.kept {
                    Some(after) => alive(after.as_ref()),
                    None => alive(data.durable.get(name)),
                };
                // Dropped: the name may still show what it held just before
                // the rename, such as a temporary file created without a
                // directory flush. Kept: a later create or unlink of the
                // name is its own change.
                let other = match &touch.before_dropped {
                    Some(before) => alive(before.as_ref()),
                    None => alive(data.current.get(name)),
                };
                pick(base, other, rng)
            }
            None => match (alive(data.current.get(name)), alive(data.durable.get(name))) {
                (Some(now), Some(then)) if same(now, then) => Some(now),
                (Some(now), Some(then)) => Some(if rng.coin() { now } else { then }),
                (Some(now), None) => rng.coin().then_some(now),
                (None, Some(then)) => rng.coin().then_some(then),
                (None, None) => None,
            },
        };
        let node = match chosen {
            None => continue,
            Some(Node::Dir(child)) => Node::Dir(dir(child, plan, rng, writeback, torn)),
            Some(Node::File(file)) => Node::File(Arc::new(FileNode::with_bytes(contents(
                file, rng, writeback, torn,
            )))),
        };
        kept.current.insert(name.clone(), node);
    }
    kept.durable = kept.current.clone();
    Arc::new(DirNode {
        data: parking_lot::Mutex::new(kept),
    })
}

/// `node`, unless it is a file whose unlink a sync already committed.
fn alive(node: Option<&Node>) -> Option<&Node> {
    node.filter(|node| !matches!(node, Node::File(file) if file.data.lock().gone))
}

/// What the file reads back as. One with no name left only has the bytes of
/// its last flush.
fn contents(
    file: &FileNode,
    rng: &mut SplitMix64,
    writeback: Writeback,
    torn: Option<(&FileNode, &[u8])>,
) -> Vec<u8> {
    let data = file.data.lock();
    if let Some((torn, bytes)) = torn
        && std::ptr::eq(torn, file)
        && !data.unlinked
    {
        bytes.to_vec()
    } else if !data.unlinked {
        crash_contents(&data.durable, &data.current, rng, writeback)
    } else {
        data.durable.clone()
    }
}

/// `base`, or `other` if it differs and a coin says so.
fn pick<'a>(
    base: Option<&'a Node>,
    other: Option<&'a Node>,
    rng: &mut SplitMix64,
) -> Option<&'a Node> {
    let alike = match (base, other) {
        (Some(a), Some(b)) => same(a, b),
        (None, None) => true,
        _ => false,
    };
    if alike || !rng.coin() { base } else { other }
}

fn same(a: &Node, b: &Node) -> bool {
    match (a, b) {
        (Node::File(a), Node::File(b)) => Arc::ptr_eq(a, b),
        (Node::Dir(a), Node::Dir(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}
