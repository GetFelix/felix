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
//! not flushed are kept as a prefix, oldest first.

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
    }
}

/// What the unflushed renames decide, before any name is looked at.
pub(super) struct Plan {
    /// For each name a rename touched, what it holds after the crash. `None`
    /// inside means the durable listing's entry stands.
    touched: BTreeMap<(usize, OsString), Option<Option<Node>>>,
}

impl Plan {
    pub(super) fn new(pending: &[Rename], rng: &mut SplitMix64) -> Self {
        // A prefix of the renames made it. The rest left their names as the
        // last flush had them.
        let kept = rng.below(pending.len() as u64 + 1) as usize;
        let mut touched = BTreeMap::new();
        for (index, rename) in pending.iter().enumerate() {
            let applied = index < kept;
            for (dir, name, now) in [
                (&rename.from_dir, &rename.from_name, None),
                (&rename.to_dir, &rename.to_name, Some(rename.node.clone())),
            ] {
                let entry = touched
                    .entry((Arc::as_ptr(dir) as usize, name.clone()))
                    .or_insert(None);
                if applied {
                    *entry = Some(now);
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

    fn get(&self, dir: &Arc<DirNode>, name: &OsString) -> Option<&Option<Option<Node>>> {
        self.touched.get(&(Arc::as_ptr(dir) as usize, name.clone()))
    }
}

/// The post-reboot copy of `dir`: everything in it is durable.
pub(super) fn dir(
    old: &Arc<DirNode>,
    plan: &Plan,
    rng: &mut SplitMix64,
    writeback: Writeback,
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
            Some(Some(after)) => after.as_ref(),
            Some(None) => data.durable.get(name),
            None => match (data.current.get(name), data.durable.get(name)) {
                (Some(now), Some(then)) if same(now, then) => Some(now),
                (Some(now), Some(then)) => Some(if rng.coin() { now } else { then }),
                (Some(now), None) => rng.coin().then_some(now),
                (None, Some(then)) => rng.coin().then_some(then),
                (None, None) => None,
            },
        };
        let node = match chosen {
            None => continue,
            Some(Node::Dir(child)) => Node::Dir(dir(child, plan, rng, writeback)),
            Some(Node::File(file)) => {
                // Only the file the name holds now has bytes beyond its last
                // flush; one that was replaced or unlinked comes back as it
                // was flushed.
                let holds_it = matches!(data.current.get(name), Some(Node::File(live)) if Arc::ptr_eq(live, file));
                Node::File(Arc::new(FileNode::with_bytes(contents(
                    file, holds_it, rng, writeback,
                ))))
            }
        };
        kept.current.insert(name.clone(), node);
    }
    kept.durable = kept.current.clone();
    Arc::new(DirNode {
        data: parking_lot::Mutex::new(kept),
    })
}

fn contents(file: &FileNode, live: bool, rng: &mut SplitMix64, writeback: Writeback) -> Vec<u8> {
    let data = file.data.lock();
    if live {
        crash_contents(&data.durable, &data.current, rng, writeback)
    } else {
        data.durable.clone()
    }
}

fn same(a: &Node, b: &Node) -> bool {
    match (a, b) {
        (Node::File(a), Node::File(b)) => Arc::ptr_eq(a, b),
        (Node::Dir(a), Node::Dir(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}
