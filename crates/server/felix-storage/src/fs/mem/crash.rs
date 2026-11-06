//! What a power loss leaves of a tree.
//!
//! The same rules as the Linux observer in `io::power_loss`: a name whose
//! entry changed since its directory was last flushed comes back either way,
//! and a file keeps its flushed bytes plus whichever unflushed pages the
//! seeded generator lets through, whole or torn on sector boundaries.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::sync::Arc;

use super::{DirData, DirNode, FileNode, Node};
use crate::io::writeback::{SplitMix64, Writeback, crash_contents};

/// The post-reboot copy of `dir`: everything in it is durable.
pub(super) fn dir(old: &Arc<DirNode>, rng: &mut SplitMix64, writeback: Writeback) -> Arc<DirNode> {
    let old = old.data.lock();
    let names: BTreeSet<&OsString> = old.durable.keys().chain(old.current.keys()).collect();
    let mut kept = DirData::default();
    for name in names {
        let chosen = match (old.current.get(name), old.durable.get(name)) {
            (Some(now), Some(then)) if same(now, then) => Some(now),
            (Some(now), Some(then)) => Some(if rng.coin() { now } else { then }),
            (Some(now), None) => rng.coin().then_some(now),
            (None, Some(then)) => rng.coin().then_some(then),
            (None, None) => None,
        };
        let node = match chosen {
            None => continue,
            Some(Node::Dir(child)) => Node::Dir(dir(child, rng, writeback)),
            Some(Node::File(file)) => {
                // Only the file the name holds now has bytes beyond its last
                // flush; one that was replaced or unlinked comes back as it
                // was flushed.
                let holds_it = matches!(old.current.get(name), Some(Node::File(live)) if Arc::ptr_eq(live, file));
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
