//! Which paths live in memory.
//!
//! A simulation registers the directory it gives each broker; a path under
//! one goes to that root's tree and every other path stays on disk. The
//! registry is process-wide because storage code reaches the filesystem by
//! path, with nothing to carry a handle through. Roots are keyed by path, so
//! simulations running side by side on different threads never meet.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::RwLock;

use super::Tree;

static ROOTS: RwLock<BTreeMap<PathBuf, Arc<Tree>>> = RwLock::new(BTreeMap::new());
/// Lets the common case, no simulation in the process, skip the lock.
static ANY: AtomicBool = AtomicBool::new(false);

/// A path inside a registered root.
pub(in crate::fs) struct Routed {
    pub(in crate::fs) tree: Arc<Tree>,
    /// The path below the root.
    pub(in crate::fs) rel: PathBuf,
}

/// Put `root` and everything below it in memory, empty. Replaces any earlier
/// registration of the same path.
pub(crate) fn register(root: &Path) {
    ROOTS.write().insert(root.to_path_buf(), Tree::new());
    ANY.store(true, Ordering::Release);
}

/// Register `root` as [`register`] does, unless it is registered already.
/// Returns whether it was free.
pub(crate) fn claim(root: &Path) -> bool {
    let mut roots = ROOTS.write();
    if roots.contains_key(root) {
        return false;
    }
    roots.insert(root.to_path_buf(), Tree::new());
    ANY.store(true, Ordering::Release);
    true
}

/// Forget `root` and drop its files.
pub(crate) fn unregister(root: &Path) {
    let mut roots = ROOTS.write();
    roots.remove(root);
    ANY.store(!roots.is_empty(), Ordering::Release);
}

/// The tree `path` lives in, if it is under a registered root.
pub(in crate::fs) fn route(path: &Path) -> Option<Routed> {
    if !ANY.load(Ordering::Acquire) {
        return None;
    }
    let roots = ROOTS.read();
    path.ancestors().find_map(|ancestor| {
        let tree = roots.get(ancestor)?;
        Some(Routed {
            tree: Arc::clone(tree),
            rel: path.strip_prefix(ancestor).ok()?.to_path_buf(),
        })
    })
}

/// Whether `path` is a directory above a registered root. Flushing it has
/// nothing to flush, since the entry that leads to a simulated root is not
/// simulated.
pub(in crate::fs) fn is_above_a_root(path: &Path) -> bool {
    ANY.load(Ordering::Acquire) && ROOTS.read().keys().any(|root| root.starts_with(path))
}

/// Whether `path` is under a registered root, so in memory.
pub(crate) fn in_memory(path: &Path) -> bool {
    route(path).is_some()
}

/// The tree registered at exactly `root`.
pub(crate) fn tree_at(root: &Path) -> Option<Arc<Tree>> {
    ROOTS.read().get(root).cloned()
}
