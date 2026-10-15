//! Which incarnation of a stream a store opens, and taking a deleted
//! incarnation's directories out of service.
//!
//! A stream deleted and created again under the same name is a different
//! stream. Its shard directories carry the incarnation the control plane gave
//! it (see `disk_log::layout`), so the new one can never open the old one's
//! records. Incarnation 0 is a stream from before incarnations existed, and
//! keeps the directory name it always had.
//!
//! Retiring renames a directory into `.deleted/` under the store's root and
//! deletes it from there in the background. The rename is the commit point: a
//! crash before it leaves the directory where it was, and a crash after it
//! leaves it where nothing opens it. A crash part-way through the delete
//! leaves debris only `.deleted/` holds, cleared at the next start.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::disk_log::layout;
use crate::io::{create_dir_all_durable, sync_dir};
use crate::{Result, StorageError};

pub(crate) mod marker;
pub use marker::{MarkerRead, StreamMarker};

/// Directory under a store's root that retired shard directories wait in.
/// Dotted, so it is never taken for a shard directory.
pub(crate) const DELETED_DIR: &str = ".deleted";

/// Tenant, namespace and stream (or cache) name.
type Scope = (String, String, String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Entry {
    Live(u64),
    Retired,
}

/// The incarnation each stream opens at, as the broker registered it.
///
/// A stream nobody registered opens at 0, which keeps a store usable on its
/// own (tools, tests), unless registration is required. A retired one opens
/// nothing until it is registered again: an open racing a delete must not
/// recreate the directory the delete just moved away.
#[derive(Debug, Default)]
pub(crate) struct Incarnations {
    streams: RwLock<HashMap<Scope, Entry>>,
    required: std::sync::atomic::AtomicBool,
}

impl Incarnations {
    /// The incarnation to open `tenant/namespace/name` at, or `None` while it
    /// is retired, or unregistered when registration is required.
    pub(crate) fn resolve(&self, tenant: &str, namespace: &str, name: &str) -> Option<u64> {
        let scope = (tenant.to_string(), namespace.to_string(), name.to_string());
        match self.streams.read().get(&scope) {
            None if self.required.load(Ordering::Relaxed) => None,
            None => Some(0),
            Some(Entry::Live(incarnation)) => Some(*incarnation),
            Some(Entry::Retired) => None,
        }
    }

    /// The incarnation set for `tenant/namespace/name`, if one is set and it
    /// is not retired.
    pub(crate) fn registered(&self, tenant: &str, namespace: &str, name: &str) -> Option<u64> {
        let scope = (tenant.to_string(), namespace.to_string(), name.to_string());
        match self.streams.read().get(&scope) {
            Some(Entry::Live(incarnation)) => Some(*incarnation),
            None | Some(Entry::Retired) => None,
        }
    }

    /// Open nothing for a stream until it is registered.
    pub(crate) fn require_registration(&self) {
        self.required.store(true, Ordering::Relaxed);
    }

    pub(crate) fn set(&self, tenant: &str, namespace: &str, name: &str, incarnation: u64) {
        let scope = (tenant.to_string(), namespace.to_string(), name.to_string());
        self.streams.write().insert(scope, Entry::Live(incarnation));
    }

    pub(crate) fn retire(&self, tenant: &str, namespace: &str, name: &str) {
        let scope = (tenant.to_string(), namespace.to_string(), name.to_string());
        self.streams.write().insert(scope, Entry::Retired);
    }
}

/// The shard directories under one store's root, by stream prefix, so finding
/// a stream's does not read the whole root each time: a cascade deleting
/// every stream of a namespace would otherwise read it once per stream.
///
/// Built from one read of the root the first time it is asked, then kept up
/// to date by the store as it creates and retires directories. The read runs
/// outside the lock; a directory noted while it runs is kept aside and added
/// once it is done.
#[derive(Debug, Default)]
pub(crate) struct DirIndex {
    state: parking_lot::Mutex<IndexState>,
}

#[derive(Debug, Default)]
struct IndexState {
    by_prefix: Option<HashMap<String, std::collections::BTreeSet<String>>>,
    /// Noted before the first read finished.
    pending: Vec<String>,
}

impl IndexState {
    fn insert(&mut self, name: &str) {
        match self.by_prefix.as_mut() {
            Some(index) => {
                if let Some(prefix) = layout::prefix_of(name) {
                    index
                        .entry(prefix.to_string())
                        .or_default()
                        .insert(name.to_string());
                }
            }
            None => self.pending.push(name.to_string()),
        }
    }
}

impl DirIndex {
    /// Note a directory the store is about to create, before it does, so
    /// one left by an open that failed part-way is found too.
    pub(crate) fn created(&self, name: &str) {
        self.state.lock().insert(name);
    }

    fn removed(&self, name: &str) {
        let mut state = self.state.lock();
        if let Some(index) = state.by_prefix.as_mut()
            && let Some(prefix) = layout::prefix_of(name)
            && let Some(names) = index.get_mut(prefix)
        {
            names.remove(name);
        }
    }

    /// Every directory of `tenant/namespace/stream` under `root`, with its
    /// shard and incarnation.
    pub(crate) fn stream_dirs(
        &self,
        root: &Path,
        tenant: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<Vec<(u32, u64, String)>> {
        if self.state.lock().by_prefix.is_none() {
            let read = read_root(root)?;
            let mut state = self.state.lock();
            if state.by_prefix.is_none() {
                state.by_prefix = Some(read);
                for name in std::mem::take(&mut state.pending) {
                    state.insert(&name);
                }
            }
        }
        let state = self.state.lock();
        let index = state.by_prefix.as_ref().expect("built above");
        let prefix = layout::stream_prefix(tenant, namespace, stream);
        Ok(index
            .get(&prefix)
            .into_iter()
            .flatten()
            .filter_map(|name| {
                layout::stream_shard_of(name, tenant, namespace, stream)
                    .map(|(shard, incarnation)| (shard, incarnation, name.clone()))
            })
            .collect())
    }

    /// Move every directory of one incarnation of `tenant/namespace/stream`
    /// under `root` out of service, whatever shards it has: the stream may
    /// have had more than it has now. Safe to repeat.
    pub(crate) fn retire_stream(
        &self,
        root: &Path,
        tenant: &str,
        namespace: &str,
        stream: &str,
        incarnation: u64,
    ) -> Result<()> {
        for (_, held, name) in self.stream_dirs(root, tenant, namespace, stream)? {
            if held == incarnation {
                retire_dir(root, &root.join(&name))?;
                self.removed(&name);
            }
        }
        Ok(())
    }
}

fn read_root(root: &Path) -> Result<HashMap<String, std::collections::BTreeSet<String>>> {
    let mut index: HashMap<String, std::collections::BTreeSet<String>> = HashMap::new();
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(index),
        Err(err) => return Err(StorageError::Io(err)),
    };
    for entry in entries {
        let entry = entry.map_err(StorageError::Io)?;
        if let Ok(name) = entry.file_name().into_string()
            && let Some(prefix) = layout::prefix_of(&name)
        {
            index.entry(prefix.to_string()).or_default().insert(name);
        }
    }
    Ok(index)
}

/// Move `dir` into `root`'s `.deleted/` and start deleting it there. Returns
/// whether there was anything to move.
pub(crate) fn retire_dir(root: &Path, dir: &Path) -> Result<bool> {
    let Some(name) = dir.file_name() else {
        return Ok(false);
    };
    if !dir.exists() {
        return Ok(false);
    }
    let deleted = root.join(DELETED_DIR);
    create_dir_all_durable(&deleted).map_err(StorageError::Io)?;
    // Unique even when the same name is retired twice before the first copy
    // is gone.
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or(0);
    let target = deleted.join(format!(
        "{}-{stamp}-{}",
        name.to_string_lossy(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::rename(dir, &target).map_err(StorageError::Io)?;
    sync_dir(root).map_err(StorageError::Io)?;
    sync_dir(&deleted).map_err(StorageError::Io)?;
    purge_in_background(root.to_path_buf());
    Ok(true)
}

/// Delete whatever `root`'s `.deleted/` holds, on a thread of its own so a
/// large directory does not hold up the caller.
pub(crate) fn purge_in_background(root: PathBuf) {
    let deleted = root.join(DELETED_DIR);
    if !deleted.exists() {
        return;
    }
    let spawned = std::thread::Builder::new()
        .name("felix-purge-deleted".into())
        .spawn(move || purge(&deleted));
    if let Err(err) = spawned {
        tracing::warn!(error = %err, "could not start deleting retired shard directories");
    }
}

fn purge(deleted: &Path) {
    let Ok(entries) = std::fs::read_dir(deleted) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        match std::fs::remove_dir_all(&path) {
            Ok(()) => {}
            // Another purge got there first.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(path = %path.display(), error = %err, "could not delete a retired shard directory");
            }
        }
    }
}

#[cfg(test)]
mod tests;
