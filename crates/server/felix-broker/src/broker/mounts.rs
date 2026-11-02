//! Which stream a topic belongs to: the MQTT mounts of each tenant.
//!
//! The control plane keeps a tenant's mounts from overlapping
//! (`docs/mqtt-design.md`, "Topic model"). Lookup still takes the longest
//! matching prefix, so a broker that is handed an overlap by a bug elsewhere
//! routes deterministically instead of by map order.
//!
//! The table is a snapshot read without a lock on every PUBLISH. It is
//! replaced whole, and only when a stream's mount changes, by the registry
//! code that holds the streams write lock; that is what keeps the table in
//! step with the registry.
use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use super::Broker;
use super::keys::StreamKey;

/// The stream a topic resolves to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountedStream {
    pub namespace: Arc<str>,
    pub stream: Arc<str>,
}

#[derive(Debug, Clone)]
struct Entry {
    prefix: Arc<str>,
    stream: MountedStream,
}

impl Entry {
    fn is(&self, key: &StreamKey) -> bool {
        *self.stream.namespace == *key.namespace && *self.stream.stream == *key.stream
    }
}

/// Every tenant's mounts. Small per tenant, so a lookup scans them.
#[derive(Debug, Default)]
pub(super) struct Mounts {
    tenants: ArcSwap<HashMap<String, Arc<[Entry]>>>,
}

impl Mounts {
    fn resolve(&self, tenant_id: &str, topic: &str) -> Option<MountedStream> {
        let tenants = self.tenants.load();
        tenants
            .get(tenant_id)?
            .iter()
            .filter(|entry| topic.starts_with(&*entry.prefix))
            .max_by_key(|entry| entry.prefix.len())
            .map(|entry| entry.stream.clone())
    }

    /// Mount `key` at `mount`, or unmount it. The caller holds the streams
    /// write lock, so two updates never interleave.
    pub(super) fn set(&self, key: &StreamKey, mount: Option<&str>) {
        let current = self.tenants.load_full();
        let entries = current.get(&key.tenant_id);
        let existing = entries.and_then(|entries| entries.iter().find(|entry| entry.is(key)));
        if existing.map(|entry| &*entry.prefix) == mount {
            return;
        }
        let mut next: Vec<Entry> = entries
            .into_iter()
            .flat_map(|entries| entries.iter())
            .filter(|entry| !entry.is(key))
            .cloned()
            .collect();
        if let Some(prefix) = mount {
            next.push(Entry {
                prefix: prefix.into(),
                stream: MountedStream {
                    namespace: key.namespace.as_str().into(),
                    stream: key.stream.as_str().into(),
                },
            });
        }
        let mut tenants = (*current).clone();
        if next.is_empty() {
            tenants.remove(&key.tenant_id);
        } else {
            tenants.insert(key.tenant_id.clone(), next.into());
        }
        self.tenants.store(Arc::new(tenants));
    }
}

impl Broker {
    /// The stream whose mount is the longest prefix of `topic`, for this
    /// tenant. Takes no lock, so the PUBLISH path can call it freely.
    pub fn resolve_mount(&self, tenant_id: &str, topic: &str) -> Option<MountedStream> {
        self.mounts.resolve(tenant_id, topic)
    }
}

#[cfg(test)]
mod tests;
