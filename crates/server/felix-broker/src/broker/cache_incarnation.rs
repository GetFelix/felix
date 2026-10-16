//! Which incarnation of a cache this broker's logs belong to, and deleting
//! one.
//!
//! A cache deleted and created again under the same name is a new cache, with
//! directories of its own in the cache store and the counter store. It
//! follows the rules streams do (see `super::incarnation`): a name never moves
//! to an incarnation below one held here or one the catalog deleted, and data
//! goes only on the control plane's word that its incarnation is deleted or
//! on an operator's removal. The marker, floors and in-play record live under
//! the cache store's root, apart from the streams'.
//!
//! A broker whose cache store keeps no log has nothing on disk to protect; it
//! drops a deleted cache's entries so a cache created again starts empty.
//!
//! As for streams, registering and deleting one name must not overlap; the
//! broker service calls them only from its catalog-sync task.

use felix_storage::{LogCache, MarkerRead, StreamMarker};

use super::Broker;
use super::keys::CacheKey;
use super::metadata::CacheMetadata;
use crate::error::{BrokerError, Result};

impl Broker {
    /// Point the cache and counter stores at `incarnation` of a cache, or
    /// with `None` at nothing, so its logs cannot be opened while it is being
    /// deleted. Registration does this; a tool on a stopped broker calls it to
    /// say which incarnation it means.
    pub fn set_cache_incarnation(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        incarnation: Option<u64>,
    ) {
        if let Some(store) = self.log_cache() {
            store.set_incarnation(tenant_id, namespace, cache, incarnation);
        }
        if let Some(counters) = &self.counters {
            counters.set_incarnation(tenant_id, namespace, cache, incarnation);
        }
    }

    /// The incarnation a cache's logs open at on this broker: the registered
    /// one, or the one a tool set. `None` for a cache it does not know, is
    /// deleting, or keeps no log of.
    pub fn cache_incarnation(&self, tenant_id: &str, namespace: &str, cache: &str) -> Option<u64> {
        if let Some(store) = self.log_cache() {
            return store.incarnation(tenant_id, namespace, cache);
        }
        self.counters
            .as_ref()
            .and_then(|counters| counters.incarnation(tenant_id, namespace, cache))
    }

    /// Open no cache's logs until the cache is registered: once caches carry
    /// incarnations, a log opened before the catalog names one would be the
    /// wrong one. One-way, and recorded on disk, so a restart requires it
    /// from the start. Kept apart from [`Broker::require_registered_streams`]:
    /// caches get incarnations under a fleet feature of their own.
    pub fn require_registered_caches(&self) {
        if self.cache_registration_required() {
            return;
        }
        // The stores first, as for streams: nothing may see the flag set while
        // an unregistered cache still opens at 0.
        if let Some(store) = self.log_cache() {
            store.require_registration();
        }
        if let Some(counters) = &self.counters {
            counters.require_registration();
        }
        if self
            .cache_registration_required
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        if let Some(store) = self.log_cache()
            && let Err(err) = store.record_incarnations_in_play()
        {
            tracing::error!(error = %err, "could not record that caches carry incarnations");
        }
    }

    /// [`Broker::require_registered_caches`] when this broker's cache store
    /// shows it has seen a cache with an incarnation. Call at start, before
    /// anything can open a shard.
    pub fn require_cache_registration_if_in_play(&self) -> Result<()> {
        let Some(store) = self.log_cache() else {
            return Ok(());
        };
        if store.incarnations_in_play().map_err(BrokerError::from)? {
            self.require_registered_caches();
        }
        Ok(())
    }

    /// Whether a cache's logs open only once it is registered.
    pub fn cache_registration_required(&self) -> bool {
        self.cache_registration_required
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Make the cache's marker name `incarnation` and nothing deleted, as a
    /// restore to a backup point does. For an operator's tool on a stopped
    /// broker; nothing is deleted.
    pub async fn restore_cache_marker(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        incarnation: u64,
    ) -> Result<()> {
        let Some(store) = self.log_cache() else {
            return Ok(());
        };
        store
            .write_marker(&StreamMarker {
                tenant: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: cache.to_string(),
                incarnation,
                retiring: false,
                shards: None,
                deleted: None,
            })
            .await?;
        store.clear_floor(tenant_id, namespace, cache).await?;
        Ok(())
    }

    /// Finish every cache delete that stopped part-way. Call once at start,
    /// before the catalog registers anything. A delete that fails again is
    /// logged and left for later.
    pub async fn finish_pending_cache_deletes(&self) -> Result<()> {
        let Some(store) = self.log_cache() else {
            return Ok(());
        };
        for marker in store.retiring_markers().await? {
            let key = CacheKey::new(
                marker.tenant.clone(),
                marker.namespace.clone(),
                marker.stream.clone(),
            );
            let _ = self.finish_cache_delete(&key, &marker).await;
        }
        Ok(())
    }

    /// The control plane's word that every incarnation of a cache up to and
    /// including `through` is deleted. The registered cache goes if it is one
    /// of them, and so does every one of them this broker has data for. Safe
    /// to repeat. Returns whether a registered cache was removed.
    pub async fn delete_cache_through(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        through: u64,
    ) -> Result<bool> {
        let key = CacheKey::new(tenant_id, namespace, cache);
        let marker = match self.cache_marker(&key).await? {
            MarkerRead::Valid(marker) => Some(marker),
            MarkerRead::Absent | MarkerRead::Unreadable => None,
        };
        let registered = self
            .caches
            .read()
            .await
            .get(&key)
            .map(|existing| existing.incarnation);
        let covered = registered.filter(|incarnation| *incarnation <= through);
        if covered.is_none()
            && let Some(marker) = &marker
            && !marker.retiring
            && marker.deleted.is_some_and(|deleted| deleted >= through)
        {
            return Ok(false);
        }
        // Recorded first: from here no registration of these incarnations is
        // taken, and a delete cut short is finished from the marker.
        let pending = StreamMarker {
            tenant: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.cache.clone(),
            incarnation: registered
                .or(marker.as_ref().map(|marker| marker.incarnation))
                .unwrap_or(through),
            retiring: true,
            shards: None,
            deleted: Some(
                marker
                    .as_ref()
                    .and_then(|marker| marker.deleted)
                    .map_or(through, |deleted| deleted.max(through)),
            ),
        };
        self.write_cache_marker(&pending).await?;
        if let Some(incarnation) = covered {
            self.drop_cache(&key).await;
            self.set_cache_incarnation(tenant_id, namespace, cache, None);
            self.close_cache_incarnation(&key, incarnation).await?;
        }
        self.finish_cache_delete(&key, &pending).await?;
        Ok(covered.is_some())
    }

    /// [`Self::delete_cache_through`] for each of a cache snapshot's
    /// tombstones. Those naming caches with nothing here to delete only raise
    /// a floor, all in one file with one flush.
    pub async fn apply_cache_tombstones(
        &self,
        tombstones: &[(&str, &str, &str, u64)],
    ) -> Result<()> {
        let mut floors = Vec::new();
        for &(tenant_id, namespace, cache, through) in tombstones {
            let key = CacheKey::new(tenant_id, namespace, cache);
            match self.cache_tombstone_work(&key, through).await? {
                Work::Done => {}
                Work::Floor => floors.push((key.tenant_id, key.namespace, key.cache, through)),
                Work::Delete => {
                    self.delete_cache_through(tenant_id, namespace, cache, through)
                        .await?;
                }
            }
        }
        if let (Some(store), false) = (self.log_cache(), floors.is_empty()) {
            store.record_floors(floors).await?;
        }
        Ok(())
    }

    /// Apply the control plane's delete of a cache. With `incarnation`, it
    /// and every earlier incarnation of the name are gone: see
    /// [`Broker::delete_cache_through`]. Without, it is the delete of a cache
    /// from before incarnations, which unregisters it and keeps its data, as
    /// a delete always did: the name may be created again at 0 and reopen it.
    pub async fn remove_cache_from_catalog(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        incarnation: Option<u64>,
    ) -> Result<bool> {
        if let Some(through) = incarnation {
            return self
                .delete_cache_through(tenant_id, namespace, cache, through)
                .await;
        }
        let key = CacheKey::new(tenant_id, namespace, cache);
        let registered = self
            .caches
            .read()
            .await
            .get(&key)
            .map(|existing| existing.incarnation);
        match registered {
            None => Ok(false),
            Some(0) => {
                self.drop_cache(&key).await;
                self.set_cache_incarnation(tenant_id, namespace, cache, None);
                self.close_cache_incarnation(&key, 0).await?;
                Ok(true)
            }
            Some(incarnation) => {
                tracing::warn!(
                    tenant_id,
                    namespace,
                    cache,
                    incarnation,
                    "a delete naming no incarnation; keeping the incarnated cache registered",
                );
                Ok(false)
            }
        }
    }

    /// Delete the registered cache for an operator or a tool: the caller is
    /// the evidence. An incarnated cache is recorded as deleted, as a
    /// tombstone would; one at incarnation 0 is not, so the name can be
    /// created again.
    pub(super) async fn remove_registered_cache(&self, key: &CacheKey) -> Result<bool> {
        let registered = self
            .caches
            .read()
            .await
            .get(key)
            .map(|existing| existing.incarnation);
        let Some(incarnation) = registered else {
            // A delete that stopped part-way, asked again.
            if let MarkerRead::Valid(marker) = self.cache_marker(key).await?
                && marker.retiring
            {
                self.finish_cache_delete(key, &marker).await?;
            }
            return Ok(false);
        };
        if incarnation != 0 {
            return self
                .delete_cache_through(&key.tenant_id, &key.namespace, &key.cache, incarnation)
                .await;
        }
        let pending = StreamMarker {
            tenant: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.cache.clone(),
            incarnation: 0,
            retiring: true,
            shards: None,
            deleted: None,
        };
        self.write_cache_marker(&pending).await?;
        self.drop_cache(key).await;
        self.set_cache_incarnation(&key.tenant_id, &key.namespace, &key.cache, None);
        self.close_cache_incarnation(key, 0).await?;
        self.finish_cache_delete(key, &pending).await?;
        Ok(true)
    }

    /// The incarnation to register `metadata` of `key` at, given the one
    /// registered now (`previous`), or `None` when the registration is
    /// refused and the cache left as it is. Refused for the reasons a stream's
    /// is; see `Broker::settle_incarnation`.
    pub(super) async fn settle_cache_incarnation(
        &self,
        key: &CacheKey,
        previous: Option<u64>,
        metadata: &CacheMetadata,
    ) -> Result<Option<u64>> {
        let incoming = metadata.incarnation;
        let (marker, damaged) = match self.cache_marker(key).await? {
            MarkerRead::Valid(pending) if pending.retiring => {
                self.finish_cache_delete(key, &pending).await?;
                match self.cache_marker(key).await? {
                    MarkerRead::Valid(marker) => (Some(marker), false),
                    _ => (None, false),
                }
            }
            MarkerRead::Valid(marker) => (Some(marker), false),
            MarkerRead::Absent => (None, false),
            MarkerRead::Unreadable => (None, true),
        };
        if incoming != 0 {
            self.require_registered_caches();
        }
        let deleted = marker
            .as_ref()
            .and_then(|marker| marker.deleted)
            .max(self.cache_floor(key).await?);
        let held = previous.or(marker
            .as_ref()
            .map(|marker| marker.incarnation)
            .filter(|held| deleted.is_none_or(|deleted| *held > deleted)));
        let refusal = if deleted.is_some_and(|deleted| incoming <= deleted) {
            Some("the catalog deleted this incarnation")
        } else if incoming == 0 {
            let incarnated = match held {
                Some(held) => held != 0,
                None => {
                    damaged
                        || self
                            .cache_incarnations_on_disk(key)
                            .await?
                            .iter()
                            .any(|held| *held != 0)
                }
            };
            incarnated.then_some("the catalog sent a cache without its incarnation")
        } else if held.is_some_and(|held| incoming < held) {
            Some("a later incarnation is held here")
        } else {
            None
        };
        if let Some(reason) = refusal {
            tracing::warn!(
                tenant_id = %key.tenant_id,
                namespace = %key.namespace,
                cache = %key.cache,
                incoming,
                held,
                deleted,
                reason,
                "refusing a cache registration from an older catalog; keeping what this broker holds",
            );
            metrics::counter!("felix_broker_cache_incarnation_refused_total").increment(1);
            return Ok(None);
        }
        if let Some(held) = held
            && held != incoming
        {
            if previous.is_some() {
                self.drop_cache(key).await;
            }
            // Kept until the catalog deletes it, where it has directories of
            // its own; a store with no log cannot keep it apart.
            self.close_cache_incarnation(key, held).await?;
            if self.log_cache().is_none() {
                self.cache
                    .forget_cache(&key.tenant_id, &key.namespace, &key.cache)
                    .await?;
            }
            tracing::info!(
                tenant_id = %key.tenant_id,
                namespace = %key.namespace,
                cache = %key.cache,
                held,
                incarnation = incoming,
                "a later cache incarnation registered; keeping the earlier one's data until the catalog deletes it",
            );
        }
        let wanted = StreamMarker {
            tenant: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.cache.clone(),
            incarnation: incoming,
            retiring: false,
            shards: None,
            deleted,
        };
        // A cache from before incarnations writes nothing new to disk.
        let needed = incoming != 0 || marker.is_some();
        if needed && marker.as_ref() != Some(&wanted) {
            self.write_cache_marker(&wanted).await?;
        }
        self.set_cache_incarnation(&key.tenant_id, &key.namespace, &key.cache, Some(incoming));
        Ok(Some(incoming))
    }

    /// Unregister `key` and end its watches, returning what was registered.
    /// Its logs and data are the caller's to close or delete.
    pub(super) async fn drop_cache(&self, key: &CacheKey) -> Option<CacheMetadata> {
        let metadata = self.caches.write().await.remove(key)?;
        if let Some(hub) = &self.cache_watches {
            for (tenant_id, namespace, cache, shard) in self.cache.open_shards() {
                if tenant_id == key.tenant_id && namespace == key.namespace && cache == key.cache {
                    hub.end_shard(&tenant_id, &namespace, &cache, shard, None);
                }
            }
        }
        Some(metadata)
    }

    /// Stop serving one incarnation of `key` and close its logs, keeping its
    /// data.
    async fn close_cache_incarnation(&self, key: &CacheKey, incarnation: u64) -> Result<()> {
        let (tenant_id, namespace, cache) = (&key.tenant_id, &key.namespace, &key.cache);
        if let Some(store) = self.log_cache() {
            store
                .close_incarnation(tenant_id, namespace, cache, incarnation)
                .await?;
        }
        if let Some(counters) = &self.counters {
            counters
                .close_incarnation(tenant_id, namespace, cache, incarnation)
                .await?;
        }
        Ok(())
    }

    /// Delete what `marker` records, in the cache store and the counter
    /// store: every incarnation up to its `deleted`, or with none, its own.
    /// Both stores are tried even when one fails; the marker then stays
    /// retiring, so the delete is finished later. Safe to repeat.
    async fn finish_cache_delete(&self, key: &CacheKey, marker: &StreamMarker) -> Result<()> {
        let through = marker.deleted.unwrap_or(marker.incarnation);
        let (tenant_id, namespace, cache) = (&key.tenant_id, &key.namespace, &key.cache);
        let mut first_error = None;
        let mut note = |outcome: Result<()>| {
            if let Err(err) = outcome {
                tracing::error!(
                    tenant_id = %tenant_id,
                    namespace = %namespace,
                    cache = %cache,
                    through,
                    error = %err,
                    "could not delete a cache's data; it will be retried",
                );
                first_error.get_or_insert(err);
            }
        };
        if let Some(store) = self.log_cache() {
            note(retire_through(Store::Entries(store), key, through).await);
        }
        if let Some(counters) = &self.counters {
            note(retire_through(Store::Counters(counters), key, through).await);
        }
        if let Some(err) = first_error {
            return Err(err);
        }
        // A store with no log has its entries in memory, under no incarnation:
        // they go unless a later incarnation is registered over them.
        if !self.caches.read().await.contains_key(key) {
            self.cache.forget_cache(tenant_id, namespace, cache).await?;
        }
        // Only the marker this delete wrote: a registration since rewrote it.
        if let MarkerRead::Valid(current) = self.cache_marker(key).await?
            && current == *marker
        {
            if marker.deleted.is_some() {
                self.write_cache_marker(&StreamMarker {
                    retiring: false,
                    ..marker.clone()
                })
                .await?;
            } else if let Some(store) = self.log_cache() {
                store.remove_marker(tenant_id, namespace, cache).await?;
            }
        }
        Ok(())
    }

    /// What a tombstone through `through` needs here. Only a floor when
    /// nothing registered is covered, no delete is in progress and neither
    /// store has a directory of a covered incarnation.
    async fn cache_tombstone_work(&self, key: &CacheKey, through: u64) -> Result<Work> {
        if self.log_cache().is_none() {
            return Ok(Work::Delete);
        }
        let marker = match self.cache_marker(key).await? {
            MarkerRead::Valid(marker) if marker.retiring => return Ok(Work::Delete),
            MarkerRead::Valid(marker) => Some(marker),
            MarkerRead::Absent | MarkerRead::Unreadable => None,
        };
        let registered = self
            .caches
            .read()
            .await
            .get(key)
            .map(|existing| existing.incarnation);
        if registered.is_some_and(|incarnation| incarnation <= through) {
            return Ok(Work::Delete);
        }
        if self
            .cache_incarnations_on_disk(key)
            .await?
            .iter()
            .any(|held| *held <= through)
        {
            return Ok(Work::Delete);
        }
        let recorded = marker
            .and_then(|marker| marker.deleted)
            .max(self.cache_floor(key).await?);
        Ok(if recorded.is_some_and(|deleted| deleted >= through) {
            Work::Done
        } else {
            Work::Floor
        })
    }

    /// The incarnations of `key` with a directory in either store.
    async fn cache_incarnations_on_disk(
        &self,
        key: &CacheKey,
    ) -> Result<std::collections::BTreeSet<u64>> {
        let mut held = std::collections::BTreeSet::new();
        if let Some(store) = self.log_cache() {
            held.extend(Store::Entries(store).incarnations(key).await?);
        }
        if let Some(counters) = &self.counters {
            held.extend(Store::Counters(counters).incarnations(key).await?);
        }
        Ok(held)
    }

    fn log_cache(&self) -> Option<&LogCache> {
        self.cache.as_log_cache()
    }

    async fn cache_marker(&self, key: &CacheKey) -> Result<MarkerRead> {
        match self.log_cache() {
            Some(store) => Ok(store
                .marker(&key.tenant_id, &key.namespace, &key.cache)
                .await?),
            None => Ok(MarkerRead::Absent),
        }
    }

    async fn cache_floor(&self, key: &CacheKey) -> Result<Option<u64>> {
        match self.log_cache() {
            Some(store) => Ok(store
                .floor(&key.tenant_id, &key.namespace, &key.cache)
                .await?),
            None => Ok(None),
        }
    }

    async fn write_cache_marker(&self, marker: &StreamMarker) -> Result<()> {
        if let Some(store) = self.log_cache() {
            store.write_marker(marker).await?;
        }
        Ok(())
    }
}

/// What one catalog tombstone needs from this broker.
enum Work {
    /// Its floor is already recorded.
    Done,
    /// Nothing here to delete: record the floor.
    Floor,
    /// A full delete.
    Delete,
}

/// One of the stores a cache keeps directories in.
#[derive(Clone, Copy)]
enum Store<'a> {
    Entries(&'a LogCache),
    Counters(&'a felix_storage::CounterStore),
}

impl Store<'_> {
    async fn incarnations(self, key: &CacheKey) -> Result<Vec<u64>> {
        let (tenant_id, namespace, cache) = (&key.tenant_id, &key.namespace, &key.cache);
        let dirs = match self {
            Store::Entries(store) => store.dirs_on_disk(tenant_id, namespace, cache).await?,
            Store::Counters(counters) => counters.dirs_on_disk(tenant_id, namespace, cache).await?,
        };
        Ok(dirs
            .into_iter()
            .map(|(_, incarnation)| incarnation)
            .collect())
    }
}

/// Retire every incarnation of `key` up to `through` that `store` has
/// directories for.
async fn retire_through(store: Store<'_>, key: &CacheKey, through: u64) -> Result<()> {
    let (tenant_id, namespace, cache) = (&key.tenant_id, &key.namespace, &key.cache);
    let held: std::collections::BTreeSet<u64> =
        store.incarnations(key).await?.into_iter().collect();
    for incarnation in held.into_iter().filter(|held| *held <= through) {
        match store {
            Store::Entries(entries) => {
                entries
                    .retire(tenant_id, namespace, cache, incarnation)
                    .await?
            }
            Store::Counters(counters) => {
                counters
                    .retire(tenant_id, namespace, cache, incarnation)
                    .await?
            }
        }
    }
    Ok(())
}
