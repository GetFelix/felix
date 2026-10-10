//! Tenant, namespace, stream and cache registries.
//!
//! These maps mirror control-plane state and gate every data-path operation.
//! Lock order is tenants -> namespaces -> streams -> topics; keep it that way.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::Broker;
use super::keys::{
    CacheKey, CacheKeyRef, NamespaceKey, NamespaceKeyRef, StreamKey, StreamKeyRef, TopicKey,
};
use super::metadata::{CacheMetadata, ConsistencyLevel, StreamMetadata};
use crate::error::{BrokerError, Result};
use crate::stream::StreamState;

impl Broker {
    /// Register a tenant. `false` if it was already registered.
    pub async fn register_tenant(&self, tenant_id: impl Into<String>) -> Result<bool> {
        let tenant_id = tenant_id.into();
        let mut guard = self.tenants.write().await;
        // Fast-path guard: no-op if tenant already exists.
        if guard.contains_key(&tenant_id) {
            return Ok(false);
        }
        guard.insert(tenant_id, ());
        Ok(true)
    }

    /// Whether a tenant is registered.
    pub async fn tenant_exists(&self, tenant_id: &str) -> bool {
        self.tenants.read().await.contains_key(tenant_id)
    }

    /// Every registered tenant, in no particular order.
    pub async fn tenant_ids(&self) -> Vec<String> {
        self.tenants.read().await.keys().cloned().collect()
    }

    /// Remove a tenant. `false` if it was not registered.
    pub async fn remove_tenant(&self, tenant_id: &str) -> Result<bool> {
        let mut guard = self.tenants.write().await;
        // Fast-path guard: no-op if tenant doesn't even exist.
        if !guard.contains_key(tenant_id) {
            return Ok(false);
        }
        Ok(guard.remove(tenant_id).is_some())
    }

    /// Register a namespace under a registered tenant. `false` if it was
    /// already registered.
    pub async fn register_namespace(
        &self,
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<bool> {
        let tenant_id = tenant_id.into();
        let namespace = namespace.into();
        // Fast-path guard: reject unknown tenant before creating namespace.
        if !self.tenants.read().await.contains_key(&tenant_id) {
            return Err(BrokerError::TenantNotFound(tenant_id));
        }
        let key = NamespaceKey::new(tenant_id, namespace);
        let mut guard = self.namespaces.write().await;
        // Fast-path guard: no-op if namespace already exists.
        if guard.contains_key(&key) {
            return Ok(false);
        }
        guard.insert(key, ());
        Ok(true)
    }

    /// Remove a namespace. `false` if it was not registered.
    pub async fn remove_namespace(&self, tenant_id: &str, namespace: &str) -> Result<bool> {
        let key = NamespaceKey::new(tenant_id, namespace);
        let mut guard = self.namespaces.write().await;
        // Fast-path guard: no-op if namespace doesn't even exist.
        if !guard.contains_key(&key) {
            return Ok(false);
        }
        Ok(guard.remove(&key).is_some())
    }

    /// Whether the tenant and the namespace are both registered.
    pub async fn namespace_exists(&self, tenant_id: &str, namespace: &str) -> bool {
        if !self.tenants.read().await.contains_key(tenant_id) {
            return false;
        }
        self.namespaces
            .read()
            .await
            .contains_key(&NamespaceKeyRef::new(tenant_id, namespace))
    }

    /// Register a stream, or refresh a registered one's metadata.
    ///
    /// A durable stream's log is opened and its replay ring refilled before
    /// the stream becomes visible. Changing durability in place is refused.
    pub async fn register_stream(
        &self,
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
        stream: impl Into<String>,
        metadata: StreamMetadata,
    ) -> Result<()> {
        // Fast-path guard: reject unknown scopes before attempting to create the stream.
        let tenant_id = tenant_id.into();
        let namespace = namespace.into();
        let stream = stream.into();
        if !self.tenants.read().await.contains_key(&tenant_id) {
            return Err(BrokerError::TenantNotFound(tenant_id));
        }
        let namespace_key = NamespaceKey::new(tenant_id.clone(), namespace.clone());
        // Fast-path guard: reject unknown namespace.
        if !self.namespaces.read().await.contains_key(&namespace_key) {
            return Err(BrokerError::NamespaceNotFound {
                tenant_id,
                namespace,
            });
        }
        let key = StreamKey::new(tenant_id, namespace, stream);

        // Before any shard opens, so each opens under its stream's bounds; on
        // an update it also reaches the shards already open.
        if metadata.durable
            && let Some(storage) = &self.durable_storage
        {
            storage.set_stream_retention(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                metadata.retention,
            )?;
            // Held before anything is acknowledged under `Quorum`, and before
            // a shard opens. Only on a change: a shard with no follower has
            // its hold lifted by replication, and a resync must not put it
            // back.
            let previous = self
                .streams
                .read()
                .await
                .get(&key)
                .map(|existing| existing.consistency);
            if previous != Some(metadata.consistency) {
                storage
                    .set_stream_retention_hold(
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        metadata.consistency == ConsistencyLevel::Quorum,
                    )
                    .await?;
            }
        }

        // Cursor sequence numbers and durable offsets share one identity. An
        // existing ephemeral stream may already have cursor history that was
        // never written to disk, so toggling durability in place would make
        // those two sequences disagree. Require an explicit remove/recreate,
        // which also invalidates old handles and subscriptions.
        if let Some(existing) = self.streams.read().await.get(&key)
            && existing.durable != metadata.durable
        {
            return Err(Self::durability_change_error(&key, existing, &metadata));
        }

        // Everything fallible happens before the stream is registered.
        //
        // Opening the log and refilling the replay ring both read from disk and
        // both can fail. Registering first and hydrating after left a window
        // where a durable stream was already publishable with an empty ring: a
        // hydration failure meant the stream existed, accepted writes, and
        // answered `CursorTooOld` for every pre-restart cursor, with nothing
        // upstream aware that it was never initialised.
        //
        // The loop exists because the state has to be built without holding the
        // registry locks — hydration awaits — so another registration can win
        // the race in between. The retry then takes the fast path.
        loop {
            // Already live: nothing to build, just refresh the metadata.
            //
            // Registration opens shard 0 only. A broker opens the other shards
            // of a stream when it is asked to serve one, because it may own any
            // subset of them and opening all of them would create a log per
            // shard on every broker in the cluster.
            let topic = TopicKey::new(&key.tenant_id, &key.namespace, &key.stream, 0);
            if let Some(state) = self.topics.read().await.get(&topic).cloned() {
                let mut streams = self.streams.write().await;
                if let Some(existing) = streams.get(&key)
                    && existing.durable != metadata.durable
                {
                    return Err(Self::durability_change_error(&key, existing, &metadata));
                }
                // The live state carries the consistency the publish path reads,
                // so refreshing the map alone left a raised stream acknowledging
                // on the leader while the catalog said a majority was required —
                // until the broker restarted.
                state.set_consistency(metadata.consistency);
                streams.insert(key, metadata);
                return Ok(());
            }

            let durable = self
                .open_durable_log(&key.tenant_id, &key.namespace, &key.stream, 0, &metadata)
                .await?;
            let handle_id = self.next_stream_handle.fetch_add(1, Ordering::Relaxed);
            let state = Arc::new(StreamState::new(
                handle_id,
                self.topic_capacity,
                self.subscriber_queue_policy,
                durable,
                metadata.consistency,
            ));
            self.bind_reads(&state, &topic);
            self.hydrate_durable_stream(&state).await?;

            // Keep the documented registry lock order: streams before topics.
            let mut streams = self.streams.write().await;
            if let Some(existing) = streams.get(&key)
                && existing.durable != metadata.durable
            {
                return Err(Self::durability_change_error(&key, existing, &metadata));
            }
            let mut topics = self.topics.write().await;
            if topics.contains_key(&topic) {
                // Lost the race while hydrating; the winner's state is the live
                // one, so discard this one and take the fast path.
                drop(topics);
                drop(streams);
                continue;
            }
            topics.insert(topic, state);
            streams.insert(key.clone(), metadata);
            return Ok(());
        }
    }

    /// Remove a stream and every shard of it this broker holds. Handles to it
    /// stop working.
    pub async fn remove_stream(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<bool> {
        // Fast-path guard: reject unknown scopes before attempting to remove the stream.
        if !self.tenants.read().await.contains_key(tenant_id) {
            return Err(BrokerError::TenantNotFound(tenant_id.to_string()));
        }
        // Fast-path guard: reject unknown namespace.
        self.assert_namespace_exists(tenant_id, namespace).await?;
        let key = StreamKey::new(tenant_id, namespace, stream);
        let removed = self.streams.write().await.remove(&key).is_some();
        if removed {
            let mut topics = self.topics.write().await;
            // Every shard of the stream, not just the one registration opened:
            // a broker may have been asked to serve several.
            topics.retain(|topic, state| {
                let same_stream = topic.tenant_id == key.tenant_id
                    && topic.namespace == key.namespace
                    && topic.stream == key.stream;
                if same_stream {
                    state.deactivate();
                }
                !same_stream
            });
        }
        Ok(removed)
    }

    /// Whether the stream and its tenant and namespace are all registered.
    pub async fn stream_exists(&self, tenant_id: &str, namespace: &str, stream: &str) -> bool {
        // Scope checks are in-memory and intended for per-request enforcement.
        if !self.tenants.read().await.contains_key(tenant_id) {
            return false;
        }
        if !self
            .namespaces
            .read()
            .await
            .contains_key(&NamespaceKeyRef::new(tenant_id, namespace))
        {
            return false;
        }
        self.streams
            .read()
            .await
            .contains_key(&StreamKeyRef::new(tenant_id, namespace, stream))
    }

    /// Register a cache, or replace a registered one's metadata.
    pub async fn register_cache(
        &self,
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
        cache: impl Into<String>,
        metadata: CacheMetadata,
    ) -> Result<()> {
        // Fast-path guard: reject unknown scopes before attempting to create the cache.
        let tenant_id = tenant_id.into();
        let namespace = namespace.into();
        let cache = cache.into();
        if !self.tenants.read().await.contains_key(&tenant_id) {
            return Err(BrokerError::TenantNotFound(tenant_id));
        }
        let namespace_key = NamespaceKey::new(tenant_id.clone(), namespace.clone());
        if !self.namespaces.read().await.contains_key(&namespace_key) {
            return Err(BrokerError::NamespaceNotFound {
                tenant_id,
                namespace,
            });
        }
        let key = CacheKey::new(tenant_id, namespace, cache);
        // Held before a write is acknowledged under `Quorum`; only on a
        // change, as for a stream.
        let previous = self
            .caches
            .read()
            .await
            .get(&key)
            .map(|existing| existing.consistency);
        if previous != Some(metadata.consistency) {
            self.cache
                .set_retention_hold(
                    &key.tenant_id,
                    &key.namespace,
                    &key.cache,
                    metadata.consistency == ConsistencyLevel::Quorum,
                )
                .await
                .map_err(BrokerError::from)?;
        }
        self.caches.write().await.insert(key, metadata);
        Ok(())
    }

    /// Remove a cache. `false` if it was not registered.
    pub async fn remove_cache(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
    ) -> Result<bool> {
        // Fast-path guard: reject unknown scopes before attempting to remove the cache.
        if !self.tenants.read().await.contains_key(tenant_id) {
            return Err(BrokerError::TenantNotFound(tenant_id.to_string()));
        }
        self.assert_namespace_exists(tenant_id, namespace).await?;
        let key = CacheKey::new(tenant_id, namespace, cache);
        Ok(self.caches.write().await.remove(&key).is_some())
    }

    /// Whether the cache and its tenant and namespace are all registered.
    pub async fn cache_exists(&self, tenant_id: &str, namespace: &str, cache: &str) -> bool {
        if !self.tenants.read().await.contains_key(tenant_id) {
            return false;
        }
        if !self
            .namespaces
            .read()
            .await
            .contains_key(&NamespaceKeyRef::new(tenant_id, namespace))
        {
            return false;
        }
        self.caches
            .read()
            .await
            .contains_key(&CacheKeyRef::new(tenant_id, namespace, cache))
    }

    /// The consistency a registered stream asked for, or `None` if the stream
    /// is not registered here.
    pub async fn stream_consistency(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Option<ConsistencyLevel> {
        self.streams
            .read()
            .await
            .get(&StreamKeyRef::new(tenant_id, namespace, stream))
            .map(|metadata| metadata.consistency)
    }

    /// Every stream registered for a tenant, as `(namespace, stream, metadata)`,
    /// sorted by namespace then stream.
    pub async fn tenant_streams(&self, tenant_id: &str) -> Vec<(String, String, StreamMetadata)> {
        let mut streams: Vec<_> = self
            .streams
            .read()
            .await
            .iter()
            .filter(|(key, _)| key.tenant_id == tenant_id)
            .map(|(key, metadata)| (key.namespace.clone(), key.stream.clone(), metadata.clone()))
            .collect();
        streams.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        streams
    }

    /// What a registered stream was created with, or `None` if it is not
    /// registered here.
    pub async fn stream_metadata(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Option<StreamMetadata> {
        self.streams
            .read()
            .await
            .get(&StreamKeyRef::new(tenant_id, namespace, stream))
            .cloned()
    }

    /// The consistency a registered cache asked for, or `None` if the cache is
    /// not registered here.
    pub async fn cache_consistency(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
    ) -> Option<ConsistencyLevel> {
        self.caches
            .read()
            .await
            .get(&CacheKeyRef::new(tenant_id, namespace, cache))
            .map(|metadata| metadata.consistency)
    }

    fn durability_change_error(
        key: &StreamKey,
        current: &StreamMetadata,
        requested: &StreamMetadata,
    ) -> BrokerError {
        BrokerError::DurabilityChangeRequiresRecreate {
            tenant_id: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.stream.clone(),
            current: current.durable,
            requested: requested.durable,
        }
    }

    /// It is up to the caller to check for the error or not.
    async fn assert_namespace_exists(&self, tenant_id: &str, namespace: &str) -> Result<()> {
        if !self
            .namespaces
            .read()
            .await
            .contains_key(&NamespaceKeyRef::new(tenant_id, namespace))
        {
            return Err(BrokerError::NamespaceNotFound {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
