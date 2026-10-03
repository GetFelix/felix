//! Cache and counter requests through a [`ClusterClient`], sent to the
//! owner of the key's shard when the client knows it, and the other requests
//! any broker answers.
//!
//! Any broker serves a cache request: one that does not own the key's shard
//! forwards it. Going to the owner saves that hop, as the owner cache does for
//! publishes. Cache answers do not name an owner the way a forwarded publish's
//! ack does, so the owners come from asking `shard_owners` once per cache.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{Attempt, ClusterClient, Next, StreamKey, next_step, wants_reconnect};
use crate::client::Client;

impl ClusterClient {
    /// Store `value` under `key`, expiring after `ttl_ms` when one is given.
    /// See [`Client::cache_put`].
    ///
    /// Goes to the owner of the key's shard when this client knows it, else
    /// through the broker in use, which forwards. Like [`Self::publish`], a
    /// put that may have been applied is not sent again: a failure is
    /// returned with the connection already replaced. It is sent again only
    /// when a known owner answered that it no longer serves the shard.
    pub async fn cache_put(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: Bytes,
        ttl_ms: Option<u64>,
    ) -> Result<()> {
        self.cache_request(tenant_id, namespace, cache, key, false, |client| {
            let value = value.clone();
            async move {
                client
                    .cache_put(tenant_id, namespace, cache, key, value, ttl_ms)
                    .await
            }
        })
        .await
    }

    /// The value stored under `key`, or `None` when there is none. See
    /// [`Client::cache_get`].
    ///
    /// Routed like [`Self::cache_put`]. A read changes nothing, so one that
    /// fails is asked again once: through the broker in use if a known owner
    /// failed, or through a new connection if the broker in use did.
    pub async fn cache_get(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Result<Option<Bytes>> {
        self.cache_request(
            tenant_id,
            namespace,
            cache,
            key,
            true,
            |client| async move { client.cache_get(tenant_id, namespace, cache, key).await },
        )
        .await
    }

    /// Remove a key, reporting the value it held. See [`Client::cache_delete`].
    ///
    /// Routed and retried like [`Self::cache_put`]: a delete that may have
    /// been applied is not sent again, since a second one would report
    /// `None` for a key the first removed.
    pub async fn cache_delete(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Result<Option<Bytes>> {
        self.cache_request(
            tenant_id,
            namespace,
            cache,
            key,
            false,
            |client| async move { client.cache_delete(tenant_id, namespace, cache, key).await },
        )
        .await
    }

    /// Add a signed delta to a counter, answering with the sum including it.
    /// See [`Client::counter_add`].
    ///
    /// Routed and retried like [`Self::cache_put`], so an add is never sent
    /// twice by this call. A counter shares its cache's shards.
    pub async fn counter_add(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        delta: i64,
    ) -> Result<i64> {
        self.cache_request(
            tenant_id,
            namespace,
            cache,
            key,
            false,
            |client| async move {
                client
                    .counter_add(tenant_id, namespace, cache, key, delta)
                    .await
            },
        )
        .await
    }

    /// Read a counter's sum, or `None` when it has never been written. See
    /// [`Client::counter_get`]. Routed and retried like [`Self::cache_get`].
    pub async fn counter_get(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Result<Option<i64>> {
        self.cache_request(
            tenant_id,
            namespace,
            cache,
            key,
            true,
            |client| async move { client.counter_get(tenant_id, namespace, cache, key).await },
        )
        .await
    }

    /// One cache request: to the key's owner when known, else the broker in
    /// use. `read` says it may be sent again whatever happened to it.
    async fn cache_request<T, F, Fut>(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        read: bool,
        request: F,
    ) -> Result<T>
    where
        F: Fn(Arc<Client>) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let cache_key: StreamKey = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
        );
        if let Some((shard, node_id, owner)) = self.cache_owner(&cache_key, key).await {
            let err = match request(owner).await {
                Ok(answer) => return Ok(answer),
                Err(err) => err,
            };
            let attempt = Attempt {
                routed: true,
                ..Attempt::default()
            };
            // Asked again next time, so a moved shard's new owner is found.
            self.cache_routes.write().await.remove(&cache_key);
            if !read && next_step(&err, attempt) != Next::Reroute {
                return Err(err.context(format!(
                    "cache request to shard {shard}'s owner {node_id} failed; \
                     the next one goes through the entry broker"
                )));
            }
            tracing::debug!(
                error = %format!("{err:#}"),
                "the cache shard's owner did not answer; asking through the entry broker",
            );
        }

        self.through_entry(read, request).await
    }

    /// How many shards `stream` was placed with. See [`Client::stream_shards`].
    ///
    /// Asked of the broker in use, and once more through another broker if
    /// that one is gone.
    pub async fn stream_shards(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<u32> {
        self.through_entry(true, |client| async move {
            client.stream_shards(tenant_id, namespace, stream).await
        })
        .await
    }

    /// `request` through the broker in use, moving to another broker when
    /// that one is gone. A `read` is asked again there. Anything else may have
    /// been applied, so its failure is returned with the connection already
    /// replaced.
    async fn through_entry<T, F, Fut>(&self, read: bool, request: F) -> Result<T>
    where
        F: Fn(Arc<Client>) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let err = match request(self.client().await).await {
            Ok(answer) => return Ok(answer),
            Err(err) if !wants_reconnect(&err) => return Err(err),
            Err(err) => err,
        };
        if let Err(reconnect_err) = self.reconnect().await {
            return Err(err.context(format!(
                "request failed and no other broker answered: {reconnect_err:#}"
            )));
        }
        if !read {
            return Err(err.context("request failed; reconnected to another broker"));
        }
        request(self.client().await).await
    }

    /// The shard `key` falls in and a client for its owner, when the owner is
    /// known and reachable.
    ///
    /// The owners are asked once per cache and kept until a request to one
    /// fails. `None` sends the request through the broker in use, which is
    /// always correct: a broker that does not own the shard forwards.
    async fn cache_owner(
        &self,
        cache_key: &StreamKey,
        key: &str,
    ) -> Option<(u32, String, Arc<Client>)> {
        let known = self.cache_routes.read().await.get(cache_key).cloned();
        let owners = match known {
            Some(owners) => owners,
            None => {
                let owners = Arc::new(self.ask_cache_owners(cache_key).await);
                self.cache_routes
                    .write()
                    .await
                    .insert(cache_key.clone(), Arc::clone(&owners));
                owners
            }
        };
        if owners.is_empty() {
            return None;
        }
        let shard = felix_wire::routing::shard_for(owners.len() as u32, Some(key.as_bytes()));
        let (node_id, addr) = owners.get(shard as usize)?.clone()?;
        match self.connect_to(addr).await {
            Ok(client) => Some((shard, node_id, client)),
            Err(err) => {
                tracing::debug!(
                    owner = %node_id,
                    %addr,
                    error = %err,
                    "could not connect to the cache shard's owner; going through the entry broker",
                );
                None
            }
        }
    }

    /// Each shard's owner, by shard, or nothing when the broker in use cannot
    /// say. A shard whose owner has no usable client address is `None`.
    async fn ask_cache_owners(&self, cache_key: &StreamKey) -> CacheOwners {
        let client = self.client().await;
        if !client.supports_shard_owners() {
            return Vec::new();
        }
        let (tenant_id, namespace, cache) = cache_key;
        match client
            .shard_owners(tenant_id, namespace, cache, crate::ShardKind::Cache)
            .await
        {
            Ok(owners) => {
                let mut resolved = Vec::with_capacity(owners.len());
                for owner in owners {
                    let addr = match owner.addr.as_deref() {
                        Some(addr) => self.resolve(addr).await,
                        None => None,
                    };
                    resolved.push(owner.node_id.zip(addr));
                }
                resolved
            }
            Err(err) => {
                tracing::debug!(
                    cache = %cache,
                    error = %err,
                    "could not learn the cache's owners; going through the entry broker",
                );
                Vec::new()
            }
        }
    }
}

/// Each shard's owner, indexed by shard: its node id and client address.
pub(super) type CacheOwners = Vec<Option<(String, SocketAddr)>>;
