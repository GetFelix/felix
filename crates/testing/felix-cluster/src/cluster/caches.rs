//! Cache and counter operations through a chosen broker.

use anyhow::{Context, Result, anyhow};

use super::Cluster;
use crate::client;

impl Cluster {
    /// Write one cache key through a named broker, whether or not it owns the
    /// key's shard.
    ///
    /// Going through a non-owner deliberately: routing the operation to the
    /// owner is the behaviour under test, so the harness exercises it rather
    /// than the easy path.
    pub async fn cache_put_via(
        &self,
        node_id: &str,
        cache: &str,
        key: &str,
        value: &[u8],
    ) -> Result<()> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        client
            .cache_put(
                &self.tenant_id,
                &self.namespace,
                cache,
                key,
                bytes::Bytes::copy_from_slice(value),
                None,
            )
            .await
            .with_context(|| format!("cache put {cache}/{key} via {node_id}"))
    }

    /// Read one cache key through a named broker.
    pub async fn cache_get_via(
        &self,
        node_id: &str,
        cache: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        let value = client
            .cache_get(&self.tenant_id, &self.namespace, cache, key)
            .await
            .with_context(|| format!("cache get {cache}/{key} via {node_id}"))?;
        Ok(value.map(|value| value.to_vec()))
    }

    /// Watch one cache key with retained delivery, through a named broker.
    ///
    /// Both halves are returned because dropping the client closes the
    /// connection the watch is delivered on. Unlike a cache get, a watch is
    /// not forwarded: a broker that does not own the key's shard answers with
    /// a redirect, which surfaces here as an error for the caller to act on.
    pub async fn cache_watch_retained_via(
        &self,
        node_id: &str,
        cache: &str,
        key: &str,
    ) -> Result<(felix_client::Client, felix_client::CacheWatch)> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        let watch = client
            .watch_cache_retained(
                &self.tenant_id,
                &self.namespace,
                cache,
                felix_client::CacheWatchFilter::Key(key.to_string()),
            )
            .await
            .with_context(|| format!("cache watch {cache}/{key} via {node_id}"))?;
        Ok((client, watch))
    }

    /// Add to a counter through a named broker, whether or not it owns the
    /// key's shard — routing to the owner is the behaviour under test.
    pub async fn counter_add_via(
        &self,
        node_id: &str,
        cache: &str,
        key: &str,
        delta: i64,
    ) -> Result<i64> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        client
            .counter_add(&self.tenant_id, &self.namespace, cache, key, delta)
            .await
            .with_context(|| format!("counter add {cache}/{key} via {node_id}"))
    }

    /// Read a counter through a named broker.
    pub async fn counter_get_via(
        &self,
        node_id: &str,
        cache: &str,
        key: &str,
    ) -> Result<Option<i64>> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let client =
            client::connect(node.client_addr, &self.tenant_id, &self.client_token()).await?;
        client
            .counter_get(&self.tenant_id, &self.namespace, cache, key)
            .await
            .with_context(|| format!("counter get {cache}/{key} via {node_id}"))
    }
}
