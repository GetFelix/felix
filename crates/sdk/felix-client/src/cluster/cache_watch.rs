//! Cache watches through a [`ClusterClient`], following each shard to the
//! broker that owns it.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};

use super::follow::{ClusterCacheWatch, WatchTarget};
use super::sharded::{ShardOffsets, ShardedCacheWatch};
use super::{ClusterClient, MAX_REDIRECTS};
use crate::cache::{CacheWatch, CacheWatchFilter};
use crate::client::Client;

impl ClusterClient {
    /// Watch a cache key or prefix, following the cluster to whichever broker
    /// owns the shard.
    ///
    /// A cache's shards have owners the way a stream's do, and a key hashes to
    /// one of them — so a watch opened against an arbitrary broker is a watch
    /// against a shard that broker may not own. [`Client::watch_cache`] reports
    /// that as a `NotLeader` error and stops; this follows it, which is what
    /// makes a watch usable against a sharded cache in a cluster at all.
    ///
    /// A prefix watch on a multi-shard cache is refused: this names no shard,
    /// and one shard alone would miss every matching key on the others. Use
    /// [`ClusterClient::watch_cache_sharded`] for that.
    ///
    /// The watch follows its shard when the shard later moves to another
    /// broker; see [`ClusterCacheWatch`].
    ///
    /// The client this wrapper holds is **not** replaced, for the same reason
    /// a subscribe redirect does not replace it: a redirect is about one
    /// shard, not about which broker is generally worth talking to.
    pub async fn watch_cache(
        self: &Arc<Self>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        filter: CacheWatchFilter,
        from_offset: Option<u64>,
    ) -> Result<ClusterCacheWatch> {
        let target = WatchTarget::new(tenant_id, namespace, cache, filter, None);
        self.open_watch(target, from_offset, false).await
    }

    /// Like [`ClusterClient::watch_cache`], but delivering each matching key's
    /// current value before live changes.
    pub async fn watch_cache_retained(
        self: &Arc<Self>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        filter: CacheWatchFilter,
    ) -> Result<ClusterCacheWatch> {
        let target = WatchTarget::new(tenant_id, namespace, cache, filter, None);
        self.open_watch(target, None, true).await
    }

    /// Watch a key prefix across **every** shard of a cache, merged into one
    /// handle.
    ///
    /// Keys sharing a prefix hash to different shards and a watch reads one,
    /// so this opens one prefix watch per shard and follows each shard's own
    /// redirect to its owner. See [`ShardedCacheWatch`] for ordering and
    /// for resuming.
    ///
    /// `resume` comes from [`ShardedCacheWatch::resume_offsets`]. A shard
    /// it lists resumes at that offset; a shard it does not list watches from
    /// now. `None` watches every shard from now.
    ///
    /// Needs a broker that advertises `FEATURE_CACHE_SHARDS`, to learn the
    /// shard count. Fails if **any** shard cannot be opened: a watch covering
    /// three shards of four looks complete to everything downstream.
    pub async fn watch_cache_sharded(
        self: &Arc<Self>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        prefix: &str,
        resume: Option<ShardOffsets>,
    ) -> Result<ShardedCacheWatch> {
        let shards = self.cache_shard_count(tenant_id, namespace, cache).await?;
        super::sharded::watch_sharded(
            self, tenant_id, namespace, cache, prefix, shards, resume, false,
        )
        .await
    }

    /// Like [`ClusterClient::watch_cache_sharded`], but delivering each
    /// matching key's current value, from every shard, before live changes.
    ///
    /// The shards finish their state phases at different times, so the merged
    /// watch marks the moment all of them have with
    /// [`crate::ShardedCacheWatchItem::StateComplete`].
    pub async fn watch_cache_sharded_retained(
        self: &Arc<Self>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        prefix: &str,
    ) -> Result<ShardedCacheWatch> {
        let shards = self.cache_shard_count(tenant_id, namespace, cache).await?;
        super::sharded::watch_sharded(
            self, tenant_id, namespace, cache, prefix, shards, None, true,
        )
        .await
    }

    /// Open a watch on whichever broker owns its shard, following that shard
    /// if it later moves.
    pub(crate) async fn open_watch(
        self: &Arc<Self>,
        target: WatchTarget,
        from_offset: Option<u64>,
        retained: bool,
    ) -> Result<ClusterCacheWatch> {
        let entry = self.client().await;
        let (client, watch) = self
            .watch_via(entry, &target, from_offset, retained)
            .await?;
        Ok(ClusterCacheWatch::new(
            Arc::clone(self),
            target,
            from_offset,
            client,
            watch,
        ))
    }

    /// Open a watch on whichever broker owns its shard, asking `first` before
    /// anyone else.
    pub(crate) async fn watch_via(
        &self,
        first: Arc<Client>,
        target: &WatchTarget,
        from_offset: Option<u64>,
        retained: bool,
    ) -> Result<(Arc<Client>, CacheWatch)> {
        let WatchTarget {
            tenant_id,
            namespace,
            cache,
            filter,
            shard,
        } = target;
        let shard = *shard;
        let mut client = first;
        // Every broker asked, so a redirect loop is reported rather than
        // followed forever.
        let mut visited: Vec<String> = Vec::new();
        let mut went_back = false;

        for _ in 0..=MAX_REDIRECTS {
            let attempt = if retained {
                client
                    .watch_cache_shard_retained(tenant_id, namespace, cache, filter.clone(), shard)
                    .await
            } else {
                client
                    .watch_cache_shard(
                        tenant_id,
                        namespace,
                        cache,
                        filter.clone(),
                        shard,
                        from_offset,
                    )
                    .await
            };
            let error = match attempt {
                Ok(watch) => return Ok((client, watch)),
                Err(err) => err,
            };
            // An owner reached by redirect that answers `shard_unavailable` or
            // `draining` has lost the shard since it was named. The entry broker
            // routes by the current assignment, so ask it again, once.
            if !visited.is_empty() && !went_back && super::route_went_stale(&error) {
                went_back = true;
                visited.clear();
                client = self.client().await;
                continue;
            }
            let Some(redirect) = error.downcast_ref::<crate::NotLeaderError>().cloned() else {
                return Err(error);
            };
            if visited.iter().any(|seen| seen == &redirect.node_id) {
                return Err(error.context(format!(
                    "redirected back to {}, which has already been asked",
                    redirect.node_id
                )));
            }
            let Some(addr) = redirect.addr.clone() else {
                return Err(error.context(
                    "the owner's client address is not published, so there is nowhere to follow to",
                ));
            };
            let addr: SocketAddr = addr
                .parse()
                .with_context(|| format!("the owner's address {addr:?} is not usable"))?;
            visited.push(redirect.node_id.clone());
            client = self
                .connect_to(addr)
                .await
                .with_context(|| format!("connect to the cache shard owner at {addr}"))?;
        }

        Err(anyhow::anyhow!(
            "a cache watch was redirected more than {MAX_REDIRECTS} times"
        ))
    }

    /// How many shards a cache has, reconnecting once if the broker in hand
    /// cannot answer.
    async fn cache_shard_count(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
    ) -> Result<u32> {
        let ask = || async {
            let client = self.client().await;
            anyhow::ensure!(
                client.supports_cache_shards(),
                "this broker does not report cache shard counts, so the number of shards to \
                 watch cannot be established"
            );
            client
                .cache_shards(tenant_id, namespace, cache)
                .await
                .with_context(|| format!("ask how many shards cache {cache} has"))
        };
        let shards = match ask().await {
            Ok(shards) => shards,
            Err(first) => {
                self.reconnect().await.map_err(|reconnect_err| {
                    first.context(format!(
                        "and no other broker answered either: {reconnect_err:#}"
                    ))
                })?;
                ask().await?
            }
        };
        anyhow::ensure!(
            shards > 0,
            "the broker knows of no cache {cache} in {tenant_id}/{namespace}"
        );
        Ok(shards)
    }
}
