//! Cache and counter requests through a [`Connection`].
//!
//! Each goes to one of the client's cache workers, round-robin; see
//! `crate::cache` for how a worker carries it.

use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use bytes::Bytes;
use felix_wire::Message;
use tokio::sync::oneshot;

use super::Connection;
use super::connect::open_cache_worker;
use crate::cache::{CacheConditionResult, CacheRequest, CacheWorker, LeaseWrite, VersionedValue};
use felix_wire::CacheCondition;

impl Connection {
    /// Cache requests in flight on each cache connection, for metrics.
    ///
    /// Approximate: the count is raised after a request is queued and lowered
    /// by the worker that answers it, and the two can race.
    pub fn cache_conn_counts(&self) -> Vec<usize> {
        self.cache_conn_counts
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect()
    }

    /// Store `value` under `key`, expiring after `ttl_ms` when one is given.
    pub async fn cache_put(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: Bytes,
        ttl_ms: Option<u64>,
    ) -> Result<()> {
        // Cache ops are delegated to a pool of single-writer cache workers.
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::CachePut {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            key: key.to_string(),
            value,
            request_id: Some(request_id),
            ttl_ms,
        };
        let (response_tx, response_rx) = oneshot::channel();
        let (worker, conn_index) = self.cache_worker().await?;
        worker
            .tx
            .send(CacheRequest::Put {
                request_id,
                message,
                response: response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("cache worker closed"))?;

        // Track the connection since it counts as inflight.
        let current = self.cache_conn_counts[conn_index].fetch_add(1, Ordering::Relaxed) + 1;
        t_gauge!("felix_client_cache_conn_ops", "conn" => conn_index.to_string())
            .set(current as f64);
        t_counter!(
            "felix_client_cache_conn_ops_total",
            "conn" => conn_index.to_string()
        )
        .increment(1);
        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("cache put response dropped"))?
    }

    /// The value stored under `key`, or `None` when there is none.
    pub async fn cache_get(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Result<Option<Bytes>> {
        // Cache ops are delegated to a pool of single-writer cache workers.
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::CacheGet {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            key: key.to_string(),
            request_id: Some(request_id),
        };
        let (response_tx, response_rx) = oneshot::channel();
        let (worker, conn_index) = self.cache_worker().await?;
        worker
            .tx
            .send(CacheRequest::Get {
                request_id,
                message,
                response: response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("cache worker closed"))?;

        // Now it's *actually* enqueued, so it counts as inflight.
        let current = self.cache_conn_counts[conn_index].fetch_add(1, Ordering::Relaxed) + 1;
        t_gauge!("felix_client_cache_conn_ops", "conn" => conn_index.to_string())
            .set(current as f64);
        t_counter!(
            "felix_client_cache_conn_ops_total",
            "conn" => conn_index.to_string()
        )
        .increment(1);
        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("cache get response dropped"))?
    }

    /// Remove a key, reporting the value it held.
    ///
    /// `Ok(None)` means the key was not there. Both are answers: a delete is not
    /// an error just because there was nothing to remove.
    ///
    /// Fails without sending anything when the broker did not advertise
    /// [`felix_wire::FEATURE_CACHE_DELETE`]. An unrecognised message type is
    /// fatal to a broker's control loop, so probing one that predates this would
    /// cost the connection rather than return an error.
    pub async fn cache_delete(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Result<Option<Bytes>> {
        if !felix_wire::supports_feature(self.server_features, felix_wire::FEATURE_CACHE_DELETE) {
            return Err(anyhow::anyhow!("this broker does not support cache delete",));
        }
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::CacheDelete {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            key: key.to_string(),
            request_id: Some(request_id),
        };
        let (response_tx, response_rx) = oneshot::channel();
        let (worker, conn_index) = self.cache_worker().await?;
        worker
            .tx
            .send(CacheRequest::Get {
                request_id,
                message,
                response: response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("cache worker closed"))?;

        let current = self.cache_conn_counts[conn_index].fetch_add(1, Ordering::Relaxed) + 1;
        t_gauge!("felix_client_cache_conn_ops", "conn" => conn_index.to_string())
            .set(current as f64);
        t_counter!(
            "felix_client_cache_conn_ops_total",
            "conn" => conn_index.to_string()
        )
        .increment(1);
        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("cache delete response dropped"))?
    }

    /// The value stored under `key` with its version, or `None` when there is
    /// none. Pass the version to [`Self::cache_put_if`] or
    /// [`Self::cache_delete_if`] to write only if nothing changed since.
    ///
    /// Fails without sending anything when the broker did not advertise
    /// [`felix_wire::FEATURE_CACHE_CONDITIONAL`].
    pub async fn cache_get_versioned(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Result<Option<VersionedValue>> {
        self.require_conditional()?;
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::CacheGet {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            key: key.to_string(),
            request_id: Some(request_id),
        };
        let (response_tx, response_rx) = oneshot::channel();
        self.enqueue_cache(CacheRequest::Versioned {
            request_id,
            message,
            response: response_tx,
        })
        .await?;
        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("cache get response dropped"))?
    }

    /// Store `value` under `key` only if the key's current entry meets
    /// `condition`: absent (never written, deleted, or expired), or at
    /// exactly the given version.
    ///
    /// The check and the write are atomic on the key's owner. A refusal is an
    /// answer, not an error: `applied` is false and `version` is the key's
    /// current version. Applied, `version` is the one this put wrote.
    ///
    /// Fails without sending anything when the broker did not advertise
    /// [`felix_wire::FEATURE_CACHE_CONDITIONAL`]; an older broker would not
    /// know the request.
    #[allow(clippy::too_many_arguments)]
    pub async fn cache_put_if(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: Bytes,
        ttl_ms: Option<u64>,
        condition: CacheCondition,
    ) -> Result<CacheConditionResult> {
        self.require_conditional()?;
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        self.conditional(
            Message::CachePutIf {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                cache: cache.to_string(),
                key: key.to_string(),
                value,
                ttl_ms,
                condition,
                request_id,
                lease_token: false,
            },
            request_id,
        )
        .await
        .map(|answer| answer.result)
    }

    /// [`Self::cache_put_if`] on a lease cache, asking for a lease token if
    /// the write applies.
    ///
    /// Winning the write is what makes the caller the lease's holder, so only
    /// an applied write gets a token, granting what the caller's lease
    /// templates name for `key`. A renewal (a put at the held version) gets a
    /// fresh one. The token lasts as long as the entry's TTL, capped by the
    /// broker and by the caller's own token. Present it with
    /// [`Self::with_identity_token`]; streams opened with it end when it
    /// expires.
    ///
    /// Fails without writing anything when the broker did not advertise
    /// [`felix_wire::FEATURE_LEASE_TOKEN`], or when it refuses the mint: the
    /// caller's token is itself a lease or delegated token, holds no template
    /// naming this cache, or `key` cannot be part of a name.
    #[allow(clippy::too_many_arguments)]
    pub async fn cache_put_if_lease(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: Bytes,
        ttl_ms: Option<u64>,
        condition: CacheCondition,
    ) -> Result<LeaseWrite> {
        self.require_conditional()?;
        if !felix_wire::supports_feature(self.server_features_hi, felix_wire::FEATURE_LEASE_TOKEN) {
            return Err(anyhow::anyhow!("this broker does not mint lease tokens"));
        }
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        self.conditional(
            Message::CachePutIf {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                cache: cache.to_string(),
                key: key.to_string(),
                value,
                ttl_ms,
                condition,
                request_id,
                lease_token: true,
            },
            request_id,
        )
        .await
    }

    /// Remove `key` only if its current version is `version`, atomically as
    /// [`Self::cache_put_if`] checks. Refused, `version` in the answer is the
    /// key's current one.
    pub async fn cache_delete_if(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        version: u64,
    ) -> Result<CacheConditionResult> {
        self.require_conditional()?;
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        self.conditional(
            Message::CacheDeleteIf {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                cache: cache.to_string(),
                key: key.to_string(),
                version,
                request_id,
            },
            request_id,
        )
        .await
        .map(|answer| answer.result)
    }

    fn require_conditional(&self) -> Result<()> {
        if !felix_wire::supports_feature(
            self.server_features,
            felix_wire::FEATURE_CACHE_CONDITIONAL,
        ) {
            return Err(anyhow::anyhow!(
                "this broker does not support conditional cache writes"
            ));
        }
        Ok(())
    }

    async fn conditional(&self, message: Message, request_id: u64) -> Result<LeaseWrite> {
        let (response_tx, response_rx) = oneshot::channel();
        self.enqueue_cache(CacheRequest::Conditional {
            request_id,
            message,
            response: response_tx,
        })
        .await?;
        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("cache response dropped"))?
    }

    /// Hand a request to a cache worker and count it as in flight.
    async fn enqueue_cache(&self, request: CacheRequest) -> Result<()> {
        let (worker, conn_index) = self.cache_worker().await?;
        worker
            .tx
            .send(request)
            .await
            .map_err(|_| anyhow::anyhow!("cache worker closed"))?;
        let current = self.cache_conn_counts[conn_index].fetch_add(1, Ordering::Relaxed) + 1;
        t_gauge!("felix_client_cache_conn_ops", "conn" => conn_index.to_string())
            .set(current as f64);
        t_counter!(
            "felix_client_cache_conn_ops_total",
            "conn" => conn_index.to_string()
        )
        .increment(1);
        Ok(())
    }

    /// Add a signed delta to a counter, answering with the sum including it.
    ///
    /// A counter is scoped exactly as a cache key is — same registered cache
    /// scope, same key-to-shard routing, same owner — but lives beside the
    /// cache, not in it: a counter and a cache value may share a key and are
    /// unrelated. **Delivery is at least once**: a retry after a lost
    /// acknowledgement counts twice, because deltas carry no dedupe identity.
    /// An application that cannot tolerate that keeps its own idempotency key
    /// outside the counter.
    ///
    /// Fails without sending anything when the broker did not advertise
    /// [`felix_wire::FEATURE_COUNTERS`] — probing an older broker would cost
    /// the connection.
    pub async fn counter_add(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        delta: i64,
    ) -> Result<i64> {
        if !felix_wire::supports_feature(self.server_features, felix_wire::FEATURE_COUNTERS) {
            return Err(anyhow::anyhow!("this broker does not support counters"));
        }
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::CounterAdd {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            key: key.to_string(),
            delta,
            request_id,
        };
        self.counter_round_trip(message, request_id)
            .await?
            .context("an add always answers with the sum it produced")
    }

    /// Read a counter's sum. `Ok(None)` means the counter has never been
    /// written — a different answer from a sum of zero, exactly as a cache
    /// miss differs from a stored empty value.
    pub async fn counter_get(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Result<Option<i64>> {
        if !felix_wire::supports_feature(self.server_features, felix_wire::FEATURE_COUNTERS) {
            return Err(anyhow::anyhow!("this broker does not support counters"));
        }
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::CounterGet {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            key: key.to_string(),
            request_id,
        };
        self.counter_round_trip(message, request_id).await
    }

    async fn counter_round_trip(&self, message: Message, request_id: u64) -> Result<Option<i64>> {
        let (response_tx, response_rx) = oneshot::channel();
        let (worker, conn_index) = self.cache_worker().await?;
        worker
            .tx
            .send(CacheRequest::Counter {
                request_id,
                message,
                response: response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("cache worker closed"))?;
        let current = self.cache_conn_counts[conn_index].fetch_add(1, Ordering::Relaxed) + 1;
        t_gauge!("felix_client_cache_conn_ops", "conn" => conn_index.to_string())
            .set(current as f64);
        t_counter!(
            "felix_client_cache_conn_ops_total",
            "conn" => conn_index.to_string()
        )
        .increment(1);
        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("counter response dropped"))?
    }

    async fn cache_worker(&self) -> Result<(&CacheWorker, usize)> {
        let workers = self
            .cache_workers
            .opened
            .get_or_try_init(|| self.open_cache_workers())
            .await?;
        // Round-robin pick only.
        //
        // IMPORTANT: do not mutate metrics/counters here.
        // We only consider an op "in-flight" once it is successfully enqueued.
        let index = self.cache_worker_rr.fetch_add(1, Ordering::Relaxed) % workers.len();
        let worker = &workers[index];
        Ok((worker, worker.conn_index))
    }

    async fn open_cache_workers(&self) -> Result<Vec<CacheWorker>> {
        // Not tracked for `is_usable`, which only judges clients that open
        // their workers up front.
        let mut connections = Vec::new();
        let mut workers = Vec::with_capacity(self.cache_workers.on_demand);
        for _ in 0..self.cache_workers.on_demand {
            workers.push(
                open_cache_worker(
                    &self.cache_node,
                    &self.credentials,
                    &self.cache_conn_counts,
                    &mut connections,
                    self.runtime_config.max_frame_bytes,
                )
                .await?,
            );
        }
        Ok(workers)
    }
}

/// A client's cache workers: opened at connect, or on its first cache
/// request, so a client that never uses the cache holds no cache connections.
pub(super) struct CacheWorkers {
    opened: tokio::sync::OnceCell<Vec<CacheWorker>>,
    /// How many to open on first use, when none were opened up front.
    on_demand: usize,
}

impl CacheWorkers {
    pub(super) fn opened(workers: Vec<CacheWorker>) -> Self {
        Self {
            opened: tokio::sync::OnceCell::new_with(Some(workers)),
            on_demand: 0,
        }
    }

    pub(super) fn on_demand(count: usize) -> Self {
        Self {
            opened: tokio::sync::OnceCell::new(),
            on_demand: count.max(1),
        }
    }

    pub(super) fn opens_on_demand(&self) -> bool {
        self.on_demand > 0
    }
}
