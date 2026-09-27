//! Where a cache operation should be applied.
//!
//! A cache key hashes to a shard the same way a routing key does for a publish,
//! and that shard has exactly one owner. Resolving here, before any storage is
//! touched, is what stops two brokers accepting writes for the same key — the
//! divergence the log-backed cache had no defence against.
use bytes::Bytes;

use crate::serving::forward::{CacheRequest, ForwardKey, ForwardTarget};
use crate::serving::quic::client_error::ClientError;
use crate::shards::lifecycle::fence::{self, FenceGuard};
use crate::shards::routing::{Dispatch, IngressRouter, dispatch_write, shard_for};
use crate::shards::{ShardKey, ShardKind};

/// Where one cache operation belongs.
#[derive(Debug)]
pub(crate) enum CacheRoute {
    /// This broker owns the shard the key falls in, at `generation`.
    /// `fenced` is the operation's place in the shard's write fence, on a
    /// cluster member whose fence was open.
    Local {
        shard: u32,
        generation: u64,
        fenced: Option<FenceGuard>,
    },
    /// Another broker owns it.
    Forward {
        key: ForwardKey,
        target: ForwardTarget,
    },
    /// Nobody can serve it right now, and this says why.
    Refused(crate::shards::routing::Reason),
}

/// Resolve a cache key to the broker that owns it.
///
/// A single-node broker has no ingress router and takes everything locally,
/// which is what an unsharded cache always did. On a cluster member an
/// operation on a shard that is mid-move waits for the cut-over and then goes
/// to the new owner, as a publish does, rather than being refused.
pub(crate) async fn resolve_cache_route(
    ingress: Option<&IngressRouter>,
    tenant_id: &str,
    namespace: &str,
    cache: &str,
    key: &str,
) -> CacheRoute {
    let Some(router) = ingress else {
        return CacheRoute::Local {
            shard: 0,
            generation: 0,
            fenced: None,
        };
    };

    // The cache's own width, not the stream of the same name: `shards_for`
    // takes the kind precisely so those two cannot be confused.
    let shards = router.shards_for(ShardKind::Cache, tenant_id, namespace, cache);
    let shard = shard_for(shards, Some(key.as_bytes()));
    let shard_key = ShardKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
        stream: cache.to_string(),
        shard,
        kind: ShardKind::Cache,
    };

    let (dispatched, fenced) = dispatch_write(Some(router), &shard_key).await;
    match dispatched {
        Dispatch::Local { generation } => CacheRoute::Local {
            shard,
            generation,
            fenced,
        },
        Dispatch::Forward {
            node_id,
            advertise_addr,
            generation,
        } => CacheRoute::Forward {
            key: ForwardKey {
                tenant_id: shard_key.tenant_id,
                namespace: shard_key.namespace,
                stream: shard_key.stream,
                shard,
            },
            target: ForwardTarget {
                node_id,
                advertise_addr,
                generation,
            },
        },
        Dispatch::Unavailable(reason) => CacheRoute::Refused(reason),
    }
}

/// Apply a cache operation wherever the key belongs.
///
/// The single chokepoint every cache request goes through, which is why the
/// ownership decision lives here: exactly one broker writes any given key, and
/// the others hand the operation to it rather than serving a second copy.
///
/// `Err` carries a message for the client. It never means "applied somewhere
/// else" — a forward that could not be resolved is refused, not silently
/// downgraded to a local write.
// A cache entry's identity is five fields, and routing needs two more sources
// to resolve it. Bundling them would move the argument list rather than shorten
// it, and the publish handlers carry the same allow for the same reason.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_cache_op(
    broker: &felix_broker::Broker,
    // Read by a write to a `Quorum` cache, which is not acknowledged until a
    // majority of the shard's replicas hold it.
    quorum: (
        Option<&crate::replication::quorum::QuorumMarks>,
        std::time::Duration,
    ),
    ingress: Option<&IngressRouter>,
    peers: Option<&crate::peer::PeerPool>,
    // The caller's token, carried on a forward for the owner to verify.
    credential: &str,
    tenant_id: &str,
    namespace: &str,
    cache: &str,
    key: &str,
    request: CacheRequest,
) -> anyhow::Result<Option<Bytes>> {
    match resolve_cache_route(ingress, tenant_id, namespace, cache, key).await {
        CacheRoute::Local {
            shard,
            generation,
            mut fenced,
        } => {
            let cache_store = broker.cache();
            let written = ShardKey {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: cache.to_string(),
                shard,
                kind: ShardKind::Cache,
            };
            let (marks, quorum_timeout) = quorum;
            Ok(match request {
                CacheRequest::Put { value, ttl_ms } => {
                    let ttl = (ttl_ms > 0).then(|| std::time::Duration::from_millis(ttl_ms));
                    let fenced =
                        fence::enter_or_keep(&mut fenced, ingress, Some(&written), generation)
                            .map_err(ClientError::from)?;
                    cache_store
                        .put(tenant_id, namespace, cache, shard, key, value, ttl)
                        .await;
                    drop(fenced);
                    crate::replication::quorum::await_cache_quorum(
                        broker,
                        &written,
                        marks,
                        ingress,
                        quorum_timeout,
                        "write",
                    )
                    .await?;
                    None
                }
                CacheRequest::Get => {
                    // A read does not hold the fence, but it follows the lease.
                    drop(fenced);
                    refuse_read_on_lapse(ingress)?;
                    let value = cache_store
                        .get(tenant_id, namespace, cache, shard, key)
                        .await;
                    crate::replication::quorum::await_cache_quorum(
                        broker,
                        &written,
                        marks,
                        ingress,
                        quorum_timeout,
                        "read",
                    )
                    .await?;
                    value
                }
                CacheRequest::Delete => {
                    let fenced =
                        fence::enter_or_keep(&mut fenced, ingress, Some(&written), generation)
                            .map_err(ClientError::from)?;
                    let removed = cache_store
                        .delete(tenant_id, namespace, cache, shard, key)
                        .await;
                    drop(fenced);
                    crate::replication::quorum::await_cache_quorum(
                        broker,
                        &written,
                        marks,
                        ingress,
                        quorum_timeout,
                        "write",
                    )
                    .await?;
                    removed
                }
                // Counter operations go through `apply_counter_op`, which owns the
                // counter store; routing them here would answer from the wrong
                // seam.
                CacheRequest::CounterAdd { .. } | CacheRequest::CounterGet => {
                    return Err(ClientError::internal(
                        "not a cache operation: counters route separately",
                    )
                    .into());
                }
            })
        }
        CacheRoute::Forward {
            key: forward_key,
            target,
        } => {
            // The route said forward and there is nothing to forward with.
            // Refusing beats serving another broker's key locally, which is
            // exactly the divergence this path exists to prevent.
            let Some(pool) = peers else {
                return Err(ClientError::internal(format!(
                    "no peer transport: this broker cannot forward to {}",
                    target.node_id
                ))
                .with_retry(felix_wire::RetryClass::Retry)
                .into());
            };
            crate::serving::forward::forward_cache_op(
                pool,
                &target,
                &forward_key,
                key,
                credential,
                &request,
            )
            .await
            .map_err(anyhow::Error::from)
        }
        CacheRoute::Refused(reason) => Err(refused(&reason)),
    }
}

/// Apply a counter operation wherever the key belongs.
///
/// A counter key routes exactly as a cache key of the same cache does — same
/// hash, same shard, same owner — which is what "scoped like cache keys"
/// means in practice: one resolution, two stores. The same refusal rules
/// apply, for the same reasons: an unroutable operation is refused rather
/// than served locally, and a failed read is an error rather than a miss.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_counter_op(
    broker: &felix_broker::Broker,
    // As for a cache op: a `Quorum` cache acknowledges a counter add, and
    // answers a counter read, only once a majority holds the counter log.
    quorum: (
        Option<&crate::replication::quorum::QuorumMarks>,
        std::time::Duration,
    ),
    ingress: Option<&IngressRouter>,
    peers: Option<&crate::peer::PeerPool>,
    credential: &str,
    tenant_id: &str,
    namespace: &str,
    cache: &str,
    key: &str,
    request: CacheRequest,
) -> anyhow::Result<Option<i64>> {
    match resolve_cache_route(ingress, tenant_id, namespace, cache, key).await {
        CacheRoute::Local {
            shard,
            generation,
            mut fenced,
        } => {
            let Some(counters) = broker.counters() else {
                return Err(ClientError::internal(
                    "this broker has no durable storage for counters",
                )
                .with_retry(felix_wire::RetryClass::Fatal)
                .into());
            };
            let (marks, quorum_timeout) = quorum;
            // A counter lives on its cache's shard, so it is fenced by it.
            let shard_key = ShardKey {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: cache.to_string(),
                shard,
                kind: ShardKind::Cache,
            };
            match request {
                CacheRequest::CounterAdd { delta } => {
                    let fenced =
                        fence::enter_or_keep(&mut fenced, ingress, Some(&shard_key), generation)
                            .map_err(ClientError::from)?;
                    let (sum, offset) = counters
                        .add(tenant_id, namespace, cache, shard, key, delta)
                        .await
                        .map_err(storage)?;
                    drop(fenced);
                    crate::replication::quorum::await_counter_quorum(
                        broker,
                        &shard_key,
                        marks,
                        ingress,
                        quorum_timeout,
                        Some(offset + 1),
                        "counter add",
                    )
                    .await?;
                    Ok(Some(sum))
                }
                CacheRequest::CounterGet => {
                    drop(fenced);
                    refuse_read_on_lapse(ingress)?;
                    let sum = counters
                        .get(tenant_id, namespace, cache, shard, key)
                        .await
                        .map_err(storage)?;
                    crate::replication::quorum::await_counter_quorum(
                        broker,
                        &shard_key,
                        marks,
                        ingress,
                        quorum_timeout,
                        None,
                        "read",
                    )
                    .await?;
                    Ok(sum)
                }
                // The counter path never builds these; reaching here is a bug
                // in this file, not in the caller.
                other => {
                    Err(ClientError::internal(format!("not a counter operation: {other:?}")).into())
                }
            }
        }
        CacheRoute::Forward {
            key: forward_key,
            target,
        } => {
            let Some(pool) = peers else {
                return Err(ClientError::internal(format!(
                    "no peer transport: this broker cannot forward to {}",
                    target.node_id
                ))
                .with_retry(felix_wire::RetryClass::Retry)
                .into());
            };
            let answer = crate::serving::forward::forward_cache_op(
                pool,
                &target,
                &forward_key,
                key,
                credential,
                &request,
            )
            .await?;
            answer
                .map(|bytes| felix_storage::counter_log::decode_sum(&bytes))
                .transpose()
                .map_err(|err| ClientError::internal(err.to_string()).into())
        }
        CacheRoute::Refused(reason) => Err(refused(&reason)),
    }
}

/// A cache write, ready to forward.
pub(crate) fn put_request(value: Bytes, ttl: Option<std::time::Duration>) -> CacheRequest {
    CacheRequest::Put {
        value,
        // Zero means "no expiry" on the wire, and a TTL that rounds to zero
        // milliseconds is indistinguishable from none anyway.
        ttl_ms: ttl.map_or(0, |ttl| ttl.as_millis() as u64),
    }
}

#[cfg(test)]
mod tests;

/// A read of a shard this broker leads is refused while its lease has
/// lapsed: another broker may be leading it, with writes this one never saw.
fn refuse_read_on_lapse(ingress: Option<&IngressRouter>) -> Result<(), ClientError> {
    if ingress.is_some_and(|ingress| !ingress.fence().lease_valid()) {
        use crate::cluster::lease::metrics;
        metrics::record_refusal(metrics::BOUNDARY_READ);
        return Err(ClientError::from(fence::Fenced::LeaseLapsed));
    }
    Ok(())
}

fn refused(reason: &crate::shards::routing::Reason) -> anyhow::Error {
    ClientError::unavailable(reason, reason.to_string()).into()
}

fn storage(err: impl std::fmt::Display) -> anyhow::Error {
    ClientError::new(felix_wire::ErrorCode::Storage, err.to_string()).into()
}
