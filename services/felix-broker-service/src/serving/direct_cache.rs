//! Cache operations on a broker in-process, without a connection.
//!
//! The same routing, fence, store and quorum wait a QUIC cache request goes
//! through; see `cache_routing`. Resolving first and applying second lets a
//! caller that has no peer transport go to the owner itself instead of being
//! forwarded.

use crate::shards::routing::StreamEngine;
use bytes::Bytes;
use felix_broker::Broker;

pub use super::direct_publish::Refusal;
use super::forward::{CacheRequest, ForwardTarget};
use crate::serving::cache_routing::{CacheRoute, apply_cache_local, resolve_cache_route};
use crate::shards::lifecycle::fence::FenceGuard;
use crate::shards::routing::IngressRouter;

/// A cache shard this broker owns, at the generation routing admitted it under.
pub struct LocalCache {
    shard: u32,
    generation: u64,
    fenced: Option<FenceGuard>,
}

/// Where a cache operation goes.
pub enum CacheResolution {
    /// This broker owns the key's shard.
    Local(LocalCache),
    /// Another broker owns it; `owner.node_id` says which.
    Forward(ForwardTarget),
}

/// What to do to a key.
#[derive(Debug, Clone)]
pub enum CacheOp {
    Put { value: Bytes },
    Get,
    Delete,
}

/// Resolve where an operation on `key` goes, as the control stream does.
pub async fn resolve_cache(
    ingress: Option<&IngressRouter>,
    tenant_id: &str,
    namespace: &str,
    cache: &str,
    key: &str,
) -> Result<CacheResolution, Refusal> {
    match resolve_cache_route(ingress, tenant_id, namespace, cache, key).await {
        CacheRoute::Local {
            shard,
            generation,
            fenced,
        } => Ok(CacheResolution::Local(LocalCache {
            shard,
            generation,
            fenced,
        })),
        CacheRoute::Forward { target, .. } => Ok(CacheResolution::Forward(target)),
        CacheRoute::Refused(reason) => {
            let err = crate::serving::quic::client_error::ClientError::unavailable(
                &reason,
                reason.to_string(),
            );
            Err(Refusal {
                code: err.code().clone(),
                retry: err.retry(),
                message: err.message().to_string(),
            })
        }
    }
}

/// Apply `op` to a key on a shard this broker owns: enter the fence, touch the
/// store, and on a `Quorum` cache wait for a majority. A put or delete that
/// returns an error may still have applied. Returns what a get found or a
/// delete removed.
#[allow(clippy::too_many_arguments)]
pub async fn apply_direct(
    broker: &Broker,
    ingress: Option<&IngressRouter>,
    engine: &StreamEngine,
    quorum_timeout: std::time::Duration,
    tenant_id: &str,
    namespace: &str,
    cache: &str,
    key: &str,
    target: LocalCache,
    op: CacheOp,
) -> anyhow::Result<Option<Bytes>> {
    let request = match op {
        CacheOp::Put { value } => CacheRequest::Put { value, ttl_ms: 0 },
        CacheOp::Get => CacheRequest::Get,
        CacheOp::Delete => CacheRequest::Delete,
    };
    let LocalCache {
        shard,
        generation,
        fenced,
    } = target;
    apply_cache_local(
        broker,
        (engine, quorum_timeout),
        ingress,
        tenant_id,
        namespace,
        cache,
        key,
        request,
        None,
        shard,
        generation,
        fenced,
    )
    .await
    .map(|answer| answer.value)
}
