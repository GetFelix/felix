//! Writing a delete for each cache entry whose TTL has passed.
//!
//! A cache read treats an expired entry as absent, but a watch only hears
//! about what reaches the log. The shard's leader therefore writes the
//! delete, through the same fence a client's write takes, and replication
//! carries it to the followers like any other record.

use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use tokio_util::sync::CancellationToken;

use felix_storage::StorageApi;

use crate::shards::lifecycle::fence::{FenceGuard, ShardFence};
use crate::shards::routing::{Dispatch, IngressRouter};
use crate::shards::{ShardKey, ShardKind};

/// How often due entries are looked for. A watcher hears of an expiry at most
/// this long after it.
pub(super) const INTERVAL: Duration = Duration::from_secs(1);

/// Deletes written per shard per pass, so one shard with a mass expiry does
/// not hold up the others.
const PER_SHARD: usize = 1024;

/// Expire due entries on every cache shard this broker leads, until
/// `shutdown`.
pub(super) async fn run(
    broker: Arc<Broker>,
    ingress: Option<Arc<IngressRouter>>,
    interval: Duration,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
        expire_once(&broker, ingress.as_deref()).await;
    }
}

/// One pass over the cache shards open here.
pub(super) async fn expire_once(broker: &Broker, ingress: Option<&IngressRouter>) {
    let cache = broker.cache();
    for (tenant_id, namespace, name, shard) in cache.open_shards() {
        let key = ShardKey {
            tenant_id,
            namespace,
            stream: name,
            shard,
            kind: ShardKind::Cache,
        };
        // A follower's copy is written by its leader, and a write here would
        // be one the leader never made.
        let Dispatch::Local { generation } = crate::shards::routing::dispatch(ingress, &key) else {
            continue;
        };
        let fenced = match ingress {
            Some(ingress) => match ingress.fence().admit(&key, generation) {
                Ok(guard) => Some((ingress.fence().as_ref(), guard)),
                Err(_) => continue,
            },
            None => None,
        };
        let still_leading = || {
            fenced
                .as_ref()
                .is_none_or(|(fence, guard)| still_leads(fence, guard))
        };
        expire_shard(cache, &key, &still_leading).await;
    }
}

/// Write one shard's due deletes while `still_leading` holds.
async fn expire_shard(
    cache: &(dyn StorageApi + Send),
    key: &ShardKey,
    still_leading: &(dyn Fn() -> bool + Send + Sync),
) {
    if let Err(err) = cache
        .expire_due(
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
            PER_SHARD,
            still_leading,
        )
        .await
    {
        tracing::warn!(
            cache = %key.stream,
            shard = key.shard,
            error = %err,
            "could not write the deletes for expired cache entries",
        );
    }
}

/// Checked before each delete, not once per pass: a pass can write up to
/// [`PER_SHARD`] records, and each must be one a client write would still be
/// let through for. A closed fence or a lapsed lease ends the pass.
fn still_leads(fence: &ShardFence, guard: &FenceGuard) -> bool {
    guard.still_open() && fence.recheck(guard).is_ok()
}

#[cfg(test)]
mod tests;
