//! Opening what the broker keeps on disk, and building the `Broker` over it.
//!
//! Durable storage is opened before the listener binds: recovering segments
//! can take time and can fail, and both are better surfaced as a startup
//! error than as a failed publish once traffic is arriving.

use std::time::Duration;

use anyhow::{Context, Result};
use felix_broker::{Broker, DurableStorage};
use felix_storage::EphemeralCache;

use crate::config::{BrokerConfig, DurableStorageConfig};

/// Open durable storage, the cache, consumer groups and counters, and build
/// the broker over them. Without durable storage everything but the cache is
/// left out and the cache lives in memory.
pub(crate) fn open(config: &BrokerConfig) -> Result<(Broker, Option<DurableStorage>)> {
    let durable_config = DurableStorageConfig::from_env()?;
    // Test-only: the cluster harness's power loss. Armed before any store
    // opens, since the model takes what is already on disk as durable.
    #[cfg(debug_assertions)]
    felix_storage::fault::arm_power_loss_from_env().context("arm the simulated power loss")?;
    let hold = retention_hold(config);
    let durable_storage = match &durable_config {
        Some(durable) => {
            tracing::info!(config = %durable.summary(), "opening durable stream storage");
            let cluster_node = config.membership.as_ref().map(|m| m.node_id.as_str());
            let stream_log = felix_storage::log::LogConfig {
                retention_hold: hold,
                ..durable
                    .stream_log(cluster_node)
                    .context("resolve the node id for offloaded segments")?
            };
            let storage = DurableStorage::open(&durable.root, stream_log)
                .with_context(|| format!("open durable storage at {}", durable.root.display()))?;
            Some(storage)
        }
        None => {
            tracing::info!(
                "durable stream storage disabled (set FELIX_DURABLE_STORAGE_DIR to enable)"
            );
            None
        }
    };

    // The cache is a log when there is a disk to put one on.
    //
    // Under `caches/`, not the stream root: a shard directory is named from a
    // hash of its tenant, namespace and stream, so a cache and a stream sharing
    // a name would otherwise interleave their records. See
    // `docs/cache-on-log.md`.
    //
    // Without durable storage the cache stays in memory, which is the only
    // thing it can be: there is nowhere to write a log.
    let cache: Box<dyn felix_storage::StorageApi + Send> = match &durable_config {
        Some(durable) => {
            let root = durable.root.join("caches");
            tracing::info!(root = %root.display(), "opening the cache on durable storage");
            Box::new(
                felix_storage::LogCache::open(
                    &root,
                    felix_storage::log::LogConfig {
                        retention_hold: hold,
                        ..durable.log.clone()
                    },
                )
                .with_context(|| format!("open the cache log at {}", root.display()))?,
            )
        }
        None => {
            tracing::info!("cache is in memory and is lost on restart");
            Box::new(EphemeralCache::new())
        }
    };

    // Consumer-group positions live on their own root, for the same reason the
    // cache does: a stream named `orders` and a cache named `orders` must not
    // share a directory, and neither must the group state for either.
    //
    // Only with durable storage. A group whose position is lost on restart
    // redelivers everything it had already processed, so an in-memory version
    // would be worse than not offering queues at all.
    let consumer_groups = match &durable_config {
        Some(durable) => {
            let root = durable.root.join("groups");
            tracing::info!(root = %root.display(), "opening consumer-group state");
            let dead_root = durable.root.join("dead-letters");
            Some((
                std::sync::Arc::new(
                    felix_broker::ConsumerGroups::open(&root, durable.log.clone()).with_context(
                        || format!("open the consumer-group log at {}", root.display()),
                    )?,
                ),
                std::sync::Arc::new(
                    felix_broker::DeadLetters::open(&dead_root, durable.log.clone()).with_context(
                        || format!("open the dead-letter log at {}", dead_root.display()),
                    )?,
                ),
            ))
        }
        None => None,
    };

    let broker = Broker::new(cache)
        .with_topic_capacity(config.subscriber_queue_capacity.max(1))
        .context("configure subscriber queue depth")?
        .with_subscriber_queue_policy(config.subscriber_queue_policy);
    let broker = match consumer_groups {
        Some((groups, dead_letters)) => broker.with_consumer_groups(
            groups,
            dead_letters,
            Duration::from_millis(config.group_visibility_timeout_ms),
            config.group_max_attempts,
        ),
        None => broker,
    };
    if let Some(reader) = broker.group_reader() {
        reader.set_max_in_flight(config.group_max_in_flight);
    }
    let broker = match durable_storage.clone() {
        Some(storage) => broker.with_durable_storage(storage),
        None => broker,
    };
    // Counters on a root of their own, beside the cache's rather than inside
    // it: a counter record is a new durable shape, and a store of its own
    // keeps its blast radius to the counters. Only with durable storage — a
    // sum any restart resets is worse than refusing to count.
    let broker = match &durable_config {
        Some(durable) => {
            let root = durable.root.join("counters");
            tracing::info!(root = %root.display(), "opening counters");
            broker.with_counters(std::sync::Arc::new(
                felix_storage::CounterStore::open(&root, durable.log.clone())
                    .with_context(|| format!("open the counter log at {}", root.display()))?,
            ))
        }
        None => broker,
    };
    Ok((broker, durable_storage))
}

/// How stream and cache logs treat a retention hold they saved.
///
/// A broker with neither membership nor a peer transport never replicates:
/// nothing would ever advance a log's commit offset, and it has no follower
/// to protect, so a hold saved while it was clustered is lifted. Read from
/// configuration, so a clustered broker whose replication has not started
/// yet keeps its holds.
fn retention_hold(config: &BrokerConfig) -> felix_storage::log::RetentionHold {
    if config.membership.is_none() && config.peer_transport.is_none() {
        felix_storage::log::RetentionHold::Lifted
    } else {
        felix_storage::log::RetentionHold::AsSaved
    }
}

#[cfg(test)]
mod tests;
