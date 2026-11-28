//! Opening the broker over its storage and wiring it to the cluster state.

use std::sync::Arc;

use anyhow::Result;
use felix_broker::{Broker, DurableStorage};

use super::cluster::ShardState;
use super::storage;
use crate::cluster::membership::Registered;
use crate::config::{BrokerConfig, DurableStorageConfig};

/// A broker opened over its storage, before anything serves.
pub(super) struct Opened {
    pub(super) broker: Arc<Broker>,
    pub(super) durable_storage: Option<DurableStorage>,
    pub(super) storage_id: felix_storage::StorageId,
}

/// The broker over its opened storage, before it is wired to the fleet.
pub(super) struct Stored {
    pub(super) broker: Arc<Broker>,
    pub(super) durable_storage: Option<DurableStorage>,
    pub(super) storage_id: felix_storage::StorageId,
}

/// Refuse a standalone start on Raft over a data root the hybrid engine
/// wrote. A Raft member never opens such a shard directory, so the broker
/// would come up with every shard refused. There is no hybrid-to-Raft
/// upgrade in 0.8.
#[cfg(feature = "raft")]
pub(super) fn refuse_hybrid_data(root: &std::path::Path) -> Result<()> {
    // Stream shards sit in the root itself; the hybrid's other stores each
    // have a root of their own (`node/storage.rs`).
    let stores = ["caches", "counters", "groups", "dead-letters"];
    let dirs = std::iter::once(root.to_path_buf()).chain(stores.map(|store| root.join(store)));
    for dir in dirs {
        if let Some(found) = felix_storage::raft_copy::hybrid_log_under(&dir)? {
            anyhow::bail!(
                "{} holds data the hybrid engine wrote, and this standalone broker runs its \
                 shards on Raft, which cannot open it and 0.8 does not migrate it. Point \
                 FELIX_DURABLE_STORAGE_DIR at an empty directory",
                found.display()
            );
        }
    }
    Ok(())
}

/// Refuse a Raft start whose broker has durable storage but a cache store
/// with no logs. Every cache of a cluster is a Raft group on a log, and a
/// broker with a disk is given cache shards, so its cache cannot stay in
/// memory. A broker with no durable storage is not refused: it routes caches
/// to the group.
pub(super) fn refuse_logless_cache(
    engine: crate::config::ShardEngine,
    broker: &felix_broker::Broker,
) -> Result<()> {
    if engine == crate::config::ShardEngine::Raft
        && broker.durable_storage().is_some()
        && broker.cache().as_log_cache().is_none()
    {
        anyhow::bail!(
            "this broker runs its shards on Raft and has durable storage \
             (FELIX_DURABLE_STORAGE_DIR), but its cache store keeps no logs. A Raft \
             cluster has no in-memory cache: open the cache on the durable storage \
             root, or unset FELIX_DURABLE_STORAGE_DIR to run a broker without a disk"
        );
    }
    Ok(())
}

/// Open storage and the broker over it.
pub(super) fn open_storage(
    config: &BrokerConfig,
    durable: Option<(DurableStorageConfig, &felix_storage::disk_log::RootLock)>,
    registered: Option<&Registered>,
) -> Result<Stored> {
    let (broker, durable_storage, storage_id, ..) =
        storage::open_with(config, durable, registered)?;
    Ok(Stored {
        broker: Arc::new(broker),
        durable_storage,
        storage_id,
    })
}

/// Set the gates that depend on what the fleet has finalized.
///
/// The order is load-bearing; each step's comment says why.
pub(super) async fn wire_broker(
    config: &BrokerConfig,
    stored: Stored,
    cluster: &Option<ShardState>,
    fleet: &Arc<felix_common::fleet::FleetGate>,
    engine: &Arc<crate::shards::routing::StreamEngine>,
) -> Result<Opened> {
    let Stored {
        broker,
        durable_storage,
        storage_id,
    } = stored;
    let ingress_router = cluster
        .as_ref()
        .map(|(_, ingress, _, _)| Arc::clone(ingress));
    // A record with its publisher is storage format v6, which an older build
    // refuses to open, so a cluster waits for its fleet to finalize it and a
    // lone broker for its operator to ask.
    if config.membership.is_some() {
        let fleet = Arc::clone(fleet);
        broker.record_publishers_when(move || {
            fleet.supports(felix_common::fleet::PUBLISHER_PRINCIPAL)
        });
    } else if config.record_publishers {
        broker.record_publishers_when(|| true);
    }
    // A record with headers is storage format v8, which an older build refuses
    // to open, so a cluster member enables them once its fleet has finalized
    // `record_headers`. A lone broker has no older peer, and a client opts in
    // by sending them. An operator who must keep the log readable by older
    // builds turns publishing them off, but reads stay on so a log that holds
    // headers still delivers them. The Kafka listener adds its own opt-in
    // (`FELIX_KAFKA_RECORD_HEADERS`), since nearly every Kafka producer sends
    // keys and this gate alone would move a lone broker to v8 for them.
    if config.membership.is_some() {
        let fleet = Arc::clone(fleet);
        broker.record_headers_when(move || fleet.supports(felix_common::fleet::RECORD_HEADERS));
    } else {
        let enabled = config.record_headers;
        broker.record_headers_when(move || enabled);
        broker.read_record_headers_when(|| true);
    }
    // Once this broker has seen streams carry incarnations, a stream's logs
    // open only once the catalog has registered it, at the incarnation it
    // names. Read from its own data, before any shard task or peer can open
    // one, not from the fleet's features, which arrive only after the catalog
    // seeds: a restart would otherwise open unregistered streams at 0.
    if let Err(err) = broker.require_registration_if_in_play() {
        tracing::error!(error = %err, "could not tell whether streams here carry incarnations; requiring registration");
        broker.require_registered_streams();
    }
    // Before the catalog registers anything: a stream deleted when the broker
    // stopped is finished first, so its registration cannot open it again.
    if let Err(err) = broker.finish_pending_deletes().await {
        tracing::error!(error = %err, "could not finish deleting streams; retrying as they are named again");
    }
    // The same for caches, which carry incarnations of their own.
    if let Err(err) = broker.require_cache_registration_if_in_play() {
        tracing::error!(error = %err, "could not tell whether caches here carry incarnations; requiring registration");
        broker.require_registered_caches();
    }
    if let Err(err) = broker.finish_pending_cache_deletes().await {
        tracing::error!(error = %err, "could not finish deleting caches; retrying as they are named again");
    }
    // A follower that predates trim records stops replicating at one, so a
    // cluster member trims only once its fleet has finalized `head_trim`. A
    // lone broker has no followers.
    // The same for a trim entry on a shard on Raft, which an older member
    // refuses whole.
    if config.membership.is_some() {
        let trims = Arc::clone(fleet);
        broker.allow_head_trims_when(move || trims.supports(felix_common::fleet::HEAD_TRIM));
        let raft_trims = Arc::clone(fleet);
        broker.allow_raft_head_trims_when(move || {
            raft_trims.supports(felix_common::fleet::HEAD_TRIM)
                && raft_trims.supports(felix_common::fleet::RAFT_HEAD_TRIM)
        });
        // A follower's read index is a Raft message an older leader does
        // not know, so until then such reads go to the leader.
        let raft_reads = Arc::clone(fleet);
        broker.allow_raft_follower_reads_when(move || {
            raft_reads.supports(felix_common::fleet::RAFT_FOLLOWER_READ)
        });
        // A Raft entry with a publisher or headers is one an older member
        // refuses whole, as a trim entry is.
        let raft_headers = Arc::clone(fleet);
        broker.allow_raft_record_headers_when(move || {
            raft_headers.supports(felix_common::fleet::RECORD_HEADERS)
                && raft_headers.supports(felix_common::fleet::RAFT_RECORD_HEADERS)
        });
        // A conditional piece is an entry and a record kind an older member
        // refuses.
        let raft_cond = Arc::clone(fleet);
        broker.allow_raft_conditional_batches_when(move || {
            raft_cond.supports(felix_common::fleet::RAFT_CONDITIONAL_BATCHES)
        });
    } else {
        broker.allow_head_trims_when(|| true);
        broker.allow_raft_head_trims_when(|| true);
        broker.allow_raft_follower_reads_when(|| true);
        broker.allow_raft_record_headers_when(|| true);
        broker.allow_raft_conditional_batches_when(|| true);
    }
    // A cursor that carries its dependency is a value an older build refuses,
    // so a cluster waits for its fleet. A lone broker writes them from the
    // start: it has no older replica to read them.
    if config.membership.is_some()
        && let Some(groups) = broker.consumer_groups()
    {
        let fleet = Arc::clone(fleet);
        groups.record_dependencies_when(move || {
            fleet.supports(felix_common::fleet::GROUP_CURSOR_DEPENDENCIES)
        });
    }
    // Readers of a replicated `Quorum` stream see only what its quorum mark
    // has passed; see `StreamEngine::committed_bound`.
    if ingress_router.is_some() {
        broker.set_read_bounds(Arc::new(crate::shards::routing::CommittedReads::new(
            Arc::clone(engine),
        )));
    }
    Ok(Opened {
        broker,
        durable_storage,
        storage_id,
    })
}

#[cfg(test)]
mod tests;
