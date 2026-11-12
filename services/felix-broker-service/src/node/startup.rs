//! Opening the broker over its storage and wiring it to the cluster state,
//! shared by the real node and the simulator's.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use felix_broker::{Broker, DurableStorage};
use felix_replication::peer::PeerRequester;
use felix_replication::{self as replication, quorum::QuorumMarks};

use super::cluster::ShardState;
use super::storage;
use crate::cluster::membership::Registered;
use crate::config::{BrokerConfig, DurableStorageConfig};

/// A broker opened over its storage, before anything serves.
pub(super) struct Opened {
    pub(super) broker: Arc<Broker>,
    pub(super) durable_storage: Option<DurableStorage>,
    pub(super) storage_id: felix_storage::StorageId,
    /// Set after a start that may have cost acknowledged records.
    pub(super) unclean: Option<Arc<replication::unclean::UncleanStart>>,
}

/// The broker over its opened storage, before it is wired to the fleet.
pub(super) struct Stored {
    pub(super) broker: Broker,
    pub(super) durable_storage: Option<DurableStorage>,
    pub(super) storage_id: felix_storage::StorageId,
    /// Set after a start that may have cost acknowledged records.
    pub(super) unclean: bool,
    /// Set when this start found the data directory older than the node's
    /// last registration.
    pub(super) replaced_storage: bool,
    /// What the directory kept of the ballots it took.
    pub(super) kept: storage::Kept,
    /// The directory's record of trusted and doubted copies, which outlives
    /// the process. `None` without a durable directory.
    pub(super) copy_trust: Option<Arc<felix_storage::run_state::copy_trust::CopyTrust>>,
    /// The ids this directory had before a crash rotated them away, keeping
    /// its ballots.
    pub(super) rotated_ids: Vec<felix_storage::StorageId>,
}

/// Open storage and the broker over it.
pub(super) fn open_storage(
    config: &BrokerConfig,
    durable: Option<DurableStorageConfig>,
    registered: Option<&Registered>,
) -> Result<Stored> {
    let root = durable.as_ref().map(|durable| durable.root.clone());
    let (broker, durable_storage, storage_id, unclean, replaced_storage, kept) =
        storage::open_with(config, durable, registered)?;
    // Read after the run began, which distrusts every copy when it rotated
    // the id. Live, so a rotation later in the run distrusts them here too.
    let copy_trust = root
        .as_ref()
        .map(|root| felix_storage::run_state::copy_trust::CopyTrust::open_live(root))
        .transpose()?;
    let rotated_ids = match &root {
        Some(root) => felix_storage::run_state::rotated_ids(root)?,
        None => Vec::new(),
    };
    Ok(Stored {
        broker,
        durable_storage,
        storage_id,
        unclean,
        replaced_storage,
        kept,
        copy_trust,
        rotated_ids,
    })
}

/// [`open_storage`] then [`wire_broker`], for a start that has nothing to
/// decide in between.
#[cfg(feature = "sim")]
pub(super) async fn open_broker(
    config: &BrokerConfig,
    durable: Option<DurableStorageConfig>,
    registered: Option<&Registered>,
    cluster: &Option<ShardState>,
    fleet: &Arc<felix_common::fleet::FleetGate>,
    quorum_marks: &Arc<QuorumMarks>,
    engine: &Arc<crate::shards::routing::StreamEngine>,
) -> Result<Opened> {
    let stored = open_storage(config, durable, registered)?;
    wire_broker(config, stored, cluster, fleet, quorum_marks, engine).await
}

/// Record this start in the quorum marks and the fence, and set the gates that
/// depend on what the fleet has finalized.
///
/// The order is load-bearing; each step's comment says why.
pub(super) async fn wire_broker(
    config: &BrokerConfig,
    stored: Stored,
    cluster: &Option<ShardState>,
    fleet: &Arc<felix_common::fleet::FleetGate>,
    quorum_marks: &Arc<QuorumMarks>,
    engine: &Arc<crate::shards::routing::StreamEngine>,
) -> Result<Opened> {
    let Stored {
        broker,
        durable_storage,
        storage_id,
        unclean,
        replaced_storage,
        kept,
        copy_trust,
        rotated_ids,
    } = stored;
    let ingress_router = cluster
        .as_ref()
        .map(|(_, ingress, _, _)| Arc::clone(ingress));
    quorum_marks.set_local_storage(storage_id, replaced_storage);
    quorum_marks.set_rotated_storage(rotated_ids);
    // Left unset, which attests nothing, until a deferred restore check sets
    // them.
    if !kept.deferred {
        quorum_marks.set_readmits(kept.readmits);
        quorum_marks.set_kept_ballots(kept.ballots);
    }
    quorum_marks.set_minted_storage(kept.minted);
    if let Some(trust) = &copy_trust {
        quorum_marks.set_copy_trust(Arc::clone(trust));
    }
    quorum_marks.set_unified_fence(
        durable_storage.is_some()
            && config
                .peer_transport
                .as_ref()
                .is_some_and(|peer| peer.fence && peer.unified_fence),
    );
    // Before anything can lead or answer a fence: a copy this broker led
    // before a crash that may have cost it acknowledged records is not one it
    // can lead from or vouch for (#1295).
    let unclean = match (&config.membership, cluster) {
        (Some(membership), Some((_, ingress, _, _))) if unclean => {
            let mut start =
                felix_replication::unclean::UncleanStart::new(membership.node_id.as_str())
                    .with_kept_promises(kept.promises);
            if let Some(trust) = copy_trust {
                start = start.with_copy_trust(trust);
            }
            let unclean = Arc::new(start);
            ingress.fence().bind_unclean_start(Arc::clone(&unclean));
            quorum_marks.set_unclean_start(Arc::clone(&unclean));
            Some(unclean)
        }
        _ => None,
    };
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
    if config.membership.is_some() {
        let fleet = Arc::clone(fleet);
        broker.allow_head_trims_when(move || fleet.supports(felix_common::fleet::HEAD_TRIM));
    } else {
        broker.allow_head_trims_when(|| true);
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
    // has passed; see `replication::engine::Engine::committed_bound`.
    if ingress_router.is_some() {
        broker.set_read_bounds(Arc::new(replication::engine::CommittedReads::new(
            Arc::clone(engine),
        )));
    }
    Ok(Opened {
        broker: Arc::new(broker),
        durable_storage,
        storage_id,
        unclean,
    })
}

/// A `Quorum` read confirms leadership by a round of fences, over the same
/// transport and to the same replicas a promotion fences. `reads_by_lease` is
/// only asked when there is a fence to confirm over.
pub(super) fn bind_read_check<R: PeerRequester + Send + Sync + 'static>(
    config: &BrokerConfig,
    cluster: &Option<ShardState>,
    peers: &Option<Arc<R>>,
    broker: &Arc<Broker>,
    quorum_marks: &Arc<QuorumMarks>,
    reads_by_lease: impl FnOnce() -> Result<bool>,
) -> Result<()> {
    if let (Some((router, ..)), Some(pool)) = (cluster, peers)
        && pool.offers_fence()
    {
        let by_lease = reads_by_lease()?;
        replication::engine::bind_read_rounds(
            quorum_marks,
            Arc::clone(pool),
            Arc::clone(broker),
            Arc::clone(router),
            Duration::from_millis(config.publish_quorum_timeout_ms.max(1)),
            by_lease,
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests;
