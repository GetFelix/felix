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

/// Open storage and the broker, record this start in the quorum marks and the
/// fence, and set the gates that depend on what the fleet has finalized.
///
/// The order is load-bearing; each step's comment says why.
pub(super) async fn open_broker(
    config: &BrokerConfig,
    durable: Option<DurableStorageConfig>,
    registered: Option<&Registered>,
    cluster: &Option<ShardState>,
    fleet: &Arc<felix_common::fleet::FleetGate>,
    quorum_marks: &Arc<QuorumMarks>,
) -> Result<Opened> {
    let ingress_router = cluster
        .as_ref()
        .map(|(_, ingress, _, _)| Arc::clone(ingress));
    let (broker, durable_storage, storage_id, unclean, replaced_storage) =
        storage::open_with(config, durable, registered)?;
    quorum_marks.set_local_storage(storage_id, replaced_storage);
    // Before anything can lead or answer a fence: a copy this broker led
    // before a crash that may have cost it acknowledged records is not one it
    // can lead from or vouch for (#1295).
    let unclean = match (&config.membership, cluster) {
        (Some(membership), Some((_, ingress, _, _))) if unclean => {
            let unclean = Arc::new(felix_replication::unclean::UncleanStart::new(
                membership.node_id.as_str(),
            ));
            ingress.fence().bind_unclean_start(Arc::clone(&unclean));
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
    // has passed; see `replication::quorum::committed_bound`.
    if let Some(ingress) = &ingress_router {
        broker.set_read_bounds(Arc::new(replication::quorum::CommittedReads::new(
            Arc::clone(quorum_marks),
            Arc::clone(ingress) as Arc<dyn replication::quorum::ShardServing>,
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
        quorum_marks.set_read_check(
            Arc::new(replication::leadership::ReadIndex::new(
                Arc::clone(pool),
                Arc::clone(broker),
                Arc::clone(router),
                Duration::from_millis(config.publish_quorum_timeout_ms.max(1)),
            )),
            reads_by_lease()?,
        );
    }
    Ok(())
}
