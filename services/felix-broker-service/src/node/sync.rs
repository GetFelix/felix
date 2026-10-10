//! The catalog sync task, and the readiness flip that waits on its first pass.

use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use felix_common::lifecycle::Readiness;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cluster::credential::NodeCredential;
use crate::config::BrokerConfig;
use crate::serving::auth::ControlPlaneKeyStore;

/// How long the first sync may spend fetching every known tenant's JWKS
/// before it reports the catalog seeded anyway.
const JWKS_WARM_BUDGET: Duration = Duration::from_secs(5);

/// Start a periodic control-plane sync to keep tenant/namespace/stream metadata
/// refreshed. When disabled, the broker relies solely on local registrations.
///
/// `seeded_tx` is signalled once the first pass is applied and `key_store`
/// holds the JWKS of every tenant it named, but only when readiness is gated
/// on it. `catalog_seeded` is cancelled at the same point either way.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_catalog_sync(
    config: &BrokerConfig,
    broker: &Arc<Broker>,
    credential: &Option<NodeCredential>,
    sync_shutdown: &CancellationToken,
    gate_readiness_on_sync: bool,
    seeded_tx: oneshot::Sender<()>,
    catalog_seeded: &CancellationToken,
    key_store: &Arc<ControlPlaneKeyStore>,
) -> Option<JoinHandle<()>> {
    if let Some(base_url) = config.controlplane_url.clone() {
        let sync_credential = credential.clone();
        let interval_ms = config.controlplane_sync_interval_ms;
        let broker = Arc::clone(broker);
        let sync_shutdown = sync_shutdown.clone();
        let seeded_tx = gate_readiness_on_sync.then_some(seeded_tx);
        let (applied_tx, applied_rx) = oneshot::channel();
        let catalog_seeded = catalog_seeded.clone();
        let warm_broker = Arc::clone(&broker);
        let key_store = Arc::clone(key_store);
        // Ends with the sync task, which drops the sender if it never seeds.
        tokio::spawn(async move {
            if applied_rx.await.is_ok() {
                // While the control plane is up, so the keys are here if it
                // goes down before a client of each tenant has connected.
                let _ = &warm_broker;
                key_store.warm(Vec::new(), JWKS_WARM_BUDGET).await;
                catalog_seeded.cancel();
                if let Some(seeded_tx) = seeded_tx {
                    let _ = seeded_tx.send(());
                }
            }
        });
        // The feeds require `node.view:cluster:*`, so a sync with nothing to
        // present is refused on every poll. Said once here, at startup, rather
        // than discovered from a wall of 401s -- and as a warning, because the
        // JWKS fetch that verifies client tokens is unauthenticated and still
        // works.
        if sync_credential.is_none() {
            tracing::warn!(
                "FELIX_CONTROLPLANE_URL is set but no FELIX_NODE_TOKEN: the control \
                 plane will refuse the metadata sync, so no tenant, namespace, stream \
                 or cache will be learned from it"
            );
        }
        Some(tokio::spawn(async move {
            // `start_sync` polls forever, so cancellation is what ends it. Dropping
            // it mid-iteration is safe: the sync is a read-only metadata refresh
            // whose cursor only advances on success, so an interrupted iteration is
            // the same case as a failed one and is simply re-fetched on next start.
            tokio::select! {
                _ = sync_shutdown.cancelled() => {
                    tracing::info!("control plane sync stopped");
                }
                result = crate::cluster::catalog_sync::start_sync_with_signal(
                    broker,
                    base_url,
                    Duration::from_millis(interval_ms),
                    Some(applied_tx),
                    sync_credential,
                ) => {
                    if let Err(err) = result {
                        tracing::warn!(error = %err, "control plane sync exited");
                    }
                }
            }
        }))
    } else {
        tracing::info!("control plane sync disabled (FELIX_CONTROLPLANE_URL not set)");
        None
    }
}

/// Flip to ready once the catalog has been applied and every durable stream
/// it named has been recovered.
///
/// This has to be armed *before* the shutdown await, not after it: a task
/// spawned below that point would only start listening for the seed signal
/// once the broker was already draining, so it would never report ready
/// while serving — and could flip back to ready in the middle of a drain.
///
/// A task rather than an inline await: `/ready` already reports false, so
/// blocking startup here would only delay the point at which an operator
/// can observe that state.
///
/// The task holds a `Readiness` clone and ends when the signal resolves or
/// its sender is dropped, so nothing keeps it alive past shutdown.
pub(super) fn spawn_readiness_flip(
    gate_readiness_on_sync: bool,
    readiness: &Readiness,
    draining: &CancellationToken,
    seeded: &CancellationToken,
    seeded_rx: oneshot::Receiver<()>,
) {
    if gate_readiness_on_sync {
        let readiness = readiness.clone();
        let draining = draining.clone();
        let seeded = seeded.clone();
        tokio::spawn(async move {
            // `Readiness` is a single flag, so it cannot distinguish "never
            // ready yet" from "already drained" — both read false. The drain
            // token is what makes the difference observable, so a seed that
            // lands mid-drain cannot flip the broker back to ready.
            tokio::select! {
                biased;
                _ = draining.cancelled() => {
                    tracing::warn!("shutdown began before the initial sync; staying unready");
                }
                result = seeded_rx => {
                    if result.is_ok() {
                        // Release the accept loop first, so a connection that
                        // arrives the instant readiness flips is answered
                        // rather than dropped.
                        seeded.cancel();
                        readiness.mark_ready();
                        tracing::info!("initial control-plane sync applied; reporting ready");
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests;
