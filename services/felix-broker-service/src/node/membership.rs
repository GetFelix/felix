//! Joining the cluster: registration and heartbeats, the lease refresh, and
//! keeping the node credential current.

use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cluster::credential::{self, NodeCredential};
use crate::cluster::lease::LeaseState;
use crate::cluster::membership::{self, MembershipTask};
use crate::config::BrokerConfig;

/// Keep the node credential current for as long as the broker runs, cluster
/// member or not: refresh it when there is a refresh token to rotate, and
/// adopt a token file that something outside the broker rewrites. Returns the
/// refresh loop, for the drain to stop.
///
/// A standalone broker needs this as much as a member does: its catalog sync
/// presents the same credential, and an exchange token lasts minutes.
pub(super) fn keep_credential_current(
    config: &BrokerConfig,
    client: &reqwest::Client,
    credential: &Option<NodeCredential>,
    shutdown: &CancellationToken,
) -> Option<JoinHandle<()>> {
    let (Some(credential), Some(base_url)) = (credential, &config.controlplane_url) else {
        return None;
    };
    // The two are not alternatives: a deployment that runs both is one where
    // either can win.
    if let Some(node_token_file) = config.node_token_file.clone() {
        tokio::spawn(credential::rotate::run(
            node_token_file,
            credential.clone(),
            credential::rotate::POLL_INTERVAL,
            shutdown.clone(),
        ));
    }
    // Published once at startup so the series exists before the first
    // refresh or rotation, which on a long-lived token is hours away.
    credential::report_expiry(credential);
    match config.node_refresh_token_file.clone() {
        Some(refresh_token_file) => Some(tokio::spawn(credential::refresh::run(
            credential::refresh::RefreshConfig {
                client: client.clone(),
                base_url: base_url.clone(),
                credential: credential.clone(),
                refresh_token_file,
            },
            shutdown.clone(),
        ))),
        None => {
            tracing::info!(
                "no FELIX_NODE_REFRESH_TOKEN_FILE: this broker runs on the credential \
                 it was given, or what FELIX_NODE_TOKEN_FILE is rewritten to",
            );
            None
        }
    }
}

/// Spawn membership when this broker has an identity. Registration waits for
/// `serving`, because advertising a node placement can route to before it can
/// answer is worse than advertising it a moment late.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn(
    config: &BrokerConfig,
    membership_client: &reqwest::Client,
    gate_readiness_on_sync: bool,
    seeded: &CancellationToken,
    lease: &Option<Arc<LeaseState>>,
    fleet: &Arc<felix_common::fleet::FleetGate>,
    credential: &Option<NodeCredential>,
    suspects: &Arc<felix_replication::suspicion::Suspects>,
    sync_shutdown: &CancellationToken,
) -> Option<MembershipTask> {
    match (&config.membership, &config.controlplane_url) {
        (Some(membership_config), Some(base_url)) => {
            let serving = if gate_readiness_on_sync {
                seeded.clone()
            } else {
                // Nothing to wait for: the accept loop is already running.
                let now = CancellationToken::new();
                now.cancel();
                now
            };
            let lease = Arc::clone(lease.as_ref().expect("a cluster member has a lease"));
            // Keeps the cheap admission flag in step with the clock, so a broker
            // that loses its lease stops accepting without waiting for a publish
            // to discover it.
            let refresh = Arc::clone(&lease).spawn_refresh(sync_shutdown.clone());
            drop(refresh);
            let node_credential = credential
                .clone()
                .expect("a cluster member has a credential");
            Some(membership::spawn(
                membership_client.clone(),
                base_url.clone(),
                membership_config.clone(),
                node_credential,
                serving,
                sync_shutdown.clone(),
                lease,
                Arc::clone(fleet),
                Arc::clone(suspects),
            ))
        }
        _ => {
            tracing::info!("cluster membership disabled (FELIX_NODE_ID not set)");
            None
        }
    }
}

/// The initial lease: conservative, and invalid until the first heartbeat.
///
/// The real duration comes from the control plane's expiry window on the first
/// accepted heartbeat, so this value only bounds how long a broker could serve
/// if that window ever stopped being reported.
pub(super) fn initial_lease() -> LeaseState {
    LeaseState::new(std::time::Duration::from_secs(10))
}

#[cfg(test)]
mod tests;
