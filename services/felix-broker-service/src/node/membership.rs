//! Joining the cluster: registration and heartbeats, the lease refresh, and
//! keeping the node credential current.

use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cluster::credential::{self, NodeCredential};
use crate::cluster::lease::LeaseState;
use crate::cluster::membership::{self, MembershipTask};
use crate::config::BrokerConfig;

/// What a cluster member runs in the background, for the drain to stop.
pub(super) struct Joined {
    pub(super) membership: MembershipTask,
    /// The credential refresh loop, when there is a refresh token to rotate.
    pub(super) credential_refresh: Option<JoinHandle<()>>,
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
    sync_shutdown: &CancellationToken,
) -> Option<Joined> {
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
            // Refresh only when the operator provided somewhere to keep the
            // rotating half. Without it the broker runs on the token it was
            // given, and leaves the cluster when that expires.
            let credential_refresh = match membership_config.refresh_token_file.clone() {
                Some(refresh_token_file) => Some(tokio::spawn(credential::refresh::run(
                    credential::refresh::RefreshConfig {
                        client: membership_client.clone(),
                        base_url: base_url.clone(),
                        credential: node_credential.clone(),
                        refresh_token_file,
                    },
                    sync_shutdown.clone(),
                ))),
                None => {
                    tracing::info!(
                        "no FELIX_NODE_REFRESH_TOKEN_FILE: this broker will run on \
                         the credential it was given and leave the cluster when it \
                         expires",
                    );
                    None
                }
            };

            // The other way a credential stays current: something outside the
            // broker rewrites the token file. Watched whenever the token came
            // from one, refresh loop or not -- the two are not alternatives, and
            // a deployment that runs both is a deployment where either can win.
            if let Some(node_token_file) = membership_config.node_token_file.clone() {
                tokio::spawn(credential::rotate::run(
                    node_token_file,
                    node_credential.clone(),
                    credential::rotate::POLL_INTERVAL,
                    sync_shutdown.clone(),
                ));
            }

            // Published once at startup so the series exists before the first
            // refresh or rotation, which on a long-lived token is hours away.
            credential::report_expiry(&node_credential);
            Some(Joined {
                membership: membership::spawn(
                    membership_client.clone(),
                    base_url.clone(),
                    membership_config.clone(),
                    node_credential,
                    serving,
                    sync_shutdown.clone(),
                    lease,
                    Arc::clone(fleet),
                ),
                credential_refresh,
            })
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
