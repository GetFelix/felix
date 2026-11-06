//! Joining the cluster: registration and heartbeats, the lease refresh, and
//! keeping the node credential current.

use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cluster::credential::{self, NodeCredential};
use crate::cluster::lease::LeaseState;
use crate::cluster::membership::{self, MembershipTask};
use crate::config::BrokerConfig;
use felix_replication::controlplane_link::ControlPlaneLink;

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
pub(super) fn spawn<L: ControlPlaneLink + Clone>(
    config: &BrokerConfig,
    link: Option<L>,
    own_thread: bool,
    gate_readiness_on_sync: bool,
    seeded: &CancellationToken,
    lease: &Option<Arc<LeaseState>>,
    fleet: &Arc<felix_common::fleet::FleetGate>,
    credential: &Option<NodeCredential>,
    suspects: &Arc<felix_replication::suspicion::Suspects>,
    incarnation: &Arc<std::sync::atomic::AtomicU64>,
    registered: tokio::sync::watch::Sender<Option<u64>>,
    storage_id: felix_storage::StorageId,
    data_dir: Option<&std::path::Path>,
    restore_check: Option<CancellationToken>,
    sync_shutdown: &CancellationToken,
) -> Option<MembershipTask> {
    match (&config.membership, link) {
        (Some(membership_config), Some(link)) => {
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
            Some(membership::spawn_with(
                link,
                membership_config.clone(),
                node_credential,
                serving,
                sync_shutdown.clone(),
                lease,
                Arc::clone(fleet),
                Arc::clone(suspects),
                Arc::clone(incarnation),
                registered,
                Some(storage_id),
                data_dir.map(std::path::Path::to_path_buf),
                restore_check,
                own_thread,
            ))
        }
        _ => {
            tracing::info!("cluster membership disabled (FELIX_NODE_ID not set)");
            None
        }
    }
}

/// How this start goes on, from the startup lookup.
#[derive(Debug)]
pub(super) enum Start {
    /// The control plane's record of this node's last registration, for
    /// `storage::open` to judge whether the data directory is older than it
    /// and whether the node held copies under another storage id. `None`
    /// from a broker that is not asked, or a node never registered. Today's
    /// start.
    Asked(Option<membership::Registered>),
    /// The control plane was down and the directory qualifies: start from its
    /// control-plane view with the registration unknown, and judge the
    /// directory once the control plane answers.
    FromView(Box<crate::cluster::controlplane_view::View>),
}

/// Ask the control plane for this node's last registration before any store
/// opens: nothing this broker announces may carry the directory's id until
/// then. Asked only of a cluster member with durable storage under
/// `durable_root`.
///
/// Asked until it answers, unless `FELIX_START_WITHOUT_CONTROLPLANE` is on:
/// then for at most `FELIX_CONTROLPLANE_STARTUP_WAIT_MS`, and a control plane
/// found down (not refusing) on every attempt lets a directory that qualifies
/// start from its view (`super::from_view::qualify`). Otherwise it is asked
/// until it answers, as without the switch.
pub(super) async fn start(
    config: &BrokerConfig,
    durable_root: Option<&std::path::Path>,
    link: &Option<impl ControlPlaneLink>,
    credential: &Option<NodeCredential>,
) -> anyhow::Result<Start> {
    let (Some(root), Some(membership), Some(link), Some(credential)) =
        (durable_root, &config.membership, link, credential)
    else {
        return Ok(Start::Asked(None));
    };
    if config.start_without_controlplane {
        let wait = std::time::Duration::from_millis(config.controlplane_startup_wait_ms);
        match membership::registered_within(link, &membership.node_id, credential, wait).await {
            membership::Bounded::Answered(registered) => return Ok(Start::Asked(registered)),
            membership::Bounded::Refusing => tracing::warn!(
                "the control plane is up and refusing this node's lookup; waiting for it \
                 rather than starting from the control-plane view"
            ),
            membership::Bounded::Down => {
                match super::from_view::qualify(root, &membership.node_id, &membership.features) {
                    Ok(view) => {
                        tracing::warn!(
                            storage_id = %view.storage_id,
                            incarnation = view.incarnation,
                            shards = view.routes.len(),
                            "the control plane did not answer; starting from the control-plane \
                             view in the data directory. This broker votes and replicates but \
                             serves no client until it registers"
                        );
                        return Ok(Start::FromView(Box::new(view)));
                    }
                    Err(refusal) => tracing::warn!(
                        reason = %refusal,
                        "the control plane did not answer and this data directory cannot start \
                         from its control-plane view; waiting for the control plane"
                    ),
                }
            }
        }
    }
    membership::await_registered(link, &membership.node_id, credential)
        .await
        .map(Start::Asked)
}

/// The control plane's record of this node's last registration, asked until
/// it answers. For a start from the view whose run then rotated the storage
/// id, which must wait as any start does.
pub(super) async fn await_registered(
    config: &BrokerConfig,
    link: &Option<impl ControlPlaneLink>,
    credential: &Option<NodeCredential>,
) -> anyhow::Result<Option<membership::Registered>> {
    let (Some(membership), Some(link), Some(credential)) = (&config.membership, link, credential)
    else {
        return Ok(None);
    };
    membership::await_registered(link, &membership.node_id, credential).await
}

/// Go on from `view` only if the run that began on `root` is still under the
/// storage id the view was registered under (`super::from_view::opened_under`).
/// Otherwise wait for the control plane as any start does, record whether it
/// names another storage id for the node, and return no view. Also returns
/// whether the node held copies under another id.
pub(super) async fn keep_view_if_opened_under(
    view: crate::cluster::controlplane_view::View,
    root: &std::path::Path,
    storage_id: felix_storage::StorageId,
    config: &BrokerConfig,
    link: &Option<impl ControlPlaneLink>,
    credential: &Option<NodeCredential>,
) -> anyhow::Result<(Option<crate::cluster::controlplane_view::View>, bool)> {
    if super::from_view::opened_under(&view, storage_id) {
        return Ok((Some(view), false));
    }
    tracing::warn!(
        storage_id = %storage_id,
        view_storage_id = %view.storage_id,
        "this start gave the data directory a new storage id; waiting for the control plane \
         instead of starting from the control-plane view"
    );
    let registered = await_registered(config, link, credential).await?;
    let replaced = super::storage::note_replaced(root, storage_id, registered.as_ref())?;
    Ok((None, replaced))
}

/// What client service and the control-plane sync wait on.
pub(super) struct Gates {
    /// Client listeners and forwarded client requests. `seeded` itself
    /// without a view; after a start from the view, a token cancelled only
    /// once this process has registered as well.
    pub(super) clients: CancellationToken,
    /// Cancelled once the restore check a start from the view deferred has
    /// passed; at once without a view.
    pub(super) restore_checked: CancellationToken,
}

impl Gates {
    pub(super) fn new(from_view: bool, seeded: &CancellationToken) -> Self {
        let restore_checked = CancellationToken::new();
        if !from_view {
            restore_checked.cancel();
        }
        Self {
            clients: if from_view {
                CancellationToken::new()
            } else {
                seeded.clone()
            },
            restore_checked,
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
