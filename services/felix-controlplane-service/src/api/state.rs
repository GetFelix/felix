//! What every handler is handed: the store, auth, and this instance's settings.
use std::sync::Arc;

use crate::api::types::{FeatureFlags, Region};
use crate::auth::oidc::UpstreamOidcValidator;
use crate::config::NodeLivenessConfig;
use crate::store::ControlPlaneAuthStore;

/// Shared state injected into every handler.
#[derive(Clone)]
pub struct AppState {
    pub region: Region,
    pub api_version: String,
    pub features: FeatureFlags,
    pub store: Arc<dyn ControlPlaneAuthStore + Send + Sync>,
    pub oidc_validator: UpstreamOidcValidator,
    pub bootstrap_enabled: bool,
    /// Accepted bootstrap tokens, current first. More than one only during a
    /// rotation, so replacing the token is a rolling deploy rather than an
    /// outage — see [`crate::api::bootstrap::initialize`].
    pub bootstrap_tokens: Vec<String>,
    pub node_liveness: NodeLivenessConfig,
    /// Whether this instance can serve, bounded and cached.
    pub readiness: Arc<crate::api::readiness::Readiness>,
    /// Requests currently being served, so a drain can say what it waited for.
    pub in_flight: felix_common::lifecycle::InFlight,
    /// Wakes this instance's placement loop and assignment long-polls.
    pub placement_wakes: Arc<crate::cluster::placement::PlacementWakes>,
    /// The limits an operator's move is held to, and the plan is previewed
    /// under: placement's own.
    pub move_policy: crate::cluster::placement::MovePolicy,
    /// A new cluster's fleet-feature finalize still being retried.
    pub new_cluster: Arc<crate::cluster::fleet::NewClusterFinalize>,
}
