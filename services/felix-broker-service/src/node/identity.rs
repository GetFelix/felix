//! Checking the control plane's identity before storage opens.
//!
//! A broker with durable storage learns which metadata group the control
//! plane is, and which shard engine the cluster was formed with, before it
//! opens anything: a refusal after a log opens would already have touched the
//! data it refuses. See [`crate::cluster::controlplane_group`].

use std::path::Path;

use anyhow::Result;

use crate::cluster::controlplane_group as group;
use crate::cluster::credential::NodeCredential;
use crate::config::ShardEngine;

/// Ask the control plane for its identity, until it answers, and judge the
/// directory at `root` against it. Returns the group the directory is
/// recorded under, which the catalog sync holds snapshots to.
///
/// `engine` is what [`group::admit`] compares with the cluster's.
///
/// A broker with no credential cannot ask and skips the check, as its
/// catalog sync, refused on every poll, cannot apply anything either.
pub(super) async fn admit(
    client: &reqwest::Client,
    base_url: &str,
    credential: &Option<NodeCredential>,
    root: &Path,
    engine: Option<ShardEngine>,
) -> Result<Option<String>> {
    let Some(credential) = credential else {
        tracing::warn!(
            "no FELIX_NODE_TOKEN: the control plane's identity was not checked against this \
             data directory"
        );
        return Ok(None);
    };
    let identity = group::await_identity(client, base_url, credential).await;
    group::admit(root, &identity, engine.map(ShardEngine::as_str))
}

/// Compare a registering broker that keeps no data directory with the
/// cluster's engine: it has no directory to judge, but a shard placed on it
/// still has to run the cluster's engine.
pub(super) async fn check_engine(
    client: &reqwest::Client,
    base_url: &str,
    credential: &Option<NodeCredential>,
    engine: ShardEngine,
) -> Result<()> {
    let Some(credential) = credential else {
        return Ok(());
    };
    let identity = group::await_identity(client, base_url, credential).await;
    group::check_engine(&identity, engine.as_str())
}
