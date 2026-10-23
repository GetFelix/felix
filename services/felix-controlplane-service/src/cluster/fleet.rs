//! Fleet features: which ones every serving broker supports, and which ones
//! an operator has enabled.
//!
//! A broker reports the features it implements when it registers
//! (`felix_common::fleet`). The intersection over live and draining brokers
//! is the *supported* set; a broker that is down or has left no longer
//! counts. Support alone turns nothing on. A feature is *enabled* only when an
//! operator finalizes it ([`check_finalize`]), which is refused unless every
//! serving broker supports it, and brokers act on the enabled set alone.
//!
//! Enabling is one-way. From then on a broker lacking the feature is refused
//! at registration ([`admit`]), so an enabled feature is never withdrawn from
//! under a serving fleet. Before that nothing is refused, which is what lets
//! one broker be rolled back mid-upgrade. Each backend serializes finalizing
//! with registration, so a broker without the feature cannot slip in while
//! it is being enabled.
//!
//! A feature brokers use only alongside others is finalized after them
//! ([`check_prerequisites`]).
use std::collections::BTreeSet;

use crate::model::Node;
use crate::store::StoreError;

/// The features every serving node in `nodes` reported.
pub fn supported<'a, I>(nodes: I) -> BTreeSet<String>
where
    I: IntoIterator<Item = &'a Node>,
{
    felix_common::fleet::minimum(
        nodes
            .into_iter()
            .filter(|node| node.status.lifecycle.is_serving())
            .map(|node| &node.status.features),
    )
}

/// The ids of the serving nodes in `nodes` that did not report `feature`.
pub fn lacking<'a, I>(nodes: I, feature: &str) -> Vec<String>
where
    I: IntoIterator<Item = &'a Node>,
{
    nodes
        .into_iter()
        .filter(|node| {
            node.status.lifecycle.is_serving() && !node.status.features.contains(feature)
        })
        .map(|node| node.node_id.clone())
        .collect()
}

/// Refuse `joining` if it lacks a feature the fleet has enabled.
pub(crate) fn admit(enabled: &BTreeSet<String>, joining: &Node) -> Result<(), StoreError> {
    let missing = felix_common::fleet::missing(enabled, &joining.status.features);
    if missing.is_empty() {
        return Ok(());
    }
    // A current build lacks `lease_free_reads` only when it keeps its reads
    // on the lease, and the operator can fix that without a new build.
    let reads_by_lease = if missing.contains(&felix_common::fleet::LEASE_FREE_READS.name()) {
        "; a broker started with FELIX_QUORUM_READS=lease does not support \
         lease_free_reads: unset FELIX_QUORUM_READS (the default reads by a round) \
         and restart the broker"
    } else {
        ""
    };
    Err(StoreError::Conflict(format!(
        "node {} does not support fleet feature(s) {} that the fleet has enabled; \
         run a build that has them, since an enabled feature cannot be disabled{reads_by_lease}",
        joining.node_id,
        missing.join(", "),
    )))
}

/// Refuse to enable `feature` unless some broker is serving and every serving
/// broker in `nodes` reported it.
///
/// With nothing serving, "every broker supports it" is vacuously true and
/// proves nothing about the next broker to start.
pub(crate) fn check_finalize<'a, I>(nodes: I, feature: &str) -> Result<(), StoreError>
where
    I: IntoIterator<Item = &'a Node>,
{
    let mut serving = 0usize;
    let mut lacking = Vec::new();
    for node in nodes {
        if !node.status.lifecycle.is_serving() {
            continue;
        }
        serving += 1;
        if !node.status.features.contains(feature) {
            lacking.push(node.node_id.as_str());
        }
    }
    if serving == 0 {
        return Err(StoreError::Conflict(format!(
            "cannot finalize {feature}: no broker is serving"
        )));
    }
    if lacking.is_empty() {
        return Ok(());
    }
    Err(StoreError::Conflict(format!(
        "cannot finalize {feature}: serving broker(s) {} do not support it; upgrade them first",
        lacking.join(", "),
    )))
}

/// The features `feature` needs enabled first that `enabled` lacks
/// ([`felix_common::fleet::prerequisites`]).
pub(crate) fn missing_prerequisites(enabled: &BTreeSet<String>, feature: &str) -> Vec<String> {
    felix_common::fleet::prerequisites(feature)
        .iter()
        .map(|needed| needed.name())
        .filter(|needed| !enabled.contains(*needed))
        .map(str::to_string)
        .collect()
}

/// Refuse to enable `feature` before the features brokers use it alongside.
/// Placement reads the enabled set directly, so enabling `lease_free_reads`
/// without `generation_start` would let it fail over on suspicion while the
/// brokers still serve on the lease.
///
/// Checked when the operator asks, not when a store applies the finalize:
/// the enabled set only grows, so a prerequisite seen enabled stays enabled,
/// and a Raft member applying the command must not decide differently from
/// one on an older release.
pub(crate) fn check_prerequisites(
    enabled: &BTreeSet<String>,
    feature: &str,
) -> Result<(), StoreError> {
    if enabled.contains(feature) {
        return Ok(());
    }
    let missing = missing_prerequisites(enabled, feature);
    if missing.is_empty() {
        return Ok(());
    }
    Err(StoreError::Conflict(format!(
        "cannot finalize {feature}: finalize {} first; brokers use {feature} only alongside them",
        missing.join(", "),
    )))
}

/// The features a cluster created on this release starts with. Each keeps a
/// recreated stream or cache apart from the deleted one, which only works if
/// the name never had a stream without an incarnation, and nothing older can
/// be rolled back to on a cluster that never ran it.
pub(crate) const NEW_CLUSTER: [felix_common::fleet::FleetFeature; 2] = [
    felix_common::fleet::STREAM_INCARNATIONS,
    felix_common::fleet::CACHE_INCARNATIONS,
];

/// Whether `store` is a cluster created on this release, so far untouched: no
/// stream or cache was ever created, and every broker it knows has registered
/// once. A broker re-registering takes the next node incarnation, so a store
/// upgraded from an older release, whose brokers registered before, never
/// reads as new.
pub(crate) async fn is_new_cluster(
    store: &(dyn crate::store::ControlPlaneStore + Send + Sync),
) -> Result<bool, StoreError> {
    if store
        .list_nodes()
        .await?
        .iter()
        .any(|node| node.status.incarnation != 0)
    {
        return Ok(false);
    }
    Ok(store.stream_changes(0).await?.items.is_empty()
        && store.cache_changes(0).await?.items.is_empty()
        && store.stream_snapshot().await?.items.is_empty()
        && store.cache_snapshot().await?.items.is_empty())
}

/// Finalize [`NEW_CLUSTER`] on a new cluster, through the same store call as
/// an operator's finalize, so the same checks hold: every serving broker
/// reported the feature, under the lock registrations take, and a broker
/// without it is refused from then on. Answers with the enabled set after.
///
/// Called after each registration while a feature is off. Once anything
/// makes the cluster not new, finalizing is the operator's again.
pub(crate) async fn finalize_new_cluster(
    store: &(dyn crate::store::ControlPlaneStore + Send + Sync),
    mut enabled: BTreeSet<String>,
) -> BTreeSet<String> {
    if NEW_CLUSTER
        .iter()
        .all(|feature| enabled.contains(feature.name()))
    {
        return enabled;
    }
    match is_new_cluster(store).await {
        Ok(true) => {}
        Ok(false) => return enabled,
        Err(err) => {
            tracing::warn!(error = %err, "could not tell whether this is a new cluster");
            return enabled;
        }
    }
    for feature in NEW_CLUSTER {
        let name = feature.name();
        if enabled.contains(name) || check_prerequisites(&enabled, name).is_err() {
            continue;
        }
        match store.finalize_fleet_feature(name).await {
            Ok(now) => {
                tracing::info!(feature = name, "fleet feature finalized for a new cluster");
                enabled = now;
            }
            // A broker without it, or a Raft member below the level it needs:
            // left to the operator, as on an upgraded cluster.
            Err(err) => tracing::info!(
                feature = name,
                error = %err,
                "not finalizing a fleet feature for a new cluster",
            ),
        }
    }
    enabled
}

#[cfg(test)]
mod tests;
