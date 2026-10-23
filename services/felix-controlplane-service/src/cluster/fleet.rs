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
//! ([`check_prerequisites`]). A new cluster finalizes [`NEW_CLUSTER`] itself
//! ([`finalize_new_cluster`]).
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

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
    // Likewise for the fence, which the follower features rest on.
    let fenceless = if [
        felix_common::fleet::MAJORITY_ACK,
        felix_common::fleet::FENCED_CACHES,
    ]
    .iter()
    .any(|feature| missing.contains(&feature.name()))
    {
        "; a broker started with FELIX_INTERNAL_FENCE=false does not support \
         majority_ack or fenced_caches: unset FELIX_INTERNAL_FENCE and restart the broker"
    } else {
        ""
    };
    Err(StoreError::Conflict(format!(
        "node {} does not support fleet feature(s) {} that the fleet has enabled; \
         run a build that has them, since an enabled feature cannot be disabled\
         {reads_by_lease}{fenceless}",
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

/// The features a cluster created on this release starts with. A finalize
/// is one-way so that an upgraded fleet can still roll back before it, and a
/// cluster that never ran an older build has nothing to roll back to. Each
/// comes after the features it needs ([`felix_common::fleet::prerequisites`]).
///
/// Every implemented feature but `publisher_principal`, which stays the
/// operator's: it grows every record by its publisher's id and keeps
/// principal ids in the log, an opt-in a lone broker also leaves off unless
/// `FELIX_RECORD_PUBLISHERS` asks for it.
pub(crate) const NEW_CLUSTER: [felix_common::fleet::FleetFeature; 9] = [
    felix_common::fleet::GENERATION_START,
    felix_common::fleet::MAJORITY_ACK,
    felix_common::fleet::LEASE_FREE_READS,
    felix_common::fleet::FENCED_CACHES,
    felix_common::fleet::JUMP_HASH_ROUTING,
    felix_common::fleet::ATOMIC_COMMIT,
    felix_common::fleet::GROUP_CURSOR_DEPENDENCIES,
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

/// A new cluster's finalize that could not finish at a registration, retried
/// in the background. One per control-plane instance, shared by its handlers.
#[derive(Debug)]
pub struct NewClusterFinalize {
    retrying: AtomicBool,
    shutdown: CancellationToken,
    give_up_after: Duration,
}

impl NewClusterFinalize {
    /// Retries stop when `shutdown` is cancelled.
    pub fn new(shutdown: CancellationToken) -> Self {
        Self {
            retrying: AtomicBool::new(false),
            shutdown,
            give_up_after: GIVE_UP_AFTER,
        }
    }
}

#[cfg(test)]
impl NewClusterFinalize {
    pub(crate) fn giving_up_after(give_up_after: Duration) -> Self {
        Self {
            give_up_after,
            ..Self::default()
        }
    }

    pub(crate) fn retrying(&self) -> bool {
        self.retrying.load(Ordering::Acquire)
    }
}

impl Default for NewClusterFinalize {
    fn default() -> Self {
        Self::new(CancellationToken::new())
    }
}

/// The first retry's delay; each later one doubles, up to [`RETRY_MAX`].
const RETRY_FIRST: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);
/// How long after the first held attempt the retry gives up. Much later the
/// cluster holds data, and its operator may want the rollback window an
/// upgraded cluster keeps.
const GIVE_UP_AFTER: Duration = Duration::from_secs(600);

type Store = Arc<dyn crate::store::ControlPlaneStore + Send + Sync>;

/// Finalize [`NEW_CLUSTER`] after a registration if the cluster is new, and
/// answer with the enabled set after.
///
/// When something that passes held it back, a control-plane member below
/// this build or a store error, the finalize is retried in the background
/// until it gets past them. The retry does not ask again whether the cluster
/// is new: its brokers re-registering, or streams created meanwhile, change
/// nothing about the rollback the check is for, and an operator could
/// finalize just the same with them. Once anything makes the cluster not new
/// before a first attempt, finalizing is the operator's again.
pub(crate) async fn finalize_new_cluster(
    store: &Store,
    pending: &Arc<NewClusterFinalize>,
    enabled: BTreeSet<String>,
) -> BTreeSet<String> {
    if all_enabled(&enabled) || pending.retrying.load(Ordering::Acquire) {
        return enabled;
    }
    match is_new_cluster(store.as_ref()).await {
        Ok(true) => {}
        Ok(false) => return enabled,
        Err(err) => {
            tracing::warn!(error = %err, "could not tell whether this is a new cluster");
            return enabled;
        }
    }
    let (enabled, again) = finalize_in_order(store.as_ref(), enabled).await;
    if again {
        retry(store, pending);
    }
    enabled
}

fn all_enabled(enabled: &BTreeSet<String>) -> bool {
    NEW_CLUSTER
        .iter()
        .all(|feature| enabled.contains(feature.name()))
}

/// Retry [`finalize_in_order`] until nothing that passes holds it back, for
/// at most [`GIVE_UP_AFTER`].
fn retry(store: &Store, pending: &Arc<NewClusterFinalize>) {
    if pending
        .retrying
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    tracing::info!("new cluster: finalizing fleet features once every control-plane member can");
    let store = Arc::clone(store);
    let pending = Arc::clone(pending);
    drop(tokio::spawn(async move {
        let give_up_at = tokio::time::Instant::now() + pending.give_up_after;
        let mut delay = RETRY_FIRST;
        loop {
            tokio::select! {
                () = pending.shutdown.cancelled() => break,
                () = tokio::time::sleep(delay) => {}
            }
            if tokio::time::Instant::now() >= give_up_at {
                tracing::warn!(
                    "new cluster: gave up finalizing fleet features; finalize them with \
                     `felix-controlplane admin features finalize <feature>` \
                     (POST /v1/fleet/features/{{feature}}/finalize) once every \
                     control-plane member runs this release"
                );
                break;
            }
            delay = (delay * 2).min(RETRY_MAX);
            let enabled = match store.enabled_fleet_features().await {
                Ok(enabled) => enabled,
                Err(err) => {
                    tracing::warn!(error = %err, "new cluster: could not read the enabled features");
                    continue;
                }
            };
            if all_enabled(&enabled) || !finalize_in_order(store.as_ref(), enabled).await.1 {
                break;
            }
        }
        pending.retrying.store(false, Ordering::Release);
    }));
}

/// Finalize [`NEW_CLUSTER`] in order, through the same store call as an
/// operator's finalize, so the same checks hold: every serving broker
/// reported the feature, under the lock registrations take, a Raft member
/// below the level it needs refuses it, and a broker without it is refused
/// from then on. A feature whose prerequisites did not finalize is skipped.
/// Answers with the enabled set after, and whether to try again.
async fn finalize_in_order(
    store: &(dyn crate::store::ControlPlaneStore + Send + Sync),
    mut enabled: BTreeSet<String>,
) -> (BTreeSet<String>, bool) {
    // `majority_ack` and the features after it are safe only once every
    // control-plane member is upgraded (docs/control-plane.md).
    if !store.every_member_current().await {
        return (enabled, true);
    }
    let mut again = false;
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
            Err(StoreError::Conflict(message)) => tracing::info!(
                feature = name,
                error = %message,
                "not finalizing a fleet feature for a new cluster",
            ),
            Err(err) => {
                tracing::warn!(feature = name, error = %err, "new cluster: finalize failed");
                again = true;
            }
        }
    }
    (enabled, again)
}

#[cfg(test)]
mod tests;
