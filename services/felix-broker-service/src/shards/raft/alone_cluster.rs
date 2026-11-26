//! A cluster's RF 1 shard re-forming itself after a relaxed crash.
//!
//! The standalone broker records its next epoch in a file beside the copy.
//! In a cluster the control plane's record decides, so the order is: plan
//! and stop the copy, write the epoch to the control plane in one checked
//! command, then start the copy from the seed that command recorded. A run
//! after a crash follows what the control plane recorded: an epoch it
//! already holds is started from, never written again. See
//! `docs/per-shard-raft.md`, "Shards with one replica".

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use felix_common::fleet;
use felix_replication::controlplane_link::{ControlPlaneLink, LinkResponse};
use felix_replication::raft::{GapSnapshot, GroupId, MemberId, mark_name, reform_report};
use felix_replication::reporter::Credential;
use felix_storage::log::LogConfig;
use felix_storage::raft_copy::{Alone, AlonePlan, CopyRoot, GapFold, LocalRecord, WireReport};

use super::RaftShards;
use crate::shards::ShardKey;
use crate::shards::watch::ShardAssignment;

/// How long the control plane's answer may take before the shard waits for
/// the next pass.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Sends a group of one's epoch to the control plane.
#[async_trait::async_trait]
pub(crate) trait AloneWriter: Send + Sync {
    async fn send(&self, body: &serde_json::Value) -> anyhow::Result<LinkResponse>;
}

/// [`AloneWriter`] over the broker's control plane link.
pub(crate) struct LinkWriter<L> {
    pub(crate) link: L,
    pub(crate) node_id: String,
    pub(crate) credential: Option<Arc<dyn Credential>>,
}

#[async_trait::async_trait]
impl<L: ControlPlaneLink> AloneWriter for LinkWriter<L> {
    async fn send(&self, body: &serde_json::Value) -> anyhow::Result<LinkResponse> {
        let bearer = self.credential.as_ref().map(|c| c.bearer());
        self.link
            .reform_alone(bearer.as_deref().map(String::as_str), &self.node_id, body)
            .await
    }
}

impl RaftShards {
    /// Let this broker's RF 1 shards ask the control plane for their
    /// re-formed epoch.
    pub(crate) fn set_alone_writer(&self, writer: Arc<dyn AloneWriter>) {
        let _ = self.alone_writer.set(writer);
    }

    /// Re-form `key`'s group of one through the control plane, when a
    /// relaxed crash or a restore rotated its copy and the assignment names
    /// this broker its only copy. Returns the assignment the control plane
    /// answered with, to start from. `None` leaves the shard as it is: not
    /// eligible, nothing owed, or the control plane did not take the epoch,
    /// in which case the shard stays not ready until an operator re-forms it.
    pub(super) async fn reform_alone_in_cluster(
        &self,
        key: &ShardKey,
        assignment: &ShardAssignment,
        root: &CopyRoot,
        dir: &Path,
        config: &LogConfig,
    ) -> anyhow::Result<Option<ShardAssignment>> {
        let Some(writer) = self.alone_writer.get() else {
            return Ok(None);
        };
        if !self.fleet.supports(fleet::RAFT_ALONE_REFORM) || !alone_here(&self.local, assignment) {
            return Ok(None);
        }
        let Some(plan) = self.stop_alone(key, assignment, root, dir, config).await? else {
            return Ok(None);
        };
        let epoch = plan.bases.len() as u64;
        let recorded = assignment.group_epoch();
        if recorded >= epoch {
            // The control plane already holds the epoch: the start reads it.
            return Ok(None);
        }
        if recorded + 1 != epoch {
            tracing::warn!(shard = ?key, recorded, planned = epoch, "the control plane is not one epoch behind this copy's plan; not re-forming it");
            return Ok(None);
        }
        let body = request(key, assignment, &plan, &self.local);
        let sent = tokio::time::timeout(REQUEST_TIMEOUT, writer.send(&body)).await;
        let answer = match sent {
            Ok(Ok(answer)) => answer,
            Ok(Err(err)) => {
                tracing::warn!(shard = ?key, error = %err, "could not ask the control plane to re-form this shard");
                return Ok(None);
            }
            Err(_) => {
                tracing::warn!(shard = ?key, "the control plane did not answer a re-form request in time");
                return Ok(None);
            }
        };
        if answer.status != reqwest::StatusCode::OK {
            tracing::warn!(shard = ?key, status = %answer.status, reason = answer.text(), "the control plane refused to re-form this shard; it stays not ready until an operator re-forms it");
            return Ok(None);
        }
        tracing::warn!(shard = ?key, epoch, "a relaxed crash rotated this shard's only copy; the control plane recorded its next epoch");
        Ok(answer.json::<ShardAssignment>().ok())
    }

    /// Plan and stop the copy in `dir`, if a re-form is owed.
    async fn stop_alone(
        &self,
        key: &ShardKey,
        assignment: &ShardAssignment,
        root: &CopyRoot,
        dir: &Path,
        config: &LogConfig,
    ) -> anyhow::Result<Option<AlonePlan>> {
        let incarnation = assignment.incarnation;
        let spacing = config.index_spacing_bytes;
        let read = |probes: &BTreeSet<u64>, epoch: u64| {
            reform_report(dir, spacing, probes, epoch, false)
                .map(|report| WireReport::from(&report))
        };
        let kind = key.kind;
        let local = self.local.as_str();
        let fold = |copy: u64| {
            Box::new(GapSnapshot::new(
                kind,
                BTreeSet::from([MemberId::new(local, copy)]),
            )) as Box<dyn GapFold>
        };
        let member = |copy: u64| MemberId::new(local, copy).to_string();
        Ok(root
            .stop_alone(Alone {
                shard: &mark_name(key),
                dir,
                label: &GroupId::new(key.clone(), incarnation).to_string(),
                incarnation,
                broker: local,
                config,
                read: &read,
                fold: &fold,
                member: &member,
                record: &LocalRecord,
            })
            .await?)
    }
}

/// Whether the assignment names `local` the only broker of the shard's
/// group: no other replica, placement, set change or move. The control
/// plane checks this again in the same command that writes the epoch.
fn alone_here(local: &str, a: &ShardAssignment) -> bool {
    a.leader == local
        && a.replicas.iter().all(|b| b == local)
        && a.successor.is_none()
        && a.pending_set.is_none()
        && a.move_reason.is_none()
        && a.state != "draining"
        && a.placed
            .as_ref()
            .is_some_and(|placed| placed.brokers == BTreeSet::from([local.to_string()]))
        && a.target
            .as_ref()
            .is_none_or(|target| target.members.keys().all(|b| b == local))
}

/// The request body: the plan as the control plane's `ReformEpochStart`.
fn request(
    key: &ShardKey,
    assignment: &ShardAssignment,
    plan: &AlonePlan,
    local: &str,
) -> serde_json::Value {
    let base = plan
        .bases
        .last()
        .copied()
        .unwrap_or(felix_storage::raft_copy::WireBase {
            base_last: None,
            first_index: 0,
        });
    let expected = plan.bases.len() as u64 - 1;
    // Mirrors the control plane's bound: the epoch's reported index and the
    // offset floor, whichever is higher.
    let planned_floor = assignment
        .raft_floor
        .filter(|floor| floor.incarnation == assignment.incarnation)
        .and_then(|floor| {
            let index = (floor.epoch == expected).then_some(floor.index);
            index.max(Some(floor.offset_floor))
        });
    serde_json::json!({
        "key": key,
        "start": {
            "incarnation": assignment.incarnation,
            "expected_epoch": expected,
            "planned_floor": planned_floor,
            "base": {
                "base_last": base.base_last.map(|(index, epoch, term)| {
                    serde_json::json!({ "index": index, "epoch": epoch, "term": term })
                }),
                "first_index": base.first_index,
            },
            "voters": { local: plan.copy },
            "starts": { local: &plan.start },
        },
    })
}
