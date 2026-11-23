//! A standalone broker's durable shards, each run as a Raft group of one.
//!
//! With no control plane placing them, this feeds [`RaftShards`] the
//! assignments a control plane would give a broker that holds every shard
//! alone: itself as leader and only voter, at the incarnation the stream or
//! cache was registered with, and the target its own copy ids make.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use felix_broker::Broker;
use felix_common::fleet::{self, FleetGate};
use felix_common::random::HashMap;
use felix_replication::raft::{
    GapSnapshot, GroupId, MemberId, RaftReply, RaftRequest, RaftTransport, Refused, mark_name,
    reform_report,
};
use felix_storage::log::LogConfig;
use felix_storage::raft_copy::{Alone, Aloned, CopyRoot, GapFold, LocalRecord, WireReport};
use tokio_util::sync::CancellationToken;

use super::RaftShards;
use crate::shards::routing::EngineShards;
use crate::shards::watch::{RaftTarget, RaftTargetVersion, ShardAssignment};
use crate::shards::{ShardKey, ShardKind};

/// The member name a standalone broker runs its groups under. It has no
/// node id, and as the only voter of every group it needs no other name.
pub(crate) const LOCAL_MEMBER: &str = "standalone";

/// How often a feed with nothing new still runs, so a member whose open
/// failed is opened again.
const IDLE_PASS: Duration = Duration::from_secs(5);

/// How often the feed looks for a catalog change, and passes again while a
/// shard is not yet led.
const BUSY_PASS: Duration = Duration::from_millis(50);

/// The transport of a broker whose groups have one voter each, which never
/// sends a message.
pub(crate) struct NoPeers;

#[async_trait::async_trait]
impl RaftTransport for NoPeers {
    async fn send(&self, _request: RaftRequest) -> Result<RaftReply, Refused> {
        Err(Refused::Unreachable(
            "a standalone broker has no peers".to_string(),
        ))
    }
}

/// The fleet gate of a standalone broker: it is the whole fleet, so every
/// Raft feature this build has is finalized from the start. A gate that
/// waited for a control plane to finalize them would refuse what a
/// standalone broker has always served.
pub(crate) fn fleet_of_one() -> FleetGate {
    let raft = || {
        fleet::IMPLEMENTED
            .iter()
            .map(|feature| feature.name())
            .filter(|name| name.starts_with("raft_"))
    };
    let gate = FleetGate::new(raft());
    gate.restart(raft());
    gate
}

impl RaftShards {
    /// Every shard of every durable stream and cache registered here, led by
    /// this broker alone. A shard whose member is open carries the target
    /// its copy id makes, which the group forms on.
    pub(crate) async fn standalone_assignments(&self) -> HashMap<ShardKey, ShardAssignment> {
        let mut all = HashMap::default();
        let mut add = |key: ShardKey, incarnation: u64| {
            let target = self.runtime.status(&key).map(|status| RaftTarget {
                // The only target this group ever has.
                version: RaftTargetVersion {
                    incarnation: 0,
                    counter: 0,
                },
                members: [(status.member.broker.to_string(), status.member.copy)].into(),
            });
            all.insert(key.clone(), alone(key, incarnation, target));
        };
        for tenant in self.broker.tenant_ids().await {
            for (namespace, stream, metadata) in self.broker.tenant_streams(&tenant).await {
                if !metadata.durable {
                    continue;
                }
                for shard in 0..metadata.shards.max(1) {
                    let key = ShardKey {
                        tenant_id: tenant.clone(),
                        namespace: namespace.clone(),
                        stream: stream.clone(),
                        shard,
                        kind: ShardKind::Stream,
                    };
                    add(key, metadata.incarnation);
                }
            }
        }
        // A cache store with no logs leaves its caches in memory.
        if self.broker.cache().as_log_cache().is_some() {
            for (tenant_id, namespace, cache, metadata) in self.broker.registered_caches().await {
                // A standalone broker routes every cache key to shard 0.
                let key = ShardKey {
                    tenant_id,
                    namespace,
                    stream: cache,
                    shard: 0,
                    kind: ShardKind::Cache,
                };
                add(key, metadata.incarnation);
            }
        }
        all
    }

    /// Re-form the group of one in `dir` if a relaxed crash rotated its
    /// copy. Under `Periodic` a machine crash rotates the storage id, which
    /// turns every copy into a new, empty member; a group of one has no peer
    /// to catch it up, so it starts a new epoch from its own kept copy. It
    /// keeps every synced record and starts past every offset it gave out.
    /// See `docs/per-shard-raft.md`, "A standalone broker re-forms itself".
    pub(super) async fn reform_alone(
        &self,
        key: &ShardKey,
        incarnation: u64,
        root: &CopyRoot,
        dir: &Path,
        config: &LogConfig,
    ) -> anyhow::Result<()> {
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
        let done = root
            .reform_alone(Alone {
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
            .await?;
        if let Aloned::Reformed { epoch } = done {
            tracing::warn!(shard = ?key, epoch, "a relaxed crash rotated this shard's copy; re-formed it alone, keeping every synced record");
        }
        Ok(())
    }

    /// Whether every shard in `fed` is led and served here.
    fn all_led(&self, fed: &[ShardKey]) -> bool {
        fed.iter().all(|key| self.leading(key).is_some())
    }
}

/// The assignment of a shard this broker holds alone.
fn alone(key: ShardKey, incarnation: u64, target: Option<RaftTarget>) -> ShardAssignment {
    ShardAssignment {
        key,
        leader: LOCAL_MEMBER.to_string(),
        replicas: vec![LOCAL_MEMBER.to_string()],
        generation: 1,
        state: "active".to_string(),
        successor: None,
        routing: Default::default(),
        incarnation,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
        target,
        targets_withheld: false,
        raft_formed: None,
        raft_voters: None,
        raft_bases: None,
        raft_seed: None,
        placed: None,
    }
}

/// Keep `shards` in step with what `broker` has registered until `stop`:
/// open, form and lead a group for each durable shard, and close one whose
/// stream or cache went away.
pub(crate) async fn run(shards: Arc<RaftShards>, broker: Arc<Broker>, stop: CancellationToken) {
    shards.alone.store(true, Ordering::Release);
    let mut seen = None;
    let mut last = tokio::time::Instant::now();
    let mut settled = false;
    loop {
        let changes = broker.catalog_changes();
        if seen != Some(changes) || !settled || last.elapsed() >= IDLE_PASS {
            seen = Some(changes);
            last = tokio::time::Instant::now();
            let mut assignments = shards.standalone_assignments().await;
            let fed: Vec<ShardKey> = assignments.keys().cloned().collect();
            shards.take(&mut assignments).await;
            shards.answered(Some(tokio::time::Instant::now()), None);
            settled = shards.all_led(&fed);
        }
        tokio::select! {
            () = stop.cancelled() => return,
            () = tokio::time::sleep(BUSY_PASS) => {}
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
