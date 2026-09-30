//! Whether the cluster serves again once a fault is healed.
//!
//! The checker judges safety after the run; this judges liveness during it.
//! After every heal, within [`RECOVERY`], each of the workload's shards must
//! have a running leader that takes a write (a list) or answers a get (the
//! cache), no move or follower replacement may still be in flight, and each
//! replicated shard must have a replica reported caught up at its current
//! generation, so that it could fail over again, and no replica its leader
//! has stopped shipping to. A cluster that does not get
//! there ends the run with a report naming each stuck shard and what it is
//! stuck in.
//!
//! The probes are recorded as operations of a client of their own, so the
//! checker holds them to the same rules as the workload's.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};

use super::campaign::Campaign;
use super::workload::Workload;
use crate::cluster::{HaltedReplica, ShardStatus};
use crate::{Cluster, wait};

/// How long after a heal every shard has to be serving again. It covers a
/// failover, a move started before the heal running to its end, and a
/// restarted follower catching up under the workload.
pub(super) const RECOVERY: Duration = Duration::from_secs(60);

/// How often a cluster that has not recovered yet is looked at again.
const RETRY: Duration = Duration::from_millis(500);

/// Wait until every shard of the workload serves, returning how long that
/// took, or fail naming the shards that never got there.
pub(super) async fn await_recovery(
    cluster: &Cluster,
    campaign: &Campaign,
    workload: &Workload,
) -> Result<Duration> {
    let started = Instant::now();
    let bound = wait::budget(RECOVERY);
    let mut served = BTreeSet::new();
    loop {
        let stuck = stuck_shards(cluster, campaign, workload, &mut served).await;
        if stuck.is_empty() {
            return Ok(started.elapsed());
        }
        if started.elapsed() >= bound {
            bail!(
                "liveness: {} shard(s) not serving {bound:?} after the heal:\n  {}",
                stuck.len(),
                stuck.join("\n  ")
            );
        }
        tokio::time::sleep(RETRY).await;
    }
}

/// One line per shard that is not serving yet. `served` holds the shards
/// whose probe has already succeeded, so each is probed once per heal.
async fn stuck_shards(
    cluster: &Cluster,
    campaign: &Campaign,
    workload: &Workload,
    served: &mut BTreeSet<(&'static str, String)>,
) -> Vec<String> {
    let statuses = match cluster.shard_statuses().await {
        Ok(statuses) => statuses,
        Err(err) => return vec![format!("the control plane cannot list shards: {err:#}")],
    };
    let halted = halted_replicas(cluster).await;
    // The prober is numbered after the workload's clients.
    let prober = campaign.clients;
    let mut stuck = Vec::new();
    for (kind, name) in campaign.shard_names() {
        let Some(status) = statuses
            .iter()
            .find(|s| s.kind == kind && s.name == name && s.shard == 0)
        else {
            stuck.push(format!("{kind} {name}/0: no assignment"));
            continue;
        };
        let shard = format!(
            "{kind} {name}/0 (leader {}, generation {})",
            status.leader, status.generation
        );
        if let Some(transition) = transition(cluster, status) {
            stuck.push(format!("{shard}: {transition}"));
            continue;
        }
        let halts: Vec<String> = halted
            .iter()
            .filter(|h| h.kind == kind && h.stream == name && h.shard == 0)
            .map(|h| format!("{} ({})", h.node_id, h.reason))
            .collect();
        if !halts.is_empty() {
            stuck.push(format!(
                "{shard}: replication to {} has stopped",
                halts.join(", ")
            ));
            continue;
        }
        if served.contains(&(kind, name.to_string())) {
            continue;
        }
        let Some(addr) = cluster.node(&status.leader).map(|node| node.client_addr) else {
            stuck.push(format!(
                "{shard}: its leader is not a broker of this cluster"
            ));
            continue;
        };
        let probe = match kind {
            "cache" => {
                let key = campaign.keys.first().map_or("k0", String::as_str);
                workload.probe_get(prober, addr, key).await
            }
            _ => workload.probe_append(prober, addr, name).await,
        };
        match probe {
            Ok(()) => {
                served.insert((kind, name.to_string()));
            }
            Err(err) => stuck.push(format!("{shard}: its leader refused a probe: {err:#}")),
        }
    }
    stuck
}

/// Every halted replica the running brokers list. A broker that cannot be
/// asked is skipped: a leader that is down is reported on its own.
async fn halted_replicas(cluster: &Cluster) -> Vec<HaltedReplica> {
    let mut halted = Vec::new();
    for node in cluster.node_ids() {
        if cluster
            .node(&node)
            .is_some_and(crate::BrokerNode::is_running)
            && let Ok(listed) = cluster.halted_replicas(&node).await
        {
            halted.extend(listed);
        }
    }
    halted
}

/// What keeps a shard from serving, as its placement shows it: a leader that
/// is down, a move or replacement still in flight, or no replica known to be
/// caught up.
fn transition(cluster: &Cluster, status: &ShardStatus) -> Option<String> {
    let reason = status
        .move_reason
        .map_or(String::new(), |reason| format!(" ({reason})"));
    if !cluster
        .node(&status.leader)
        .is_some_and(crate::BrokerNode::is_running)
    {
        Some("its leader is not running".to_string())
    } else if status.fenced {
        Some(format!(
            "fenced for a move's cut-over to {}{reason}",
            status.successor.as_deref().unwrap_or("nobody")
        ))
    } else if let Some(successor) = &status.successor {
        Some(format!("moving to {successor}{reason}"))
    } else if let Some(joining) = &status.joining {
        Some(format!("replacing a follower with {joining}{reason}"))
    } else if !status.replicas.is_empty() && !status.caught_up_reported {
        Some(format!(
            "no replica of {} reported caught up at this generation, so it cannot fail over",
            status.replicas.join(",")
        ))
    } else {
        None
    }
}
