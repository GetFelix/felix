//! Whether the cluster serves again once a fault is healed.
//!
//! The checker judges safety after the run; this judges liveness during it.
//! After every heal, within [`RECOVERY`], each of the workload's shards must
//! have a running leader that takes a write (a list) or answers a get (the
//! cache), no move or follower replacement may still be in flight, and each
//! replicated shard must have a replica reported caught up at its current
//! generation, so that it could fail over again, and no replica its leader
//! has stopped shipping to. On Raft the group's own members say instead: a
//! member leads by its own report and a majority holds the log through its
//! commit index. A cluster that does not get
//! there ends the run with a report naming each stuck shard and what it is
//! stuck in.
//!
//! The probes are recorded as operations of a client of their own, so the
//! checker holds them to the same rules as the workload's.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use super::campaign::Campaign;
use super::model::Opened;
use super::workload::Workload;
use crate::cluster::{HaltedReplica, ShardStatus};
use crate::{Cluster, wait};

/// How long after a heal every shard has to be serving again. It covers a
/// failover, a move started before the heal running to its end, and a
/// restarted follower catching up under the workload.
pub(super) const RECOVERY: Duration = Duration::from_secs(60);

/// How often a cluster that has not recovered yet is looked at again.
const RETRY: Duration = Duration::from_millis(500);

/// How long after a leader is killed with the control plane down some other
/// broker must serve each of its shards: the suspect window, a pre-vote, a
/// place delay or two, the ballot and one fence, with room for a split
/// round's backoff.
pub(super) const ELECTED_WITHIN: Duration = Duration::from_secs(20);

/// How long a leader about to be killed has for its shards to be ready to
/// elect from.
const READY_TO_ELECT: Duration = Duration::from_secs(30);

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
        let stuck = if cluster.raft() {
            stuck_raft_shards(cluster, campaign, workload, &mut served).await
        } else {
            stuck_shards(cluster, campaign, workload, &mut served).await
        };
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

/// The shards `node` leads, once each has a replica reported caught up at its
/// generation and nothing in flight, so its followers can elect. Called with
/// the control plane up, just before [`FaultKind::ElectWithoutControlPlane`]
/// crashes it.
///
/// [`FaultKind::ElectWithoutControlPlane`]: super::FaultKind::ElectWithoutControlPlane
pub(super) async fn ready_to_elect(
    cluster: &Cluster,
    campaign: &Campaign,
    node: &str,
) -> Result<Vec<ShardStatus>> {
    if cluster.raft() {
        return raft_ready_to_elect(cluster, campaign, node).await;
    }
    let last = std::sync::Mutex::new(Vec::new());
    let found = wait::until_some(
        READY_TO_ELECT,
        &format!("the shards {node} leads to be ready to elect"),
        || async {
            let statuses = cluster.shard_statuses().await.ok()?;
            let led: Vec<ShardStatus> = statuses
                .into_iter()
                .filter(|status| {
                    status.leader == node
                        && campaign
                            .shard_names()
                            .any(|(kind, name)| status.kind == kind && status.name == name)
                })
                .collect();
            let waiting: Vec<String> = led
                .iter()
                .filter_map(|status| {
                    let why = transition(cluster, status)?;
                    Some(format!(
                        "{} {}/{}: {why}",
                        status.kind, status.name, status.shard
                    ))
                })
                .collect();
            let ready = waiting.is_empty();
            *last.lock().expect("ready lock") = waiting;
            ready.then_some(led)
        },
    )
    .await;
    let led = found.with_context(|| {
        format!(
            "still waiting on {}",
            last.lock().expect("ready lock").join("; ")
        )
    })?;
    if led.is_empty() {
        bail!("{node} leads none of the workload's shards");
    }
    Ok(led)
}

/// Wait until each shard in `led`, which `killed` led until it was killed,
/// has a new leader that stood for it, opened it at a later generation and
/// serves a probe, all while the control plane stays down. Returns one line per shard naming
/// who serves it and how soon. The opens it reads go into `opens` for rule
/// 12.
pub(super) async fn await_election(
    cluster: &Cluster,
    campaign: &Campaign,
    workload: &Workload,
    killed: &str,
    led: &[ShardStatus],
    opens: &mut BTreeSet<Opened>,
) -> Result<Vec<String>> {
    if cluster.raft() {
        return raft_await_election(cluster, campaign, workload, killed, led, opens).await;
    }
    let started = Instant::now();
    let bound = wait::budget(ELECTED_WITHIN);
    // The prober is numbered after the workload's clients.
    let prober = campaign.clients;
    let mut lines = Vec::new();
    let mut pending: Vec<&ShardStatus> = led.iter().collect();
    let mut last_refusal = std::collections::BTreeMap::new();
    let mut stood = BTreeSet::new();
    while !pending.is_empty() {
        if cluster.control_plane_running() {
            bail!("the control plane came back before the election was checked");
        }
        // Nothing restarts while this waits, so the current runs hold every
        // new open.
        super::leaders::read_current(cluster, opens, &mut stood);
        let mut still = Vec::new();
        for status in pending {
            let elected = super::leaders::elected(
                opens,
                &stood,
                &status.name,
                status.shard,
                status.generation,
                killed,
            );
            let shard = format!("{} {}/{}", status.kind, status.name, status.shard);
            let Some(open) = elected else {
                still.push(status);
                continue;
            };
            let Some(addr) = cluster
                .node(&open.node)
                .filter(|node| node.is_running())
                .map(|node| node.client_addr)
            else {
                still.push(status);
                continue;
            };
            let probe = match status.kind {
                "cache" => {
                    let key = campaign.keys.first().map_or("k0", String::as_str);
                    workload.probe_get(prober, addr, key).await
                }
                _ => workload.probe_append(prober, addr, &status.name).await,
            };
            match probe {
                Ok(()) => lines.push(format!(
                    "{shard}: {} elected at generation {} (was {} at {}), serving {:.1}s after \
                     the kill with the control plane down",
                    open.node,
                    open.generation,
                    killed,
                    status.generation,
                    started.elapsed().as_secs_f64()
                )),
                Err(err) => {
                    last_refusal.insert(shard, format!("{} refused a probe: {err:#}", open.node));
                    still.push(status);
                }
            }
        }
        pending = still;
        if pending.is_empty() {
            break;
        }
        if started.elapsed() >= bound {
            let stuck: Vec<String> = pending
                .iter()
                .map(|status| {
                    let shard = format!("{} {}/{}", status.kind, status.name, status.shard);
                    let why = last_refusal.get(&shard).cloned().unwrap_or_else(|| {
                        // Whether anyone stood tells a failed election from
                        // one never started.
                        let stood = super::leaders::stood(cluster, &status.name, status.shard);
                        format!(
                            "no broker that stood opened it at a later generation; stood: {}",
                            if stood.is_empty() {
                                "nobody".to_string()
                            } else {
                                stood.join(", ")
                            }
                        )
                    });
                    format!("{shard} (generation {}): {why}", status.generation)
                })
                .collect();
            bail!(
                "liveness: with the control plane down, {} shard(s) {killed} led had no elected \
                 leader serving {bound:?} after the kill:\n  {}",
                stuck.len(),
                stuck.join("\n  ")
            );
        }
        tokio::time::sleep(RETRY).await;
    }
    Ok(lines)
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

/// [`stuck_shards`] on Raft, where placement's generation and replica
/// reports say nothing about the group: each shard needs a member that leads
/// by its own report, a majority holding the log through its commit index,
/// and a probe that leader serves.
async fn stuck_raft_shards(
    cluster: &Cluster,
    campaign: &Campaign,
    workload: &Workload,
    served: &mut BTreeSet<(&'static str, String)>,
) -> Vec<String> {
    let mut stuck = Vec::new();
    for (kind, name) in campaign.shard_names() {
        let (leader, term) = match cluster.raft_serving(kind, name).await {
            Ok(found) => found,
            Err(why) => {
                stuck.push(format!("{kind} {name}/0: {why}"));
                continue;
            }
        };
        if served.contains(&(kind, name.to_string())) {
            continue;
        }
        let shard = format!("{kind} {name}/0 (leader {leader}, term {term})");
        let Some(addr) = cluster.node(&leader).map(|node| node.client_addr) else {
            stuck.push(format!(
                "{shard}: its leader is not a broker of this cluster"
            ));
            continue;
        };
        match probe(campaign, workload, kind, name, addr).await {
            Ok(()) => {
                served.insert((kind, name.to_string()));
            }
            Err(err) => stuck.push(format!("{shard}: its leader refused a probe: {err:#}")),
        }
    }
    stuck
}

/// [`ready_to_elect`] on Raft: every shard of the workload has a leader with
/// a majority holding its commit index, and the ones `node` leads are
/// returned with their term as the generation.
async fn raft_ready_to_elect(
    cluster: &Cluster,
    campaign: &Campaign,
    node: &str,
) -> Result<Vec<ShardStatus>> {
    let last = std::sync::Mutex::new(Vec::new());
    let found = wait::until_some(
        READY_TO_ELECT,
        &format!("the shards {node} leads to be ready to elect"),
        || async {
            let mut led = Vec::new();
            let mut waiting = Vec::new();
            for (kind, name) in campaign.shard_names() {
                match cluster.raft_serving(kind, name).await {
                    Ok((leader, term)) if leader == node => led.push(ShardStatus {
                        kind,
                        name: name.to_string(),
                        shard: 0,
                        leader,
                        generation: term,
                        replicas: Vec::new(),
                        successor: None,
                        joining: None,
                        fenced: false,
                        move_reason: None,
                        caught_up_reported: true,
                    }),
                    Ok(_) => {}
                    Err(why) => waiting.push(format!("{kind} {name}/0: {why}")),
                }
            }
            let ready = waiting.is_empty();
            *last.lock().expect("ready lock") = waiting;
            ready.then_some(led)
        },
    )
    .await;
    let led = found.with_context(|| {
        format!(
            "still waiting on {}",
            last.lock().expect("ready lock").join("; ")
        )
    })?;
    if led.is_empty() {
        bail!("{node} leads none of the workload's shards");
    }
    Ok(led)
}

/// [`await_election`] on Raft, where a broker logs no fenced open: each shard
/// in `led` needs a member other than `killed` that leads at a later term,
/// holds a majority through its commit index, serves a probe and logged its
/// open at that term. The opens come from the broker logs, as on hybrid, so
/// rule 21 sees every member that opened a term, not just the one probed.
async fn raft_await_election(
    cluster: &Cluster,
    campaign: &Campaign,
    workload: &Workload,
    killed: &str,
    led: &[ShardStatus],
    opens: &mut BTreeSet<Opened>,
) -> Result<Vec<String>> {
    let started = Instant::now();
    let bound = wait::budget(ELECTED_WITHIN);
    let mut lines = Vec::new();
    let mut pending: Vec<&ShardStatus> = led.iter().collect();
    let mut last = std::collections::BTreeMap::new();
    while !pending.is_empty() {
        if cluster.control_plane_running() {
            bail!("the control plane came back before the election was checked");
        }
        let mut still = Vec::new();
        for status in pending {
            let shard = format!("{} {}/{}", status.kind, status.name, status.shard);
            let (leader, term) = match cluster.raft_serving(status.kind, &status.name).await {
                Ok((leader, term)) if leader != killed && term > status.generation => {
                    (leader, term)
                }
                Ok((leader, term)) => {
                    last.insert(shard, format!("{leader} still leads at term {term}"));
                    still.push(status);
                    continue;
                }
                Err(why) => {
                    last.insert(shard, why);
                    still.push(status);
                    continue;
                }
            };
            let Some(addr) = cluster
                .node(&leader)
                .filter(|node| node.is_running())
                .map(|node| node.client_addr)
            else {
                still.push(status);
                continue;
            };
            match probe(campaign, workload, status.kind, &status.name, addr).await {
                Ok(()) => {
                    // Rule 21 reads only what the brokers logged. The
                    // leader that took the probe must have logged its open.
                    let mut stood = BTreeSet::new();
                    super::leaders::read_current(cluster, opens, &mut stood);
                    let logged = Opened {
                        name: status.name.clone(),
                        shard: status.shard,
                        generation: term,
                        node: leader.clone(),
                    };
                    if !opens.contains(&logged) {
                        last.insert(
                            shard,
                            format!("{leader} serves at term {term} but logged no open for it"),
                        );
                        still.push(status);
                        continue;
                    }
                    lines.push(format!(
                        "{shard}: {leader} elected at term {term} (was {killed} at {}), serving \
                         {:.1}s after the kill with the control plane down",
                        status.generation,
                        started.elapsed().as_secs_f64()
                    ));
                }
                Err(err) => {
                    last.insert(shard, format!("{leader} refused a probe: {err:#}"));
                    still.push(status);
                }
            }
        }
        pending = still;
        if pending.is_empty() {
            break;
        }
        if started.elapsed() >= bound {
            let stuck: Vec<String> = pending
                .iter()
                .map(|status| {
                    let shard = format!("{} {}/{}", status.kind, status.name, status.shard);
                    let why = last.get(&shard).cloned().unwrap_or_default();
                    format!("{shard} (term {}): {why}", status.generation)
                })
                .collect();
            bail!(
                "liveness: with the control plane down, {} shard(s) {killed} led had no elected \
                 leader serving {bound:?} after the kill:\n  {}",
                stuck.len(),
                stuck.join("\n  ")
            );
        }
        tokio::time::sleep(RETRY).await;
    }
    Ok(lines)
}

/// A write to list `name`, or a get from the cache, through `addr`, as the
/// prober numbered after the workload's clients.
async fn probe(
    campaign: &Campaign,
    workload: &Workload,
    kind: &str,
    name: &str,
    addr: std::net::SocketAddr,
) -> Result<()> {
    let prober = campaign.clients;
    match kind {
        "cache" => {
            let key = campaign.keys.first().map_or("k0", String::as_str);
            workload.probe_get(prober, addr, key).await
        }
        _ => workload.probe_append(prober, addr, name).await,
    }
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
