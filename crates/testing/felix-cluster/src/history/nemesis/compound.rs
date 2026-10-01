//! The compound faults: how [`RandomNemesis::adversarial`] picks them, and
//! how each is injected and healed.
//!
//! These break the one-fault-at-a-time rule the rest of the nemesis keeps.
//! Two brokers down at once can take a majority, so the clients see the
//! cluster stop; what the campaign checks is that nothing acknowledged is
//! lost while it is stopped, and that it serves again once healed.
//!
//! [`RandomNemesis::adversarial`]: super::RandomNemesis::adversarial

use std::time::Duration;

use anyhow::{Context, Result, anyhow};

use super::{ClusterView, Fault, FaultKind, LINK_DELAY, Nemesis, RandomNemesis, restart_if_down};
use crate::fault::Endpoint;
use crate::history::rng::Rng;
use crate::{Cluster, wait};

/// The kinds an [`FaultKind::Overlap`] draws its two faults from: the single
/// faults that make sense on two brokers at once.
pub(super) const OVERLAP_KINDS: &[FaultKind] = &[
    FaultKind::Kill,
    FaultKind::Pause,
    FaultKind::Partition,
    FaultKind::DropOutbound,
    FaultKind::DelayOutbound,
    FaultKind::SlowFsync,
    FaultKind::MoveShard,
    FaultKind::Drain,
];

/// What a [`FaultKind::ControlPlaneCrash`] has in flight when the control
/// plane goes down: a move, a drain, or a leader killed so its shards must
/// fail over.
pub(super) const IN_FLIGHT_KINDS: &[FaultKind] =
    &[FaultKind::MoveShard, FaultKind::Drain, FaultKind::Kill];

/// How many times a [`FaultKind::RestartLoop`] starts its broker again.
pub(super) const RESTARTS: u32 = 3;

/// How long a restarted broker runs before the loop kills it again: enough
/// to start catching up, not enough to finish under the workload.
const RESTART_GAP: Duration = Duration::from_millis(500);

/// How long a move runs before [`FaultKind::InterruptedMove`] kills one end.
/// Short, so the kill lands while the destination is still copying.
const MOVE_HEAD_START: Duration = Duration::from_millis(300);

/// How long what a [`FaultKind::ControlPlaneCrash`] started runs before the
/// control plane goes down: long enough for a move or drain to be under way.
const CRASH_HEAD_START: Duration = Duration::from_millis(300);

/// How long an isolated leader may keep its shards before the isolation is
/// reported as having caused no failover. The harness control plane expires a
/// silent broker after a second.
const FAILOVER: Duration = Duration::from_secs(20);

/// Isolate a leader when there is one, since isolating a follower forces no
/// failover.
pub(super) fn pick_isolate(rng: &mut Rng, view: &ClusterView, node: String) -> Fault {
    Fault::Isolate {
        node: a_leader(rng, view, node),
    }
}

pub(super) fn pick_kill_two(rng: &mut Rng, node: String, peers: &[String]) -> Fault {
    let mut faults = vec![Fault::Kill { node }];
    if !peers.is_empty() {
        faults.push(Fault::Kill {
            node: rng.pick(peers).clone(),
        });
    }
    Fault::Several {
        kind: FaultKind::KillTwo,
        faults,
    }
}

pub(super) fn pick_partition_and_delay(
    rng: &mut Rng,
    view: &ClusterView,
    node: String,
    peers: &[String],
) -> Fault {
    let mut faults = vec![Fault::Partition { node }];
    if !peers.is_empty() {
        let slow = rng.pick(peers).clone();
        let peers = view
            .nodes
            .iter()
            .filter(|peer| **peer != slow)
            .cloned()
            .collect();
        faults.push(Fault::DelayOutbound {
            node: slow,
            peers,
            by: LINK_DELAY,
        });
    }
    Fault::Several {
        kind: FaultKind::PartitionAndDelay,
        faults,
    }
}

/// Two faults from [`OVERLAP_KINDS`], the second aimed away from every broker
/// the first one is.
pub(super) fn pick_overlap(rng: &mut Rng, view: &ClusterView) -> Option<Fault> {
    let first = pick_one(rng, view, &[])?;
    let second = pick_one(rng, view, &first.targets())?;
    Some(Fault::Several {
        kind: FaultKind::Overlap,
        faults: vec![first, second],
    })
}

pub(super) fn pick_interrupted_move(rng: &mut Rng, view: &ClusterView) -> Option<Fault> {
    let Some(Fault::MoveShard {
        kind,
        name,
        shard,
        from,
        to,
    }) = RandomNemesis::new(vec![FaultKind::MoveShard]).next_fault(rng, view)
    else {
        return None;
    };
    let victim = if rng.percent(50) {
        from.clone()
    } else {
        to.clone()
    };
    Some(Fault::InterruptedMove {
        kind,
        name,
        shard,
        from,
        to,
        victim,
    })
}

pub(super) fn pick_control_plane_crash(
    rng: &mut Rng,
    view: &ClusterView,
    node: String,
) -> Option<Fault> {
    let during = match *rng.pick(IN_FLIGHT_KINDS) {
        FaultKind::MoveShard => {
            RandomNemesis::new(vec![FaultKind::MoveShard]).next_fault(rng, view)?
        }
        FaultKind::Drain => Fault::Drain { node },
        _ => Fault::Kill {
            node: a_leader(rng, view, node),
        },
    };
    Some(Fault::ControlPlaneCrash {
        during: Box::new(during),
    })
}

/// Cut `node` off from its peers through the partition file and from the
/// control plane through its proxy, then wait for the control plane to give
/// its shards to someone else.
pub(super) async fn isolate(cluster: &mut Cluster, node: &str) -> Result<()> {
    cluster.partition_node(node)?;
    for fault in crate::Fault::partition(Endpoint::node(node), Endpoint::ControlPlane) {
        cluster.inject(&fault).await?;
    }
    let still_led = std::sync::Mutex::new(Vec::new());
    wait::until(
        FAILOVER,
        &format!("{node}'s shards to fail over"),
        || async {
            let Ok(owners) = cluster.shard_owners().await else {
                return false;
            };
            let mut led: Vec<String> = owners
                .into_iter()
                .filter(|(_, leader)| leader == node)
                .map(|(shard, _)| shard)
                .collect();
            led.sort();
            let done = led.is_empty();
            *still_led.lock().expect("still-led lock") = led;
            done
        },
    )
    .await
    .with_context(|| {
        format!(
            "{node} still leads {}",
            still_led.lock().expect("still-led lock").join(", ")
        )
    })
}

pub(super) async fn rejoin(cluster: &mut Cluster, node: &str) -> Result<()> {
    for fault in crate::Fault::partition(Endpoint::node(node), Endpoint::ControlPlane) {
        cluster.heal(&fault).await?;
    }
    cluster.heal_partitions()
}

pub(super) async fn inject_all(cluster: &mut Cluster, faults: &[Fault]) -> Result<()> {
    for fault in faults {
        Box::pin(fault.inject(cluster))
            .await
            .with_context(|| format!("{fault}"))?;
    }
    Ok(())
}

/// Heal every fault in [`heal_order`], going on after one fails to heal.
pub(super) async fn heal_all(cluster: &mut Cluster, faults: &[Fault]) -> Result<()> {
    let mut first_error = None;
    for fault in heal_order(faults) {
        if let Err(err) = Box::pin(fault.heal(cluster)).await {
            first_error.get_or_insert(anyhow!("{fault}: {err:#}"));
        }
    }
    first_error.map_or(Ok(()), Err)
}

pub(super) async fn interrupt_move(
    cluster: &mut Cluster,
    kind: &str,
    name: &str,
    shard: u32,
    to: &str,
    victim: &str,
) -> Result<()> {
    cluster.start_move_of(kind, name, shard, to).await?;
    tokio::time::sleep(MOVE_HEAD_START).await;
    cluster.kill_node(victim)
}

/// Start `during`, give it a head start, then crash the control plane.
pub(super) async fn crash_control_plane(cluster: &mut Cluster, during: &Fault) -> Result<()> {
    Box::pin(during.inject(cluster))
        .await
        .with_context(|| format!("{during}"))?;
    tokio::time::sleep(CRASH_HEAD_START).await;
    cluster.crash_control_plane().await
}

/// Bring the control plane back, then heal what it crashed in the middle of.
/// A move or restore it had started either resumes or is dropped; healing a
/// move or drain waits for that.
pub(super) async fn recover_control_plane(cluster: &mut Cluster, during: &Fault) -> Result<()> {
    cluster.recover_control_plane().await?;
    Box::pin(during.heal(cluster))
        .await
        .with_context(|| format!("{during}"))
}

/// Start `node` and kill it again before it can catch up, `restarts - 1`
/// times, then start it and leave it up.
pub(super) async fn restart_loop(cluster: &mut Cluster, node: &str, restarts: u32) -> Result<()> {
    for _ in 1..restarts {
        restart_if_down(cluster, node).await?;
        tokio::time::sleep(RESTART_GAP).await;
        cluster.kill_node(node)?;
    }
    restart_if_down(cluster, node).await
}

/// Last in first out, except that the assignment faults go last of all:
/// healing one waits for moves to finish, and a move needs the brokers the
/// other faults took away.
pub(super) fn heal_order(faults: &[Fault]) -> Vec<&Fault> {
    let mut order: Vec<&Fault> = faults.iter().rev().collect();
    order.sort_by_key(|fault| fault.family() == super::FaultFamily::Assignment);
    order
}

/// `node` if it leads a shard, or else a broker that does: faulting a
/// follower forces no failover.
fn a_leader(rng: &mut Rng, view: &ClusterView, node: String) -> String {
    if view.leaders.is_empty() || view.leaders.contains(&node) {
        node
    } else {
        rng.pick(&view.leaders).clone()
    }
}

/// One fault of a kind from [`OVERLAP_KINDS`], aimed at none of `exclude`.
fn pick_one(rng: &mut Rng, view: &ClusterView, exclude: &[String]) -> Option<Fault> {
    let kind = *rng.pick(OVERLAP_KINDS);
    let keep = |nodes: &[String]| -> Vec<String> {
        nodes
            .iter()
            .filter(|node| !exclude.contains(node))
            .cloned()
            .collect()
    };
    let narrowed = ClusterView {
        nodes: keep(&view.nodes),
        leaders: keep(&view.leaders),
        shards: view
            .shards
            .iter()
            .filter(|shard| !exclude.contains(&shard.leader))
            .cloned()
            .collect(),
    };
    let fault = RandomNemesis::new(vec![kind]).next_fault(rng, &narrowed)?;
    // A move's destination is drawn from every other broker, so it can still
    // land on an excluded one.
    if fault.targets().iter().any(|node| exclude.contains(node)) {
        return None;
    }
    Some(fault)
}
