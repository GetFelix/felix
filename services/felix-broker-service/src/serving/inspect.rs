//! `shard_inspect`: this broker's own view of one shard, for an operator.
//!
//! Read-only and bounded: the answer is one shard, its replica set and the
//! fence, all read from snapshots the lifecycle and the replication driver
//! publish. Nothing here takes the lifecycle's lock, waits on a driver pass, or
//! opens a log that is not open already.

use std::time::Duration;

use felix_broker::Broker;
use felix_replication::status::ShardStatus;
use felix_wire::{
    InspectedAssignment, InspectedFence, InspectedLease, InspectedReplica, ShardInspection,
};

use crate::serving::quic::handlers::publish::PublishContext;
use crate::shards::lifecycle::Phase;
use crate::shards::lifecycle::fence::PhaseRecord;
use crate::shards::{ShardKey, ShardKind};

/// Everything the answer is built from, gathered first so the decision is a
/// pure function a test can drive phase by phase.
#[derive(Debug, Clone, Default)]
pub(crate) struct Observed {
    /// `None` on a broker that is not in a cluster: it leads everything it
    /// knows of.
    pub(crate) node_id: Option<String>,
    pub(crate) shards: u32,
    pub(crate) route: Option<felix_router::Route>,
    pub(crate) phase: Option<PhaseRecord>,
    pub(crate) status: Option<ShardStatus>,
    pub(crate) lease: Option<Lease>,
    /// Writes at the held generation get in without the lease.
    pub(crate) lease_free: bool,
    pub(crate) tail: Option<u64>,
    pub(crate) committed: Option<u64>,
    pub(crate) accepted_generation: Option<u64>,
    /// How long until the next fence attempt, read off the board's deadline.
    pub(crate) fence_retry_in: Option<Duration>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Lease {
    pub(crate) valid: bool,
    pub(crate) remaining: Duration,
}

/// Gather what this broker knows of `key` and build the answer.
pub(crate) async fn inspect(
    broker: &Broker,
    publish_ctx: &PublishContext,
    key: &ShardKey,
) -> ShardInspection {
    let mut observed = Observed::default();
    match publish_ctx.ingress.as_deref() {
        Some(ingress) => {
            observed.node_id = Some(ingress.local_node_id().to_string());
            observed.shards = ingress
                .placed_shards_for(key.kind, &key.tenant_id, &key.namespace, &key.stream)
                .unwrap_or(0);
            observed.route = ingress.route(key);
            observed.phase = ingress.fence().phase_of(key);
            if let Some(phase) = &observed.phase {
                observed.lease_free = ingress.fence().lease_free(key, phase.generation);
            }
        }
        None => {
            let exists = match key.kind {
                ShardKind::Stream => {
                    broker
                        .stream_exists(&key.tenant_id, &key.namespace, &key.stream)
                        .await
                }
                ShardKind::Cache => {
                    broker
                        .cache_exists(&key.tenant_id, &key.namespace, &key.stream)
                        .await
                }
            };
            observed.shards = u32::from(exists);
        }
    }
    observed.status = publish_ctx
        .shard_status
        .as_ref()
        .and_then(|board| board.get(key));
    observed.fence_retry_in = observed
        .status
        .as_ref()
        .and_then(|status| status.fence.as_ref()?.retry_at)
        .map(|at| at.saturating_duration_since(tokio::time::Instant::now()));
    observed.lease = publish_ctx.lease.as_ref().map(|lease| Lease {
        valid: lease.is_valid_now(),
        remaining: lease.remaining(),
    });
    if let (Some(marks), Some(route)) = (&publish_ctx.marks, &observed.route) {
        observed.committed = marks.offset(key, route.generation);
    }
    // Only a log this broker has open: opening one would create it.
    if key.kind == ShardKind::Stream
        && let Some(log) = broker.durable_storage().and_then(|storage| {
            storage.opened_stream(&key.tenant_id, &key.namespace, &key.stream, key.shard)
        })
    {
        observed.tail = log.tail_offset().await.ok();
        observed.accepted_generation = Some(log.accepted_generation());
    }
    build(observed)
}

/// The answer, from what was observed.
pub(crate) fn build(observed: Observed) -> ShardInspection {
    let Observed {
        node_id,
        shards,
        route,
        phase,
        status,
        lease,
        lease_free,
        tail,
        committed,
        accepted_generation,
        fence_retry_in,
    } = observed;
    let tail = tail.or(status.as_ref().and_then(|status| status.tail));
    let lease_view = lease.map(|lease| InspectedLease {
        held: lease.valid,
        remaining_ms: u64::try_from(lease.remaining.as_millis()).unwrap_or(u64::MAX),
    });
    let Some(node_id) = node_id else {
        // Not in a cluster: no generations, no replicas, no fence.
        let known = shards > 0;
        return ShardInspection {
            node_id: String::new(),
            shards,
            role: if known { "leader" } else { "none" }.to_string(),
            phase: if known { "active" } else { "unassigned" }.to_string(),
            serving: known,
            reason: (!known).then(|| "not_assigned_here".to_string()),
            detail: (!known).then(|| "this broker knows no such stream or cache".to_string()),
            generation: None,
            assignment: None,
            fence: None,
            lease: lease_view,
            tail,
            committed,
            accepted_generation,
            replicas: Vec::new(),
        };
    };

    let role = match &route {
        Some(route) if route.leader.node_id == node_id => "leader",
        Some(route) if route.replicas.iter().any(|r| r.node_id == node_id) => "follower",
        _ => "none",
    };
    let assignment = route.as_ref().map(|route| InspectedAssignment {
        generation: route.generation,
        leader: route.leader.node_id.clone(),
        replicas: route
            .replicas
            .iter()
            .filter(|replica| replica.node_id != route.leader.node_id)
            .map(|replica| replica.node_id.clone())
            .collect(),
        draining: route.draining,
        successor: route.successor.clone(),
    });
    let others: Vec<String> = assignment
        .as_ref()
        .map(|assignment| assignment.replicas.clone())
        .unwrap_or_default();
    let fencing = phase
        .as_ref()
        .is_some_and(|phase| phase.phase == Phase::Fencing);
    // The board's fence is for the generation it was recorded at; one from an
    // earlier promotion says nothing about this one.
    let board_fence = status.as_ref().and_then(|status| {
        let current = route
            .as_ref()
            .is_some_and(|route| route.generation == status.generation);
        status.fence.as_ref().filter(|_| current && fencing)
    });
    let fence = board_fence.map(|fence| InspectedFence {
        took: fence.took.clone(),
        pending: fence.pending.clone(),
        attempts: fence.attempts,
        retry_in_ms: fence_retry_in.map(|wait| u64::try_from(wait.as_millis()).unwrap_or(u64::MAX)),
    });

    let (reason, detail) = if role != "leader" {
        let detail = match &route {
            Some(route) => format!(
                "{} leads it at generation {}",
                route.leader.node_id, route.generation
            ),
            None => "no assignment for this shard has reached this broker".to_string(),
        };
        (Some("not_assigned_here"), Some(detail))
    } else {
        not_serving(
            phase.as_ref(),
            route.as_ref(),
            lease,
            lease_free,
            board_fence.map(|fence| (fence.took.len(), others.len(), fence.why.as_deref())),
        )
    };

    let replicas = if role != "leader" {
        Vec::new()
    } else if fencing {
        let took = board_fence
            .map(|fence| fence.took.as_slice())
            .unwrap_or(&[]);
        others
            .iter()
            .map(|node| InspectedReplica {
                node_id: node.clone(),
                role: replica_role(route.as_ref(), node).to_string(),
                next_offset: None,
                lag: None,
                fence: board_fence.map(|_| took.contains(node)),
                state: "fencing".to_string(),
                halted: None,
            })
            .collect()
    } else {
        status
            .as_ref()
            .map(|status| {
                status
                    .followers
                    .iter()
                    .map(|follower| InspectedReplica {
                        node_id: follower.node_id.clone(),
                        role: if follower.learner {
                            "learner"
                        } else {
                            "follower"
                        }
                        .to_string(),
                        next_offset: Some(follower.next_offset),
                        lag: follower.lag,
                        fence: None,
                        state: follower.state.to_string(),
                        halted: follower.halted.map(str::to_string),
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    ShardInspection {
        node_id,
        shards,
        role: role.to_string(),
        phase: phase
            .as_ref()
            .map_or(Phase::Unassigned, |phase| phase.phase)
            .label()
            .to_string(),
        serving: reason.is_none(),
        reason: reason.map(str::to_string),
        detail,
        generation: phase.as_ref().map(|phase| phase.generation),
        assignment,
        fence,
        lease: lease_view,
        tail,
        committed: committed.filter(|_| role == "leader"),
        accepted_generation,
        replicas,
    }
}

/// Why a shard this broker leads by its routing is not serving, or `None`
/// when it is. `fence` is how many replicas took the fence, of how many, and
/// why the latest attempt stopped.
fn not_serving(
    phase: Option<&PhaseRecord>,
    route: Option<&felix_router::Route>,
    lease: Option<Lease>,
    lease_free: bool,
    fence: Option<(usize, usize, Option<&str>)>,
) -> (Option<&'static str>, Option<String>) {
    let Some(phase) = phase else {
        return (
            Some("opening"),
            Some("assigned here, and this broker has not started opening it".to_string()),
        );
    };
    let wanted = route.map_or(phase.generation, |route| route.generation);
    match phase.phase {
        Phase::Unassigned | Phase::Opening => (
            Some("opening"),
            Some("recovering the local log before it serves".to_string()),
        ),
        Phase::Fencing => (
            Some("fencing"),
            Some(match fence {
                // The count says it all unless the attempt stopped for
                // another reason, such as a replica on a newer generation.
                Some((took, of, Some(why))) if !why.contains("replicas took the fence") => {
                    format!("{took} of {of} replicas took the fence; {why}")
                }
                Some((took, of, _)) => format!("{took} of {of} replicas took the fence"),
                None => "waiting for the first fence attempt to finish".to_string(),
            }),
        ),
        Phase::Failed => (
            Some("failed"),
            Some(
                phase
                    .error
                    .clone()
                    .unwrap_or_else(|| "the local log could not be opened".to_string()),
            ),
        ),
        Phase::Draining | Phase::Closed => (
            Some("draining"),
            Some(match route.and_then(|route| route.successor.as_deref()) {
                Some(successor) => format!("moving to {successor}; writes wait for the cut-over"),
                None => "stopped serving it at this generation".to_string(),
            }),
        ),
        Phase::Active if phase.generation < wanted => (
            Some("behind_generation"),
            Some(format!(
                "serving generation {} while the assignment is at {wanted}",
                phase.generation
            )),
        ),
        Phase::Active if lease.is_some_and(|lease| !lease.valid) && !lease_free => (
            Some("lease_lapsed"),
            Some("this broker's lease ran out; writes resume once it renews".to_string()),
        ),
        Phase::Active => (None, None),
    }
}

/// `learner` for a move's destination, `follower` for anyone else.
fn replica_role(route: Option<&felix_router::Route>, node: &str) -> &'static str {
    if route.and_then(|route| route.successor.as_deref()) == Some(node) {
        "learner"
    } else {
        "follower"
    }
}

#[cfg(test)]
mod tests;
