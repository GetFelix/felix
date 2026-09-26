//! Moves an operator starts or cancels, and abandoning a shard's log.
//!
//! Each is one assignment write, decided like a placement step from the store
//! and fresh reports. The caller writes it only at the placement token and
//! generation it was decided from (`put_shard_assignment_if`), so a cancel or
//! start decided just before another step lands nothing rather than undoing
//! it or taking a slot that step took; the caller re-reads and decides again.
//! That is how a request on any instance keeps to the same limits as the
//! lease holder's placement.
use super::moves::{AtGeneration, MovePolicy, Moves, start, undo_replacement, undo_staged};
use super::plan::{assignment_for, plan_abandoning};
use super::{Blocked, CaughtUp, Decision, MoveStep, Unplaceable};
use crate::model::{
    Cache, MoveReason, Node, NodeLifecycle, ShardAssignment, ShardKey, ShardKind, ShardState,
    Stream,
};

/// What an operator's request is decided against: one read of the catalog.
pub struct Catalog<'a> {
    pub streams: &'a [Stream],
    pub caches: &'a [Cache],
    pub nodes: &'a [Node],
    pub existing: &'a [ShardAssignment],
    pub caught_up: &'a dyn CaughtUp,
    pub policy: MovePolicy,
}

/// The write an operator's request calls for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorStep {
    pub step: MoveStep,
    pub assignment: ShardAssignment,
    /// The generation it was decided from, and the only one it may be
    /// written over.
    pub expected_generation: u64,
}

/// An [`OperatorStep`] and the placement token read before the catalog it was
/// decided from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FencedStep {
    pub(super) step: OperatorStep,
    pub(super) fence: u64,
}

/// Why a request was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// The shard has no assignment, or its stream or cache is gone.
    UnknownShard,
    /// The destination is not a registered node.
    UnknownNode(String),
    /// The destination is registered but not live.
    NotLive {
        node: String,
        lifecycle: NodeLifecycle,
    },
    /// The destination already leads the shard.
    AlreadyLeads(String),
    /// The destination is at its `max_shards` cap.
    AtCapacity(String),
    /// The destination's region may not hold this stream's data: it is not
    /// the stream's region and has no bridge from it.
    RegionNotAllowed {
        node: String,
        region: String,
        home: String,
    },
    /// A move or replacement is already in progress; cancel it first.
    Moving,
    /// The leader is down; failover places the shard, not a move.
    LeaderUnavailable(String),
    /// A move limit is reached.
    Blocked(Blocked),
    /// Nothing to cancel: no move in progress, or it has cut over.
    NotMoving,
    /// Nothing to abandon: the shard is not waiting on a log that is out of
    /// reach. Its leader serves it, or a replica can take over without loss.
    NotStranded,
    /// The log was given up, but no node can take the shard.
    Unplaceable(Unplaceable),
}

impl Refused {
    /// A stable name for the API's error code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::UnknownShard => "unknown_shard",
            Self::UnknownNode(_) => "unknown_node",
            Self::NotLive { .. } => "destination_not_live",
            Self::AlreadyLeads(_) => "already_leader",
            Self::AtCapacity(_) => "at_capacity",
            Self::RegionNotAllowed { .. } => "region_not_allowed",
            Self::Moving => "already_moving",
            Self::LeaderUnavailable(_) => "leader_unavailable",
            Self::Blocked(_) => "move_limit",
            Self::NotMoving => "not_moving",
            Self::NotStranded => "not_stranded",
            Self::Unplaceable(_) => "unplaceable",
        }
    }
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownShard => write!(f, "no such shard, or it has no assignment"),
            Self::UnknownNode(node) => write!(f, "node {node} is not registered"),
            Self::NotLive { node, lifecycle } => {
                write!(f, "node {node} is {lifecycle:?}, not live")
            }
            Self::AlreadyLeads(node) => write!(f, "node {node} already leads this shard"),
            Self::AtCapacity(node) => write!(f, "node {node} is at its max_shards capacity"),
            Self::RegionNotAllowed { node, region, home } => write!(
                f,
                "node {node} is in region {region}, which has no bridge from this stream's region {home}"
            ),
            Self::Moving => write!(
                f,
                "a move or replacement is already in progress for this shard; cancel it first"
            ),
            Self::LeaderUnavailable(node) => write!(
                f,
                "leader {node} is not serving; failover places this shard, not a move"
            ),
            Self::Blocked(blocked) => write!(f, "{blocked}"),
            Self::NotMoving => write!(
                f,
                "no move is in progress for this shard; it may already have cut over"
            ),
            Self::NotStranded => write!(
                f,
                "this shard is not waiting on a lost log: its leader is serving, or a replica can take over"
            ),
            Self::Unplaceable(why) => write!(f, "{why}"),
        }
    }
}

/// Start moving `key`'s leadership to `destination`.
///
/// Held to the same limits as placement's own moves, but not to a pause:
/// pausing is how an operator keeps placement out of the way while moving
/// shards by hand.
pub fn start_move(
    catalog: &Catalog<'_>,
    key: &ShardKey,
    destination: &str,
) -> Result<OperatorStep, Refused> {
    let existing = assignment_of(catalog, key)?;
    let (_, durable, home) = placeable_of(catalog, key).ok_or(Refused::UnknownShard)?;
    let node = catalog
        .nodes
        .iter()
        .find(|node| node.node_id == destination)
        .ok_or_else(|| Refused::UnknownNode(destination.to_string()))?;
    if node.status.lifecycle != NodeLifecycle::Live {
        return Err(Refused::NotLive {
            node: destination.to_string(),
            lifecycle: node.status.lifecycle,
        });
    }
    if let Some(home) = home
        && !catalog.policy.regions.can_route(home, &node.spec.region)
    {
        return Err(Refused::RegionNotAllowed {
            node: destination.to_string(),
            region: node.spec.region.clone(),
            home: home.clone(),
        });
    }
    if existing.leader == destination {
        return Err(Refused::AlreadyLeads(destination.to_string()));
    }
    if moving(existing) {
        return Err(Refused::Moving);
    }
    // The same condition that sends a shard down placement's move path: a
    // leader still serving, and a log to move unless the leader is live.
    let leader_live = lifecycle_of(catalog, &existing.leader) == Some(NodeLifecycle::Live);
    let leader_serving = leader_live
        || (lifecycle_of(catalog, &existing.leader) == Some(NodeLifecycle::Draining) && durable);
    if !leader_serving {
        return Err(Refused::LeaderUnavailable(existing.leader.clone()));
    }
    if let Some(max) = node.spec.capacity.max_shards
        && !existing.replicas.iter().any(|r| r == destination)
        && roles_on(catalog, destination) >= max
    {
        return Err(Refused::AtCapacity(destination.to_string()));
    }
    let mut moves = Moves::counting(catalog.existing, catalog.policy.clone());
    moves
        .begin_requested(&existing.leader, destination)
        .map_err(Refused::Blocked)?;
    let caught_up = AtGeneration::new(catalog.caught_up, existing.generation);
    let (step, assignment) = start(
        existing,
        destination,
        MoveReason::Operator,
        &caught_up,
        catalog.caught_up.as_of_millis(),
        &moves,
    );
    Ok(OperatorStep {
        step,
        assignment,
        expected_generation: existing.generation,
    })
}

/// Cancel `key`'s move or replacement, whoever started it.
///
/// Before the fence the destination is dropped, as a timeout would. After
/// the fence the leader that stopped serves again at a new generation: it
/// still holds every write it accepted, since nobody else has led since,
/// and the ones still in flight when it stopped land in the same log. Once
/// the move has cut over there is nothing to cancel.
///
/// Either way the start time stays, so placement queues this shard behind
/// others for its next move. Pause placement first to stop it choosing the
/// same move again.
pub fn cancel_move(catalog: &Catalog<'_>, key: &ShardKey) -> Result<OperatorStep, Refused> {
    let existing = assignment_of(catalog, key)?;
    let (replication_factor, _, _) = placeable_of(catalog, key).ok_or(Refused::UnknownShard)?;
    let (step, assignment) = if existing.state == ShardState::Draining {
        // A leader that is down cannot take the shard back; failover
        // promotes a replica that holds the log instead.
        if !matches!(
            lifecycle_of(catalog, &existing.leader),
            Some(NodeLifecycle::Live | NodeLifecycle::Draining)
        ) {
            return Err(Refused::LeaderUnavailable(existing.leader.clone()));
        }
        let successor = existing.successor.clone();
        let replicas = match successor.as_deref() {
            Some(successor) => undo_staged(existing, successor, replication_factor).replicas,
            None => existing.replicas.clone(),
        };
        (
            MoveStep::Retake { successor },
            ShardAssignment {
                replicas,
                generation: 0,
                state: ShardState::Assigning,
                successor: None,
                joining: None,
                move_reason: None,
                ..existing.clone()
            },
        )
    } else if let Some(successor) = existing.successor.as_deref() {
        (
            MoveStep::Cancel {
                successor: successor.to_string(),
            },
            undo_staged(existing, successor, replication_factor),
        )
    } else if let Some(joining) = existing.joining.as_deref() {
        (
            MoveStep::Cancel {
                successor: joining.to_string(),
            },
            undo_replacement(existing, joining),
        )
    } else {
        return Err(Refused::NotMoving);
    };
    Ok(OperatorStep {
        step,
        assignment,
        expected_generation: existing.generation,
    })
}

/// Give up the log of a shard whose only copies are out of reach, and place
/// it afresh.
///
/// Only for a durable shard that placement is holding unplaced: its leader is not
/// serving and no replica holds everything it may have acknowledged. The new
/// leader is chosen as for any new shard and starts from whatever it holds,
/// usually nothing, at a new generation. Every record only the old leader
/// held is lost, including acknowledged ones, and a returning old leader does
/// not get them back.
pub fn abandon_log(catalog: &Catalog<'_>, key: &ShardKey) -> Result<OperatorStep, Refused> {
    let existing = assignment_of(catalog, key)?;
    placeable_of(catalog, key).ok_or(Refused::UnknownShard)?;
    let decide = |abandoned: Option<&ShardKey>| {
        plan_abandoning(
            catalog.streams,
            catalog.caches,
            catalog.nodes,
            catalog.existing,
            catalog.caught_up,
            catalog.policy.clone(),
            abandoned,
        )
        .shards
        .into_iter()
        .find(|shard| &shard.key == key)
        .map(|shard| shard.decision)
    };
    // Decided by placement itself, so this refuses exactly when placement
    // would do something better than lose the log.
    if !matches!(
        decide(None),
        Some(Decision::Unplaceable(
            Unplaceable::NoCaughtUpReplica | Unplaceable::OwnerUnavailable { .. }
        ))
    ) {
        return Err(Refused::NotStranded);
    }
    match decide(Some(key)) {
        Some(Decision::Place(leader, replicas)) => Ok(OperatorStep {
            step: MoveStep::Discard {
                from: existing.leader.clone(),
                to: leader.clone(),
            },
            assignment: assignment_for(key, &leader, replicas),
            expected_generation: existing.generation,
        }),
        Some(Decision::Unplaceable(why)) => Err(Refused::Unplaceable(why)),
        _ => Err(Refused::NotStranded),
    }
}

/// Whether a move or replacement is in progress.
pub(super) fn moving(assignment: &ShardAssignment) -> bool {
    assignment.successor.is_some()
        || assignment.joining.is_some()
        || assignment.state == ShardState::Draining
}

fn assignment_of<'a>(
    catalog: &Catalog<'a>,
    key: &ShardKey,
) -> Result<&'a ShardAssignment, Refused> {
    catalog
        .existing
        .iter()
        .find(|assignment| &assignment.key == key)
        .ok_or(Refused::UnknownShard)
}

/// The replication factor, durability and home region of the stream or cache
/// `key` is a shard of, if it still exists.
fn placeable_of<'a>(
    catalog: &Catalog<'a>,
    key: &ShardKey,
) -> Option<(u32, bool, Option<&'a String>)> {
    match key.kind {
        ShardKind::Stream => catalog
            .streams
            .iter()
            .find(|s| {
                s.tenant_id == key.tenant_id
                    && s.namespace == key.namespace
                    && s.stream == key.stream
            })
            .filter(|s| key.shard < s.shards)
            .map(|s| (s.replication_factor.max(1), s.durable, s.region.as_ref())),
        ShardKind::Cache => catalog
            .caches
            .iter()
            .find(|c| {
                c.tenant_id == key.tenant_id
                    && c.namespace == key.namespace
                    && c.cache == key.stream
            })
            .filter(|c| key.shard < c.shards)
            .map(|c| (c.replication_factor.max(1), true, None)),
    }
}

fn lifecycle_of(catalog: &Catalog<'_>, node_id: &str) -> Option<NodeLifecycle> {
    catalog
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .map(|node| node.status.lifecycle)
}

/// Roles `node_id` holds, leaders and followers, which is what `max_shards`
/// caps.
fn roles_on(catalog: &Catalog<'_>, node_id: &str) -> u32 {
    catalog
        .existing
        .iter()
        .filter(|assignment| assignment.nodes().any(|node| node == node_id))
        .count() as u32
}

/// How many times a request is decided again after another writer got in
/// first. Placement writes a shard at most once a pass, and a pass is a burst
/// of writes, so losing this many races in a row means something is writing
/// in a loop.
const ATTEMPTS: usize = 5;

/// Pause before deciding again after a placement pass got in first, so the
/// rest of its burst lands before the next read rather than during it.
const FENCED_BACKOFF: std::time::Duration = std::time::Duration::from_millis(20);

/// Why an operator's request was not carried out.
#[derive(Debug)]
pub enum OperatorError {
    Refused(Refused),
    Store(crate::store::StoreError),
    /// The shard changed under every attempt.
    Contended,
}

/// Decide an operator's request against a fresh read and write it at the
/// generation it was decided from. A write that finds the shard moved on
/// lands nothing, and the request is decided again from a new read: a cancel
/// that raced a cut-over then finds nothing to cancel, rather than handing
/// the shard back to a leader that no longer has every acknowledged write.
pub async fn run_operator(
    store: &dyn crate::store::ControlPlaneStore,
    liveness: &crate::config::NodeLivenessConfig,
    policy: MovePolicy,
    wakes: &super::PlacementWakes,
    decide: impl Fn(&Catalog<'_>) -> Result<OperatorStep, Refused>,
) -> Result<(MoveStep, ShardAssignment), OperatorError> {
    for _ in 0..ATTEMPTS {
        let (fence, read) = super::PlacementRead::load_fenced(store, liveness)
            .await
            .map_err(OperatorError::Store)?;
        let step = decide(&read.catalog(policy.clone())).map_err(OperatorError::Refused)?;
        let decided = FencedStep { step, fence };
        match write_operator_step(store, &decided)
            .await
            .map_err(OperatorError::Store)?
        {
            OperatorWrite::Written(written) => {
                wakes.assignment_written();
                // The next step, a fence or the move this one freed a slot
                // for, need not wait for the tick.
                wakes.request_pass();
                return Ok((decided.step.step, *written));
            }
            OperatorWrite::Stale => continue,
            OperatorWrite::Fenced => tokio::time::sleep(FENCED_BACKOFF).await,
        }
    }
    Err(OperatorError::Contended)
}

/// What [`write_operator_step`] did.
#[derive(Debug)]
pub(super) enum OperatorWrite {
    Written(Box<ShardAssignment>),
    /// The shard moved on since the read.
    Stale,
    /// Another placement write landed since the read.
    Fenced,
}

/// Write one decided step, only at the token and generation it was decided
/// from.
pub(super) async fn write_operator_step(
    store: &dyn crate::store::ControlPlaneStore,
    fenced: &FencedStep,
) -> crate::store::StoreResult<OperatorWrite> {
    let decided = &fenced.step;
    let key = &decided.assignment.key;
    match store
        .put_shard_assignment_if(
            decided.assignment.clone(),
            Some(decided.expected_generation),
            fenced.fence,
        )
        .await?
    {
        crate::store::AssignmentWrite::Fenced { token } => {
            metrics::counter!(super::PLACEMENT_WRITES_FENCED_TOTAL).increment(1);
            tracing::info!(
                kind = %key.kind,
                name = %key.stream,
                shard = key.shard,
                step = decided.step.label(),
                fence = fenced.fence,
                token,
                "another placement write landed while an operator's request was decided; deciding again",
            );
            Ok(OperatorWrite::Fenced)
        }
        crate::store::AssignmentWrite::Written(written) => {
            metrics::counter!(super::SHARD_MOVE_STEPS_TOTAL, "step" => decided.step.label())
                .increment(1);
            tracing::info!(
                kind = %key.kind,
                name = %key.stream,
                shard = key.shard,
                step = decided.step.label(),
                detail = ?decided.step,
                leader = %written.leader,
                successor = ?written.successor,
                generation = written.generation,
                "shard move changed by an operator",
            );
            Ok(OperatorWrite::Written(Box::new(written)))
        }
        crate::store::AssignmentWrite::Stale { current } => {
            metrics::counter!(super::SHARD_ASSIGNMENT_WRITE_CONFLICTS_TOTAL).increment(1);
            tracing::info!(
                kind = %key.kind,
                name = %key.stream,
                shard = key.shard,
                step = decided.step.label(),
                planned_from = decided.expected_generation,
                current = ?current,
                "shard changed while an operator's request was decided; deciding again",
            );
            Ok(OperatorWrite::Stale)
        }
    }
}
