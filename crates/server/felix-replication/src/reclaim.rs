//! A leader that gave its copy up after a crash (`crate::unclean`) asks every
//! follower where its copy ends, so placement can decide what to do with the
//! shard from what the copies hold now rather than from a report made before
//! the crash (#1295).
//!
//! Each follower is sent a fence at the generation this broker led. One that
//! took that generation from it changes nothing; one still at an older
//! generation takes it, as it would this broker's batch at it. Only a probe every follower answered is reported: a follower not
//! heard from may hold what this broker lost. When none is ahead of this
//! broker's copy, nothing a client was promised is held beyond it, and
//! placement may name this broker leader again at a new generation. When one
//! is ahead, the furthest are reported caught up: every copy is accounted
//! for, so the furthest holds whatever any copy holds.

use std::sync::Arc;

use felix_broker::{Broker, LogKind};
use felix_router::{Route, ShardKey, ShardKind};
use felix_storage::StorageId;
use felix_wire::internal::{Fence, InternalMessage};

use crate::peer::{Incarnation, PeerRequester};
use crate::replica::last_generation;
use crate::reporter::ShardReport;

/// What a probe found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Probe {
    pub report: ShardReport,
    /// No follower's copy is ahead of this broker's on any of the shard's logs.
    pub reclaimable: bool,
}

/// Probe every follower `route` names for the shard this broker gave up at
/// `route.generation`. `None` when the shard has no followers, or any of
/// them did not answer with its copy and identity.
pub async fn probe<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    local_node_id: &str,
    key: &ShardKey,
    route: &Route,
) -> Option<Probe> {
    let followers: Vec<_> = route
        .replicas
        .iter()
        .filter(|replica| replica.node_id != local_node_id)
        .collect();
    if followers.is_empty() {
        return None;
    }
    let logs: &[LogKind] = match key.kind {
        ShardKind::Stream => &[LogKind::Stream],
        ShardKind::Cache => &[LogKind::Cache, LogKind::Counters],
    };
    let shard = crate::incarnation::shard_ref(broker, key, route.generation);
    let mut reclaimable = true;
    let mut report: Option<ShardReport> = None;
    for &kind in logs {
        let Some(log) = broker
            .shard_log_for(
                kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                None,
                crate::incarnation::expected(broker, key.kind, &shard),
            )
            .await
        else {
            // A cache without a counter store has no counter log to compare.
            if kind == LogKind::Counters && broker.counters().is_none() {
                continue;
            }
            return None;
        };
        let tail = log.tail_offset().await.ok()?;
        let own = (last_generation(&log.generations(), tail), tail);
        let mut answers = Vec::with_capacity(followers.len());
        for follower in &followers {
            let request = InternalMessage::Fence(Fence {
                correlation_id: 0,
                shard: shard.clone(),
                log: crate::ship::replica_log(kind),
            });
            let (answer, identity) = requester
                .request_identified(&follower.node_id, follower.advertise_addr, request)
                .await
                .ok()?;
            let InternalMessage::FenceOk(ok) = answer else {
                return None;
            };
            let storage = identity.storage?;
            let incarnation = match identity.incarnation {
                Incarnation::At(at) => at,
                _ => return None,
            };
            answers.push((follower.node_id.clone(), ok, storage, incarnation));
        }
        let furthest = answers
            .iter()
            .map(|(_, ok, _, _)| (ok.last_generation, ok.log_end))
            .max()
            .unwrap_or_default();
        reclaimable &= furthest <= own;
        // The shard's own log is what placement reads; a counter log only
        // decides whether the copy can be reclaimed.
        if report.is_none() {
            report = Some(given_up_report(
                key,
                route.generation,
                local_node_id,
                tail,
                own,
                furthest,
                &answers,
            ));
        }
    }
    let report = report?.at_incarnation(crate::incarnation::of(broker, key));
    Some(Probe {
        report,
        reclaimable,
    })
}

fn given_up_report(
    key: &ShardKey,
    generation: u64,
    local_node_id: &str,
    tail: u64,
    own: (u64, u64),
    furthest: (u64, u64),
    answers: &[(String, felix_wire::internal::FenceOk, StorageId, u64)],
) -> ShardReport {
    let caught_up = if furthest > own {
        answers
            .iter()
            .filter(|(_, ok, _, _)| (ok.last_generation, ok.log_end) == furthest)
            .map(|(node, ..)| node.clone())
            .collect()
    } else {
        Vec::new()
    };
    let mut offsets: Vec<(String, u64)> = answers
        .iter()
        .map(|(node, ok, _, _)| (node.clone(), ok.log_end))
        .collect();
    offsets.push((local_node_id.to_string(), tail));
    ShardReport {
        key: key.clone(),
        generation,
        caught_up,
        offsets,
        tail,
        drained: false,
        halted: Vec::new(),
        counter_offsets: Vec::new(),
        incarnation: 0,
        by_round: false,
        storage_ids: answers
            .iter()
            .map(|(node, _, storage, _)| (node.clone(), *storage))
            .collect(),
        node_incarnations: answers
            .iter()
            .map(|(node, _, _, incarnation)| (node.clone(), Some(*incarnation)))
            .collect(),
        given_up: true,
    }
}

#[cfg(test)]
mod tests;
