//! A leader that gave its copy up after a crash (`crate::unclean`) asks every
//! follower where its copy ends, so placement can decide what to do with the
//! shard from what the copies hold now rather than from a report made before
//! the crash (#1295).
//!
//! Each follower is sent a fence at the generation this broker led. One that
//! took that generation from it changes nothing; one still at an older
//! generation takes it, as it would this broker's batch at it. A follower not
//! heard from may hold what this broker lost, so nothing is reported while
//! its answer could still change the verdict. When none is ahead of this
//! broker's copy, nothing a client was promised is held beyond it, and
//! placement may name this broker leader again at a new generation. When one
//! is level with it or ahead, the furthest are reported caught up and
//! placement promotes one of them instead: every copy is accounted for, so
//! the furthest holds whatever any copy holds. Reclaim is only for when every
//! follower is behind.
//!
//! A follower that answers but does not vote (a learner, or a copy replaced
//! and not yet re-admitted) has no position. When the followers that do have
//! one do not meet every majority of the replicas, the report is held: it
//! names nobody caught up and the copy is not reclaimable, so placement finds
//! no replica to promote and an operator may abandon the log (#2066).

use std::sync::Arc;

use felix_broker::{Broker, LogKind};
use felix_common::membership::RefusedGeneration;
use felix_router::{Route, ShardKey, ShardKind};
use felix_storage::StorageId;
use felix_wire::internal::{Fence, InternalMessage};

use crate::peer::{Incarnation, PeerRequester};
use crate::replica::{last_generation, not_a_voter};
use crate::reporter::ShardReport;

/// What a probe found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Probe {
    pub report: ShardReport,
    /// No follower's copy is ahead of this broker's on any of the shard's logs.
    pub reclaimable: bool,
}

/// Where the followers stand on one of the shard's logs. Compared by (last
/// generation, end): an end alone can rank a copy that holds a newer
/// generation's records below one that does not.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LogPosition {
    /// The furthest follower is level with this broker or ahead.
    not_behind: bool,
    /// The followers at the furthest position among them.
    furthest: Vec<String>,
}

/// A follower's answer to the probe: its node id, where its copy ends, and
/// the identity it answered under.
type Answer = (String, felix_wire::internal::FenceOk, StorageId, u64);

/// Probe every follower `route` names for the shard this broker gave up at
/// `route.generation`. `None` when the shard has no followers, or while a
/// follower that did not answer could still change the verdict.
///
/// A follower that answers but does not vote has no position. Unless the
/// followers that do have one meet every majority of the route's set, and of
/// the set a pending change leaves, the probe is a held report: nobody is
/// named caught up and the copy is not reclaimable.
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
    // A pending change leaves a set whose majority could hold acknowledged
    // records too, as the acknowledgement path counts it (`held_by_both`).
    let mut sets: Vec<Vec<&str>> =
        vec![route.replicas.iter().map(|r| r.node_id.as_str()).collect()];
    if let Some(pending) = &route.pending {
        sets.push(pending.leaving.iter().map(String::as_str).collect());
    }
    let mut holds = true;
    let mut reclaimable = true;
    let mut report: Option<ShardReport> = None;
    let mut positions = Vec::with_capacity(logs.len());
    let mut start_holders: Option<Vec<String>> = None;
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
        let mut silent = 0;
        let mut refused: Vec<&str> = Vec::new();
        for follower in &followers {
            let request = InternalMessage::Fence(Fence {
                correlation_id: 0,
                shard: shard.clone(),
                log: crate::ship::replica_log(kind),
                election: None,
            });
            let Ok((answer, identity)) = requester
                .request_identified(&follower.node_id, follower.advertise_addr, request)
                .await
            else {
                silent += 1;
                continue;
            };
            match answer {
                InternalMessage::FenceOk(ok) => {
                    let (Some(storage), Incarnation::At(incarnation)) =
                        (identity.storage, identity.incarnation)
                    else {
                        silent += 1;
                        continue;
                    };
                    answers.push((follower.node_id.clone(), ok, storage, incarnation));
                }
                // It answered, but does not vote: its position counts for nothing.
                InternalMessage::ReplicateError(err) if not_a_voter(&err) => {
                    refused.push(follower.node_id.as_str());
                }
                _ => return None,
            }
        }
        // Silence decides nothing: wait while the silent members, counted,
        // could still let a claim hold.
        let counted: Vec<&str> = answers.iter().map(|(node, ..)| node.as_str()).collect();
        let mut possible = counted.clone();
        possible.extend(
            followers
                .iter()
                .map(|follower| follower.node_id.as_str())
                .filter(|node| !counted.contains(node) && !refused.contains(node)),
        );
        if silent > 0 && meets_every_majority(&possible, &sets) {
            return None;
        }
        holds &= meets_every_majority(&counted, &sets);
        start_holders = Some(still_holding_start(
            start_holders,
            &answers,
            route.generation,
        ));
        let furthest = answers
            .iter()
            .map(|(_, ok, _, _)| (ok.last_generation, ok.log_end))
            .max()
            .unwrap_or_default();
        reclaimable &= furthest <= own;
        positions.push(LogPosition {
            not_behind: furthest >= own,
            furthest: answers
                .iter()
                .filter(|(_, ok, _, _)| (ok.last_generation, ok.log_end) == furthest)
                .map(|(node, ..)| node.clone())
                .collect(),
        });
        // The shard's own log gives the report's positions; a counter log
        // only weighs in on who is caught up and whether the copy is
        // reclaimable.
        if report.is_none() {
            report = Some(given_up_report(
                key,
                route.generation,
                local_node_id,
                tail,
                &answers,
            ));
        }
    }
    let mut report = report?;
    // A re-admitted learner counts again only once its leader reports it
    // holding the start record. A leader that crashed before a pass said so
    // can only say it here, or the learner stays one for good.
    report.holds_start = start_holders.unwrap_or_default();
    // A refusing or silent member has no position. Unless the followers that
    // have one meet every majority, a record could sit only on those that do
    // not, so nobody is named and nothing is reclaimed: a held report.
    if holds {
        report.caught_up = caught_up(&positions);
    }
    let report = report.at_incarnation(crate::incarnation::of(broker, key));
    Some(Probe {
        report,
        reclaimable: reclaimable && holds,
    })
}

/// Whether `trusted` followers share a member with every majority of each of
/// `sets`, the leader that gave up counted in each set it belongs to.
fn meets_every_majority(trusted: &[&str], sets: &[Vec<&str>]) -> bool {
    sets.iter().all(|set| {
        let held = set.iter().filter(|node| trusted.contains(node)).count();
        2 * held >= set.len()
    })
}

/// The followers of `held`, the ones found holding `generation`'s start
/// record on every log checked before this one (`None` before the first),
/// whose answer on this log does too.
///
/// A copy whose last record is of `generation` holds its start record, the
/// generation's first. One ahead of it would have refused the fence, and one
/// that refused or was not heard from has no answer to judge.
fn still_holding_start(
    held: Option<Vec<String>>,
    answers: &[Answer],
    generation: u64,
) -> Vec<String> {
    let here = answers
        .iter()
        .filter(|(_, ok, _, _)| ok.last_generation == generation)
        .map(|(node, ..)| node.clone());
    match held {
        None => here.collect(),
        Some(held) => here.filter(|node| held.contains(node)).collect(),
    }
}

/// Who the probe names caught up, from each of the shard's logs, the
/// shard's own first.
///
/// Nobody when every follower is behind this broker on the shard's own log:
/// promoting one would lose what this broker kept, so placement reclaims.
/// Otherwise the followers furthest on every log, which hold whatever any
/// copy holds. When no follower is furthest on every log (a cache whose
/// cache and counter logs are led by different followers), the furthest on
/// the shard's own log, as before a level follower was named.
fn caught_up(positions: &[LogPosition]) -> Vec<String> {
    let Some(first) = positions.first().filter(|log| log.not_behind) else {
        return Vec::new();
    };
    let on_every_log: Vec<String> = first
        .furthest
        .iter()
        .filter(|node| positions.iter().all(|log| log.furthest.contains(node)))
        .cloned()
        .collect();
    if on_every_log.is_empty() {
        first.furthest.clone()
    } else {
        on_every_log
    }
}

/// The report for a shard this broker was named leader of and refused to open
/// at `refused.generation`, having promised it to another node or lacking the
/// set its joint fence leaves there. It measures
/// nothing: the broker never opened the shard, and placement needs only the
/// refusal. Without it the control plane, which still names this broker at
/// that generation, would wait on silence.
pub fn refusal_report(key: &ShardKey, refused: RefusedGeneration) -> ShardReport {
    ShardReport {
        key: key.clone(),
        generation: refused.generation,
        caught_up: Vec::new(),
        offsets: Vec::new(),
        tail: 0,
        drained: false,
        halted: Vec::new(),
        counter_offsets: Vec::new(),
        incarnation: 0,
        by_round: false,
        storage_ids: Vec::new(),
        node_incarnations: Vec::new(),
        given_up: false,
        inherited_storage: Vec::new(),
        joint_start_acked: None,
        holds_start: Vec::new(),
        refused: Some(refused),
        elected: None,
    }
}

/// The report on a probe, naming nobody caught up or holding the start; the
/// caller fills those in.
fn given_up_report(
    key: &ShardKey,
    generation: u64,
    local_node_id: &str,
    tail: u64,
    answers: &[Answer],
) -> ShardReport {
    let mut offsets: Vec<(String, u64)> = answers
        .iter()
        .map(|(node, ok, _, _)| (node.clone(), ok.log_end))
        .collect();
    offsets.push((local_node_id.to_string(), tail));
    ShardReport {
        key: key.clone(),
        generation,
        caught_up: Vec::new(),
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
        // Every follower answered the probe, so each is named by its answer.
        inherited_storage: Vec::new(),
        joint_start_acked: None,
        refused: None,
        holds_start: Vec::new(),
        elected: None,
    }
}

#[cfg(test)]
mod tests;
