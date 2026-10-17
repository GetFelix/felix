//! A promoted leader fences a majority before it serves.
//!
//! `AnswerFence` and `OpenForWrites` in `docs/formal/FelixShard.tla`, under
//! `FenceOnPromote`. The new leader has already persisted its own generation
//! when it opened the shard; here it asks every replica to take it too. Once a
//! majority, itself included, has, no older leader can find a majority for
//! anything, because every majority it could reach includes a replica that
//! now refuses it. Before opening for writes the leader takes the log of the
//! answer furthest ahead by (last generation, length), if that is ahead of its
//! own: whatever a majority held when it answered is in that log.
//!
//! A cache shard is fenced the same way, its cache log first and then its
//! counter log, each taking the log furthest ahead among the answers.
//!
//! A stream shard's dead-letter and group cursor logs are fenced beside its
//! records, both in one round, and taken together from the one replica
//! whose pair is furthest ahead: a cursor is only good with the dead letters
//! its leader had written before it. See [`fence_groups`].
//!
//! Only when every replica offered the capabilities. Otherwise the shard
//! opens as it always has, on the lease, and a mixed fleet behaves exactly as
//! before. See "Fencing a promotion" in `docs/replication-design.md`.
//!
//! A failover's assignment names the storage identity each replica had in
//! the report it was decided on ([`Route::fence_storage_ids`]). An answer
//! counts only from that copy, by the identity its connection's handshake
//! gave: a replica wiped or restarted after a crash under a new identity may
//! have lost what it answered for before (#1335). `FenceCounts` in the model.

use std::collections::BTreeMap;
use std::sync::Arc;

use felix_broker::replication::{self, Divergence};
use felix_broker::{Broker, LogKind};
use felix_router::{Route, ShardKey, ShardKind};
use felix_storage::StorageId;
use felix_wire::internal::{
    ErrorCode, Fence, FenceOk, InternalMessage, PeerCapabilities, ReplicaLog, ReplicateFetch,
    ShardRef,
};
use futures::StreamExt;

use crate::metrics;
use crate::peer::{PeerError, PeerRequester};
use crate::replica::last_generation;

/// How much one catch-up read asks a replica for.
const FETCH_BYTES: u32 = 1024 * 1024;

/// What a replica must have offered for a stream shard to be fenced.
pub const REQUIRED: PeerCapabilities = PeerCapabilities::FENCE.union(PeerCapabilities::TAIL_FETCH);

/// What a replica must have offered for a cache shard to be fenced: its
/// counter log is fenced and read as well.
pub const REQUIRED_FOR_CACHES: PeerCapabilities = REQUIRED.union(PeerCapabilities::CACHE_FENCE);

/// Where a promoted shard waits before it serves, as the broker keeps it.
#[async_trait::async_trait]
pub trait PromotionGate: Send + Sync {
    /// The generation `key` is waiting at to be opened for writes, if it is.
    fn awaiting(&self, key: &crate::ShardKey) -> Option<u64>;
    /// Open `key` for writes at `generation`.
    ///
    /// Once the fleet has finalized `generation_start`, the broker writes its
    /// generation-start record first. False when the shard stays closed and
    /// waits for another fence.
    async fn open(&self, key: &crate::ShardKey, generation: u64) -> bool;
}

/// A gate nothing waits at, for a broker that does not fence.
pub struct NoGate;

#[async_trait::async_trait]
impl PromotionGate for NoGate {
    fn awaiting(&self, _key: &crate::ShardKey) -> Option<u64> {
        None
    }
    async fn open(&self, _key: &crate::ShardKey, _generation: u64) -> bool {
        true
    }
}

/// How a promoted shard came to open, or why it has not yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// A majority took the fence. `caught_up_from` names the replica whose
    /// log was ahead and was taken.
    Fenced { caught_up_from: Option<String> },
    /// Some replica did not offer the fence, so the shard opens on the lease.
    Lease { lacking: String },
    /// Not yet: no majority answered, or the catch-up did not finish. The
    /// shard stays closed and the next pass tries again.
    Pending {
        why: String,
        /// The replicas that took the fence in this attempt, on the log it
        /// stopped at. What an operator reads to see who is missing.
        took: Vec<String>,
    },
}

impl Outcome {
    fn pending(why: impl Into<String>, took: Vec<String>) -> Self {
        Outcome::Pending {
            why: why.into(),
            took,
        }
    }
}

/// Fence `route`'s replicas for the shard this broker was just promoted to
/// lead, and take the tail of any ahead of it.
///
/// Does not open the shard; the caller does, on anything but `Pending`.
///
/// Without `lease_fallback` the shard never opens on the lease: a replica
/// whose capabilities are unknown or lacking is one that has not answered,
/// and the shard waits for a majority that has. That is the rule once the
/// followers decide acknowledgements, since an older leader then no longer
/// stops writing when its lease lapses.
pub async fn fence_shard<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    local_node_id: &str,
    key: &ShardKey,
    route: &Route,
    lease_fallback: bool,
) -> Outcome {
    let replicas: Vec<_> = route
        .replicas
        .iter()
        .filter(|replica| replica.node_id != local_node_id)
        .collect();
    // The shard's own log first: its fence is what refuses the old leader on
    // every log the shard has. A cache's counters are fenced after it, for
    // where each replica's counter log ends.
    let (required, logs): (_, &[LogKind]) = match key.kind {
        ShardKind::Stream => (REQUIRED, &[LogKind::Stream]),
        ShardKind::Cache => (REQUIRED_FOR_CACHES, &[LogKind::Cache, LogKind::Counters]),
    };

    // Every replica, not just a majority. A fenced majority keeps an older
    // leader out whoever the rest are, but what the fence is for later --
    // acknowledging on the followers alone -- counts every follower's promise.
    for replica in replicas.iter().filter(|_| lease_fallback) {
        let offered = match requester.recorded_capabilities(&replica.node_id) {
            Some(offered) => Some(offered),
            None => requester
                .capabilities(&replica.node_id, replica.advertise_addr)
                .await
                .ok(),
        };
        if !offered.is_some_and(|offered| offered.contains(required)) {
            return Outcome::Lease {
                lacking: replica.node_id.clone(),
            };
        }
    }

    let shard = crate::incarnation::shard_ref(broker, key, route.generation);
    let reported_ids = route.fence_storage_ids.as_deref();
    let own_logs = async {
        let mut caught_up_from = None;
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
                // A log that should be here and could not be opened is not a
                // shard with nothing to fence. Opening it anyway would serve
                // unfenced where the lease no longer stops the old leader.
                let lacking = match kind {
                    LogKind::Stream => {
                        return Err(Outcome::pending(
                            "this broker has no log for the shard",
                            Vec::new(),
                        ));
                    }
                    // A cache kept in memory replicates nothing, so there is no
                    // log to fence and none a pass could acknowledge on.
                    LogKind::Cache if !broker.cache().has_log() => {
                        return Err(Outcome::Lease {
                            lacking: "a cache log on this broker".to_string(),
                        });
                    }
                    LogKind::Cache => "this broker could not open the shard's cache log",
                    // No counter store here, so no counters to take.
                    LogKind::Counters if broker.counters().is_none() => continue,
                    _ => "this broker could not open the shard's counter log",
                };
                if lease_fallback {
                    return Err(Outcome::Lease {
                        lacking: lacking.to_string(),
                    });
                }
                return Err(Outcome::pending(lacking, Vec::new()));
            };
            let fenced = fence_log(
                requester,
                broker,
                key,
                &shard,
                &replicas,
                kind,
                &log,
                lease_fallback,
                reported_ids,
            )
            .await;
            caught_up_from = caught_up_from.or(fenced?);
        }
        Ok(caught_up_from)
    };
    // The group logs' round goes out beside the records' rather than after
    // it, so a promotion waits on one round of fences. Each group log keeps
    // the promise on its own log, so it refuses the old leader there whichever
    // fence lands first.
    let (own, groups) = match key.kind {
        ShardKind::Stream => {
            futures::join!(
                own_logs,
                fence_groups(requester, broker, key, &shard, &replicas, reported_ids)
            )
        }
        ShardKind::Cache => (own_logs.await, Ok(None)),
    };
    let caught_up_from = match own {
        Ok(from) => from,
        Err(outcome) => return outcome,
    };
    // Taken only once the records are: a shard that stays closed takes
    // nothing.
    let groups = match groups {
        Ok(Some(round)) => take_groups(requester, broker, key, &shard, round).await,
        Ok(None) => Ok(None),
        Err(outcome) => Err(outcome),
    };
    match groups {
        Ok(from) => Outcome::Fenced {
            caught_up_from: caught_up_from.or(from),
        },
        Err(outcome) => outcome,
    }
}

/// Fence one of the shard's logs on a majority, and take the log of the
/// answer furthest ahead if it is ahead of this broker's. `Ok` names the
/// replica it was taken from, if one was; `Err` is the outcome to stop at.
#[allow(clippy::too_many_arguments)]
async fn fence_log<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    key: &ShardKey,
    shard: &ShardRef,
    replicas: &[&felix_router::NodeRef],
    kind: LogKind,
    log: &felix_broker::StreamLog,
    lease_fallback: bool,
    reported_ids: Option<&BTreeMap<String, String>>,
) -> Result<Option<String>, Outcome> {
    // Answers until a majority, this broker included, has taken the fence.
    // Later answers are not waited for: the model opens on the first majority.
    // A majority of the replicas and this broker, which counts itself.
    let voters = replicas.len() + 1;
    let needed = voters / 2 + 1;
    let mut answered: Vec<(String, std::net::SocketAddr, FenceOk)> = Vec::new();
    let took = |answered: &[(String, std::net::SocketAddr, FenceOk)]| {
        answered.iter().map(|(node, _, _)| node.clone()).collect()
    };
    // Answers from a copy the report did not name: they took the promise,
    // which still refuses the old leader, but count for nothing here.
    let mut other_copies = 0;
    let mut asks = futures::stream::FuturesUnordered::new();
    for replica in replicas {
        asks.push(ask_fence(
            requester,
            replica.node_id.clone(),
            replica.advertise_addr,
            shard.clone(),
            crate::ship::replica_log(kind),
            reported_ids.is_some(),
        ));
    }
    while answered.len() + 1 < needed {
        let Some((node_id, addr, answer, identity)) = asks.next().await else {
            break;
        };
        match answer {
            Ok(InternalMessage::FenceOk(_)) if !reported_copy(reported_ids, &node_id, identity) => {
                other_copies += 1;
                uncounted(key, kind, &node_id, reported_ids, identity);
            }
            Ok(InternalMessage::FenceOk(ok)) => answered.push((node_id, addr, ok)),
            Ok(InternalMessage::ReplicateError(err)) if err.code == ErrorCode::FencedEpoch => {
                // A replica has taken a newer leader: this promotion is over.
                return Err(Outcome::pending(
                    format!("{node_id} has accepted a newer generation: {}", err.detail),
                    took(&answered),
                ));
            }
            Err(PeerError::Unsupported { .. }) if lease_fallback => {
                return Err(Outcome::Lease { lacking: node_id });
            }
            Ok(InternalMessage::ForwardPublishError(err))
                if lease_fallback && err.code == ErrorCode::UnsupportedKind =>
            {
                return Err(Outcome::Lease { lacking: node_id });
            }
            other => {
                tracing::debug!(
                    node_id = %node_id,
                    stream = %key.stream,
                    shard = key.shard,
                    log = ?kind,
                    answer = ?other.as_ref().map(InternalMessage::kind),
                    "a replica did not take the fence",
                );
            }
        }
    }
    drop(asks);
    if answered.len() + 1 < needed {
        let mut why = format!(
            "{} of {} replicas took the fence on the {kind:?} log, {} needed with this broker",
            answered.len(),
            replicas.len(),
            needed - 1
        );
        if other_copies > 0 {
            why.push_str(&format!(
                "; {other_copies} answered from another copy than the failover's report named"
            ));
        }
        return Err(Outcome::pending(why, took(&answered)));
    }

    // The tail that wins, by the order promotion by log order uses (`Ahead`).
    let own_end = log.tail_offset().await.map_err(|err| {
        Outcome::pending(
            format!("could not read the shard's tail: {err}"),
            took(&answered),
        )
    })?;
    let own = (last_generation(&log.generations(), own_end), own_end);
    let ahead = answered
        .iter()
        .filter(|(_, _, ok)| (ok.last_generation, ok.log_end) > own)
        .max_by_key(|(_, _, ok)| (ok.last_generation, ok.log_end));
    let Some((node_id, addr, best)) = ahead else {
        return Ok(None);
    };
    let from = Source {
        node_id,
        addr: *addr,
        target: best,
    };
    catch_up(requester, broker, log, kind, key, shard, from)
        .await
        .map_err(|why| Outcome::pending(why, took(&answered)))?;
    Ok(Some(node_id.clone()))
}

/// A majority's answers to a stream shard's group-log fence, and this
/// broker's own two logs.
struct GroupRound {
    dead_letters: felix_broker::StreamLog,
    cursors: felix_broker::StreamLog,
    /// Each replica that took both fences, with its dead-letter answer and
    /// then its cursor answer.
    answered: Vec<(String, std::net::SocketAddr, FenceOk, FenceOk)>,
}

/// Fence a stream shard's dead-letter and cursor logs on a majority, a
/// replica counting only once it has taken both.
///
/// `Ok(None)` when there is nothing to take: no consumer groups here, or too
/// few replicas took both fences, because they predate `GROUP_FENCE` or did
/// not answer. This broker then serves the pair it holds. That pair is
/// consistent, since a follower gets dead letters before the cursors that
/// pass them, so serving it may redeliver but never skips a record; and the
/// records' fence already refuses the old leader on both logs. So no group
/// log can keep a shard closed, except a newer leader's answer.
async fn fence_groups<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    key: &ShardKey,
    shard: &ShardRef,
    replicas: &[&felix_router::NodeRef],
    reported_ids: Option<&BTreeMap<String, String>>,
) -> Result<Option<GroupRound>, Outcome> {
    if broker.group_reader().is_none() {
        return Ok(None);
    }
    let expected = crate::incarnation::expected(broker, key.kind, shard);
    let open = |kind| {
        broker.shard_log_for(
            kind,
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
            None,
            expected,
        )
    };
    let (Some(dead_letters), Some(cursors)) = (
        open(LogKind::GroupDeadLetters).await,
        open(LogKind::GroupCursors).await,
    ) else {
        return Err(Outcome::pending(
            "this broker could not open the shard's group logs",
            Vec::new(),
        ));
    };
    let needed = crate::quorum::majority_of(replicas.len());
    let mut asks = futures::stream::FuturesUnordered::new();
    for replica in replicas {
        let (node_id, addr) = (replica.node_id.clone(), replica.advertise_addr);
        asks.push(async move {
            let (dead_letters, cursors) = futures::join!(
                ask_fence(
                    requester,
                    node_id.clone(),
                    addr,
                    shard.clone(),
                    ReplicaLog::GroupDeadLetters,
                    reported_ids.is_some(),
                ),
                ask_fence(
                    requester,
                    node_id.clone(),
                    addr,
                    shard.clone(),
                    ReplicaLog::GroupCursors,
                    reported_ids.is_some(),
                ),
            );
            // Both answers from the copy the report named, or neither counts.
            let copy = reported_copy(reported_ids, &node_id, dead_letters.3)
                && reported_copy(reported_ids, &node_id, cursors.3);
            (node_id, addr, dead_letters.2, cursors.2, copy)
        });
    }
    let mut answered = Vec::new();
    let took = |answered: &[(String, std::net::SocketAddr, FenceOk, FenceOk)]| {
        answered.iter().map(|(node, ..)| node.clone()).collect()
    };
    while answered.len() + 1 < needed {
        let Some((node_id, addr, dead, cursor, copy)) = asks.next().await else {
            break;
        };
        if let Some(detail) = newer_leader(&dead).or(newer_leader(&cursor)) {
            return Err(Outcome::pending(
                format!("{node_id} has accepted a newer generation: {detail}"),
                took(&answered),
            ));
        }
        match (dead, cursor) {
            (Ok(InternalMessage::FenceOk(_)), Ok(InternalMessage::FenceOk(_))) if !copy => {
                tracing::info!(
                    node_id = %node_id,
                    stream = %key.stream,
                    shard = key.shard,
                    "not counting a group logs' fence answer from another copy than the \
                     failover's report named",
                );
            }
            (Ok(InternalMessage::FenceOk(dead)), Ok(InternalMessage::FenceOk(cursor))) => {
                answered.push((node_id, addr, dead, cursor));
            }
            (dead, cursor) => tracing::debug!(
                node_id = %node_id,
                stream = %key.stream,
                shard = key.shard,
                dead_letters = ?dead.as_ref().map(InternalMessage::kind),
                cursors = ?cursor.as_ref().map(InternalMessage::kind),
                "a replica did not take the group logs' fence",
            ),
        }
    }
    if answered.len() + 1 < needed {
        tracing::warn!(
            stream = %key.stream,
            shard = key.shard,
            took = answered.len(),
            needed = needed - 1,
            "too few replicas took the group logs' fence; serving this broker's own",
        );
        return Ok(None);
    }
    Ok(Some(GroupRound {
        dead_letters,
        cursors,
        answered,
    }))
}

/// What a fence answer from a replica that took a newer leader says.
fn newer_leader(answer: &Result<InternalMessage, PeerError>) -> Option<&str> {
    match answer {
        Ok(InternalMessage::ReplicateError(err)) if err.code == ErrorCode::FencedEpoch => {
            Some(&err.detail)
        }
        _ => None,
    }
}

/// Take the dead-letter log from the answer whose pair is furthest ahead, if
/// it is ahead of this broker's, ranked by dead letters first since a lost
/// dead letter is a skipped record and a lost cursor only a redelivery; and
/// that replica's cursors, each group going to the further of its two
/// positions.
///
/// The cursors need not match the dead-letter log beside them: each cursor
/// record says which dead letters it needs, and the shard's cursors are
/// checked against the dead-letter log it ends up with before it is served
/// (`GroupReader`). So this broker's own further cursors are kept as long as
/// they still stand, and taking the replica's appends rather than replaces,
/// so no group is erased.
async fn take_groups<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    key: &ShardKey,
    shard: &ShardRef,
    round: GroupRound,
) -> Result<Option<String>, Outcome> {
    let GroupRound {
        dead_letters,
        cursors,
        answered,
    } = round;
    let took: Vec<String> = answered.iter().map(|(node, ..)| node.clone()).collect();
    let pending = |why: String| Outcome::pending(why, took.clone());
    let position = |log: &felix_broker::StreamLog| {
        let log = log.clone();
        async move {
            let end = log
                .tail_offset()
                .await
                .map_err(|err| format!("could not read the tail of a group log: {err}"))?;
            Ok::<_, String>((last_generation(&log.generations(), end), end))
        }
    };
    let own = (
        position(&dead_letters).await.map_err(pending)?,
        position(&cursors).await.map_err(pending)?,
    );
    let of = |ok: &FenceOk| (ok.last_generation, ok.log_end);
    let best = answered
        .iter()
        .max_by_key(|(_, _, dead, cursor)| (of(dead), of(cursor)))
        .filter(|(_, _, dead, cursor)| (of(dead), of(cursor)) > own);
    let Some((node_id, addr, dead, cursor)) = best else {
        return Ok(None);
    };
    let from = |target| Source {
        node_id,
        addr: *addr,
        target,
    };
    if of(dead) > own.0 {
        catch_up(
            requester,
            broker,
            &dead_letters,
            LogKind::GroupDeadLetters,
            key,
            shard,
            from(dead),
        )
        .await
        .map_err(pending)?;
    }
    if of(cursor) != own.1 {
        let groups = broker
            .consumer_groups()
            .ok_or_else(|| pending("this broker keeps no consumer groups".to_string()))?;
        let records = read_all(
            requester,
            node_id,
            *addr,
            shard,
            LogKind::GroupCursors,
            cursor.log_end,
        )
        .await
        .map_err(pending)?;
        groups
            .adopt(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                &records,
            )
            .await
            .map_err(|err| pending(format!("could not take {node_id}'s cursors: {err}")))?;
    }
    if let Some(reader) = broker.group_reader() {
        reader.invalidate(&key.tenant_id, &key.namespace, &key.stream, key.shard);
    }
    Ok(Some(node_id.clone()))
}

/// Every record of `node_id`'s `kind` log below `end`, from where its log
/// begins.
async fn read_all<R: PeerRequester>(
    requester: &R,
    node_id: &str,
    addr: std::net::SocketAddr,
    shard: &ShardRef,
    kind: LogKind,
    end: u64,
) -> Result<Vec<bytes::Bytes>, String> {
    let mut records = Vec::new();
    let mut next = 0;
    while next < end {
        let request = InternalMessage::ReplicateFetch(ReplicateFetch {
            correlation_id: 0,
            shard: shard.clone(),
            log: crate::ship::replica_log(kind),
            from_offset: next,
            max_bytes: FETCH_BYTES,
            labelled: false,
            timed: false,
        });
        let batch = match requester.request(node_id, addr, request).await {
            Ok(
                InternalMessage::ReplicateGroupRecords(batch)
                | InternalMessage::ReplicateRecords(batch)
                | InternalMessage::ReplicateMarkedRecords(batch),
            ) => batch,
            other => {
                return Err(format!(
                    "reading {node_id}'s {kind:?} log from {next} failed: {:?}",
                    other.map(|answer| answer.kind())
                ));
            }
        };
        if batch.payloads.is_empty() {
            break;
        }
        next = batch.first_offset + batch.payloads.len() as u64;
        records.extend(batch.payloads);
    }
    Ok(records)
}

/// The replica whose log a promoted leader takes, and how far it reaches.
struct Source<'a> {
    node_id: &'a str,
    addr: std::net::SocketAddr,
    target: &'a FenceOk,
}

/// Drop this broker's records from `offset`, superseded by `from`'s log.
/// Storage refuses to cut below the commit offset.
async fn drop_superseded(
    broker: &Arc<Broker>,
    log: &felix_broker::StreamLog,
    kind: LogKind,
    key: &ShardKey,
    offset: u64,
    from: &str,
) -> Result<(), String> {
    log.truncate(offset)
        .await
        .map_err(|err| format!("could not drop this broker's records from {offset}: {err}"))?;
    if let Err(err) = broker
        .reset_log(
            kind,
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
            offset,
        )
        .await
    {
        tracing::warn!(stream = %key.stream, shard = key.shard, log = ?kind, error = %err,
            "dropped superseded records but could not reset what was read from them");
    }
    metrics::record_promotion_truncated();
    tracing::warn!(
        stream = %key.stream,
        shard = key.shard,
        log = ?kind,
        offset,
        from,
        "dropped this broker's records a replica's newer log superseded",
    );
    Ok(())
}

/// A replica's answer to a fence, and with `identified` the storage identity
/// the connection it came over gave in its handshake.
async fn ask_fence<R: PeerRequester>(
    requester: &R,
    node_id: String,
    addr: std::net::SocketAddr,
    shard: ShardRef,
    log: ReplicaLog,
    identified: bool,
) -> (
    String,
    std::net::SocketAddr,
    Result<InternalMessage, PeerError>,
    Option<StorageId>,
) {
    let request = InternalMessage::Fence(Fence {
        correlation_id: 0,
        shard,
        log,
    });
    if !identified {
        let answer = requester.request(&node_id, addr, request).await;
        return (node_id, addr, answer, None);
    }
    match requester.request_identified(&node_id, addr, request).await {
        Ok((answer, identity)) => (node_id, addr, Ok(answer), identity.storage),
        Err(err) => (node_id, addr, Err(err), None),
    }
}

/// Whether an answer from `node_id`, over a connection whose handshake gave
/// `identity`, may count toward the fence's majority. Every answer may when
/// the assignment names no identities.
///
/// A replica the assignment names counts only from that identity: under
/// another, or with none, it cannot show it still holds what it answered
/// for. Placement names every member of the old set it can: by the report,
/// or with an empty identity, which nothing matches, for one the report could
/// not vouch for (a member its leader had not asked yet). A replica it does
/// not name gave the old leader nothing to count, or predates identities,
/// and counts as it always has. The fence's majority both refuses the old
/// leader and holds what it acknowledged, so a promise alone cannot stand in
/// for an answer.
fn reported_copy(
    expected: Option<&BTreeMap<String, String>>,
    node_id: &str,
    identity: Option<StorageId>,
) -> bool {
    let Some(expected) = expected else {
        return true;
    };
    match (expected.get(node_id), identity) {
        (None, _) => true,
        (Some(named), Some(identity)) => {
            !named.is_empty() && named.eq_ignore_ascii_case(&identity.to_hex())
        }
        (Some(_), None) => false,
    }
}

fn uncounted(
    key: &ShardKey,
    kind: LogKind,
    node_id: &str,
    expected: Option<&BTreeMap<String, String>>,
    identity: Option<StorageId>,
) {
    tracing::warn!(
        node_id,
        stream = %key.stream,
        shard = key.shard,
        log = ?kind,
        named = expected.and_then(|ids| ids.get(node_id)).map(String::as_str),
        answered_from = identity.map(|id| id.to_hex()),
        "not counting a fence answer from another copy than the failover's report named: \
         it may have lost records it held before",
    );
}

/// Take `from`'s copy of the `kind` log where it is ahead of this broker's:
/// compare from where the two may disagree, drop this broker's own records
/// past the first disagreement, and append the rest.
///
/// Compared from the later of the commit offset and where this broker's last
/// generation began, as a follower compares a new leader's batches
/// (`unverified_from` in `replica.rs`): below that the records are committed,
/// or written by the generation both logs share.
async fn catch_up<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    log: &felix_broker::StreamLog,
    kind: LogKind,
    key: &ShardKey,
    shard: &ShardRef,
    from: Source<'_>,
) -> Result<(), String> {
    let Source {
        node_id: from,
        addr,
        target,
    } = from;
    let tail = log.tail_offset().await.map_err(|err| err.to_string())?;
    let last_start = log
        .generations()
        .iter()
        .rev()
        .find(|epoch| epoch.start_offset < tail)
        .map_or(0, |epoch| epoch.start_offset);
    // Never past the replica's end: a log taken from a replica behind this
    // one, as a pair's cursor log can be, loses what this broker held past it.
    let mut next = log
        .commit_offset()
        .max(last_start)
        .min(tail)
        .min(target.log_end);
    let compared_from = next;
    // Taken with the generations that wrote them, so this broker's fence
    // answers and the followers it ships to do not see them as newer.
    let offered = requester.recorded_capabilities(from);
    let labelled =
        offered.is_some_and(|offered| offered.contains(PeerCapabilities::GENERATION_LABELS));
    // And with the times its leader stored, so this broker reports them once
    // it serves.
    let timed =
        labelled && offered.is_some_and(|offered| offered.contains(PeerCapabilities::RECORD_TIMES));
    let mut taken_labels = labelled.then(Vec::new);
    while next < target.log_end {
        let request = InternalMessage::ReplicateFetch(ReplicateFetch {
            correlation_id: 0,
            shard: shard.clone(),
            log: crate::ship::replica_log(kind),
            from_offset: next,
            max_bytes: FETCH_BYTES,
            labelled,
            timed,
        });
        let batch = match requester.request(from, addr, request).await {
            Ok(
                InternalMessage::ReplicateRecords(batch)
                | InternalMessage::ReplicateMarkedRecords(batch)
                | InternalMessage::ReplicateCacheRecords(batch)
                | InternalMessage::ReplicateCounterRecords(batch)
                | InternalMessage::ReplicateGroupRecords(batch)
                | InternalMessage::ReplicateDeadLetterRecords(batch),
            ) => batch,
            other => {
                return Err(format!(
                    "reading {from}'s {kind:?} log from {next} failed: {:?}",
                    other.map(|answer| answer.kind())
                ));
            }
        };
        if batch.payloads.is_empty() {
            // It holds less than it said: a batch from its old leader it was
            // storing as it answered was refused and never landed. What it
            // does hold has been taken.
            break;
        }
        let applied = replication::apply(
            log,
            batch.first_offset,
            batch.checksum,
            &batch.payloads,
            &batch.marks,
            &batch.publishers,
            batch.times.as_deref().unwrap_or_default(),
        )
        .await
        .map_err(|err| err.to_string())?;
        match (&mut taken_labels, batch.generations.as_deref()) {
            (Some(taken), Some(generations)) => taken.extend_from_slice(generations),
            _ => taken_labels = None,
        }
        match applied {
            Ok(applied) => {
                let level = log
                    .tail_offset()
                    .await
                    .is_ok_and(|tail| applied.durable_offset >= tail);
                if let Some(generations) = batch.generations.as_deref()
                    && level
                {
                    let start = applied.durable_offset - applied.appended as u64;
                    crate::replica::label_appended(
                        log,
                        start,
                        applied.durable_offset,
                        shard.generation,
                        Some(generations),
                    )
                    .map_err(|err| format!("could not label the records from {start}: {err}"))?;
                }
                next = applied.durable_offset;
            }
            // This broker's own records from `offset` are a generation the
            // replica's log superseded. The model replaces the whole log; here
            // only the part that differs goes.
            Err(Divergence::Conflict { offset, .. }) => {
                drop_superseded(broker, log, kind, key, offset, from).await?;
            }
            Err(Divergence::Gap { expected, .. }) => next = expected,
            Err(other) => return Err(format!("reading {from}'s {kind:?} log: {other}")),
        }
    }
    // What this broker holds past the replica's end was written under an
    // older generation than the replica's last record, and goes with the
    // rest of what the model replaces.
    let tail = log.tail_offset().await.map_err(|err| err.to_string())?;
    if tail > next {
        drop_superseded(broker, log, kind, key, next, from).await?;
    }
    let tail = log.tail_offset().await.map_err(|err| err.to_string())?;
    // A batch that only matched records this broker held past it was not
    // labelled then: those records kept this broker's own labels. With the
    // log now ending where the replica's does, its labels go over all of it,
    // as the model's log' = log[f] carries them.
    if let Some(taken) = taken_labels.filter(|taken| !taken.is_empty()) {
        crate::replica::label_appended(log, compared_from, tail, shard.generation, Some(&taken))
            .map_err(|err| format!("could not label the records from {compared_from}: {err}"))?;
    }
    // A cache's index, a counter shard's sums and the group logs' entries
    // catch up with the tail on their next read; a stream's commit order has
    // to be told.
    if kind == LogKind::Stream
        && let Err(err) = broker
            .adopt_replicated(&key.tenant_id, &key.namespace, &key.stream, key.shard, tail)
            .await
    {
        tracing::warn!(stream = %key.stream, shard = key.shard, error = %err,
            "took a replica's tail but could not advance the stream's own view of it");
    }
    tracing::info!(
        stream = %key.stream,
        shard = key.shard,
        log = ?kind,
        from,
        tail,
        "took a replica's log that was ahead of this broker's before serving",
    );
    Ok(())
}

#[cfg(test)]
mod tests;
