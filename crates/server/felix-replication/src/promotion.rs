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
//! Only when every replica offered the capabilities. Otherwise the shard
//! opens as it always has, on the lease, and a mixed fleet behaves exactly as
//! before. See "Fencing a promotion" in `docs/replication-design.md`.

use std::sync::Arc;

use felix_broker::Broker;
use felix_broker::replication::{self, Divergence};
use felix_router::{Route, ShardKey, ShardKind};
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

/// What a replica must have offered for the shard to be fenced.
pub const REQUIRED: PeerCapabilities = PeerCapabilities::FENCE.union(PeerCapabilities::TAIL_FETCH);

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
    Pending(String),
}

/// Fence `route`'s replicas for the shard this broker was just promoted to
/// lead, and take the tail of any ahead of it.
///
/// Does not open the shard; the caller does, on anything but `Pending`.
///
/// Without `lease_fallback` a stream shard never opens on the lease: a
/// replica whose capabilities are unknown or lacking is one that has not
/// answered, and the shard waits for a majority that has. That is the rule
/// once the followers decide acknowledgements, since an older leader then no
/// longer stops writing when its lease lapses.
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
    // A cache shard's log opens lazily and is compacted underneath, so the
    // catch-up is built for stream shards only.
    if key.kind != ShardKind::Stream {
        return Outcome::Lease {
            lacking: "a cache shard".to_string(),
        };
    }

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
        if !offered.is_some_and(|offered| offered.contains(REQUIRED)) {
            return Outcome::Lease {
                lacking: replica.node_id.clone(),
            };
        }
    }

    let Some(log) = broker
        .shard_log(
            felix_broker::LogKind::Stream,
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
        )
        .await
    else {
        return Outcome::Pending("this broker has no log for the shard".to_string());
    };
    let shard = ShardRef {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
        generation: route.generation,
    };

    // Answers until a majority, this broker included, has taken the fence.
    // Later answers are not waited for: the model opens on the first majority.
    // A majority of the replicas and this broker, which counts itself.
    let voters = replicas.len() + 1;
    let needed = voters / 2 + 1;
    let mut answered: Vec<(String, std::net::SocketAddr, FenceOk)> = Vec::new();
    let mut asks = futures::stream::FuturesUnordered::new();
    for replica in &replicas {
        asks.push(ask_fence(
            requester,
            replica.node_id.clone(),
            replica.advertise_addr,
            shard.clone(),
        ));
    }
    while answered.len() + 1 < needed {
        let Some((node_id, addr, answer)) = asks.next().await else {
            break;
        };
        match answer {
            Ok(InternalMessage::FenceOk(ok)) => answered.push((node_id, addr, ok)),
            Ok(InternalMessage::ReplicateError(err)) if err.code == ErrorCode::FencedEpoch => {
                // A replica has taken a newer leader: this promotion is over.
                return Outcome::Pending(format!(
                    "{node_id} has accepted a newer generation: {}",
                    err.detail
                ));
            }
            Err(PeerError::Unsupported { .. }) if lease_fallback => {
                return Outcome::Lease { lacking: node_id };
            }
            Ok(InternalMessage::ForwardPublishError(err))
                if lease_fallback && err.code == ErrorCode::UnsupportedKind =>
            {
                return Outcome::Lease { lacking: node_id };
            }
            other => {
                tracing::debug!(
                    node_id = %node_id,
                    stream = %key.stream,
                    shard = key.shard,
                    answer = ?other.as_ref().map(InternalMessage::kind),
                    "a replica did not take the fence",
                );
            }
        }
    }
    drop(asks);
    if answered.len() + 1 < needed {
        return Outcome::Pending(format!(
            "{} of {} replicas took the fence, {} needed with this broker",
            answered.len(),
            replicas.len(),
            needed - 1
        ));
    }

    // The tail that wins, by the order promotion by log order uses (`Ahead`).
    let own_end = match log.tail_offset().await {
        Ok(tail) => tail,
        Err(err) => return Outcome::Pending(format!("could not read the shard's tail: {err}")),
    };
    let own = (last_generation(&log.generations(), own_end), own_end);
    let ahead = answered
        .iter()
        .filter(|(_, _, ok)| (ok.last_generation, ok.log_end) > own)
        .max_by_key(|(_, _, ok)| (ok.last_generation, ok.log_end));
    let Some((node_id, addr, best)) = ahead else {
        return Outcome::Fenced {
            caught_up_from: None,
        };
    };
    match catch_up(requester, broker, &log, key, &shard, node_id, *addr, best).await {
        Ok(()) => Outcome::Fenced {
            caught_up_from: Some(node_id.clone()),
        },
        Err(why) => Outcome::Pending(why),
    }
}

/// Drop this broker's records from `offset`, superseded by `from`'s log.
/// Storage refuses to cut below the commit offset.
async fn drop_superseded(
    broker: &Arc<Broker>,
    log: &felix_broker::StreamLog,
    key: &ShardKey,
    offset: u64,
    from: &str,
) -> Result<(), String> {
    log.truncate(offset)
        .await
        .map_err(|err| format!("could not drop this broker's records from {offset}: {err}"))?;
    if let Err(err) = broker
        .reset_replicated(
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
            offset,
        )
        .await
    {
        tracing::warn!(stream = %key.stream, shard = key.shard, error = %err,
            "dropped superseded records but could not reset the stream's tail");
    }
    metrics::record_promotion_truncated();
    tracing::warn!(
        stream = %key.stream,
        shard = key.shard,
        offset,
        from,
        "dropped this broker's records a replica's newer log superseded",
    );
    Ok(())
}

async fn ask_fence<R: PeerRequester>(
    requester: &R,
    node_id: String,
    addr: std::net::SocketAddr,
    shard: ShardRef,
) -> (
    String,
    std::net::SocketAddr,
    Result<InternalMessage, PeerError>,
) {
    let request = InternalMessage::Fence(Fence {
        correlation_id: 0,
        shard,
        log: ReplicaLog::Stream,
    });
    let answer = requester.request(&node_id, addr, request).await;
    (node_id, addr, answer)
}

/// Take `from`'s log where it is ahead of this broker's: compare from where
/// the two may disagree, drop this broker's own records past the first
/// disagreement, and append the rest.
///
/// Compared from the later of the commit offset and where this broker's last
/// generation began, as a follower compares a new leader's batches
/// (`unverified_from` in `replica.rs`): below that the records are committed,
/// or written by the generation both logs share.
#[allow(clippy::too_many_arguments)]
async fn catch_up<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    log: &felix_broker::StreamLog,
    key: &ShardKey,
    shard: &ShardRef,
    from: &str,
    addr: std::net::SocketAddr,
    target: &FenceOk,
) -> Result<(), String> {
    let tail = log.tail_offset().await.map_err(|err| err.to_string())?;
    let last_start = log
        .generations()
        .iter()
        .rev()
        .find(|epoch| epoch.start_offset < tail)
        .map_or(0, |epoch| epoch.start_offset);
    let mut next = log.commit_offset().max(last_start).min(tail);
    let compared_from = next;
    // Taken with the generations that wrote them, so this broker's fence
    // answers and the followers it ships to do not see them as newer.
    let labelled = requester
        .recorded_capabilities(from)
        .is_some_and(|offered| offered.contains(PeerCapabilities::GENERATION_LABELS));
    let mut taken_labels = labelled.then(Vec::new);
    while next < target.log_end {
        let request = InternalMessage::ReplicateFetch(ReplicateFetch {
            correlation_id: 0,
            shard: shard.clone(),
            log: ReplicaLog::Stream,
            from_offset: next,
            max_bytes: FETCH_BYTES,
            labelled,
        });
        let batch = match requester.request(from, addr, request).await {
            Ok(
                InternalMessage::ReplicateRecords(batch)
                | InternalMessage::ReplicateMarkedRecords(batch),
            ) => batch,
            other => {
                return Err(format!(
                    "reading {from}'s log from {next} failed: {:?}",
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
                drop_superseded(broker, log, key, offset, from).await?;
            }
            Err(Divergence::Gap { expected, .. }) => next = expected,
            Err(other) => return Err(format!("reading {from}'s log: {other}")),
        }
    }
    // What this broker holds past the replica's end was written under an
    // older generation than the replica's last record, and goes with the
    // rest of what the model replaces.
    let tail = log.tail_offset().await.map_err(|err| err.to_string())?;
    if tail > next {
        drop_superseded(broker, log, key, next, from).await?;
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
    if let Err(err) = broker
        .adopt_replicated(&key.tenant_id, &key.namespace, &key.stream, key.shard, tail)
        .await
    {
        tracing::warn!(stream = %key.stream, shard = key.shard, error = %err,
            "took a replica's tail but could not advance the stream's own view of it");
    }
    tracing::info!(
        stream = %key.stream,
        shard = key.shard,
        from,
        tail,
        "took a replica's log that was ahead of this broker's before serving",
    );
    Ok(())
}

#[cfg(test)]
mod tests;
