//! Confirming, for a read, that this broker still leads a shard: one round of
//! fences at its own generation, read-index style, instead of the lease.
//!
//! `ConfirmRead` and `EndRead` in `docs/formal/FelixShardReads.tla`. A newer
//! leader fences a majority before it serves, and a write it acknowledges is
//! held by a majority that accepted its generation. Either majority shares a
//! replica with any majority that answered this broker's fence at the old
//! generation, and that replica would have refused. So a round a majority
//! answered, started after the read took its value, proves no newer leader
//! acknowledged anything before the read began. See "Reads without the lease"
//! in `docs/replication-design.md`.
//!
//! The round is the promotion fence sent at the generation the replica
//! already accepted, which it takes without writing anything. Concurrent
//! reads of a shard share rounds: a read waits for one that started after it
//! arrived, so at most one round per shard is in flight and one more queued.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use felix_router::ShardRouter;
use felix_wire::internal::{Fence, InternalMessage, ReplicaLog, ShardRef};
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::sync::watch;

use crate::peer::PeerRequester;
use crate::quorum::{LeadershipCheck, QuorumError};
use crate::{ShardKey, ShardKind};

/// Confirms leadership for reads over the peer transport.
pub struct ReadIndex<R> {
    inner: Arc<Inner<R>>,
}

struct Inner<R> {
    requester: R,
    broker: Arc<Broker>,
    router: Arc<ShardRouter>,
    /// How long one round waits for a majority.
    timeout: Duration,
    rounds: Mutex<HashMap<(ShardKey, u64), Arc<Rounds>>>,
}

/// The rounds of one shard at one generation.
struct Rounds {
    state: Mutex<RoundState>,
    finished: watch::Sender<Finished>,
}

#[derive(Default)]
struct RoundState {
    /// The number the next round started gets. Rounds are numbered from 1.
    next: u64,
    running: bool,
    /// A read arrived while a round was running, so another starts after it.
    queued: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct Finished {
    /// The last round that finished.
    through: u64,
    /// The last round a majority answered.
    confirmed: u64,
}

impl<R: PeerRequester + Send + Sync + 'static> ReadIndex<R> {
    pub fn new(
        requester: R,
        broker: Arc<Broker>,
        router: Arc<ShardRouter>,
        timeout: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                requester,
                broker,
                router,
                timeout,
                rounds: Mutex::new(HashMap::new()),
            }),
        }
    }
}

#[async_trait::async_trait]
impl<R: PeerRequester + Send + Sync + 'static> LeadershipCheck for ReadIndex<R> {
    async fn confirm(&self, key: &ShardKey, generation: u64) -> Result<(), QuorumError> {
        let rounds = Arc::clone(
            self.inner
                .rounds
                .lock()
                .entry((key.clone(), generation))
                .or_insert_with(|| {
                    Arc::new(Rounds {
                        state: Mutex::new(RoundState {
                            next: 1,
                            ..RoundState::default()
                        }),
                        finished: watch::Sender::new(Finished::default()),
                    })
                }),
        );
        // A round already running may have sent to a replica before this
        // read took its value, so it cannot vouch for the read: wait for the
        // next one.
        let (needed, mut finished) = {
            let mut state = rounds.state.lock();
            let needed = state.next;
            if state.running {
                state.queued = true;
            } else {
                state.running = true;
                state.next += 1;
                spawn_round(
                    Arc::clone(&self.inner),
                    Arc::clone(&rounds),
                    key.clone(),
                    generation,
                    needed,
                );
            }
            (needed, rounds.finished.subscribe())
        };
        let done = finished
            .wait_for(|finished| finished.through >= needed)
            .await
            .map(|finished| finished.confirmed >= needed)
            .unwrap_or(false);
        if done {
            Ok(())
        } else {
            crate::metrics::record_quorum(crate::metrics::QUORUM_NOT_LEADING);
            Err(QuorumError::LeadershipLost {
                what: "read",
                detail: "no majority confirmed this broker still leads",
            })
        }
    }
}

fn spawn_round<R: PeerRequester + Send + Sync + 'static>(
    inner: Arc<Inner<R>>,
    rounds: Arc<Rounds>,
    key: ShardKey,
    generation: u64,
    number: u64,
) {
    tokio::spawn(async move {
        let confirmed = tokio::time::timeout(inner.timeout, inner.round(&key, generation))
            .await
            .unwrap_or(false);
        let mut state = rounds.state.lock();
        rounds.finished.send_modify(|finished| {
            finished.through = number;
            if confirmed {
                finished.confirmed = number;
            }
        });
        if state.queued {
            state.queued = false;
            let next = state.next;
            state.next += 1;
            drop(state);
            spawn_round(inner, rounds, key, generation, next);
        } else {
            state.running = false;
            drop(state);
            // Nothing waits on this shard now; a read that still holds the
            // entry has its answer, and the next one starts a fresh set.
            let mut all = inner.rounds.lock();
            if all
                .get(&(key.clone(), generation))
                .is_some_and(|current| Arc::ptr_eq(current, &rounds))
                && !rounds.state.lock().running
            {
                all.remove(&(key, generation));
            }
        }
    });
}

impl<R: PeerRequester + Send + Sync> Inner<R> {
    /// One round: whether a majority of `key`'s replicas, this broker
    /// included, answered that no leader newer than `generation` reached it.
    async fn round(&self, key: &ShardKey, generation: u64) -> bool {
        let routing_key = felix_router::ShardKey {
            tenant_id: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.stream.clone(),
            shard: key.shard,
            kind: match key.kind {
                ShardKind::Stream => felix_router::ShardKind::Stream,
                ShardKind::Cache => felix_router::ShardKind::Cache,
            },
        };
        let local = self.router.local_node_id().to_string();
        let Some(route) = self.router.snapshot().get(&routing_key).cloned() else {
            return false;
        };
        if route.generation != generation || route.leader.node_id != local {
            return false;
        }
        let (log_kind, log) = match key.kind {
            ShardKind::Stream => (felix_broker::LogKind::Stream, ReplicaLog::Stream),
            ShardKind::Cache => (felix_broker::LogKind::Cache, ReplicaLog::Cache),
        };
        // This broker counts itself only while its own log has taken no newer
        // leader's fence, as the quorum mark does (`held_at_generation`), nor
        // for a cache, its counter log, which a newer leader reaches alone.
        let accepted = |kind| async move {
            self.broker
                .shard_log(kind, &key.tenant_id, &key.namespace, &key.stream, key.shard)
                .await
                .map(|log| log.accepted_generation())
        };
        let mut own = accepted(log_kind)
            .await
            .is_some_and(|accepted| accepted <= generation);
        if own && key.kind == ShardKind::Cache {
            own = accepted(felix_broker::LogKind::Counters)
                .await
                .is_none_or(|accepted| accepted <= generation);
        }
        // Every replica, as the promotion fence counts them, so this majority
        // shares one with any a newer leader fenced or acknowledged on.
        let replicas: Vec<_> = route
            .replicas
            .iter()
            .filter(|replica| replica.node_id != local)
            .collect();
        let needed = crate::quorum::majority_of(replicas.len());
        let mut answered = usize::from(own);
        if answered >= needed {
            return true;
        }
        let shard = ShardRef {
            tenant_id: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.stream.clone(),
            shard: key.shard,
            generation,
        };
        let mut asks: futures::stream::FuturesUnordered<_> = replicas
            .iter()
            .map(|replica| {
                self.requester.request(
                    &replica.node_id,
                    replica.advertise_addr,
                    InternalMessage::Fence(Fence {
                        correlation_id: 0,
                        shard: shard.clone(),
                        log,
                    }),
                )
            })
            .collect();
        while let Some(answer) = asks.next().await {
            // A refusal of any kind is a replica that does not vouch for this
            // generation: a newer leader's fence, or one it cannot check.
            if matches!(answer, Ok(InternalMessage::FenceOk(_))) {
                answered += 1;
                if answered >= needed {
                    return true;
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests;
