//! The shards of in-memory streams on a broker whose durable shards run on
//! Raft.
//!
//! An in-memory stream has no log, so no Raft group runs it. Each shard has
//! one owner, the broker the control plane names leader. It serves at the
//! assigned generation while the routes name that generation and the broker
//! holds its control-plane lease; nothing is replicated, a publish is
//! acknowledged on enqueue, and a new owner starts empty.
//!
//! The routing feed hands this module every stream shard the Raft engine left
//! (`Durable::No` in `shards/raft.rs`). There is no fence round, no disk log
//! and no replication pass: the lease and the generation are the whole fence.
//! A broker that loses its lease refuses at the write fence
//! ([`ShardFence::admit`]) and its readers are ended
//! ([`ShardReaders::watch_leadership`]); the control plane places the shard
//! elsewhere only after that lease has run out. A broker that sees the shard
//! placed elsewhere closes it with `close_stream_shard`, which tells every
//! reader the generation it moved at. See `docs/hybrid-retirement.md`,
//! "In-memory shards".

use std::sync::Arc;

use felix_common::random::HashMap;

use super::lifecycle::fence::ShardFence;
use super::lifecycle::{Phase, QUIESCE_BOUND, ShardReaders, no_owner};
use super::watch::ShardAssignment;
use super::{ShardKey, ShardKind};

/// The in-memory stream shards this broker owns, and the fence they serve
/// through.
pub(crate) struct MemoryShards {
    local: String,
    broker: Arc<felix_broker::Broker>,
    /// The same fence every write path enters, so a memory shard is refused
    /// where any other shard is.
    fence: Arc<ShardFence>,
    readers: ShardReaders,
    /// Shards serving here, and what they serve at.
    served: parking_lot::Mutex<HashMap<ShardKey, Served>>,
}

impl MemoryShards {
    pub(crate) fn new(
        local: impl Into<String>,
        broker: Arc<felix_broker::Broker>,
        fence: Arc<ShardFence>,
        readers: ShardReaders,
    ) -> Self {
        Self {
            local: local.into(),
            broker,
            fence,
            readers,
            served: parking_lot::Mutex::default(),
        }
    }

    /// The shards serving here and the generation each serves at.
    pub(crate) fn servable(&self) -> HashMap<ShardKey, u64> {
        self.served
            .lock()
            .iter()
            .map(|(key, served)| (key.clone(), served.generation))
            .collect()
    }

    /// Take every stream shard out of `assignments`. Called with what the
    /// Raft engine left, which is only shards of streams registered in
    /// memory, so no durable shard reaches the hybrid lifecycle either.
    pub(crate) fn take(
        &self,
        assignments: &mut HashMap<ShardKey, ShardAssignment>,
    ) -> HashMap<ShardKey, ShardAssignment> {
        let keys: Vec<ShardKey> = assignments
            .keys()
            .filter(|key| key.kind == ShardKind::Stream)
            .cloned()
            .collect();
        let mut taken = HashMap::default();
        for key in keys {
            if let Some(assignment) = assignments.remove(&key) {
                self.fence.serve_in_memory(&key);
                taken.insert(key, assignment);
            }
        }
        self.fence.retain_in_memory(|key| taken.contains_key(key));
        taken
    }

    /// Bring the shards served here in line with `assignments`, the ones
    /// [`Self::take`] returned.
    pub(crate) async fn apply(&self, assignments: &HashMap<ShardKey, ShardAssignment>) {
        let mut keys: Vec<&ShardKey> = assignments.keys().collect();
        keys.sort();
        let mut closes = Vec::new();
        let mut opens = Vec::new();
        for key in keys {
            let assignment = &assignments[key];
            let current = self.served.lock().get(key).copied();
            let serves = assignment.leader == self.local
                && !assignment.is_draining()
                && self.in_memory_at(key, assignment.incarnation).await;
            if !serves {
                if let Some(served) = current {
                    closes.push((key.clone(), served, handoff(assignment, &self.local)));
                }
                continue;
            }
            let wanted = Served {
                generation: assignment.generation,
                incarnation: assignment.incarnation,
            };
            match current {
                Some(served) if served == wanted => {}
                // The next generation on the same owner: nobody served in
                // between, so it carries on with what it holds.
                Some(served) if served.continues_to(wanted) => {
                    self.fence.open(key, wanted.generation);
                    self.fence
                        .record_phase(key, Phase::Active, wanted.generation);
                    self.served.lock().insert(key.clone(), wanted);
                }
                // A skipped generation may have had another owner: start
                // empty, and tell every reader.
                Some(served) => {
                    closes.push((key.clone(), served, handoff(assignment, &self.local)));
                    opens.push((key.clone(), wanted));
                }
                None => opens.push((key.clone(), wanted)),
            }
        }
        let gone: Vec<(ShardKey, Served)> = {
            let served = self.served.lock();
            let mut gone: Vec<_> = served
                .iter()
                .filter(|(key, _)| !assignments.contains_key(*key))
                .map(|(key, served)| (key.clone(), *served))
                .collect();
            gone.sort_by(|a, b| a.0.cmp(&b.0));
            gone
        };
        for (key, served) in gone {
            closes.push((key, served, no_owner(served.generation)));
        }
        // Every closing shard stops taking writes before anything waits, and
        // the waits share one bound, so a pass that closes many shards holds
        // up the feed (and the Raft shards it carries) for that bound at most.
        for (key, served, _) in &closes {
            self.shut(key, *served);
        }
        let closing: felix_common::random::HashSet<&ShardKey> =
            closes.iter().map(|(key, _, _)| key).collect();
        let (after, now): (Vec<_>, Vec<_>) = opens
            .into_iter()
            .partition(|(key, _)| closing.contains(key));
        for (key, wanted) in now {
            self.open(&key, wanted).await;
        }
        let deadline = tokio::time::Instant::now() + QUIESCE_BOUND;
        futures::future::join_all(
            closes
                .iter()
                .map(|(key, served, to)| self.finish_close(key, *served, to.clone(), deadline)),
        )
        .await;
        // A key reopened at a new generation only once its close is done.
        for (key, wanted) in after {
            self.open(&key, wanted).await;
        }
    }

    /// Whether `key` is a shard of a stream registered here in memory, at
    /// `incarnation` when the assignment names one. Anything else is not
    /// served this pass: a stream recreated durable is the Raft engine's
    /// once the catalog says so.
    async fn in_memory_at(&self, key: &ShardKey, incarnation: u64) -> bool {
        self.broker
            .stream_metadata(&key.tenant_id, &key.namespace, &key.stream)
            .await
            .is_some_and(|metadata| {
                !metadata.durable && (incarnation == 0 || metadata.incarnation == incarnation)
            })
    }

    async fn open(&self, key: &ShardKey, wanted: Served) {
        // A new owner starts empty; group state from an earlier term goes.
        self.readers.begin_term(key).await;
        self.fence.open(key, wanted.generation);
        self.fence
            .record_phase(key, Phase::Active, wanted.generation);
        self.served.lock().insert(key.clone(), wanted);
        tracing::info!(
            stream = %key.stream,
            shard = key.shard,
            generation = wanted.generation,
            "in-memory shard serving",
        );
    }

    /// Stop serving `key`: no write enters from here on, and it is no
    /// longer servable.
    fn shut(&self, key: &ShardKey, served: Served) {
        self.fence.close(key);
        self.fence
            .record_phase(key, Phase::Closed, served.generation);
        self.served.lock().remove(key);
    }

    /// Drop what a [`Self::shut`] shard holds once the writes already in it
    /// are done, or at `deadline`. Readers are ended with `to`, which names
    /// the generation the shard moved at.
    async fn finish_close(
        &self,
        key: &ShardKey,
        served: Served,
        to: felix_broker::ShardHandoff,
        deadline: tokio::time::Instant,
    ) {
        if tokio::time::timeout_at(deadline, self.fence.quiesce(key))
            .await
            .is_err()
        {
            tracing::warn!(
                stream = %key.stream,
                shard = key.shard,
                "closing an in-memory shard with writes still in flight",
            );
        }
        if let Err(err) = self.readers.close(key, Some(to)).await {
            tracing::warn!(
                stream = %key.stream,
                shard = key.shard,
                error = %err,
                "could not cleanly close an in-memory shard",
            );
        }
        tracing::info!(
            stream = %key.stream,
            shard = key.shard,
            generation = served.generation,
            "in-memory shard closed",
        );
    }
}

/// What one shard serves at here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Served {
    generation: u64,
    incarnation: u64,
}

impl Served {
    /// Whether `next` is the generation right after this one, for the same
    /// stream. Every placement write bumps the generation by one, so no other
    /// broker can have owned the shard in between.
    fn continues_to(self, next: Served) -> bool {
        self.incarnation == next.incarnation && next.generation == self.generation + 1
    }
}

/// Where a reader of a shard this broker stops serving resumes, by the
/// assignment that stopped it.
fn handoff(assignment: &ShardAssignment, local: &str) -> felix_broker::ShardHandoff {
    let node_id = if assignment.leader == local {
        assignment.successor.clone()
    } else {
        Some(assignment.leader.clone())
    };
    felix_broker::ShardHandoff {
        node_id,
        addr: None,
        generation: assignment.generation,
    }
}

#[cfg(test)]
mod tests;
