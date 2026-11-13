//! One engine per shard across the brokers that hold it.
//!
//! A durable stream shard is replicated by the hybrid engine or by a Raft
//! group, and its replicas must agree which: a broker on the other engine
//! keeps a log of its own that no quorum covers. [`EngineMatch`] sits in
//! front of this broker's engine in the routing feed and refuses every such
//! shard placed with a peer on the other engine. See
//! `docs/per-shard-raft.md`.

use std::collections::BTreeSet;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use felix_broker::Broker;
use felix_common::random::{HashMap, HashSet};
use felix_replication::peer::PeerRequester;
use felix_wire::internal::PeerCapabilities;

use super::routing::{Dispatch, EngineShards, Reason, Taken};
use super::watch::ShardAssignment;
use super::{ShardKey, ShardKind};

/// Shards refused because a replica runs the other engine, by this broker's
/// `engine`. Counts each shard once per time it becomes refused.
pub(crate) const REFUSED_TOTAL: &str = "felix_broker_shard_engine_refused_total";

/// How long one ask may take, so a peer that never answers is asked again.
const ASK_WITHIN: Duration = Duration::from_secs(5);

type Addresses = Arc<dyn Fn(&str) -> Option<SocketAddr> + Send + Sync>;

/// Refuses the durable stream shards this broker shares with a peer on the
/// other engine, and leaves the rest to `inner`.
pub(crate) struct EngineMatch<R> {
    broker: Arc<Broker>,
    peers: Arc<R>,
    addresses: Addresses,
    local: String,
    /// Whether this broker runs its durable stream shards on Raft.
    raft: bool,
    inner: Option<Arc<dyn EngineShards>>,
    /// Whether each peer runs Raft, as its listener last said. A peer whose
    /// listener offers no Raft runs the hybrid, which covers a build from
    /// before the choice existed.
    engines: Arc<parking_lot::Mutex<HashMap<String, bool>>>,
    /// Peers with an ask in flight.
    asking: Arc<parking_lot::Mutex<HashSet<String>>>,
    /// Runs a feed pass, so a shard closes when the answer arrives and not a
    /// tick later.
    wake: Arc<std::sync::OnceLock<Arc<tokio::sync::Notify>>>,
    refused: parking_lot::Mutex<HashMap<ShardKey, String>>,
    /// Whether `refused` holds anything, so a request does not take its lock
    /// in a fleet that agrees.
    any_refused: AtomicBool,
}

impl<R: PeerRequester + Send + Sync + 'static> EngineMatch<R> {
    pub(crate) fn new(
        broker: Arc<Broker>,
        peers: Arc<R>,
        addresses: Addresses,
        local: String,
        raft: bool,
        inner: Option<Arc<dyn EngineShards>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            broker,
            peers,
            addresses,
            local,
            raft,
            inner,
            engines: Arc::default(),
            asking: Arc::default(),
            wake: Arc::default(),
            refused: parking_lot::Mutex::default(),
            any_refused: AtomicBool::new(false),
        })
    }

    /// Ask `peer` which engine it runs, off the pass: a peer that is down
    /// must not slow the feed. Every pass asks again, so a peer not reached
    /// yet is learnt once it is up, and one that restarted on the other
    /// engine is noticed. Until a peer answers, its shards are served.
    fn ask(&self, peer: &str) {
        let Some(addr) = (self.addresses)(peer) else {
            return;
        };
        if !self.asking.lock().insert(peer.to_string()) {
            return;
        }
        let (peers, engines) = (Arc::clone(&self.peers), Arc::clone(&self.engines));
        let (asking, wake) = (Arc::clone(&self.asking), Arc::clone(&self.wake));
        let name = peer.to_string();
        tokio::spawn(async move {
            let answer = tokio::time::timeout(ASK_WITHIN, peers.capabilities(&name, addr)).await;
            asking.lock().remove(&name);
            let Ok(Ok(offered)) = answer else {
                return;
            };
            let raft = offered.contains(PeerCapabilities::RAFT);
            if engines.lock().insert(name, raft) != Some(raft)
                && let Some(wake) = wake.get()
            {
                wake.notify_one();
            }
        });
    }

    /// The peer of `assignment` known to run the other engine, if any.
    fn other_engine(&self, assignment: &ShardAssignment) -> Option<String> {
        let engines = self.engines.lock();
        members(assignment)
            .filter(|peer| **peer != self.local)
            .find(|peer| engines.get(*peer).is_some_and(|raft| *raft != self.raft))
            .cloned()
    }

    fn engine_name(raft: bool) -> &'static str {
        if raft { "raft" } else { "hybrid" }
    }
}

fn members(assignment: &ShardAssignment) -> impl Iterator<Item = &String> {
    std::iter::once(&assignment.leader).chain(&assignment.replicas)
}

impl<R: PeerRequester + Send + Sync + 'static> EngineShards for EngineMatch<R> {
    fn take<'a>(
        &'a self,
        assignments: &'a mut HashMap<ShardKey, ShardAssignment>,
    ) -> Pin<Box<dyn Future<Output = Taken> + Send + 'a>> {
        Box::pin(async move {
            let shared: Vec<&ShardAssignment> = assignments
                .values()
                .filter(|assignment| {
                    assignment.key.kind == ShardKind::Stream
                        && members(assignment).any(|member| *member == self.local)
                })
                .collect();
            let peers: BTreeSet<String> = shared
                .iter()
                .flat_map(|assignment| members(assignment))
                .filter(|member| **member != self.local)
                .cloned()
                .collect();
            let suspects: Vec<ShardKey> = shared
                .iter()
                .map(|assignment| assignment.key.clone())
                .collect();
            for peer in &peers {
                self.ask(peer);
            }

            let mut refused = HashMap::default();
            let mut taken = Taken::new();
            for key in suspects {
                let Some(peer) = assignments.get(&key).and_then(|a| self.other_engine(a)) else {
                    continue;
                };
                // A stream in memory runs on the hybrid on every broker.
                let durable = self
                    .broker
                    .stream_metadata(&key.tenant_id, &key.namespace, &key.stream)
                    .await
                    .is_some_and(|metadata| metadata.durable);
                if !durable {
                    continue;
                }
                let Some(assignment) = assignments.remove(&key) else {
                    continue;
                };
                taken.push((key.clone(), assignment.routing));
                refused.insert(key, peer);
            }
            {
                let mut held = self.refused.lock();
                for (key, peer) in &refused {
                    if held.contains_key(key) {
                        continue;
                    }
                    let (ours, theirs) =
                        (Self::engine_name(self.raft), Self::engine_name(!self.raft));
                    tracing::error!(
                        shard = ?key,
                        peer = %peer,
                        "refusing a durable stream shard: this broker runs the {ours} shard \
                         engine and {peer} runs {theirs}; set FELIX_SHARD_ENGINE the same on \
                         every broker"
                    );
                    metrics::counter!(REFUSED_TOTAL, "engine" => ours).increment(1);
                }
                self.any_refused
                    .store(!refused.is_empty(), Ordering::Release);
                *held = refused;
            }
            if let Some(inner) = &self.inner {
                taken.extend(inner.take(assignments).await);
            }
            taken
        })
    }

    fn serves_reads(&self, key: &ShardKey) -> bool {
        !(self.any_refused.load(Ordering::Acquire) && self.refused.lock().contains_key(key))
            && self
                .inner
                .as_ref()
                .is_some_and(|inner| inner.serves_reads(key))
    }

    fn dispatch(
        &self,
        key: &ShardKey,
        nodes: &dyn Fn(&str) -> Option<SocketAddr>,
    ) -> Option<Dispatch> {
        if self.any_refused.load(Ordering::Acquire)
            && let Some(peer) = self.refused.lock().get(key)
        {
            let (ours, theirs) = (Self::engine_name(self.raft), Self::engine_name(!self.raft));
            return Some(Dispatch::Unavailable(Reason::OwnerUnavailable(format!(
                "the shard's brokers run different shard engines ({ours} here, {theirs} on \
                 {peer}); set FELIX_SHARD_ENGINE the same on every broker"
            ))));
        }
        self.inner.as_ref()?.dispatch(key, nodes)
    }

    fn wake_with(&self, wake: Arc<tokio::sync::Notify>) {
        let _ = self.wake.set(wake);
    }

    fn shutdown(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            if let Some(inner) = &self.inner {
                inner.shutdown().await;
            }
        })
    }
}

#[cfg(test)]
mod tests;
