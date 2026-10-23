//! Finding a stream's shard owners before any ack names them.
//!
//! An owner learned from a forwarded ack never reaches a publish that asks for
//! no ack, and costs every shard's first batch a hop. So the cluster is asked
//! (`Client::shard_owners`) the first time a stream is published to, and a
//! snapshot of the answer is kept per stream.
//!
//! - **The first publish waits, and so do the ones racing it**, up to
//!   [`DISCOVERY_TIMEOUT`], so a batch split over several shards sends every
//!   piece to its owner. A broker that does not answer costs one timeout, not
//!   one per publish: the stream is then served through the entry broker.
//! - **Refreshes never block a publish.** An old snapshot (or one a publish
//!   found wrong) is replaced by a task of its own while publishes carry on.
//! - **No connection is opened during discovery.** A shard's owner is turned
//!   into a cached [`Owner`] the first time that shard is published to, and
//!   connected then, so one unreachable broker costs only its own shards, once
//!   per [`UNREACHABLE_FOR`].
//! - **State is bounded.** Streams not published to for [`IDLE_AFTER`], and
//!   the least recent ones beyond [`MAX_STREAMS`], are dropped with the owners
//!   cached for them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use felix_wire::routing::ShardRouting;
use tokio::sync::{OnceCell, RwLock};

use super::nodes::Nodes;
use super::{Owner, ShardKey, StreamKey};
use crate::client::Client;
use crate::publish::StreamWidths;

/// How long the shard-owner question may take.
pub(super) const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(1);

/// How long a snapshot is trusted before a refresh starts.
const OWNER_TTL: Duration = Duration::from_secs(5);

/// The least time between refreshes of a stream a publish found wrong.
const RETRY_AFTER_STALE: Duration = Duration::from_millis(250);

/// How long an owner that would not connect is left alone.
const UNREACHABLE_FOR: Duration = Duration::from_secs(30);

/// How long connecting to an owner may take before the batch goes through the
/// entry broker instead.
pub(super) const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Streams tracked per client.
const MAX_STREAMS: usize = 1024;

/// How long a stream may go unpublished-to before it is forgotten.
const IDLE_AFTER: Duration = Duration::from_secs(600);

/// What discovery knows about one stream.
pub(super) struct StreamState {
    /// Held by the publish doing the first discovery; the ones racing it queue
    /// here and find the snapshot made when they get in.
    gate: tokio::sync::Mutex<()>,
    meta: Mutex<Meta>,
}

struct Meta {
    /// When the snapshot was last attempted; `None` before the first.
    at: Option<Instant>,
    /// A publish found a cached owner or width wrong since.
    stale: bool,
    refreshing: bool,
    used: Instant,
    snapshot: Option<Arc<Snapshot>>,
    /// Brokers that would not connect, and when.
    unreachable: HashMap<String, Instant>,
}

/// The cluster's account of a stream's owners: one broker per shard, under
/// the width the shard numbers mean.
struct Snapshot {
    width: (u32, ShardRouting),
    owners: HashMap<u32, Advertised>,
}

struct Advertised {
    node_id: String,
    addr: std::net::SocketAddr,
    generation: u64,
}

impl StreamState {
    fn new() -> Self {
        Self {
            gate: tokio::sync::Mutex::new(()),
            meta: Mutex::new(Meta {
                at: None,
                stale: false,
                refreshing: false,
                used: Instant::now(),
                snapshot: None,
                unreachable: HashMap::new(),
            }),
        }
    }

    fn attempted(&self) -> bool {
        self.meta.lock().unwrap().at.is_some()
    }

    /// Whether a refresh should start, claiming it so only one does.
    fn claim_refresh(&self) -> bool {
        let mut meta = self.meta.lock().unwrap();
        let Some(at) = meta.at else {
            return false;
        };
        let due = if meta.stale {
            at.elapsed() >= RETRY_AFTER_STALE
        } else {
            at.elapsed() >= OWNER_TTL
        };
        if due && !meta.refreshing {
            meta.refreshing = true;
            return true;
        }
        false
    }

    fn finish(&self, snapshot: Option<Snapshot>) {
        let mut meta = self.meta.lock().unwrap();
        meta.at = Some(Instant::now());
        meta.refreshing = false;
        meta.stale = snapshot.is_none();
        meta.snapshot = snapshot.map(Arc::new);
    }
}

/// What discovery needs from the client, cloneable into a refresh task.
#[derive(Clone)]
pub(super) struct Discovery {
    client: Arc<RwLock<Arc<Client>>>,
    widths: Arc<StreamWidths>,
    nodes: Arc<Nodes>,
    owners: Arc<RwLock<HashMap<ShardKey, Arc<Owner>>>>,
    streams: Arc<Mutex<HashMap<StreamKey, Arc<StreamState>>>>,
}

impl Discovery {
    pub(super) fn new(
        client: &Arc<RwLock<Arc<Client>>>,
        widths: &Arc<StreamWidths>,
        nodes: &Arc<Nodes>,
        owners: &Arc<RwLock<HashMap<ShardKey, Arc<Owner>>>>,
    ) -> Self {
        Self {
            client: Arc::clone(client),
            widths: Arc::clone(widths),
            nodes: Arc::clone(nodes),
            owners: Arc::clone(owners),
            streams: Arc::default(),
        }
    }

    /// The stream's state, made on first use. Forgets idle streams, and their
    /// cached owners, when a new one is added.
    pub(super) async fn state(&self, key: &StreamKey) -> Arc<StreamState> {
        let (state, dropped) = {
            let mut streams = self.streams.lock().unwrap();
            let known = streams.get(key).cloned();
            match known {
                Some(state) => {
                    state.meta.lock().unwrap().used = Instant::now();
                    (state, Vec::new())
                }
                None => {
                    let dropped = prune(&mut streams, MAX_STREAMS - 1, IDLE_AFTER, Instant::now());
                    let state = Arc::new(StreamState::new());
                    streams.insert(key.clone(), Arc::clone(&state));
                    (state, dropped)
                }
            }
        };
        if !dropped.is_empty() {
            self.owners.write().await.retain(|shard, _| {
                !dropped.contains(&(shard.0.clone(), shard.1.clone(), shard.2.clone()))
            });
        }
        state
    }

    /// Make sure the stream has been asked about, and start a refresh if its
    /// snapshot is old. Waits only for the first discovery.
    pub(super) async fn ensure(&self, key: &StreamKey, state: &Arc<StreamState>) {
        if state.attempted() {
            if state.claim_refresh() {
                let (this, key, state) = (self.clone(), key.clone(), Arc::clone(state));
                tokio::spawn(async move {
                    let snapshot = this.ask(&key).await;
                    this.install(&key, &state, snapshot).await;
                });
            }
            return;
        }
        // Queued behind whoever is asking, but not for longer than the ask can
        // take: past that the batch goes through the entry broker.
        let Ok(_turn) = tokio::time::timeout(DISCOVERY_TIMEOUT * 2, state.gate.lock()).await else {
            return;
        };
        if state.attempted() {
            return;
        }
        let snapshot = self.ask(key).await;
        self.install(key, state, snapshot).await;
    }

    async fn install(&self, key: &StreamKey, state: &StreamState, snapshot: Option<Snapshot>) {
        // Owners cached from an older answer that this one replaces go, so the
        // next publish for those shards takes the new owner.
        if let Some(snapshot) = &snapshot {
            self.owners.write().await.retain(|shard, owner| {
                (&shard.0, &shard.1, &shard.2) != (&key.0, &key.1, &key.2)
                    || snapshot
                        .owners
                        .get(&shard.3)
                        .is_none_or(|new| new.generation <= owner.generation)
            });
        }
        state.finish(snapshot);
    }

    /// Ask the entry broker. `None` when it cannot say: it predates the
    /// question, does not answer in time, or describes another width than the
    /// stream's (which is changing).
    async fn ask(&self, key: &StreamKey) -> Option<Snapshot> {
        let client = Arc::clone(&*self.client.read().await);
        if !client.supports_shard_owners() {
            return None;
        }
        let asked = tokio::time::timeout(
            DISCOVERY_TIMEOUT,
            client.shard_owners(&key.0, &key.1, &key.2, crate::ShardKind::Stream),
        )
        .await;
        let owners = match asked {
            Ok(Ok(owners)) => owners,
            Ok(Err(err)) => {
                tracing::debug!(error = %err, "could not discover shard owners");
                return None;
            }
            Err(_) => {
                tracing::debug!("discovering shard owners timed out");
                return None;
            }
        };
        let (_, width) = self.widths.shard_of(&key.0, &key.1, &key.2, b"").await;
        if !width.learned || width.shards as usize != owners.len() {
            return None;
        }
        let mut found = HashMap::new();
        for owner in owners {
            let (Some(node_id), Some(addr)) = (owner.node_id, owner.addr) else {
                continue;
            };
            let Some(addr) = self.nodes.resolve(&addr).await else {
                continue;
            };
            found.insert(
                owner.shard,
                Advertised {
                    node_id,
                    addr,
                    generation: owner.generation,
                },
            );
        }
        Some(Snapshot {
            width: (width.shards, width.routing),
            owners: found,
        })
    }

    /// The owner to send `shard`'s batch to: the one cached, else the
    /// snapshot's, cached now. `None` goes through the entry broker.
    pub(super) async fn route(&self, shard: &ShardKey, state: &StreamState) -> Option<Arc<Owner>> {
        if let Some(owner) = self.owners.read().await.get(shard) {
            return Some(Arc::clone(owner));
        }
        let (snapshot, unreachable) = {
            let meta = state.meta.lock().unwrap();
            let snapshot = Arc::clone(meta.snapshot.as_ref()?);
            let advertised = snapshot.owners.get(&shard.3)?;
            let skipped = meta
                .unreachable
                .get(&advertised.node_id)
                .is_some_and(|at| at.elapsed() < UNREACHABLE_FOR);
            (snapshot, skipped)
        };
        if unreachable {
            return None;
        }
        let advertised = snapshot.owners.get(&shard.3)?;
        let owner = Arc::new(Owner {
            node_id: advertised.node_id.clone(),
            generation: advertised.generation,
            addr: advertised.addr,
            client: OnceCell::new(),
            width: Some(snapshot.width),
        });
        let mut owners = self.owners.write().await;
        Some(Arc::clone(owners.entry(shard.clone()).or_insert(owner)))
    }

    /// A publish found the cached owners or the width wrong: refresh soon,
    /// and until then trust no snapshot, so the stale owner is not picked
    /// again.
    pub(super) fn mark_stale(&self, key: &StreamKey) {
        let state = self.streams.lock().unwrap().get(key).cloned();
        if let Some(state) = state {
            let mut meta = state.meta.lock().unwrap();
            meta.stale = true;
            meta.snapshot = None;
        }
    }

    /// An owner would not connect: leave it alone for a while.
    pub(super) fn mark_unreachable(&self, key: &StreamKey, node_id: &str) {
        let state = self.streams.lock().unwrap().get(key).cloned();
        if let Some(state) = state {
            state
                .meta
                .lock()
                .unwrap()
                .unreachable
                .insert(node_id.to_string(), Instant::now());
        }
    }
}

/// Drop streams idle for `idle`, then the least recently used until at most
/// `keep` remain. Returns the streams dropped.
fn prune(
    streams: &mut HashMap<StreamKey, Arc<StreamState>>,
    keep: usize,
    idle: Duration,
    now: Instant,
) -> Vec<StreamKey> {
    let used = |state: &Arc<StreamState>| state.meta.lock().unwrap().used;
    let mut dropped: Vec<StreamKey> = streams
        .iter()
        .filter(|(_, state)| now.saturating_duration_since(used(state)) >= idle)
        .map(|(key, _)| key.clone())
        .collect();
    for key in &dropped {
        streams.remove(key);
    }
    if streams.len() > keep {
        let mut by_use: Vec<(Instant, StreamKey)> = streams
            .iter()
            .map(|(key, state)| (used(state), key.clone()))
            .collect();
        by_use.sort();
        for (_, key) in by_use.into_iter().take(streams.len() - keep) {
            streams.remove(&key);
            dropped.push(key);
        }
    }
    dropped
}

#[cfg(test)]
pub(super) fn prune_for_test(
    streams: &mut HashMap<StreamKey, Arc<StreamState>>,
    keep: usize,
    idle: Duration,
    now: Instant,
) -> Vec<StreamKey> {
    prune(streams, keep, idle, now)
}

#[cfg(test)]
pub(super) fn state_used_at(at: Instant) -> Arc<StreamState> {
    let state = StreamState::new();
    state.meta.lock().unwrap().used = at;
    Arc::new(state)
}
