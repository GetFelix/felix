//! What the control plane last told this broker about its own shards, kept in
//! the data directory so a restart while the control plane is down has
//! something to start from.
//!
//! The view holds the fleet's finalized features, the routes of every shard
//! this broker is in, the addresses of the nodes those routes name, and the
//! catalog entries those shards open at. One background task builds it from
//! what the broker holds in memory and rewrites the file when it changed, at
//! most once per [`WRITE_EVERY`] and once more at a clean stop. Nothing on the
//! publish path touches it.
//!
//! Only state the control plane has backed is written: nothing until this
//! process has registered, taken the registration's features, and seeded its
//! routes and catalog from snapshots. Each part of a view is something the
//! control plane told this broker, read within one build: routes, nodes and
//! catalog are read one after another, so they can be as far apart as the
//! feeds themselves are at any moment. See
//! `docs/durable-storage.md`, "The control-plane view", and the envelope in
//! `felix_storage::controlplane_view`.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use felix_common::fleet::FleetGate;
use felix_router::ShardRouter;
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cluster::catalog_sync::wire;
use crate::shards::watch::{ShardAssignment, ShardOwnership};
use crate::shards::{ShardKey, ShardKind};

/// The least time between two writes. A crash loses at most the changes of
/// the last interval, which is safe: the file then holds an older view.
pub(crate) const WRITE_EVERY: Duration = Duration::from_secs(1);

/// The body of `controlplane.view`. JSON, so a field can be added with a
/// default, as on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct View {
    pub(crate) node_id: String,
    /// The storage id this node registered with, in hex. A start may use the
    /// view only on a directory that still has it.
    pub(crate) storage_id: String,
    /// The node incarnation that registration answered.
    pub(crate) incarnation: u64,
    /// The features the fleet had finalized, by name: bit positions depend
    /// on what a build reports.
    pub(crate) fleet: Vec<String>,
    /// Every assignment naming this node, as the routing feed holds it.
    pub(crate) routes: Vec<ShardAssignment>,
    /// The nodes those routes name, where the catalog knew them.
    pub(crate) nodes: Vec<Node>,
    pub(crate) catalog: Catalog,
}

/// A node a persisted route names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Node {
    pub(crate) node_id: String,
    pub(crate) advertise_addr: SocketAddr,
    pub(crate) region: String,
}

/// The catalog entries the persisted routes' streams and caches registered
/// with, in the control plane's own shape so they apply as a sync would.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Catalog {
    pub(crate) streams: Vec<wire::Stream>,
    pub(crate) caches: Vec<wire::Cache>,
}

impl View {
    /// The view in `body`, or `None` when it does not parse or is another
    /// node's. Either way the caller waits for the control plane.
    pub(crate) fn decode(body: &[u8], node_id: &str) -> Option<Self> {
        let view: Self = match serde_json::from_slice(body) {
            Ok(view) => view,
            Err(err) => {
                tracing::warn!(error = %err, "the control-plane view does not parse; ignoring it");
                return None;
            }
        };
        if view.node_id != node_id {
            tracing::warn!(
                node_id,
                view_node_id = %view.node_id,
                "the control-plane view is another node's; ignoring it"
            );
            return None;
        }
        Some(view)
    }
}

/// The view recorded under `root` for `node_id`, if there is one to trust.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "read by the start without the control plane")
)]
pub(crate) fn read(root: &std::path::Path, node_id: &str) -> Option<View> {
    View::decode(&felix_storage::controlplane_view::read(root)?, node_id)
}

/// What the view is built from.
pub(crate) struct Sources {
    pub(crate) node_id: String,
    pub(crate) storage_id: felix_storage::StorageId,
    /// The incarnation of this process's registration, `None` until it has
    /// one.
    pub(crate) registered: watch::Receiver<Option<u64>>,
    pub(crate) fleet: Arc<FleetGate>,
    pub(crate) catalog_seeded: CancellationToken,
    pub(crate) ownership: Arc<RwLock<ShardOwnership>>,
    pub(crate) router: Arc<ShardRouter>,
    pub(crate) broker: Arc<Broker>,
}

impl Sources {
    /// The view as of now, or `None` while some part of it is not yet backed
    /// by the control plane in this process.
    pub(crate) async fn build(&self) -> Option<View> {
        let incarnation = (*self.registered.borrow())?;
        if !self.fleet.view().adopted() || !self.catalog_seeded.is_cancelled() {
            return None;
        }
        let mut routes: Vec<ShardAssignment> = {
            let owned = self.ownership.read().await;
            if !owned.is_seeded() {
                return None;
            }
            owned
                .assignments()
                .values()
                .filter(|assignment| members(assignment).any(|node| node == self.node_id))
                .cloned()
                .collect()
        };
        routes.sort_by(|a, b| a.key.cmp(&b.key));
        let named: BTreeSet<&str> = routes.iter().flat_map(members).collect();
        let nodes = named
            .into_iter()
            // A node the catalog has no address for is left out.
            .filter_map(|node_id| self.router.node(node_id))
            .map(|node| Node {
                node_id: node.node_id,
                advertise_addr: node.advertise_addr,
                region: node.region,
            })
            .collect();
        Some(View {
            node_id: self.node_id.clone(),
            storage_id: self.storage_id.to_hex(),
            incarnation,
            fleet: self
                .fleet
                .enabled()
                .into_iter()
                .map(str::to_owned)
                .collect(),
            catalog: self.catalog(&routes).await,
            routes,
            nodes,
        })
    }

    /// The entries `routes` open at. A stream or cache the catalog has not
    /// registered yet is left out; its shard then waits for the control plane.
    async fn catalog(&self, routes: &[ShardAssignment]) -> Catalog {
        let names: BTreeMap<(ShardKind, &str, &str, &str), ()> = routes
            .iter()
            .map(|route| {
                let ShardKey {
                    tenant_id,
                    namespace,
                    stream,
                    kind,
                    ..
                } = &route.key;
                (
                    (
                        *kind,
                        tenant_id.as_str(),
                        namespace.as_str(),
                        stream.as_str(),
                    ),
                    (),
                )
            })
            .collect();
        let mut catalog = Catalog::default();
        for (kind, tenant, namespace, name) in names.into_keys() {
            match kind {
                ShardKind::Stream => {
                    if let Some(metadata) =
                        self.broker.stream_metadata(tenant, namespace, name).await
                    {
                        catalog
                            .streams
                            .push(wire::Stream::of(tenant, namespace, name, &metadata));
                    }
                }
                ShardKind::Cache => {
                    if let Some(metadata) =
                        self.broker.cache_metadata(tenant, namespace, name).await
                    {
                        catalog
                            .caches
                            .push(wire::Cache::of(tenant, namespace, name, &metadata));
                    }
                }
            }
        }
        catalog
    }
}

/// Every node `assignment` names: its set, the set it is moving to, and where
/// a move is taking it.
fn members(assignment: &ShardAssignment) -> impl Iterator<Item = &str> {
    std::iter::once(assignment.leader.as_str())
        .chain(assignment.replicas.iter().map(String::as_str))
        .chain(
            assignment
                .pending_set
                .iter()
                .flat_map(|pending| pending.replicas.iter().map(String::as_str)),
        )
        .chain(assignment.successor.as_deref())
}

/// Keep the view under `root` current until `shutdown`, then write it once
/// more.
pub(crate) fn spawn(
    sources: Sources,
    root: PathBuf,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    let sources = Arc::new(sources);
    tokio::spawn(run(
        move || {
            let sources = Arc::clone(&sources);
            async move { sources.build().await }
        },
        root,
        shutdown,
    ))
}

/// The writer's loop, over any way of building the view.
async fn run<B, F>(build: B, root: PathBuf, shutdown: CancellationToken)
where
    B: Fn() -> F,
    F: Future<Output = Option<View>>,
{
    let mut writer = Writer::new(root);
    let mut ticker = tokio::time::interval(WRITE_EVERY);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => {}
        }
        writer.write(build().await).await;
    }
    // A change made since the last tick would otherwise be lost to a stop
    // that is not a crash.
    writer.write(build().await).await;
}

/// Writes a view when it differs from the last one written.
pub(crate) struct Writer {
    root: PathBuf,
    last: Option<Vec<u8>>,
}

impl Writer {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self { root, last: None }
    }

    /// Record `view` if it is new. Returns whether the file was written.
    /// A failed write is logged and tried again with the next view, and the
    /// file keeps the older one.
    pub(crate) async fn write(&mut self, view: Option<View>) -> bool {
        let Some(view) = view else {
            return false;
        };
        let body = match serde_json::to_vec(&view) {
            Ok(body) => body,
            Err(err) => {
                tracing::error!(error = %err, "could not encode the control-plane view");
                return false;
            }
        };
        if self.last.as_ref() == Some(&body) {
            return false;
        }
        let root = self.root.clone();
        let written = {
            let body = body.clone();
            tokio::task::spawn_blocking(move || {
                felix_storage::controlplane_view::record(&root, &body)
            })
            .await
        };
        match written {
            Ok(Ok(())) => {
                self.last = Some(body);
                true
            }
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "could not write the control-plane view; keeping the older one");
                false
            }
            Err(err) => {
                tracing::error!(error = %err, "writing the control-plane view panicked");
                false
            }
        }
    }
}

#[cfg(test)]
mod tests;
