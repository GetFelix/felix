//! What the control plane last told this broker about its own shards, kept in
//! the data directory so a restart while the control plane is down has
//! something to start from.
//!
//! The view holds the fleet's finalized features, the routes of every shard
//! this broker is in, the addresses of the nodes those routes name, and the
//! catalog entries those shards open at. One background task builds it from
//! what the broker holds in memory and rewrites the file when it changed, at
//! most once per [`WRITE_EVERY`] and once more at a clean stop. A tick on
//! which none of its inputs moved builds nothing. Nothing on the publish path
//! touches it.
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
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// [`ShardOwnership::changes`] of `ownership`.
    pub(crate) ownership_changes: Arc<AtomicU64>,
    pub(crate) router: Arc<ShardRouter>,
    pub(crate) broker: Arc<Broker>,
}

/// Everything a view is built from, as far as telling whether it moved: one
/// that compares equal to the last builds the same view.
#[derive(Debug)]
pub(crate) struct Stamp {
    registered: Option<u64>,
    adopted: bool,
    fleet: Vec<String>,
    catalog_seeded: bool,
    ownership: u64,
    /// Node addresses: each publish swaps in a new one.
    routes: Arc<felix_router::Routes>,
    catalog: u64,
}

impl PartialEq for Stamp {
    fn eq(&self, other: &Self) -> bool {
        self.registered == other.registered
            && self.adopted == other.adopted
            && self.fleet == other.fleet
            && self.catalog_seeded == other.catalog_seeded
            && self.ownership == other.ownership
            && Arc::ptr_eq(&self.routes, &other.routes)
            && self.catalog == other.catalog
    }
}

impl Sources {
    /// Where every input stands, without the locks or reads a build takes.
    /// Taken before [`Self::build`], so a change during the build shows as a
    /// new stamp next time.
    pub(crate) fn stamp(&self) -> Stamp {
        Stamp {
            registered: *self.registered.borrow(),
            adopted: self.fleet.view().adopted(),
            fleet: self
                .fleet
                .enabled()
                .into_iter()
                .map(str::to_owned)
                .collect(),
            catalog_seeded: self.catalog_seeded.is_cancelled(),
            ownership: self.ownership_changes.load(Ordering::Acquire),
            routes: self.router.routes(),
            catalog: self.broker.catalog_changes(),
        }
    }

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

/// The file, shared by the writer and the stop that forgets it.
///
/// Every write and the removal take one lock, and a forgotten file is never
/// written again by the writer. So a write still in flight when the stop
/// gives up waiting cannot put the view back after a deregistration.
#[derive(Clone)]
pub(crate) struct ViewFile {
    root: PathBuf,
    forgotten: Arc<std::sync::Mutex<bool>>,
}

impl ViewFile {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            root,
            forgotten: Arc::default(),
        }
    }

    /// Record `body` unless the file was forgotten. Blocking.
    fn record(&self, body: &[u8]) -> felix_storage::Result<()> {
        let forgotten = self.forgotten.lock().unwrap_or_else(|err| err.into_inner());
        if *forgotten {
            return Ok(());
        }
        felix_storage::controlplane_view::record(&self.root, body)
    }

    /// Remove the file durably and stop the writer recording it. Returns
    /// what [`Self::restore`] needs to put it back. Blocking.
    fn forget(&self) -> felix_storage::Result<Forgotten> {
        let mut forgotten = self.forgotten.lock().unwrap_or_else(|err| err.into_inner());
        *forgotten = true;
        let held = felix_storage::controlplane_view::read(&self.root);
        let marker = self.marker();
        felix_storage::controlplane_view::forget(&self.root)?;
        Ok(Forgotten { held, marker })
    }

    /// The replaced-storage marker's bytes, `Some(None)` without one, or
    /// `None` when it cannot be read.
    fn marker(&self) -> Option<Option<Vec<u8>>> {
        match std::fs::read(self.root.join(felix_storage::run_state::REPLACED_FILE_NAME)) {
            Ok(bytes) => Some(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Some(None),
            Err(_) => None,
        }
    }

    /// Put `body` back after [`Self::forget`], unless the directory was
    /// marked replaced since (`marker` is what it held then): the view
    /// predates that marker, and only a registration after it may write one.
    /// An older marker, unchanged, says nothing about this view. Blocking.
    fn restore(&self, body: &[u8], marker: Option<Option<Vec<u8>>>) -> felix_storage::Result<()> {
        let _forgotten = self.forgotten.lock().unwrap_or_else(|err| err.into_inner());
        let now = self.marker();
        if marker.is_none() || now != marker {
            return Ok(());
        }
        felix_storage::controlplane_view::record(&self.root, body)
    }

    /// Run `record`, which marks the directory replaced and removes the view,
    /// under the lock every write and restore takes, and stop the writer
    /// recording the view after it. Blocking.
    pub(crate) fn forget_while_recording<R>(&self, record: impl FnOnce() -> R) -> R {
        let mut forgotten = self.forgotten.lock().unwrap_or_else(|err| err.into_inner());
        *forgotten = true;
        record()
    }

    /// Remove the file durably and stop the writer recording it, for a broker
    /// that stops because the control plane refused it or found its directory
    /// replaced. Returns whether it is gone.
    pub(crate) async fn forget_now(&self) -> bool {
        let file = self.clone();
        let forgotten = tokio::task::spawn_blocking(move || file.forget()).await;
        let gone = matches!(forgotten, Ok(Ok(_)));
        if !gone {
            tracing::error!(
                outcome = ?forgotten.map(|forgotten| forgotten.map_err(|err| err.to_string())),
                "could not remove the control-plane view",
            );
        }
        gone
    }

    /// Forget the view before deregistering, and put it back if the control
    /// plane never got the request: `last`, or when this process wrote none,
    /// what the file held, so a broker that started from its view and stops
    /// before it registered keeps it.
    ///
    /// Once the control plane has a deregistration it may move this node's
    /// shards to sets without it, so a later start must not stand or vote
    /// from the old ones: it waits for the control plane instead. A crash
    /// between the two steps leaves no view, which is the same wait. When the
    /// file cannot be removed this does not deregister, and returns `false`:
    /// the control plane then expires the node as if it had crashed, and a
    /// node that is down is what the view is for.
    pub(crate) async fn forget_while<D>(&self, last: Option<Vec<u8>>, deregister: D) -> bool
    where
        D: Future<Output = bool>,
    {
        let forgotten = {
            let file = self.clone();
            tokio::task::spawn_blocking(move || file.forget()).await
        };
        let Forgotten { held, marker } = match forgotten {
            Ok(Ok(forgotten)) => forgotten,
            forgotten => {
                tracing::error!(
                    outcome = ?forgotten.map(|forgotten| forgotten.map_err(|err| err.to_string())),
                    "could not remove the control-plane view; not deregistering, so the control \
                     plane expires this node instead",
                );
                return false;
            }
        };
        if !deregister.await
            && let Some(last) = last.or(held)
        {
            // The control plane never saw the request, so the view is as good
            // as it was.
            let file = self.clone();
            let restored = tokio::task::spawn_blocking(move || file.restore(&last, marker)).await;
            if !matches!(restored, Ok(Ok(()))) {
                tracing::warn!(
                    "could not put the control-plane view back; the next start waits for the \
                     control plane"
                );
            }
        }
        true
    }
}

/// What [`ViewFile::forget`] removed, and the replaced-storage marker as it
/// stood then.
#[derive(Debug)]
struct Forgotten {
    held: Option<Vec<u8>>,
    marker: Option<Option<Vec<u8>>>,
}

/// The running writer.
pub(crate) struct ViewWriter {
    task: JoinHandle<Option<Vec<u8>>>,
    shutdown: CancellationToken,
    file: ViewFile,
}

impl ViewWriter {
    pub(crate) fn file(&self) -> ViewFile {
        self.file.clone()
    }

    /// Write once more and stop. Returns the body this process last wrote,
    /// or `None` if it wrote none. Dropped before it finishes, the task is
    /// aborted.
    pub(crate) async fn stop(self) -> Option<Vec<u8>> {
        struct AbortOnDrop(JoinHandle<Option<Vec<u8>>>);
        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        self.shutdown.cancel();
        let mut task = AbortOnDrop(self.task);
        (&mut task.0).await.ok().flatten()
    }
}

/// Keep the view under `root` current until it is stopped, then write it
/// once more.
pub(crate) fn spawn(sources: Sources, file: ViewFile) -> ViewWriter {
    let sources = Arc::new(sources);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(
        {
            let sources = Arc::clone(&sources);
            move || sources.stamp()
        },
        move || {
            let sources = Arc::clone(&sources);
            async move { sources.build().await }
        },
        file.clone(),
        shutdown.clone(),
    ));
    ViewWriter {
        task,
        shutdown,
        file,
    }
}

/// The writer's loop, over any way of stamping and building the view.
/// Returns the body it last wrote.
///
/// No periodic rebuild backs the stamp up: every input a build reads is in
/// it, so an unchanged stamp builds the view already on disk.
async fn run<S, T, B, F>(
    stamp: S,
    build: B,
    file: ViewFile,
    shutdown: CancellationToken,
) -> Option<Vec<u8>>
where
    S: Fn() -> T,
    T: PartialEq,
    B: Fn() -> F,
    F: Future<Output = Option<View>>,
{
    let mut writer = Writer::new(file);
    let mut ticker = tokio::time::interval(WRITE_EVERY);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut built: Option<T> = None;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => {}
        }
        let now = stamp();
        if built.as_ref() == Some(&now) {
            continue;
        }
        writer.write(build().await).await;
        // A failed write is tried again on the next tick, unchanged or not.
        built = (!writer.failed).then_some(now);
    }
    // A change made since the last tick would otherwise be lost to a stop
    // that is not a crash.
    writer.write(build().await).await;
    writer.last
}

/// Writes a view when it differs from the last one written.
pub(crate) struct Writer {
    file: ViewFile,
    last: Option<Vec<u8>>,
    /// The last view could not be written.
    failed: bool,
}

impl Writer {
    pub(crate) fn new(file: ViewFile) -> Self {
        Self {
            file,
            last: None,
            failed: false,
        }
    }

    /// Record `view` if it is new. Returns whether the file was written.
    /// A failed write is logged and tried again with the next view, and the
    /// file keeps the older one.
    pub(crate) async fn write(&mut self, view: Option<View>) -> bool {
        self.failed = false;
        let Some(view) = view else {
            return false;
        };
        let body = match serde_json::to_vec(&view) {
            Ok(body) => body,
            Err(err) => {
                tracing::error!(error = %err, "could not encode the control-plane view");
                self.failed = true;
                return false;
            }
        };
        if self.last.as_ref() == Some(&body) {
            return false;
        }
        let written = {
            let file = self.file.clone();
            let body = body.clone();
            tokio::task::spawn_blocking(move || file.record(&body)).await
        };
        match written {
            Ok(Ok(())) => {
                self.last = Some(body);
                true
            }
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "could not write the control-plane view; keeping the older one");
                self.failed = true;
                false
            }
            Err(err) => {
                tracing::error!(error = %err, "writing the control-plane view panicked");
                self.failed = true;
                false
            }
        }
    }
}

#[cfg(test)]
mod tests;
