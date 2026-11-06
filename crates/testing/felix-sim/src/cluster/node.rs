//! One broker's slot in the cluster: its data directory, its switch to the
//! control plane, and whichever incarnation is running.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use felix_broker_service::sim::{
    CatalogFetcher, ShardKey, ShardSnapshot, ShardStand, SimBroker, SimBrokerConfig,
};
use felix_replication::peer::identity::{Incarnation, PeerIdentity};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::census::NodeView;
use super::{NodeHandles, SimConfig};
use crate::controlplane::SimControlPlane;
use crate::link::{Cut, NodeLink, REQUEST_TIMEOUT};
use crate::net::{SimNet, SimNode};

/// How long a crash waits for the dead incarnation's tasks to notice, so a
/// restart does not open files one of them is still writing.
const CRASH_DRAIN: Duration = Duration::from_millis(250);

/// A snapshot that takes longer than this is reported as missing, so a
/// stuck broker cannot hold up a check or a crash.
const SNAPSHOT_WITHIN: Duration = Duration::from_secs(2);

enum State {
    Down,
    /// Opening, waiting on the control plane or the catalog. Cancelled by a
    /// crash.
    Starting(CancellationToken),
    Up(Running),
}

struct Running {
    handles: NodeHandles,
    broker: Arc<SimBroker>,
}

/// A broker's data directory. It lives in memory, so a run creates, renames
/// and syncs files at no cost, and `felix_storage::sim_fs::crash` can say
/// what a power loss would have left. The path is only a name: nothing is
/// created on disk.
struct DataDir {
    path: PathBuf,
}

impl DataDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "felix-sim-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        felix_storage::sim_fs::register_root(&path);
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DataDir {
    fn drop(&mut self) {
        felix_storage::sim_fs::unregister_root(&self.path);
    }
}

pub(crate) struct Node {
    pub(crate) id: String,
    advertise: SocketAddr,
    dir: DataDir,
    pub(crate) cut: Cut,
    state: Mutex<State>,
    index: usize,
}

impl Node {
    pub(crate) fn new(index: usize, _config: &SimConfig) -> Result<Self> {
        Ok(Self {
            id: format!("b{}", index + 1),
            advertise: SocketAddr::from(([10, 0, 0, (index + 1) as u8], 5001)),
            dir: DataDir::new(),
            cut: Cut::new(),
            state: Mutex::new(State::Down),
            index,
        })
    }

    pub(crate) fn handles(&self) -> Option<NodeHandles> {
        match &*self.state.lock() {
            State::Up(running) => Some(running.handles.clone()),
            _ => None,
        }
    }

    pub(crate) fn is_down(&self) -> bool {
        matches!(&*self.state.lock(), State::Down)
    }

    pub(crate) fn is_starting(&self) -> bool {
        matches!(&*self.state.lock(), State::Starting(_))
    }

    fn broker(&self) -> Option<Arc<SimBroker>> {
        match &*self.state.lock() {
            State::Up(running) => Some(Arc::clone(&running.broker)),
            _ => None,
        }
    }

    /// The node incarnation the running broker registered under, if it has.
    pub(crate) fn registered_incarnation(&self) -> Option<u64> {
        *self.broker()?.incarnation_watch().borrow()
    }

    /// Mark the broker as starting. `None` unless it was down.
    pub(crate) fn begin_start(&self) -> Option<CancellationToken> {
        let mut state = self.state.lock();
        if !matches!(&*state, State::Down) {
            return None;
        }
        let token = CancellationToken::new();
        *state = State::Starting(token.clone());
        Some(token)
    }

    /// A start that failed leaves the broker down, unless a crash got there
    /// first.
    pub(crate) fn start_failed(&self, start: &CancellationToken) {
        let mut state = self.state.lock();
        if !start.is_cancelled() && matches!(&*state, State::Starting(_)) {
            *state = State::Down;
        }
    }

    /// Open the broker on its directory, join the network and run it.
    pub(crate) async fn boot(
        &self,
        cp: &SimControlPlane,
        net: &SimNet,
        config: &SimConfig,
        run: u64,
    ) -> Result<()> {
        let mut broker_config = SimBrokerConfig::new(&self.id, self.dir.path());
        broker_config.advertise_addr = self.advertise;
        broker_config.fsync_mode = config.fsync_mode;
        let link = NodeLink::new(cp.link(), self.cut.clone());
        let opened = SimBroker::open(broker_config, link, self.catalog(cp))
            .await
            .with_context(|| format!("open {} (start {run})", self.id))?;

        // No await from here to the end: a crash lands between awaits, so it
        // finds either a starting broker or a running one, never half of one.
        let token = match &*self.state.lock() {
            State::Starting(token) if !token.is_cancelled() => token.clone(),
            _ => return Ok(()),
        };
        let registered = opened.incarnation_watch();
        let peer = Arc::new(net.start(SimNode {
            node_id: self.id.clone(),
            capabilities: opened.capabilities(),
            outgoing: opened.outgoing_capabilities(),
            identity: PeerIdentity {
                storage: Some(opened.storage_id()),
                incarnation: Incarnation::Unregistered,
            },
            handler: opened.handler(),
        }));
        let broker = Arc::new(opened.run(Arc::clone(&peer)));
        let handles = NodeHandles {
            index: self.index,
            node_id: self.id.clone(),
            broker: Arc::clone(broker.broker()),
            ingress: Arc::clone(broker.ingress()),
            lease: Arc::clone(broker.lease()),
            marks: Arc::clone(broker.quorum_marks()),
            crashed: token,
        };
        // A peer's handshake carries the incarnation the broker registered
        // under, once it has.
        tokio::spawn({
            let (net, crashed) = (net.clone(), handles.crashed.clone());
            let mut registered = registered;
            async move {
                loop {
                    if let Some(at) = *registered.borrow() {
                        net.set_identity(&peer, Incarnation::At(at));
                    }
                    tokio::select! {
                        () = crashed.cancelled() => return,
                        changed = registered.changed() => if changed.is_err() { return },
                    }
                }
            }
        });
        *self.state.lock() = State::Up(Running { handles, broker });
        Ok(())
    }

    /// Stop whatever is running or starting, as a crash does.
    pub(crate) async fn crash(&self, net: &SimNet) {
        let prior = std::mem::replace(&mut *self.state.lock(), State::Down);
        let broker = match prior {
            State::Down => return,
            State::Starting(token) => {
                token.cancel();
                None
            }
            State::Up(running) => {
                running.handles.crashed.cancel();
                Some(running.broker)
            }
        };
        net.crash(&self.id);
        let mut stopped_under = None;
        if let Some(mut broker) = broker {
            stopped_under = Some(broker.storage_id());
            // A census may hold the broker for the length of one snapshot.
            loop {
                match Arc::try_unwrap(broker) {
                    Ok(broker) => {
                        broker.crash();
                        break;
                    }
                    Err(shared) => {
                        broker = shared;
                        // A sleep, not a yield: virtual time must be free to
                        // move while the snapshot waits on a timer.
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            }
        }
        tokio::time::sleep(CRASH_DRAIN).await;
        // A crash of the process loses nothing the kernel accepted, so it is
        // equivalent to a stop with everything synced. Say so in the run
        // state: otherwise a restart takes a new storage id on a platform
        // that cannot sync the filesystem after a crash (everything but
        // Linux), and the broker refuses to lead what it led.
        if let Some(id) = stopped_under
            && let Err(err) = felix_storage::run_state::mark_clean(self.dir.path(), id)
        {
            tracing::error!(node = %self.id, error = %err, "could not record the crash as clean");
        }
    }

    /// The catalog sync's read of the control plane, behind this broker's
    /// switch and the HTTP client's deadline.
    fn catalog(&self, cp: &SimControlPlane) -> CatalogFetcher {
        let (cp, cut) = (cp.clone(), self.cut.clone());
        Arc::new(move |path: String| {
            let (cp, cut) = (cp.clone(), cut.clone());
            Box::pin(async move {
                let call = async { cp.request(axum::http::Method::GET, &path, None).await };
                let response = cut.through(REQUEST_TIMEOUT, call).await?;
                anyhow::ensure!(
                    response.status.is_success(),
                    "GET {path}: {}",
                    response.status
                );
                Ok::<Bytes, anyhow::Error>(response.body)
            })
        })
    }

    /// What the running broker believes about `key`, read at one instant.
    pub(crate) fn stand(&self, key: &ShardKey) -> Option<ShardStand> {
        Some(self.broker()?.shard_stand(key))
    }

    pub(crate) async fn snapshot(&self, key: &ShardKey) -> Option<ShardSnapshot> {
        let broker = self.broker()?;
        tokio::time::timeout(SNAPSHOT_WITHIN, broker.shard_snapshot(key))
            .await
            .ok()
    }

    /// Everything a census prints about this broker for `key`.
    pub(crate) async fn view(&self, key: &ShardKey) -> NodeView {
        let Some(broker) = self.broker() else {
            return NodeView {
                node_id: self.id.clone(),
                state: if self.is_starting() {
                    "starting"
                } else {
                    "down"
                },
                cut: self.cut.is_cut(),
                ..NodeView::default()
            };
        };
        let incarnation = *broker.incarnation_watch().borrow();
        NodeView {
            node_id: self.id.clone(),
            state: "up",
            cut: self.cut.is_cut(),
            seeded: broker.seeded().is_cancelled(),
            incarnation,
            lease_remaining: Some(broker.lease().remaining()),
            snapshot: tokio::time::timeout(SNAPSHOT_WITHIN, broker.shard_snapshot(key))
                .await
                .ok(),
        }
    }
}
