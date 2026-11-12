//! A control plane that is a real three-member metadata Raft group.
//!
//! Each member is what the `felix-controlplane` binary runs on the Raft
//! backend: its own redb directory, a peer listener, the full HTTP API over
//! the Raft store, the expiry sweep and the placement reconciler, both gated
//! on this member holding the leadership. Brokers reach the group through
//! one stable address that forwards to any live member, as a load balancer
//! does, so killing a member does not move the control plane's address.
//!
//! Members talk to each other through a forwarder per direction, so a
//! partition is the forwarders refusing: it cuts the isolated member from
//! the others and the others from it, and nothing else.
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use felix_controlplane_service::api::types::{FeatureFlags, Region};
use felix_controlplane_service::api::{AppState, build_router};
use felix_controlplane_service::config::NodeLivenessConfig;
use felix_controlplane_service::model::Tenant;
use felix_controlplane_service::raft::{
    AppStateMachine, LeadershipGate, NodeId, PeerSecurity, RaftHandle, RaftSettings,
};
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::raft::RaftStore;
use felix_controlplane_service::store::raft::state_machine::MetadataStateMachine;
use felix_controlplane_service::store::{
    ControlPlaneAuthStore, ControlPlaneStore, StoreConfig, StoreError,
};
use tokio_util::sync::CancellationToken;

use crate::ports;

/// How many members the group has.
pub(crate) const MEMBERS: usize = 3;

/// The live members, by member: API address, the token that cuts its
/// connections, its store and its Raft handle.
type Live = Arc<Mutex<Vec<Option<(SocketAddr, CancellationToken, Arc<RaftStore>, RaftHandle)>>>>;

/// A forwarded address that can be cut: while closed it takes no traffic,
/// and closing it drops the connections already through.
#[derive(Clone)]
struct Gate(Arc<Mutex<(bool, CancellationToken)>>);

impl Gate {
    fn open() -> Self {
        Self(Arc::new(Mutex::new((true, CancellationToken::new()))))
    }

    fn set(&self, open: bool) {
        let mut state = self.0.lock().expect("gate lock");
        if open == state.0 {
            return;
        }
        if !open {
            state.1.cancel();
        } else {
            state.1 = CancellationToken::new();
        }
        state.0 = open;
    }

    fn current(&self) -> Option<CancellationToken> {
        let state = self.0.lock().expect("gate lock");
        state.0.then(|| state.1.clone())
    }
}

/// Accept on `listener` and forward each connection to the first of
/// `targets()` that answers, until the connection's token is cancelled.
fn forward<F>(
    runtime: &tokio::runtime::Handle,
    listener: tokio::net::TcpListener,
    targets: F,
) -> tokio::task::JoinHandle<()>
where
    F: Fn() -> Vec<(SocketAddr, CancellationToken)> + Send + Sync + 'static,
{
    runtime.spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                return;
            };
            let candidates = targets();
            tokio::spawn(async move {
                for (addr, cut) in candidates {
                    let Ok(Ok(mut outbound)) = tokio::time::timeout(
                        Duration::from_secs(1),
                        tokio::net::TcpStream::connect(addr),
                    )
                    .await
                    else {
                        continue;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                        _ = cut.cancelled() => {}
                    }
                    return;
                }
            });
        }
    })
}

struct Running {
    handle: RaftHandle,
    store: Arc<RaftStore>,
    /// Cancelled when the member is killed: cuts the connections the front
    /// forwards to it and stops its background loops.
    stop: CancellationToken,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

struct Member {
    id: NodeId,
    dir: tempfile::TempDir,
    peer: SocketAddr,
    api: SocketAddr,
    running: Option<Running>,
}

/// What a member needs to start, shared by every start of every member.
struct Setup {
    runtime: tokio::runtime::Handle,
    liveness: NodeLivenessConfig,
    security: PeerSecurity,
    /// `links[from][to]`: where `from` dials to reach `to`.
    links: Vec<Vec<Option<SocketAddr>>>,
    /// Interval the placement reconciler runs at, once asked for.
    placement: Mutex<Option<Duration>>,
    placement_wakes: Arc<felix_controlplane_service::cluster::placement::PlacementWakes>,
}

pub(crate) struct Group {
    setup: Arc<Setup>,
    members: tokio::sync::Mutex<Vec<Member>>,
    /// The gates of the forwarders from member `from` to member `to`.
    gates: Vec<Vec<Option<Gate>>>,
    /// Live API addresses and their stop tokens, read by the front.
    live: Live,
    forwarders: Vec<tokio::task::JoinHandle<()>>,
    /// Which member is isolated, if one is.
    isolated: Mutex<Option<usize>>,
    /// Tenants the group acknowledged to the harness. Every member must
    /// still hold each of them once the group has converged.
    acked: Mutex<Vec<String>>,
}

impl Group {
    /// Start three members and form the group. `front` is the address brokers use.
    pub(super) async fn start(
        runtime: tokio::runtime::Handle,
        liveness: NodeLivenessConfig,
        front: SocketAddr,
    ) -> Result<Self> {
        let mut members = Vec::new();
        for i in 0..MEMBERS {
            members.push(Member {
                id: (i + 1) as NodeId,
                dir: tempfile::tempdir().context("member directory")?,
                peer: ports::free_tcp()?,
                api: ports::free_tcp()?,
                running: None,
            });
        }
        let mut links = vec![vec![None; MEMBERS]; MEMBERS];
        let mut gates = vec![vec![None; MEMBERS]; MEMBERS];
        let mut forwarders = Vec::new();
        for from in 0..MEMBERS {
            for to in 0..MEMBERS {
                if from == to {
                    continue;
                }
                let addr = ports::free_tcp()?;
                let listener = tokio::net::TcpListener::bind(addr)
                    .await
                    .context("bind link")?;
                let gate = Gate::open();
                let target = members[to].peer;
                let link_gate = gate.clone();
                forwarders.push(forward(&runtime, listener, move || {
                    link_gate
                        .current()
                        .map(|cut| vec![(target, cut)])
                        .unwrap_or_default()
                }));
                links[from][to] = Some(addr);
                gates[from][to] = Some(gate);
            }
        }
        let live: Live = Arc::new(Mutex::new(vec![None; MEMBERS]));
        let front_live = Arc::clone(&live);
        let listener = tokio::net::TcpListener::bind(front)
            .await
            .context("bind front")?;
        let rotation = std::sync::atomic::AtomicUsize::new(0);
        forwarders.push(forward(&runtime, listener, move || {
            let mut live: Vec<_> = front_live
                .lock()
                .expect("live lock")
                .iter()
                .flatten()
                .map(|(addr, cut, _, _)| (*addr, cut.clone()))
                .collect();
            if !live.is_empty() {
                let turn = rotation.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let len = live.len();
                live.rotate_left(turn % len);
            }
            live
        }));

        let setup = Arc::new(Setup {
            runtime,
            liveness,
            security: PeerSecurity {
                cluster_id: "felix-cluster-metadata".to_string(),
                token: Some("0123456789abcdef0123456789abcdef".to_string()),
                tls: None,
            },
            links,
            placement: Mutex::new(None),
            placement_wakes: Default::default(),
        });
        let group = Self {
            setup,
            members: tokio::sync::Mutex::new(members),
            gates,
            live,
            forwarders,
            isolated: Mutex::new(None),
            acked: Mutex::new(Vec::new()),
        };
        {
            let mut members = group.members.lock().await;
            for index in 0..MEMBERS {
                group.run(&mut members, index).await?;
            }
            let addrs: BTreeMap<NodeId, String> =
                members.iter().map(|m| (m.id, m.peer.to_string())).collect();
            let first = &members[0].running.as_ref().expect("running").handle;
            first
                .initialize(addrs)
                .await
                .context("form the metadata group")?;
        }
        group.wait_for_leader(Duration::from_secs(30)).await?;
        Ok(group)
    }

    /// A store that writes through whichever member is up, for the harness's
    /// own seeding. Member `index`'s, when it is running.
    pub(super) async fn store_of(&self, index: usize) -> Option<Arc<RaftStore>> {
        let members = self.members.lock().await;
        members[index]
            .running
            .as_ref()
            .map(|r| Arc::clone(&r.store))
    }

    /// The store of a running member, for the harness's own reads and writes:
    /// the leader's when one is up, because a follower's copy can lag. It
    /// changes as members are killed and restarted, so a caller asks again
    /// each time rather than holding one.
    pub(super) fn live_store(&self) -> Option<Arc<RaftStore>> {
        // A cut-off member still believes it leads until it notices, and its
        // reads are stale and its writes cannot commit.
        let isolated = *self.isolated.lock().expect("isolated lock");
        let live = self.live.lock().expect("live lock");
        let running = || {
            live.iter()
                .enumerate()
                .filter(|(index, _)| Some(*index) != isolated)
                .filter_map(|(_, member)| member.as_ref())
        };
        running()
            .find(|(_, _, _, handle)| {
                let status = handle.status();
                status.leader == Some(status.id)
            })
            .or_else(|| running().next())
            .map(|(_, _, store, _)| Arc::clone(store))
    }

    /// Start member `index` on its directory: a new member on an empty one,
    /// a restart on a used one.
    async fn run(&self, members: &mut [Member], index: usize) -> Result<()> {
        let setup = Arc::clone(&self.setup);
        let (id, dir, peer, api) = {
            let m = &members[index];
            (m.id, m.dir.path().to_path_buf(), m.peer, m.api)
        };
        let started = self
            .setup
            .runtime
            .spawn(async move {
                let machine = Arc::new(MetadataStateMachine::new(Arc::new(InMemoryStore::new(
                    StoreConfig {
                        changes_limit: felix_controlplane_service::config::DEFAULT_CHANGES_LIMIT,
                        change_retention_max_rows: Some(10_000),
                    },
                ))));
                let mut settings = RaftSettings::new(id, dir, setup.security.clone());
                for (to, addr) in setup.links[index].iter().enumerate() {
                    if let Some(addr) = addr {
                        settings
                            .peer_addrs
                            .insert((to + 1) as NodeId, addr.to_string());
                    }
                }
                let handle =
                    RaftHandle::start(settings, Arc::clone(&machine) as Arc<dyn AppStateMachine>)
                        .await?;
                let store = Arc::new(RaftStore::new(handle.clone(), machine));
                let stop = CancellationToken::new();
                let mut tasks = Vec::new();

                let peers = tokio::net::TcpListener::bind(peer)
                    .await
                    .context("bind peer listener")?;
                let router = handle.rpc_router();
                tasks.push(tokio::spawn(async move {
                    let _ = axum::serve(peers, router).await;
                }));

                let state = AppState {
                    region: Region {
                        region_id: "local".to_string(),
                        display_name: "Local".to_string(),
                    },
                    api_version: "v1".to_string(),
                    features: FeatureFlags {
                        durable_storage: false,
                        tiered_storage: false,
                        bridges: false,
                    },
                    store: Arc::clone(&store) as Arc<dyn ControlPlaneAuthStore + Send + Sync>,
                    oidc_validator:
                        felix_controlplane_service::auth::oidc::UpstreamOidcValidator::default(),
                    bootstrap_enabled: false,
                    bootstrap_tokens: Vec::new(),
                    node_liveness: setup.liveness.clone(),
                    readiness: Arc::new(
                        felix_controlplane_service::api::readiness::Readiness::new(Arc::new(
                            felix_controlplane_service::api::readiness::AlwaysReady,
                        )),
                    ),
                    in_flight: Default::default(),
                    placement_wakes: Arc::clone(&setup.placement_wakes),
                    move_policy: Default::default(),
                    new_cluster: Default::default(),
                };
                let api_listener = tokio::net::TcpListener::bind(api)
                    .await
                    .context("bind api")?;
                tasks.push(tokio::spawn(async move {
                    let _ =
                        axum::serve(api_listener, build_router(state).into_make_service()).await;
                }));

                let gate = LeadershipGate::Leader(handle.clone());
                tasks.push(
                    felix_controlplane_service::cluster::membership::spawn_expiry_sweep(
                        Arc::clone(&store) as Arc<dyn ControlPlaneStore + Send + Sync>,
                        setup.liveness.clone(),
                        gate.clone(),
                        stop.clone(),
                    ),
                );
                if let Some(interval) = *setup.placement.lock().expect("placement lock") {
                    tasks.push(spawn_placement(
                        &setup,
                        &store,
                        gate,
                        interval,
                        stop.clone(),
                    ));
                }
                anyhow::Ok(Running {
                    handle,
                    store,
                    stop,
                    tasks,
                })
            })
            .await
            .context("start metadata member")??;
        self.live.lock().expect("live lock")[index] = Some((
            members[index].api,
            started.stop.clone(),
            Arc::clone(&started.store),
            started.handle.clone(),
        ));
        members[index].running = Some(started);
        Ok(())
    }

    /// Run placement on every member that holds the leadership, now and after
    /// each restart.
    pub(super) async fn run_placement(&self, interval: Duration) {
        *self.setup.placement.lock().expect("placement lock") = Some(interval);
        let mut members = self.members.lock().await;
        for member in members.iter_mut() {
            if let Some(running) = member.running.as_mut() {
                let _enter = self.setup.runtime.enter();
                running.tasks.push(spawn_placement(
                    &self.setup,
                    &running.store,
                    LeadershipGate::Leader(running.handle.clone()),
                    interval,
                    running.stop.clone(),
                ));
            }
        }
    }

    async fn wait_for_leader(&self, within: Duration) -> Result<()> {
        let deadline = Instant::now() + within;
        loop {
            {
                let members = self.members.lock().await;
                let running: Vec<_> = members.iter().filter_map(|m| m.running.as_ref()).collect();
                if !running.is_empty() && running.iter().all(|r| r.handle.status().leader.is_some())
                {
                    return Ok(());
                }
            }
            if Instant::now() > deadline {
                bail!("the metadata group has no leader after {within:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The member the running members agree leads, if they do.
    pub(crate) async fn leader(&self) -> Option<usize> {
        let members = self.members.lock().await;
        let leader = members
            .iter()
            .filter_map(|m| m.running.as_ref())
            .find_map(|r| r.handle.status().leader)?;
        members
            .iter()
            .position(|m| m.id == leader && m.running.is_some())
    }

    /// Stop member `index`: connections cut, loops stopped, the Raft handle
    /// shut down, state left on its directory. The member goes through a
    /// graceful shutdown, not a crash, because it runs in this process and
    /// cannot be killed without taking the harness with it.
    pub(crate) async fn kill(&self, index: usize) -> Result<()> {
        let mut members = self.members.lock().await;
        let running = members[index]
            .running
            .take()
            .with_context(|| format!("metadata member {index} is already down"))?;
        self.live.lock().expect("live lock")[index] = None;
        running.stop.cancel();
        for task in &running.tasks {
            task.abort();
        }
        let _ = running.handle.shutdown().await;
        Ok(())
    }

    /// Kill the leader. Returns which member it was.
    pub(crate) async fn kill_leader(&self) -> Result<usize> {
        let leader = self.leader().await.context("no leader to kill")?;
        self.kill(leader).await?;
        Ok(leader)
    }

    /// Start member `index` again on its directory.
    pub(crate) async fn restart(&self, index: usize) -> Result<()> {
        let mut members = self.members.lock().await;
        if members[index].running.is_some() {
            return Ok(());
        }
        self.run(&mut members, index).await
    }

    /// Cut member `index` off from the other two, in both directions.
    pub(crate) async fn isolate(&self, index: usize) {
        *self.isolated.lock().expect("isolated lock") = Some(index);
        self.set_links(index, false);
    }

    /// Isolate the leader. Returns which member it was.
    pub(crate) async fn isolate_leader(&self) -> Result<usize> {
        let leader = self.leader().await.context("no leader to isolate")?;
        self.isolate(leader).await;
        Ok(leader)
    }

    /// Reconnect whichever member is isolated.
    pub(crate) fn heal_partition(&self) {
        if let Some(index) = self.isolated.lock().expect("isolated lock").take() {
            self.set_links(index, true);
        }
    }

    fn set_links(&self, index: usize, open: bool) {
        for other in 0..MEMBERS {
            if other == index {
                continue;
            }
            for (from, to) in [(index, other), (other, index)] {
                if let Some(gate) = &self.gates[from][to] {
                    gate.set(open);
                }
            }
        }
    }

    /// Remember `id` as acknowledged without writing it, to test that the
    /// check for lost writes notices.
    #[cfg(test)]
    pub(super) fn claim_acknowledged(&self, id: &str) {
        self.acked.lock().expect("acked lock").push(id.to_string());
    }

    /// Write a new tenant through the group and remember it once the group
    /// acknowledges it. Retries while a leader is being elected, for up to 30s.
    pub(crate) async fn acknowledge_write(&self) -> Result<()> {
        let id = format!("ack-{}", self.acked.lock().expect("acked lock").len());
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut last = "no member is running".to_string();
        loop {
            if let Some(store) = self.live_store() {
                let write = store.create_tenant(Tenant {
                    tenant_id: id.clone(),
                    display_name: id.clone(),
                });
                match tokio::time::timeout(Duration::from_secs(5), write).await {
                    Ok(Ok(_)) => {
                        self.acked.lock().expect("acked lock").push(id);
                        return Ok(());
                    }
                    // A retry after a timeout may find the first attempt landed.
                    // A Conflict also comes from the metadata-version gate,
                    // so it counts only if the tenant is there.
                    Ok(Err(StoreError::Conflict(reason))) => {
                        let landed = store
                            .list_tenants()
                            .await
                            .is_ok_and(|t| t.iter().any(|t| t.tenant_id == id));
                        if landed {
                            self.acked.lock().expect("acked lock").push(id);
                            return Ok(());
                        }
                        last = format!("conflict: {reason}");
                    }
                    Ok(Err(err)) => last = format!("{err}"),
                    Err(_) => last = "no answer within 5s".to_string(),
                }
            }
            if Instant::now() > deadline {
                bail!("the metadata group acknowledged no write within 30s: {last}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Bring every member back and wait until they agree: one leader, every
    /// acknowledged write present, and the same metadata state. Members at
    /// the same applied index must hold identical state; members at different
    /// indexes (replica reports keep landing) must agree on everything the
    /// reports and heartbeats do not touch. Members that never agree are a
    /// metadata bug.
    pub(crate) async fn converge(&self, within: Duration) -> Result<()> {
        self.heal_partition();
        for index in 0..MEMBERS {
            self.restart(index).await?;
        }
        self.wait_for_leader(within).await?;
        let deadline = Instant::now() + within;
        loop {
            let (views, mismatch) = self.views().await?;
            match mismatch {
                None => return Ok(()),
                Some(reason) if Instant::now() > deadline => {
                    bail!("metadata members did not converge: {reason}; {views}")
                }
                Some(_) => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
    }

    /// Each member's applied index and state, and the first way they differ
    /// or lack an acknowledged write.
    async fn views(&self) -> Result<(String, Option<String>)> {
        let members = self.members.lock().await;
        let mut seen: Vec<View> = Vec::new();
        for member in members.iter() {
            let running = member.running.as_ref().context("a member is down")?;
            // The index before and after: a state read while it moved is not
            // a state at either.
            let before = running.handle.status().last_applied_index;
            let state = State::read(&running.store).await?;
            let after = running.handle.status().last_applied_index;
            seen.push(View {
                id: member.id,
                applied: (before == after).then_some(before).flatten(),
                state,
            });
        }
        let summary = seen
            .iter()
            .map(|v| {
                format!(
                    "member {}: applied {:?}, {} stable and {} volatile entries",
                    v.id,
                    v.applied,
                    v.state.stable.len(),
                    v.state.volatile.len()
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        let acked = self.acked.lock().expect("acked lock").clone();
        for view in &seen {
            if let Some(lost) = acked.iter().find(|id| !view.state.tenants.contains(*id)) {
                return Ok((
                    summary,
                    Some(format!(
                        "member {} lost the acknowledged write {lost}",
                        view.id
                    )),
                ));
            }
        }
        let first = &seen[0];
        let mismatch = seen.iter().skip(1).find_map(|other| {
            let same_index = first.applied.is_some() && first.applied == other.applied;
            let (what, differ) = if same_index {
                ("state", first.state != other.state)
            } else {
                ("stable state", first.state.stable != other.state.stable)
            };
            differ.then(|| {
                format!(
                    "{what} differs between member {} and {} (applied {:?} and {:?})",
                    first.id, other.id, first.applied, other.applied
                )
            })
        });
        Ok((summary, mismatch))
    }

    pub(super) async fn shutdown(&self) {
        self.heal_partition();
        let mut members = self.members.lock().await;
        for member in members.iter_mut() {
            if let Some(running) = member.running.take() {
                running.stop.cancel();
                for task in &running.tasks {
                    task.abort();
                }
                let _ = running.handle.shutdown().await;
            }
        }
        for task in &self.forwarders {
            task.abort();
        }
    }
}

struct View {
    id: NodeId,
    /// The applied index the state was read at, when it held still while
    /// being read.
    applied: Option<u64>,
    state: State,
}

/// What a member holds, as sorted lines.
#[derive(PartialEq)]
struct State {
    tenants: Vec<String>,
    /// Tenants, namespaces, streams, caches, shard assignments and the nodes'
    /// specs: nothing a heartbeat or a replica report rewrites.
    stable: Vec<String>,
    /// Node status and replica reports, which change while the group runs.
    volatile: Vec<String>,
}

impl State {
    async fn read(store: &RaftStore) -> Result<Self> {
        fn line(kind: &str, item: &impl serde::Serialize) -> String {
            format!(
                "{kind} {}",
                serde_json::to_string(item).expect("metadata encodes")
            )
        }
        let mut tenants = Vec::new();
        let mut stable = Vec::new();
        let mut volatile = Vec::new();
        for tenant in store.list_tenants().await.context("list tenants")? {
            tenants.push(tenant.tenant_id.clone());
            stable.push(line("tenant", &tenant));
            for namespace in store
                .list_namespaces(&tenant.tenant_id)
                .await
                .context("list namespaces")?
            {
                stable.push(line("namespace", &namespace));
                let (tenant_id, name) = (&tenant.tenant_id, &namespace.namespace);
                for stream in store
                    .list_streams(tenant_id, name)
                    .await
                    .context("list streams")?
                {
                    stable.push(line("stream", &stream));
                }
                for cache in store
                    .list_caches(tenant_id, name)
                    .await
                    .context("list caches")?
                {
                    stable.push(line("cache", &cache));
                }
            }
        }
        for node in store.list_nodes().await.context("list nodes")? {
            stable.push(line("node", &(&node.node_id, &node.spec)));
            volatile.push(line("node-status", &(&node.node_id, &node.status)));
        }
        for assignment in store
            .list_shard_assignments()
            .await
            .context("list assignments")?
        {
            stable.push(line("assignment", &assignment));
        }
        for report in store
            .list_replica_reports()
            .await
            .context("list replica reports")?
        {
            volatile.push(line("report", &report));
        }
        tenants.sort();
        stable.sort();
        volatile.sort();
        Ok(Self {
            tenants,
            stable,
            volatile,
        })
    }
}

fn spawn_placement(
    setup: &Setup,
    store: &Arc<RaftStore>,
    gate: LeadershipGate,
    interval: Duration,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let _enter = setup.runtime.enter();
    felix_controlplane_service::cluster::placement::spawn_reconciler(
        Arc::clone(store) as Arc<dyn ControlPlaneStore + Send + Sync>,
        setup.liveness.clone(),
        felix_controlplane_service::cluster::placement::MovePolicy {
            settle_millis: None,
            ..Default::default()
        },
        interval,
        "felix-cluster".to_string(),
        gate,
        Arc::clone(&setup.placement_wakes),
        stop,
    )
}
