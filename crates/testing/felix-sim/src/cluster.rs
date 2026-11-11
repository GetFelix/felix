//! A whole cluster on one virtual clock.
//!
//! [`SimCluster`] is three to five real brokers (each [`SimBroker`] on its own
//! data directory), one [`SimControlPlane`] and one [`SimNet`], all in one
//! process on a paused, single-threaded tokio runtime. Nothing here touches a
//! socket, and waiting costs no real time, but opening each shard's log does,
//! so a run takes seconds of real time (release) for two minutes of cluster time.
//!
//! [`SimCluster::start`] builds the cluster and waits until it works:
//!
//! 1. A tenant, a namespace, one durable `Quorum` stream of 50 to 200 shards
//!    and one `Quorum` cache, all replicated three ways.
//! 2. Every broker started and registered.
//! 3. The fence and election features finalized, as an operator would
//!    (unless [`SimConfig::finalize_election`] is off).
//! 4. Every shard served by some broker.
//!
//! After that the nemesis and the clients take over (see [`crate::run`]).
//!
//! Faults reach the cluster through a few handles: [`SimCluster::crash`] and
//! [`SimCluster::restart`] for a broker, [`SimCluster::net`] for links,
//! [`SimCluster::cut_controlplane`] for one broker's path to the control plane,
//! and [`SimCluster::cp`] for the control plane itself.
//!
//! # What a crash is
//!
//! A crash stops every task of the broker at once and drops its in-memory
//! state, with no drain, deregistration or handoff. The data directory keeps
//! whatever the broker had written, which is what a process crash leaves: the
//! kernel keeps its page cache, so no unsynced bytes are lost. A restart opens
//! the same directory as a new incarnation.
//!
//! [`SimCluster::crash`] records the stop as clean. [`SimCluster::kill`]
//! leaves the run state as a kill after a failed fsync does, so the restart
//! takes a new storage id and runs `UncleanStart`.
//!
//! # What a power loss is
//!
//! [`SimCluster::power_loss`] cuts the power under one broker's data
//! directory, in a run that fsyncs on commit. The cut lands inside the next
//! append to one of its logs, so that append comes back torn, and every
//! unsynced byte elsewhere may be lost or torn (`felix_storage::sim_fs`). The
//! broker stops in the same instant, and its directory comes back as the
//! device held it. Recovery must repair the torn tail on the restart; the
//! [`TornTailRepaired`](crate::check::TornTailRepaired) checker says whether
//! it did.

mod census;
pub(crate) mod kills;
mod node;
pub(crate) mod power;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use felix_broker::Broker;
use felix_broker_service::cluster::lease::LeaseState;
use felix_broker_service::shards::routing::IngressRouter;
use felix_broker_service::sim::{ShardKey, ShardKind, ShardSnapshot, ShardStand};
use felix_cluster::history::rng::Rng;
use felix_common::fleet;
use felix_replication::peer::PeerTransportConfig;
use felix_replication::quorum::QuorumMarks;
use felix_storage::log::FsyncMode;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub use self::census::{Census, NodeView};
use self::kills::KillLog;
use self::node::Node;
use self::power::PowerLog;
pub use self::power::PowerLoss;
use crate::clock::{self, Owner};
use crate::controlplane::{Answer, CpConfig, SimControlPlane};
use crate::nemesis::Stop;
use crate::nemesis::clocks::{ClockPlan, ClockSkew};
use crate::net::{Link, SimNet};

/// The tenant, namespace, stream and cache every run uses.
pub const TENANT: &str = "sim";
pub const NAMESPACE: &str = "ns";
pub const STREAM: &str = "events";
pub const CACHE: &str = "kv";

/// What a run is built from. [`SimConfig::for_seed`] draws the shape from the
/// seed; set fields afterwards to pin one.
#[derive(Debug, Clone)]
pub struct SimConfig {
    pub brokers: usize,
    /// Shards of the stream.
    pub shards: u32,
    /// Shards of the cache.
    pub cache_shards: u32,
    pub replication_factor: u32,
    /// Logical clients publishing and putting.
    pub clients: usize,
    /// How many keys the cache clients share.
    pub cache_keys: usize,
    /// How long the clients run, from the end of startup.
    pub run_for: Duration,
    /// How long the nemesis may inject faults, from the end of startup. At
    /// its end everything is healed.
    pub fault_window: Duration,
    /// How long after the heal every shard must be served by one broker.
    pub settle_within: Duration,
    /// Whether to inject faults at all.
    pub faults: bool,
    /// Whether the nemesis also acts as an operator on placement: moves,
    /// drains, seat replacement, node removal, abandoned logs and kills
    /// between promotion and open. Off by default, so a seed's schedule stays
    /// what it was; [`SimConfig::with_placement_faults`] turns it on.
    pub placement_faults: bool,
    /// The most real time a run may take, startup included. A run past it
    /// stops and reports a stall. Separately, a run whose virtual time stands
    /// still for four minutes of real time stalls: a broker loop that never
    /// waits.
    pub real_time_limit: Duration,
    /// The storage sync mode. `None` costs the least real time; see
    /// [`SimBrokerConfig::fsync_mode`](felix_broker_service::sim::SimBrokerConfig::fsync_mode)
    /// for what a restart does to the storage id under it.
    pub fsync_mode: FsyncMode,
    /// Crashes that would be kills cut the power instead. Needs
    /// `FsyncMode::OnCommit`: under the others a broker acknowledges records
    /// it has not synced, and the simulator cannot make the restart see a new
    /// boot of the machine, which is how a real broker learns to doubt them.
    pub power_loss: bool,
    pub controlplane: CpConfig,
    /// Whether each node's clocks are skewed, and how far
    /// ([`crate::nemesis::clocks`]). Anything but `Off` needs a build with
    /// `--cfg tokio_unstable`.
    pub clocks: ClockSkew,
    /// A clock plan to use instead of drawing one from the seed under
    /// `clocks`, to pin a particular skew.
    pub clock_plan: Option<ClockPlan>,
    /// Whether startup finalizes the fence and election features. A test
    /// that finalizes them itself, at a moment it picks, turns this off.
    pub finalize_election: bool,
    /// When `record_generations` is finalized, counted from the start of the
    /// run's clients and faults (zero finalizes it with the election features
    /// at startup). `Some` also gives the brokers small segments, so their
    /// logs roll onto format v9 after it.
    pub finalize_record_generations: Option<Duration>,
}

impl SimConfig {
    /// A run's shape from its seed: 3 to 5 brokers, 50 to 200 shards.
    pub fn for_seed(seed: u64) -> Self {
        let mut rng = Rng::new(seed ^ 0x5348_4150_4553);
        Self {
            brokers: 3 + rng.below(3) as usize,
            shards: 50 + rng.below(151) as u32,
            cache_shards: 8,
            replication_factor: 3,
            clients: 6,
            cache_keys: 12,
            run_for: Duration::from_secs(120),
            fault_window: Duration::from_secs(90),
            settle_within: Duration::from_secs(60),
            faults: true,
            placement_faults: false,
            real_time_limit: Duration::from_secs(480),
            fsync_mode: FsyncMode::None,
            power_loss: false,
            controlplane: CpConfig::default(),
            clocks: ClockSkew::Off,
            clock_plan: None,
            finalize_election: true,
            finalize_record_generations: None,
        }
    }

    /// Whether any node's clock differs from the shared one.
    pub fn skews_clocks(&self) -> bool {
        self.clocks != ClockSkew::Off || self.clock_plan.is_some()
    }
}

/// Copies placement may have in flight at once with placement faults on. The
/// default of one would leave a drain moving a shard at a time, too slow for
/// two promotions to land on a shard while its seat is being replaced.
const PLACEMENT_FAULT_MOVES: usize = 4;

impl SimConfig {
    /// The same run with placement faults on (see
    /// [`SimConfig::placement_faults`]).
    pub fn with_placement_faults(mut self) -> Self {
        self.placement_faults = true;
        self.controlplane.moves.max_concurrent = PLACEMENT_FAULT_MOVES;
        self
    }

    /// The same run with power cuts in place of kills (see
    /// [`SimConfig::power_loss`]), on brokers that fsync on commit.
    pub fn with_power_loss(mut self) -> Self {
        self.power_loss = true;
        self.fsync_mode = FsyncMode::OnCommit;
        self
    }
}

/// The handles a client needs to reach one running broker.
///
/// They belong to one incarnation: after a crash they keep working against
/// the dead broker's memory, which is why every client call is raced against
/// [`NodeHandles::crashed`].
#[derive(Clone)]
pub struct NodeHandles {
    pub index: usize,
    pub node_id: String,
    pub broker: Arc<Broker>,
    pub ingress: Arc<IngressRouter>,
    pub lease: Arc<LeaseState>,
    pub marks: Arc<QuorumMarks>,
    /// The same marks read against this node's router.
    pub engine: Arc<felix_broker_service::shards::routing::StreamEngine>,
    /// Cancelled when this incarnation crashes.
    pub crashed: CancellationToken,
    /// Whether the run cuts the power ([`SimConfig::power_loss`]).
    pub power_loss: bool,
}

/// The cluster. Cloning the `Arc` is how tasks share it.
pub struct SimCluster {
    pub config: SimConfig,
    pub seed: u64,
    /// Each node's clocks at the start and the changes the run makes.
    pub clock_plan: ClockPlan,
    cp: SimControlPlane,
    net: SimNet,
    nodes: Vec<Node>,
    started: Instant,
    /// Starts of any broker so far, so each run of one is told apart.
    starts: AtomicU64,
    stalled: CancellationToken,
    /// Tells the stall watch to stop when the cluster is dropped.
    done: Arc<AtomicBool>,
    kills: parking_lot::Mutex<KillLog>,
    power: parking_lot::Mutex<PowerLog>,
}

impl Drop for SimCluster {
    fn drop(&mut self) {
        self.done.store(true, Ordering::Relaxed);
    }
}

impl SimCluster {
    /// Build the cluster and wait until every shard is served.
    ///
    /// Installs the virtual lease clock for this thread, so call it on the
    /// runtime the run will use.
    pub async fn start(seed: u64, config: SimConfig) -> Result<Arc<Self>> {
        if config.power_loss && config.fsync_mode != FsyncMode::OnCommit {
            bail!(
                "power loss needs FsyncMode::OnCommit, not {:?}",
                config.fsync_mode
            );
        }
        let clock_plan = config.clock_plan.clone().unwrap_or_else(|| {
            ClockPlan::generate(seed, config.clocks, config.brokers, config.fault_window)
        });
        clock::install(clock_plan.initial.clone());
        // Election backoff draws its jitter on this thread.
        felix_common::jitter::sim::seed(seed);
        let cp = clock::on(
            Owner::ControlPlane,
            SimControlPlane::new(config.controlplane.clone()),
        )
        .await?;
        let net = SimNet::new(PeerTransportConfig::default(), seed);
        let nodes = (0..config.brokers)
            .map(|index| Node::new(index, &config))
            .collect::<Result<Vec<_>>>()?;
        let (stalled, done) = spawn_stall_watch(config.real_time_limit);
        let reconcile = config.controlplane.reconcile_interval();
        let cluster = Arc::new(Self {
            config,
            seed,
            clock_plan,
            cp,
            net,
            nodes,
            started: Instant::now(),
            starts: AtomicU64::new(0),
            stalled,
            done,
            kills: parking_lot::Mutex::new(KillLog::new(reconcile)),
            power: Default::default(),
        });
        cluster.reset_links();
        cluster.create_catalog().await?;
        cluster.spawn_controlplane_driver();
        for index in 0..cluster.nodes.len() {
            cluster.restart(index);
        }
        cluster
            .wait_registered(Duration::from_secs(120))
            .await
            .context("the brokers did not all register")?;
        if cluster.config.finalize_election {
            cluster
                .cp
                .finalize_election()
                .await
                .context("finalize the election features")?;
        }
        if cluster.config.finalize_record_generations == Some(Duration::ZERO) {
            cluster
                .cp
                .finalize_feature(fleet::RECORD_GENERATIONS.name())
                .await
                .context("finalize record_generations")?;
        }
        cluster
            .wait_every_shard_served(Duration::from_secs(180))
            .await
            .context("startup: not every shard is served")?;
        Ok(cluster)
    }

    async fn create_catalog(&self) -> Result<()> {
        let config = &self.config;
        self.cp.create_tenant(TENANT).await?;
        self.cp.create_namespace(TENANT, NAMESPACE).await?;
        self.cp
            .create_durable_stream(
                TENANT,
                NAMESPACE,
                STREAM,
                config.shards,
                config.replication_factor,
            )
            .await?;
        self.cp
            .create_quorum_cache(
                TENANT,
                NAMESPACE,
                CACHE,
                config.cache_shards,
                config.replication_factor,
            )
            .await?;
        Ok(())
    }

    /// Run the control plane's loops: the expiry sweep and the placement pass
    /// on their intervals, and a pass whenever a registration or report asks
    /// for one. A paused or down control plane is not stepped.
    fn spawn_controlplane_driver(&self) {
        let cp = self.cp.clone();
        clock::as_owner(Owner::ControlPlane, || {
            tokio::spawn(async move {
                let config = cp.config().clone();
                let mut sweep = tokio::time::interval(config.sweep_interval());
                let mut reconcile = tokio::time::interval(config.reconcile_interval());
                sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        _ = sweep.tick() => { cp.sweep().await; }
                        _ = reconcile.tick() => { cp.reconcile().await; }
                        () = cp.pass_requested() => { cp.reconcile().await; }
                    }
                }
            })
        });
    }

    /// Cancelled when the run stalls or uses its real-time limit
    /// ([`SimConfig::real_time_limit`]).
    pub fn stalled(&self) -> &CancellationToken {
        &self.stalled
    }

    pub fn cp(&self) -> &SimControlPlane {
        &self.cp
    }

    pub fn net(&self) -> &SimNet {
        &self.net
    }

    /// Put every link back to its healthy state: a short, seeded delay, and
    /// no loss. A real network is never instant, and the delay is what lets
    /// requests from different clients overlap.
    pub fn reset_links(&self) {
        self.net.heal();
        let mut rng = Rng::new(self.seed ^ 0x4c49_4e4b);
        for from in 0..self.nodes.len() {
            for to in 0..self.nodes.len() {
                if from != to {
                    let delay = rng.between(Duration::from_micros(300), Duration::from_millis(3));
                    self.net.set_link(
                        self.node_id(from),
                        self.node_id(to),
                        Link {
                            delay,
                            ..Link::default()
                        },
                    );
                }
            }
        }
    }

    /// Virtual time since the cluster was built.
    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    pub fn broker_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn node_id(&self, index: usize) -> &str {
        &self.nodes[index].id
    }

    pub fn index_of(&self, node_id: &str) -> Option<usize> {
        self.nodes.iter().position(|node| node.id == node_id)
    }

    /// The running broker at `index`, or `None` while it is down or starting.
    pub fn handles(&self, index: usize) -> Option<NodeHandles> {
        self.nodes[index].handles()
    }

    /// Every running broker.
    pub fn running(&self) -> Vec<NodeHandles> {
        (0..self.nodes.len())
            .filter_map(|index| self.handles(index))
            .collect()
    }

    pub fn is_running(&self, index: usize) -> bool {
        self.handles(index).is_some()
    }

    /// Whether the broker at `index` is down, with no start under way.
    pub fn is_down(&self, index: usize) -> bool {
        self.nodes[index].is_down()
    }

    /// Stop the broker at `index` as a crash does, whether it is running or
    /// still starting, and record the stop as clean. A no-op on one that is
    /// down.
    pub async fn crash(&self, index: usize) {
        self.note_state().await;
        self.nodes[index].crash(&self.net, Stop::Clean).await;
    }

    /// Like [`Self::crash`], but the data directory is left as a kill does
    /// when the run may have lost records it acknowledged: the next start
    /// takes a new storage id and runs its unclean-start handling. Only a
    /// run that does not fsync on commit (the default) can be left that way.
    pub async fn kill(&self, index: usize) {
        self.note_state().await;
        let at = self.elapsed();
        if self.nodes[index].crash(&self.net, Stop::Kill).await {
            self.kills.lock().kill(&self.nodes[index].id, at);
        }
    }

    /// Cut the power under the broker at `index`, running or starting, and
    /// leave it down with what its device held. The cut waits up to
    /// [`power::APPEND_WITHIN`] for an append to land in, then goes without
    /// one. Returns whether it landed in an append. A no-op that returns
    /// `false` on a broker that is down.
    pub async fn power_loss(self: &Arc<Self>, index: usize) -> bool {
        if self.nodes[index].is_down() {
            return false;
        }
        self.note_state().await;
        let at = self.elapsed();
        let node = &self.nodes[index];
        let seed = self.seed ^ self.power.lock().next_seed();
        let cut = CancellationToken::new();
        let torn_slot = Arc::new(parking_lot::Mutex::new(None));
        node.arm_power_cut(seed, {
            let (cluster, cut, slot) = (Arc::downgrade(self), cut.clone(), Arc::clone(&torn_slot));
            move |segment: &std::path::Path, from: u64| {
                // Inside the broker's write: stop it before it runs on.
                if let Some(cluster) = cluster.upgrade() {
                    cluster.nodes[index].power_off(&cluster.net);
                }
                *slot.lock() = Some((segment.to_path_buf(), from));
                cut.cancel();
            }
        });
        let mid_append = tokio::select! {
            biased;
            () = cut.cancelled() => true,
            () = tokio::time::sleep(power::APPEND_WITHIN) => false,
        };
        let torn = torn_slot.lock().take();
        if !mid_append {
            node.cut_power(seed);
            node.power_off(&self.net);
        }
        node.crash(&self.net, Stop::PowerLoss).await;
        node.reboot();
        // Counted once the broker is down: a start that ran into this cut can
        // finish its recovery after the cut was asked for, and that repair
        // belongs to the previous loss.
        let repaired_before = node.torn_tails_repaired().len();
        let opened_before = node.segments_opened().len();
        // A segment whose name did not survive, or that holds nothing past
        // where the append began, leaves recovery nothing to repair.
        let torn =
            torn.filter(|(segment, from)| power::needs_repair(node.file_len(segment), *from));
        let torn = torn.map(|(segment, _)| segment);
        self.power.lock().record(PowerLoss {
            node: index,
            at,
            torn,
            repaired_before,
            opened_before,
            restarted_at: None,
        });
        mid_append
    }

    /// Every power loss so far.
    pub fn power_losses(&self) -> Vec<PowerLoss> {
        self.power.lock().losses().to_vec()
    }

    /// The segments, below the data directory, whose torn tails recovery
    /// repaired on the broker at `index`, in order over the whole run.
    pub fn torn_tails_repaired(&self, index: usize) -> Vec<std::path::PathBuf> {
        self.nodes[index].torn_tails_repaired()
    }

    /// The segments, below the data directory, the broker at `index` has
    /// opened, in order, once for each stretch between power losses.
    pub fn segments_opened(&self, index: usize) -> Vec<std::path::PathBuf> {
        self.nodes[index].segments_opened()
    }

    /// Shards (`stream/3`) the broker holds on purpose after unclean kills,
    /// until an operator abandons a member, so they are not expected to be
    /// served. See `kills.rs` for the rule.
    pub async fn held_shards(&self) -> BTreeSet<String> {
        self.holds().await.held
    }

    /// The held shards, and the unserved ones with a copy still untrusted
    /// although a broker served the shard while it was up.
    pub(crate) async fn holds(&self) -> Holds {
        self.note_state().await;
        let Ok(mut views) = self.cp.shard_views().await else {
            return Holds::default();
        };
        let unserved: Vec<ShardKey> = self
            .unserved()
            .await
            .into_iter()
            .filter(|(_, serving)| serving.is_empty())
            .map(|(key, _)| key)
            .collect();
        let names: BTreeSet<String> = unserved.iter().map(describe).collect();
        views.retain(|shard, _| names.contains(shard));
        let mut trust = kills::Trust::new();
        for key in &unserved {
            for node in &self.nodes {
                if let Some(copy) = node.copy_trust(key).await {
                    trust.insert((describe(key), node.id.clone()), copy);
                }
            }
        }
        let kills = self.kills.lock();
        Holds {
            held: kills.held(&views, &trust),
            reasons: kills
                .held(&views, &trust)
                .into_iter()
                .map(|shard| {
                    let why = kills.explain(&shard, &views[&shard], &trust);
                    (shard, why)
                })
                .collect(),
            vouchable: kills.vouchable_untrusted(&views, &trust),
        }
    }

    /// Record who could vouch whom and which restarted copies are trusted
    /// now. Called before each fault and by the sampler, so a stop or a loss
    /// of trust in between is judged against what was seen just before it.
    pub(crate) async fn note_state(&self) {
        self.note_serving().await;
        self.note_trusted().await;
    }

    /// Record which running brokers are up beside a broker serving each
    /// shard, so the hold rule knows which copies a leader could have
    /// vouched, and in what role (learner or voter) it reached each. The
    /// brokers are read without awaiting, so they are seen at one instant.
    async fn note_serving(&self) {
        let Ok(views) = self.cp.shard_views().await else {
            return;
        };
        let at = self.elapsed();
        let mut kills = self.kills.lock();
        for key in self.shard_keys() {
            let stands = self.stands(&key);
            if !stands.iter().any(|(_, stand)| stand.serving) {
                continue;
            }
            let shard = describe(&key);
            // A voter is vouched over a link that is up; readmitting a
            // learner also needs a control plane that answers.
            let delay = kills::cp_delay(self.cp.fault());
            let servers: Vec<(usize, u64)> = stands
                .iter()
                .filter(|(_, stand)| stand.serving)
                .map(|(index, stand)| (*index, stand.generation.unwrap_or(0)))
                .collect();
            let link_up = |from: usize, to: usize| {
                !self
                    .net
                    .link(&self.nodes[from].id, &self.nodes[to].id)
                    .partitioned
            };
            for (index, _) in &stands {
                let accepted = self.nodes[*index].accepted_generation(&key);
                let serving = kills::unsuperseded(&servers, accepted);
                if kills::reaches(&serving, *index, &link_up) {
                    let id = &self.nodes[*index].id;
                    let learner = views
                        .get(&shard)
                        .is_some_and(|view| view.joining.contains(id));
                    kills.served(&shard, id, at, delay, learner);
                }
            }
        }
    }

    /// Note the copies of brokers that started unclean which are trusted
    /// now, so a later loss of trust is not put down to that start.
    async fn note_trusted(&self) {
        let starts = self.kills.lock().unclean_nodes();
        for node in self.nodes.iter().filter(|node| starts.contains(&node.id)) {
            for key in self.shard_keys() {
                if node.copy_trust(&key).await == Some(kills::Copy::Trusted) {
                    self.kills.lock().trusted(&describe(&key), &node.id);
                }
            }
        }
    }

    /// Abandon `key`'s log through the operator API. On success the shard is
    /// no longer held.
    pub async fn abandon(&self, key: &ShardKey) -> Answer {
        let answer = self.cp.abandon(key).await;
        if answer.ok {
            self.kills.lock().abandon(&describe(key), self.elapsed());
        }
        answer
    }

    /// Whether the broker at `index` is running and started after an unclean
    /// stop.
    pub fn started_unclean(&self, index: usize) -> Option<bool> {
        self.nodes[index].started_unclean()
    }

    /// Start the broker at `index` on its data directory, as a new
    /// incarnation. The start runs in the background: it waits for the
    /// control plane to answer, as a real start does, so it can take as long
    /// as a fault on the control plane lasts. A no-op unless the broker is
    /// down.
    pub fn restart(self: &Arc<Self>, index: usize) {
        let Some(start) = self.nodes[index].begin_start() else {
            return;
        };
        let now = self.elapsed();
        let lost_power = self.power.lock().restart(index, now);
        let cluster = Arc::clone(self);
        let run = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
        tokio::spawn(async move {
            let node = &cluster.nodes[index];
            let booted = tokio::select! {
                () = start.cancelled() => return,
                booted = node.boot(&cluster.cp, &cluster.net, &cluster.config, run) => booted,
            };
            if let Err(err) = booted {
                tracing::error!(node = %node.id, error = %format!("{err:#}"), "broker did not start");
                node.start_failed(&start);
                return;
            }
            // A broker that doubts what it acknowledged before the power went
            // holds shards as it does after a kill, so the copy counts as one.
            if node.started_unclean() == Some(true) {
                let mut kills = cluster.kills.lock();
                kills.started_unclean(&node.id, cluster.elapsed());
                if let Some(loss) = lost_power {
                    kills.kill(&node.id, loss.at);
                }
            }
        });
    }

    /// Cut the broker at `index` off from the control plane, or reconnect it.
    pub fn cut_controlplane(&self, index: usize, cut: bool) {
        self.nodes[index].cut.set(cut);
    }

    pub fn controlplane_cut(&self, index: usize) -> bool {
        self.nodes[index].cut.is_cut()
    }

    /// Every shard of the stream and of the cache.
    pub fn shard_keys(&self) -> impl Iterator<Item = ShardKey> + use<> {
        let stream = self.config.shards;
        let cache = self.config.cache_shards;
        (0..stream).map(stream_key).chain((0..cache).map(cache_key))
    }

    /// What broker `index` believes about `key`. `None` if it is not running.
    pub async fn snapshot(&self, index: usize, key: &ShardKey) -> Option<ShardSnapshot> {
        self.nodes[index].snapshot(key).await
    }

    /// What every running broker believes about `key`, all read at the same
    /// instant: nothing here awaits, so on the run's single-threaded runtime no
    /// broker moves between two of the reads.
    pub fn stands(&self, key: &ShardKey) -> Vec<(usize, ShardStand)> {
        (0..self.nodes.len())
            .filter_map(|index| Some((index, self.nodes[index].stand(key)?)))
            .collect()
    }

    /// How many running brokers would take a write for `key` right now.
    pub async fn servers_of(&self, key: &ShardKey) -> Vec<usize> {
        let mut serving = Vec::new();
        for index in 0..self.nodes.len() {
            if let Some(snapshot) = self.snapshot(index, key).await
                && snapshot.serving
            {
                serving.push(index);
            }
        }
        serving
    }

    /// Keys with no server, and keys with more than one.
    pub async fn unserved(&self) -> Vec<(ShardKey, Vec<usize>)> {
        let mut bad = Vec::new();
        for key in self.shard_keys() {
            let serving = self.servers_of(&key).await;
            if serving.len() != 1 {
                bad.push((key, serving));
            }
        }
        bad
    }

    async fn wait_registered(&self, within: Duration) -> Result<()> {
        let deadline = Instant::now() + within;
        loop {
            let registered = self
                .nodes
                .iter()
                .filter(|node| node.registered_incarnation().is_some())
                .count();
            if registered == self.nodes.len() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("{registered} of {} brokers registered", self.nodes.len());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn wait_every_shard_served(&self, within: Duration) -> Result<()> {
        let deadline = Instant::now() + within;
        loop {
            let bad = self.unserved().await;
            if bad.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!(
                    "{} of {} shards are not served by exactly one broker, for example {}",
                    bad.len(),
                    self.shard_keys().count(),
                    describe(&bad[0].0)
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// A table of what every broker and the control plane believe about `key`.
    pub async fn census(&self, key: &ShardKey) -> Census {
        census::take(self, key).await
    }

    pub(crate) fn nodes(&self) -> &[Node] {
        &self.nodes
    }
}

/// How long virtual time may stand still, in real time, before the run is
/// called stalled.
const STALL_AFTER: Duration = Duration::from_secs(240);

/// Watch for a run that has stopped making virtual progress.
///
/// Tokio's paused clock moves only when every task is idle. A broker loop
/// that always has work, such as a retry that is due the moment it is looked
/// at, never lets the runtime idle, so the clock stops and the run hangs. A
/// real clock would not wait for the broker; here the stall is the finding.
///
/// A task on the runtime records the virtual time every 50 ms, which it can
/// only do while the clock moves. A thread of its own watches that record,
/// and cancels the returned token when it has not changed for [`STALL_AFTER`]
/// of real time, or when the run has taken `limit` of real time in all.
fn spawn_stall_watch(limit: Duration) -> (CancellationToken, Arc<AtomicBool>) {
    let progress = Arc::new(AtomicU64::new(0));
    let stalled = CancellationToken::new();
    let done = Arc::new(AtomicBool::new(false));
    tokio::spawn({
        let progress = Arc::clone(&progress);
        let began = Instant::now();
        async move {
            loop {
                progress.store(began.elapsed().as_millis() as u64 + 1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    });
    std::thread::Builder::new()
        .name("sim-stall-watch".into())
        .spawn({
            let (progress, stalled, done) = (progress, stalled.clone(), Arc::clone(&done));
            move || {
                let began = std::time::Instant::now();
                let (mut seen, mut since) = (0, std::time::Instant::now());
                while !done.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(500));
                    let now = progress.load(Ordering::Relaxed);
                    if now != seen {
                        (seen, since) = (now, std::time::Instant::now());
                    }
                    if since.elapsed() > STALL_AFTER || began.elapsed() > limit {
                        stalled.cancel();
                        return;
                    }
                }
            }
        })
        .expect("start the stall watch");
    (stalled, done)
}

/// What [`SimCluster::holds`] found among the shards nobody serves.
#[derive(Debug, Default)]
pub(crate) struct Holds {
    /// Held on purpose: exempt from serving.
    pub(crate) held: BTreeSet<String>,
    /// For each held shard, the injected faults that explain its copies.
    pub(crate) reasons: BTreeMap<String, Vec<String>>,
    /// With a copy still untrusted although a leader could have vouched it.
    pub(crate) vouchable: BTreeSet<String>,
}

/// The key of stream shard `shard`.
pub fn stream_key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard,
        kind: ShardKind::Stream,
    }
}

/// The key of cache shard `shard`.
pub fn cache_key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: CACHE.to_string(),
        shard,
        kind: ShardKind::Cache,
    }
}

/// `stream/3` or `cache/0`, for messages.
pub fn describe(key: &ShardKey) -> String {
    let kind = match key.kind {
        ShardKind::Stream => "stream",
        ShardKind::Cache => "cache",
    };
    format!("{kind}/{}", key.shard)
}

#[cfg(test)]
mod tests;
