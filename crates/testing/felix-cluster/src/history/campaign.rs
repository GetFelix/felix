//! A campaign: start the clients, let the nemesis loose for a while, heal
//! everything, take the final reads, and hand back the history.
//!
//! The seed fixes the fault schedule and every client's choice of operation
//! and list. It does not fix the interleaving, which is up to the scheduler
//! and the brokers, so a failing seed makes a failure likely to recur rather
//! than certain to.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};

use super::model::{Consistency, History, ListSpec, millis};
use super::nemesis::{ClusterView, Nemesis, ShardView};
use super::rng::Rng;
use super::workload::{ClientKind, Workload};
use crate::{CacheSpec, Cluster, ClusterConfig, StreamSpec, wait};

/// The seed, as a number or `random`.
pub const SEED_VAR: &str = "FELIX_HISTORY_SEED";

/// How long the nemesis runs, in seconds.
pub const DURATION_VAR: &str = "FELIX_HISTORY_DURATION_SECS";

/// Which replication path the brokers take: `lease` or `lease-free`.
pub const MODE_VAR: &str = "FELIX_HISTORY_MODE";

/// Which faults a long run injects: `all` for every single fault kind (the
/// default), or `adversarial` for the compound ones on four brokers.
pub const NEMESIS_VAR: &str = "FELIX_HISTORY_NEMESIS";

/// The fleet features the lease-free mode finalizes.
pub const LEASE_FREE_FEATURES: &[&str] = &[
    "generation_start",
    "majority_ack",
    "lease_free_reads",
    ATOMIC_COMMIT,
];

/// Finalized in every mode, so the workload can commit. It changes no
/// acknowledgement or read path, only whether a commit is accepted.
pub const ATOMIC_COMMIT: &str = "atomic_commit";

/// How long every broker has to turn the finalized features on.
const FEATURES_TIMEOUT: Duration = Duration::from_secs(30);

/// How often the control plane re-plans. Short, so a failover starts soon
/// after a fault rather than at the end of a long quiet period.
const PLACEMENT_INTERVAL: Duration = Duration::from_millis(500);

/// How long taking a list's base or final read may take, faults healed.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Which path `Quorum` writes and reads take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Only [`ATOMIC_COMMIT`] finalized: acknowledgements and reads rest on
    /// the leader's lease.
    Lease,
    /// [`LEASE_FREE_FEATURES`] finalized once the cluster is up: writes are
    /// acknowledged by a majority at the leader's generation and cache reads
    /// confirm leadership by a majority round.
    LeaseFree,
}

impl Mode {
    /// `FELIX_HISTORY_MODE`, or `default` when it is unset.
    pub fn from_env(default: Mode) -> Result<Self> {
        match std::env::var(MODE_VAR) {
            Err(_) => Ok(default),
            Ok(value) => match value.as_str() {
                "lease" => Ok(Mode::Lease),
                "lease-free" => Ok(Mode::LeaseFree),
                _ => bail!("{MODE_VAR}={value:?} is not `lease` or `lease-free`"),
            },
        }
    }

    pub fn features(self) -> &'static [&'static str] {
        match self {
            Mode::Lease => &[ATOMIC_COMMIT],
            Mode::LeaseFree => LEASE_FREE_FEATURES,
        }
    }
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Mode::Lease => "lease",
            Mode::LeaseFree => "lease-free",
        })
    }
}

/// What to run.
#[derive(Debug, Clone)]
pub struct Campaign {
    pub seed: u64,
    pub mode: Mode,
    /// How long the nemesis runs. The clients start before it and stop after.
    pub duration: Duration,
    /// How many brokers. Every list and the cache have three replicas, so a
    /// fourth broker is one a replica set can change to.
    pub nodes: usize,
    pub clients: usize,
    /// One single-shard `Quorum` stream per list.
    pub lists: Vec<String>,
    /// A single-shard `Quorum` cache, and the keys the clients use as
    /// registers.
    pub cache: String,
    pub keys: Vec<String>,
    /// How long one operation may take before its outcome is recorded as
    /// unknown.
    pub op_timeout: Duration,
    /// Quiet time between faults.
    pub quiet: (Duration, Duration),
    /// How long a fault stays in effect before it is healed.
    pub hold: (Duration, Duration),
}

impl Campaign {
    /// A campaign with the given defaults, overridden by
    /// `FELIX_HISTORY_SEED`, `FELIX_HISTORY_DURATION_SECS` and
    /// `FELIX_HISTORY_MODE`.
    pub fn from_env(
        default_seed: u64,
        default_duration: Duration,
        default_mode: Mode,
    ) -> Result<Self> {
        let seed = match std::env::var(SEED_VAR) {
            Err(_) => default_seed,
            Ok(value) if value == "random" => random_seed(),
            Ok(value) => value
                .parse()
                .with_context(|| format!("{SEED_VAR}={value:?} is not a number or `random`"))?,
        };
        let duration = match std::env::var(DURATION_VAR) {
            Err(_) => default_duration,
            Ok(value) => Duration::from_secs(
                value
                    .parse()
                    .with_context(|| format!("{DURATION_VAR}={value:?} is not whole seconds"))?,
            ),
        };
        Ok(Self {
            seed,
            mode: Mode::from_env(default_mode)?,
            duration,
            nodes: 3,
            clients: 6,
            lists: (0..3).map(|i| format!("history-{i}")).collect(),
            cache: "history-cache".to_string(),
            keys: (0..3).map(|i| format!("k{i}")).collect(),
            op_timeout: Duration::from_secs(10),
            quiet: (Duration::from_secs(1), Duration::from_secs(4)),
            hold: (Duration::from_secs(2), Duration::from_secs(6)),
        })
    }

    /// Four brokers rather than three, so a drain replaces follower copies
    /// and two brokers down at once can leave some shards a majority. What
    /// [`RandomNemesis::adversarial`](super::RandomNemesis::adversarial) is
    /// meant to run against.
    pub fn with_spare_broker(mut self) -> Self {
        self.nodes = 4;
        self
    }

    /// [`Self::nodes`] brokers, every list a `Quorum` stream and the cache a
    /// `Quorum` cache on three of them, set up for whatever `nemesis` may
    /// inject: proxied links for link faults, and
    /// flush-and-acknowledge-on-commit for fsync faults.
    pub fn cluster_config(&self, nemesis: &impl Nemesis) -> ClusterConfig {
        let mut broker_env = Vec::new();
        if nemesis.needs_fsync_on_commit() {
            for (name, value) in [
                ("FELIX_DURABLE_FSYNC_MODE", "on_commit"),
                ("FELIX_ACK_ON_COMMIT", "true"),
            ] {
                broker_env.push((name.to_string(), value.to_string()));
            }
        }
        ClusterConfig {
            nodes: self.nodes,
            streams: self
                .lists
                .iter()
                .map(|list| StreamSpec::quorum(list, 1, 3))
                .collect(),
            caches: vec![CacheSpec::quorum(&self.cache, 1, 3)],
            proxy_links: nemesis.needs_proxy_links(),
            broker_env,
            ..ClusterConfig::default()
        }
    }

    /// Start the cluster for `nemesis` and finalize the mode's fleet
    /// features, returning once every broker has turned them on.
    pub async fn start(&self, nemesis: &impl Nemesis) -> Result<Cluster> {
        let cluster = Cluster::start(self.cluster_config(nemesis)).await?;
        cluster
            .finalize_fleet_features(self.mode.features(), FEATURES_TIMEOUT)
            .await
            .with_context(|| format!("mode {}: the fleet features did not turn on", self.mode))?;
        Ok(cluster)
    }

    /// Run the campaign against `cluster` and return what happened.
    ///
    /// Errors only when the harness cannot do its part: a fault that cannot
    /// be healed, or a list with no complete final read. The error then
    /// carries the fault timeline and a [`Campaign::state_dump`]. Anything
    /// the brokers get wrong is in the history, for the checker.
    pub async fn run(&self, cluster: &mut Cluster, nemesis: &mut impl Nemesis) -> Result<History> {
        let workload = Arc::new(Workload::new(cluster, self));
        match self.run_with(cluster, nemesis, &workload).await {
            Ok(history) => Ok(history),
            Err(err) => {
                let (_, _, faults) = workload.take();
                let mut timeline = String::from("fault timeline:\n");
                for fault in faults {
                    timeline.push_str(&format!("  {} {}\n", millis(fault.at), fault.what));
                }
                let dump = self.state_dump(cluster).await;
                Err(anyhow!("{err:#}\n{timeline}{dump}"))
            }
        }
    }

    /// Who leads each of the workload's shards, at which generation and with
    /// which replicas, how far each replica had got, and what each broker's
    /// metrics say. What to read next to the fault timeline when a run fails.
    pub async fn state_dump(&self, cluster: &Cluster) -> String {
        super::dump::state(cluster, self).await
    }

    async fn run_with(
        &self,
        cluster: &mut Cluster,
        nemesis: &mut impl Nemesis,
        workload: &Arc<Workload>,
    ) -> Result<History> {
        let mut rng = Rng::new(self.seed);
        cluster.run_placement(PLACEMENT_INTERVAL);

        let mut lists = std::collections::BTreeMap::new();
        for list in &self.lists {
            // The harness's readiness probe is already in each stream; the
            // workload's records start at the tail.
            let read = settled_read(cluster, workload, list).await?;
            let base = read.tail.unwrap_or(0);
            workload.set_base(list, base);
            lists.insert(
                list.clone(),
                ListSpec {
                    consistency: Consistency::Quorum,
                    base,
                },
            );
        }

        let clients: Vec<_> = (0..self.clients)
            .map(|process| {
                let kind = if process % 2 == 0 {
                    ClientKind::Plain
                } else {
                    ClientKind::Idempotent
                };
                let client_rng = rng.fork();
                tokio::spawn(Arc::clone(workload).run_client(process, kind, client_rng))
            })
            .collect();

        let result = self.unleash(cluster, nemesis, workload, &mut rng).await;
        workload.stop();
        for client in clients {
            client.await.context("a client task panicked")?;
        }
        result?;

        let mut final_reads = std::collections::BTreeMap::new();
        for list in &self.lists {
            let read = settled_read(cluster, workload, list).await?;
            final_reads.insert(list.clone(), read.elements);
        }
        let (ops, registers, faults) = workload.take();
        let (commit_values, commit_reads) = workload.take_commits();
        Ok(History {
            lists,
            ops,
            final_reads,
            registers,
            commit_values,
            commit_reads,
            faults,
        })
    }

    /// Inject and heal faults until the duration is up.
    async fn unleash(
        &self,
        cluster: &mut Cluster,
        nemesis: &mut impl Nemesis,
        workload: &Workload,
        rng: &mut Rng,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + self.duration;
        loop {
            let quiet = rng.between(self.quiet.0, self.quiet.1);
            if tokio::time::Instant::now() + quiet >= deadline {
                tokio::time::sleep_until(deadline).await;
                return Ok(());
            }
            tokio::time::sleep(quiet).await;

            let view = self.view(cluster).await;
            let Some(fault) = nemesis.next_fault(rng, &view) else {
                continue;
            };
            workload.fault(format!("start {fault}"));
            if let Err(err) = fault.inject(cluster).await {
                // A pause that timed out waiting for the stop may still take
                // effect, so heal anyway rather than leave it behind.
                workload.fault(format!("could not {fault}: {err:#}"));
                let _ = fault.heal(cluster).await;
                workload.set_addrs(cluster.broker_addrs());
            } else {
                tokio::time::sleep(rng.between(self.hold.0, self.hold.1)).await;
                fault
                    .heal(cluster)
                    .await
                    .with_context(|| format!("heal {fault}"))?;
                workload.set_addrs(cluster.broker_addrs());
                workload.fault(format!("healed {fault}"));
            }
            // Nothing is drawn from `rng` here, so the check does not change
            // which faults a seed picks.
            let took = super::liveness::await_recovery(cluster, self, workload)
                .await
                .with_context(|| format!("after {fault}"))?;
            workload.fault(format!("every shard serving {} later", millis_of(took)));
        }
    }

    async fn view(&self, cluster: &Cluster) -> ClusterView {
        let owners = cluster.shard_owners().await.unwrap_or_default();
        let mut shards = Vec::new();
        for (kind, name) in self.shard_names() {
            let prefix = format!("{kind}/{}/{}/{name}/", cluster.tenant_id, cluster.namespace);
            let mut found: Vec<ShardView> = owners
                .iter()
                .filter_map(|(key, leader)| {
                    Some(ShardView {
                        kind,
                        name: name.to_string(),
                        shard: key.strip_prefix(&prefix)?.parse().ok()?,
                        leader: leader.clone(),
                    })
                })
                .collect();
            found.sort_by_key(|view| view.shard);
            shards.extend(found);
        }
        let mut leaders = Vec::new();
        for shard in &shards {
            if !leaders.contains(&shard.leader) {
                leaders.push(shard.leader.clone());
            }
        }
        ClusterView {
            nodes: cluster.node_ids(),
            leaders,
            shards,
        }
    }

    /// Every stream and cache the workload uses, lists first, as `(kind,
    /// name)`.
    pub(super) fn shard_names(&self) -> impl Iterator<Item = (&'static str, &str)> {
        self.lists
            .iter()
            .map(|list| ("stream", list.as_str()))
            .chain(std::iter::once(("cache", self.cache.as_str())))
    }
}

/// A complete read of `list` from its current leader, retried until the
/// cluster has settled enough to give one.
async fn settled_read(
    cluster: &Cluster,
    workload: &Workload,
    list: &str,
) -> Result<super::workload::Read> {
    let last = std::sync::Mutex::new(String::from("no attempt"));
    let found = wait::until_some(
        SETTLE_TIMEOUT,
        &format!("a complete read of {list}"),
        || {
            let last = &last;
            async move {
                let attempt = async {
                    let owner = cluster.shard_owner_of("stream", list, 0).await?;
                    let node = cluster
                        .node(&owner)
                        .ok_or_else(|| anyhow!("unknown leader {owner}"))?;
                    let read = workload
                        .read(node.client_addr, list, None, Duration::from_secs(20))
                        .await?;
                    if !read.complete {
                        bail!("the read stopped short of the tail");
                    }
                    Ok(read)
                };
                match attempt.await {
                    Ok(read) => Some(read),
                    Err(err) => {
                        *last.lock().expect("last error lock") = format!("{err:#}");
                        None
                    }
                }
            }
        },
    )
    .await;
    let last = last.into_inner().expect("last error lock");
    found.with_context(|| format!("last attempt: {last}"))
}

fn millis_of(elapsed: Duration) -> String {
    millis(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX))
}

fn random_seed() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos() as u64);
    // Mixed so two runs started in the same second still differ widely.
    Rng::new(nanos ^ u64::from(std::process::id())).next_u64()
}
