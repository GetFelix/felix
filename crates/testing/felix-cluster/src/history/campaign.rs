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

use super::model::{Consistency, History, ListSpec};
use super::nemesis::{ClusterView, Nemesis};
use super::rng::Rng;
use super::workload::{ClientKind, Workload};
use crate::{Cluster, ClusterConfig, StreamSpec, wait};

/// The seed, as a number or `random`.
pub const SEED_VAR: &str = "FELIX_HISTORY_SEED";

/// How long the nemesis runs, in seconds.
pub const DURATION_VAR: &str = "FELIX_HISTORY_DURATION_SECS";

/// How often the control plane re-plans. Short, so a failover starts soon
/// after a fault rather than at the end of a long quiet period.
const PLACEMENT_INTERVAL: Duration = Duration::from_millis(500);

/// How long taking a list's base or final read may take, faults healed.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);

/// What to run.
#[derive(Debug, Clone)]
pub struct Campaign {
    pub seed: u64,
    /// How long the nemesis runs. The clients start before it and stop after.
    pub duration: Duration,
    pub clients: usize,
    /// One single-shard `Quorum` stream per list.
    pub lists: Vec<String>,
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
    /// `FELIX_HISTORY_SEED` and `FELIX_HISTORY_DURATION_SECS`.
    pub fn from_env(default_seed: u64, default_duration: Duration) -> Result<Self> {
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
            duration,
            clients: 6,
            lists: (0..3).map(|i| format!("history-{i}")).collect(),
            op_timeout: Duration::from_secs(10),
            quiet: (Duration::from_secs(1), Duration::from_secs(4)),
            hold: (Duration::from_secs(2), Duration::from_secs(6)),
        })
    }

    /// Three brokers, and every list a `Quorum` stream on all three, set up
    /// for whatever `nemesis` may inject: proxied links for link faults, and
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
            nodes: 3,
            streams: self
                .lists
                .iter()
                .map(|list| StreamSpec::quorum(list, 1, 3))
                .collect(),
            proxy_links: nemesis.needs_proxy_links(),
            broker_env,
            ..ClusterConfig::default()
        }
    }

    /// Run the campaign against `cluster` and return what happened.
    ///
    /// Errors only when the harness cannot do its part: a fault that cannot
    /// be healed, or a list with no complete final read. Anything the
    /// brokers get wrong is in the history, for the checker.
    pub async fn run(&self, cluster: &mut Cluster, nemesis: &mut impl Nemesis) -> Result<History> {
        let mut rng = Rng::new(self.seed);
        let workload = Arc::new(Workload::new(
            cluster.tenant_id.clone(),
            cluster.namespace.clone(),
            cluster.client_token.clone(),
            self.lists.clone(),
            self.op_timeout,
            cluster.broker_addrs(),
        ));
        cluster.run_placement(PLACEMENT_INTERVAL);

        let mut lists = std::collections::BTreeMap::new();
        for list in &self.lists {
            // The harness's readiness probe is already in each stream; the
            // workload's records start at the tail.
            let read = settled_read(cluster, &workload, list).await?;
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
                tokio::spawn(Arc::clone(&workload).run_client(process, kind, client_rng))
            })
            .collect();

        let result = self.unleash(cluster, nemesis, &workload, &mut rng).await;
        workload.stop();
        for client in clients {
            client.await.context("a client task panicked")?;
        }
        result?;

        let mut final_reads = std::collections::BTreeMap::new();
        for list in &self.lists {
            let read = settled_read(cluster, &workload, list).await?;
            final_reads.insert(list.clone(), read.elements);
        }
        let (ops, faults) = workload.take();
        Ok(History {
            lists,
            ops,
            final_reads,
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
                continue;
            }
            tokio::time::sleep(rng.between(self.hold.0, self.hold.1)).await;
            fault
                .heal(cluster)
                .await
                .with_context(|| format!("heal {fault}"))?;
            workload.set_addrs(cluster.broker_addrs());
            workload.fault(format!("healed {fault}"));
        }
    }

    async fn view(&self, cluster: &Cluster) -> ClusterView {
        let mut leaders = Vec::new();
        for list in &self.lists {
            if let Ok(owners) = cluster.shard_owners_for(list).await {
                for owner in owners.into_values() {
                    if !leaders.contains(&owner) {
                        leaders.push(owner);
                    }
                }
            }
        }
        ClusterView {
            nodes: cluster.node_ids(),
            leaders,
        }
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

fn random_seed() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos() as u64);
    // Mixed so two runs started in the same second still differ widely.
    Rng::new(nanos ^ u64::from(std::process::id())).next_u64()
}
