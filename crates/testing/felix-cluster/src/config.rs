//! What a test asks for when it starts a cluster.
use std::time::Duration;

/// How to build a cluster.
#[derive(Clone)]
pub struct ClusterConfig {
    pub nodes: usize,
    pub tenant_id: String,
    pub namespace: String,
    /// Streams to create.
    pub streams: Vec<StreamSpec>,
    /// Caches to create. Empty by default, so a test that says nothing about
    /// caches builds the cluster it always did.
    pub caches: Vec<CacheSpec>,
    /// Inherit the parent's stdout/stderr rather than discarding it. Useful when
    /// running the harness by hand; noisy inside a test.
    pub inherit_output: bool,
    /// How many client-facing QUIC listeners each broker binds, on consecutive
    /// ports. One by default, which is what every test that says nothing about
    /// listeners gets.
    pub quic_listeners: usize,
    /// Each broker's `FELIX_CONTROLPLANE_SYNC_INTERVAL_MS`. Short by default so
    /// ownership converges while a person watches; a test that measures what
    /// the broker's wakes buy sets the production default instead.
    pub sync_interval_ms: u64,
    /// Each broker's `FELIX_REGION_ID`, by index. A broker past the end of
    /// the list is in `local`, so a test that says nothing about regions
    /// gets one region.
    pub regions: Vec<String>,
    /// Each broker's `FELIX_NODE_ZONE`, by index. A broker past the end of
    /// the list registers no zone, which is what every test that says nothing
    /// about zones gets.
    pub zones: Vec<String>,
    /// Extra environment for every broker, set after the harness's own so a
    /// test can override any of them. Empty by default.
    pub broker_env: Vec<(String, String)>,
    /// Extra environment for the broker at each index, set after
    /// `broker_env`. A broker past the end of the list gets none: how a test
    /// builds a fleet whose brokers are not all configured alike.
    pub node_env: Vec<Vec<(String, String)>>,
    /// Extra TCP listeners every broker binds, as `(env var, value template)`;
    /// `{port}` in the template becomes a port the harness picks. Picked afresh
    /// on every spawn, so a broker that loses the port to another process is
    /// started again on a different one. Read it back with
    /// [`BrokerNode::tcp_port`](crate::BrokerNode::tcp_port). Empty by default.
    pub tcp_listeners: Vec<(String, String)>,
    /// Give every broker a plaintext Kafka listener. Off by default. It binds
    /// every interface and advertises the address a kcat container reaches
    /// the host by, since the tests drive it from Docker or Podman.
    pub kafka: bool,
    /// How that Kafka listener runs. Plaintext SASL by default.
    pub kafka_listener: KafkaListener,
    /// Route every broker-to-broker and broker-to-control-plane link through
    /// the harness's own proxies, so [`Fault::Drop`](crate::Fault::Drop) and
    /// [`Fault::Delay`](crate::Fault::Delay) can act on them. Off by default:
    /// a test that injects no link fault talks over the direct path it always
    /// did.
    pub proxy_links: bool,
    /// Run the control plane as a real three-member metadata Raft group
    /// instead of one in-memory store, so faults can reach its members.
    pub metadata_group: bool,
    /// Run every broker's storage under the simulated power-loss model, so
    /// [`Cluster::power_off`](crate::Cluster::power_off) can cut the power.
    /// Off by default: every flush then also reads what it made durable. Needs
    /// brokers built with debug assertions, on Linux.
    pub power_loss: bool,
    /// Run with joint fences on: every broker offers `ELECTION` and reports
    /// `joint_fence` along with what this build reports, and
    /// [`Cluster::start`](crate::Cluster::start) finalizes it before it
    /// returns. No release reports `joint_fence` yet, so this is the only way
    /// to turn it on. Needs brokers built with debug assertions, which honour
    /// `FELIX_TEST_FLEET_FEATURES`. Off by default.
    pub joint_fence: bool,
    /// How long the control plane waits on a silent broker before it marks
    /// it down, plus the quarter it adds before re-placing its shards. One
    /// second by default, so failure tests see a stopped broker leave in
    /// seconds; a test that needs elections to run first sets a deployment's
    /// 15 seconds.
    pub node_expiry: Duration,
    /// The shard engine the control plane runs as, its `FELIX_SHARD_ENGINE`,
    /// for the in-memory store and the metadata group alike. `None` is Raft
    /// when any broker runs Raft, else the hybrid. Brokers take theirs from
    /// `broker_env`.
    pub control_plane_engine: Option<String>,
    /// Let [`Cluster::start`](crate::Cluster::start) return while new Raft
    /// groups still have their first leadership to hand over. Off by default:
    /// a start then waits until every Raft shard is led by the broker
    /// placement names, so the owner a test reads next is the broker that
    /// serves and stays it (#3157). Only a test about the hand-over itself
    /// wants it pending.
    pub pending_raft_hand_overs: bool,
    /// Start as a fleet upgraded from a release before its features existed,
    /// so the control plane finalizes none of them at the first registration
    /// and every one is the test's to finalize. `Some(false)` asks for a new
    /// cluster, with the new-cluster features finalized (#1774).
    ///
    /// `None`, the default, decides by engine. A Raft cluster comes up new.
    /// A hybrid one comes up as it did before that rule, upgraded whenever
    /// streams or caches are seeded, because its tests predate the rule.
    pub upgraded_fleet: Option<bool>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            nodes: 3,
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            streams: vec![StreamSpec::new("orders", 1)],
            caches: Vec::new(),
            inherit_output: false,
            quic_listeners: 1,
            sync_interval_ms: 200,
            regions: Vec::new(),
            zones: Vec::new(),
            broker_env: Vec::new(),
            node_env: Vec::new(),
            tcp_listeners: Vec::new(),
            kafka: false,
            kafka_listener: KafkaListener::default(),
            proxy_links: false,
            metadata_group: false,
            power_loss: false,
            joint_fence: false,
            node_expiry: Duration::from_secs(1),
            control_plane_engine: None,
            pending_raft_hand_overs: false,
            upgraded_fleet: None,
        }
    }
}

/// How each broker's Kafka listener runs when [`ClusterConfig::kafka`] is on.
/// The default is plaintext with SASL required, what every Kafka test that
/// says nothing about the listener gets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KafkaListener {
    /// Serve TLS with the broker's generated client certificate, the one it
    /// exports to [`BrokerNode::client_certificate`](crate::BrokerNode::client_certificate).
    pub tls: bool,
    /// Take a connection that skips SASL as the cluster's tenant
    /// (`FELIX_KAFKA_ANONYMOUS_TENANT`), the development switch.
    pub anonymous: bool,
    /// Store Kafka keys and headers (`FELIX_KAFKA_RECORD_HEADERS`). A cluster
    /// stores them only once the fleet has finalized `record_headers`.
    pub record_headers: bool,
}

impl ClusterConfig {
    /// Whether any broker starts with `FELIX_SHARD_ENGINE=raft`.
    pub(crate) fn raft_brokers(&self) -> bool {
        match self.named_engine() {
            Some(named) => named == "raft",
            None => default_engine() == "raft",
        }
    }

    /// Whether a start waits for every Raft shard's placed leader to lead.
    /// The hybrid has no hand-over: placement's owner serves from the start.
    pub(crate) fn awaits_placed_raft_leaders(&self) -> bool {
        self.raft_brokers() && !self.pending_raft_hand_overs
    }

    /// Whether the start leaves the history of an upgraded fleet in the store
    /// (see [`ClusterConfig::upgraded_fleet`]).
    pub(crate) fn starts_upgraded(&self) -> bool {
        self.upgraded_fleet.unwrap_or_else(|| {
            !self.raft_brokers() && !(self.streams.is_empty() && self.caches.is_empty())
        })
    }

    fn named_engine(&self) -> Option<&str> {
        std::iter::once(&self.broker_env)
            .chain(&self.node_env)
            .flatten()
            .filter(|(name, _)| name == "FELIX_SHARD_ENGINE")
            .map(|(_, value)| value.as_str())
            .next_back()
    }
}

/// The shard engine brokers start with unless a test names one, from
/// `FELIX_TEST_SHARD_ENGINE` (`raft` or `hybrid`). Raft when unset; CI sets
/// `hybrid` for the fallback runs that stay until the hybrid is deleted.
pub(crate) fn default_engine() -> &'static str {
    engine_named(std::env::var("FELIX_TEST_SHARD_ENGINE").ok().as_deref())
}

fn engine_named(value: Option<&str>) -> &'static str {
    match value {
        Some("hybrid") => "hybrid",
        _ => "raft",
    }
}

#[cfg(test)]
mod tests;

/// Say a test skipped itself on Raft, where a green result would otherwise
/// pass for a run. Written to the real stderr, which libtest does not capture,
/// and appended to the job summary when CI names one.
fn report_skip(reason: &str) {
    use std::io::Write;
    let thread = std::thread::current();
    let line = format!("{}: {reason}", thread.name().unwrap_or("unnamed test"));
    let _ = writeln!(std::io::stderr(), "{line}");
    if let Some(summary) = std::env::var_os("GITHUB_STEP_SUMMARY")
        && let Ok(mut file) = std::fs::OpenOptions::new().append(true).open(summary)
    {
        let _ = writeln!(file, "- {line}");
    }
}

/// Whether brokers start on Raft unless a test names an engine.
pub fn suite_runs_on_raft() -> bool {
    default_engine() == "raft"
}

/// True when the suite runs on Raft, for a test that moves a shard between
/// brokers. A Raft shard's leader moves only within its voting set, and
/// placement does not yet manage that set (#2235), so such a test runs on
/// hybrid only until it does.
pub fn skip_on_raft_until_moves_land() -> bool {
    let skip = default_engine() == "raft";
    if skip {
        report_skip("skipped on Raft: shard moves need membership from placement (#2235)");
    }
    skip
}

/// True when the suite runs on Raft, for a test that fails there on Raft
/// work not built yet, tracked by `issue`. The test still runs on hybrid.
pub fn skip_on_raft_until(issue: u32, reason: &str) -> bool {
    let skip = default_engine() == "raft";
    if skip {
        report_skip(&format!("skipped on Raft: {reason} (#{issue})"));
    }
    skip
}

/// True when the suite runs on Raft, for a test that asserts a hybrid-only
/// behaviour (a lease that lapses, for instance). It retires with hybrid.
pub fn skip_on_raft_retire_with_hybrid(reason: &str) -> bool {
    let skip = default_engine() == "raft";
    if skip {
        report_skip(&format!("skipped on Raft, retires with hybrid: {reason}"));
    }
    skip
}

/// A stream to create when the cluster starts.
///
/// Replication and consistency are separate from the shard count because a
/// failure test needs them apart: a shard with no replicas has nothing to fail
/// over to, and a `Leader` stream acknowledges before a follower has the record,
/// so neither proves anything about the other.
#[derive(Clone, Debug)]
pub struct StreamSpec {
    pub name: String,
    pub shards: u32,
    /// Copies of each shard, leader included. `1` is leader-only.
    pub replication_factor: u32,
    /// `"Leader"` or `"Quorum"`, as the control plane spells them.
    pub consistency: String,
    /// The region the stream's data belongs to. `None` places it anywhere.
    pub region: Option<String>,
    /// Whether the stream is durable. On by default; off makes it in-memory,
    /// for measurements that must not include the disk.
    pub durable: bool,
    /// The MQTT topic prefix the stream is mounted at, if any.
    pub mount: Option<String>,
}

impl StreamSpec {
    /// The same stream, kept in memory only. The broker needs no storage
    /// directory for it.
    pub fn in_memory(self) -> Self {
        Self {
            durable: false,
            ..self
        }
    }

    /// The same stream, mounted at the MQTT topic prefix `prefix`.
    pub fn mounted_at(self, prefix: impl Into<String>) -> Self {
        Self {
            mount: Some(prefix.into()),
            ..self
        }
    }

    /// The same stream, homed in `region`.
    pub fn in_region(self, region: impl Into<String>) -> Self {
        Self {
            region: Some(region.into()),
            ..self
        }
    }

    /// An unreplicated, leader-acknowledged stream — the default a cluster gets
    /// when a test says nothing about replication.
    pub fn new(name: impl Into<String>, shards: u32) -> Self {
        Self {
            name: name.into(),
            shards,
            replication_factor: 1,
            consistency: "Leader".to_string(),
            region: None,
            durable: true,
            mount: None,
        }
    }

    /// Replicated across `replication_factor` brokers, acknowledged by a
    /// majority. What a failover test needs: records that are on more than one
    /// broker by the time the publish returns.
    pub fn quorum(name: impl Into<String>, shards: u32, replication_factor: u32) -> Self {
        Self {
            name: name.into(),
            shards,
            replication_factor,
            consistency: "Quorum".to_string(),
            region: None,
            durable: true,
            mount: None,
        }
    }

    /// Replicated, but acknowledged by the leader alone.
    pub fn replicated(name: impl Into<String>, shards: u32, replication_factor: u32) -> Self {
        Self {
            name: name.into(),
            shards,
            replication_factor,
            consistency: "Leader".to_string(),
            region: None,
            durable: true,
            mount: None,
        }
    }
}

/// A cache the cluster should serve.
#[derive(Clone, Debug)]
pub struct CacheSpec {
    pub name: String,
    /// How many shards the keyspace is split across. More than one is what
    /// makes a test actually exercise routing rather than a single owner.
    pub shards: u32,
    pub replication_factor: u32,
    /// `"Leader"` or `"Quorum"`, as the control plane spells them.
    pub consistency: String,
}

impl CacheSpec {
    /// An unreplicated, leader-acknowledged cache.
    pub fn new(name: impl Into<String>, shards: u32) -> Self {
        Self {
            name: name.into(),
            shards,
            replication_factor: 1,
            consistency: "Leader".to_string(),
        }
    }

    /// Replicated, with each write acknowledged only once a majority holds it.
    pub fn quorum(name: impl Into<String>, shards: u32, replication_factor: u32) -> Self {
        Self {
            name: name.into(),
            shards,
            replication_factor,
            consistency: "Quorum".to_string(),
        }
    }

    /// Replicated across `replication_factor` brokers, leader included. What a
    /// failover test needs: a cache whose contents are on more than one broker
    /// before its leader is killed.
    pub fn replicated(name: impl Into<String>, shards: u32, replication_factor: u32) -> Self {
        Self {
            name: name.into(),
            shards,
            replication_factor,
            consistency: "Leader".to_string(),
        }
    }
}
