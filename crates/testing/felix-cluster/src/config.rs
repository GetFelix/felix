//! What a test asks for when it starts a cluster.

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
    /// Give every broker a plaintext Kafka listener. Off by default. It binds
    /// every interface and advertises the address a kcat container reaches
    /// the host by, since the tests drive it from Docker or Podman.
    pub kafka: bool,
    /// Route every broker-to-broker and broker-to-control-plane link through
    /// the harness's own proxies, so [`Fault::Drop`](crate::Fault::Drop) and
    /// [`Fault::Delay`](crate::Fault::Delay) can act on them. Off by default:
    /// a test that injects no link fault talks over the direct path it always
    /// did.
    pub proxy_links: bool,
    /// Run every broker's storage under the simulated power-loss model, so
    /// [`Cluster::power_off`](crate::Cluster::power_off) can cut the power.
    /// Off by default: every flush then also reads what it made durable. Needs
    /// brokers built with debug assertions, on Linux.
    pub power_loss: bool,
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
            kafka: false,
            proxy_links: false,
            power_loss: false,
        }
    }
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
}

impl StreamSpec {
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
