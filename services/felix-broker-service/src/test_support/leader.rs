//! A cluster member leading one shard of each kind through the real shard
//! lifecycle, so its write fence is open exactly as a running broker's is.
//!
//! [`Leader::fence_move`] closes it the way a move does, and leaves the
//! servable set the ingress router reads untouched. That is the window every
//! write-fence test is about: admission still says the shard is served here,
//! and only the fence can refuse the write.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use felix_broker::{Broker, CacheMetadata, StreamMetadata};
use felix_router::{NodeRef, RegionRouter, ShardRouter};
use felix_storage::LogCache;
use felix_storage::log::{FsyncMode, LogConfig};
use tempfile::TempDir;

use crate::shards::lifecycle::{Action, ShardLifecycle};
use crate::shards::routing::hold::MoveHold;
use crate::shards::routing::{IngressRouter, routing_table_from};
use crate::shards::watch::ShardAssignment;
use crate::shards::{ShardKey, ShardKind};

pub(crate) const TENANT: &str = "t1";
pub(crate) const NAMESPACE: &str = "ns";
/// A durable stream, whose publishes take the split claim-then-complete path.
pub(crate) const DURABLE: &str = "orders";
/// An ephemeral stream, whose publishes complete inline.
pub(crate) const EPHEMERAL: &str = "events";
pub(crate) const CACHE: &str = "sessions";
pub(crate) const GENERATION: u64 = 1;
const NODE: &str = "broker-a";
/// Where [`Leader::cut_over`] moves a shard.
pub(crate) const SUCCESSOR: &str = "broker-b";

pub(crate) struct Leader {
    pub(crate) broker: Arc<Broker>,
    pub(crate) ingress: Arc<IngressRouter>,
    pub(crate) router: Arc<ShardRouter>,
    pub(crate) lifecycle: ShardLifecycle,
    assignments: HashMap<ShardKey, ShardAssignment>,
    nodes: HashMap<String, NodeRef>,
    _dir: TempDir,
}

impl Leader {
    pub(crate) async fn start() -> Self {
        Self::start_holding(MoveHold::disabled()).await
    }

    /// [`Self::start`], holding writes to a moving shard under `hold`.
    pub(crate) async fn start_holding(hold: MoveHold) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        };
        let root = dir.path();
        let broker = Broker::new(Box::new(
            LogCache::open(root.join("caches"), config.clone()).expect("cache"),
        ))
        .with_durable_storage(
            felix_broker::DurableStorage::open(root.join("streams"), config.clone())
                .expect("streams"),
        )
        .with_consumer_groups(
            Arc::new(
                felix_broker::ConsumerGroups::open(root.join("groups"), config.clone())
                    .expect("groups"),
            ),
            Arc::new(
                felix_broker::DeadLetters::open(root.join("dead-letters"), config.clone())
                    .expect("dead letters"),
            ),
            Duration::from_secs(30),
            3,
        )
        .with_counters(Arc::new(
            felix_storage::CounterStore::open(root.join("counters"), config).expect("counters"),
        ));
        broker.register_tenant(TENANT).await.expect("tenant");
        broker
            .register_namespace(TENANT, NAMESPACE)
            .await
            .expect("namespace");
        for (stream, durable) in [(DURABLE, true), (EPHEMERAL, false)] {
            broker
                .register_stream(
                    TENANT,
                    NAMESPACE,
                    stream,
                    StreamMetadata {
                        durable,
                        ..Default::default()
                    },
                )
                .await
                .expect("stream");
        }
        broker
            .register_cache(TENANT, NAMESPACE, CACHE, CacheMetadata::default())
            .await
            .expect("cache");

        let keys = [stream_key(DURABLE), stream_key(EPHEMERAL), cache_key()];
        let assignments: HashMap<ShardKey, ShardAssignment> = keys
            .iter()
            .map(|key| (key.clone(), assignment(key, "active")))
            .collect();
        let nodes: HashMap<String, NodeRef> = [(NODE, 7001), (SUCCESSOR, 7002)]
            .into_iter()
            .map(|(node_id, port)| {
                (
                    node_id.to_string(),
                    NodeRef {
                        node_id: node_id.to_string(),
                        advertise_addr: std::net::SocketAddr::from(([127, 0, 0, 1], port)),
                        region: "us-west-2".to_string(),
                        live: true,
                    },
                )
            })
            .collect();
        let router = Arc::new(ShardRouter::new(
            NODE,
            "us-west-2",
            RegionRouter::new("us-west-2".to_string()),
        ));
        router.publish(routing_table_from(&assignments, &nodes), &nodes);

        let mut lifecycle = ShardLifecycle::new(NODE);
        for (key, assignment) in &assignments {
            lifecycle.observe(key, Some(assignment));
            lifecycle.opened(key, GENERATION);
        }
        let ingress = Arc::new(
            IngressRouter::new(Arc::clone(&router), Arc::clone(lifecycle.fence()))
                .with_move_hold(hold),
        );
        ingress.publish_servable(lifecycle.servable());

        Self {
            broker: Arc::new(broker),
            ingress,
            router,
            lifecycle,
            assignments,
            nodes,
            _dir: dir,
        }
    }

    /// Where `stream`'s shard 0 would write next: how many records it holds.
    pub(crate) async fn tail(&self, stream: &str) -> u64 {
        self.broker
            .cursor_tail(TENANT, NAMESPACE, stream, 0)
            .await
            .expect("tail")
            .next_seq()
    }

    /// Bind a freshly renewed lease to the fence, as a running broker does,
    /// and hand it back so a test can let it lapse.
    pub(crate) fn hold_lease(&self) -> Arc<crate::cluster::lease::LeaseState> {
        let lease = Arc::new(crate::cluster::lease::LeaseState::new(Duration::from_secs(
            3600,
        )));
        lease.renew();
        self.ingress.fence().bind_lease(Arc::clone(&lease));
        lease
    }

    /// What a move does to the old leader: the same generation, now draining.
    pub(crate) fn fence_move(&mut self, key: &ShardKey) {
        let action = self
            .lifecycle
            .observe(key, Some(&assignment(key, "draining")));
        assert!(
            matches!(action, Action::Release { .. }),
            "expected a release, got {action:?}"
        );
    }

    /// What the rest of a move does here: the view catches up with the
    /// fence, then names [`SUCCESSOR`] the leader at the next generation.
    pub(crate) fn cut_over(&mut self, key: &ShardKey) {
        let moved = ShardAssignment {
            leader: SUCCESSOR.to_string(),
            generation: GENERATION + 1,
            ..assignment(key, "active")
        };
        self.lifecycle.observe(key, Some(&moved));
        self.assignments.insert(key.clone(), moved);
        self.ingress.publish(
            routing_table_from(&self.assignments, &self.nodes),
            &self.nodes,
            self.lifecycle.servable(),
        );
    }
}

pub(crate) fn stream_key(stream: &str) -> ShardKey {
    ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: stream.to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    }
}

pub(crate) fn cache_key() -> ShardKey {
    ShardKey {
        kind: ShardKind::Cache,
        ..stream_key(CACHE)
    }
}

fn assignment(key: &ShardKey, state: &str) -> ShardAssignment {
    ShardAssignment {
        key: key.clone(),
        leader: NODE.to_string(),
        replicas: Vec::new(),
        generation: GENERATION,
        state: state.to_string(),
        successor: None,
    }
}
