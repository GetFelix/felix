//! Broker configuration: what the environment and an optional YAML file say,
//! merged, checked, and printable for `--print-config`.
//!
//! [`BrokerConfig::from_env_or_yaml`] is the entry point. It reads the
//! environment, folds the config file over it, and then validates the result
//! as a whole, because some settings are only wrong in combination.

mod defaults;
pub mod durable;
mod env;
mod file;
pub mod kafka;
pub mod limits;
mod membership;
mod print;
mod subscriber;
pub mod tls;
mod validate;

pub use durable::DurableStorageConfig;
pub use kafka::KafkaListenerConfig;
pub use limits::{LimitsConfig, TenantQuota};
pub use membership::MembershipConfig;
pub use subscriber::{SubStreamMode, SubscriberLaneShard};
pub use tls::{ClientTlsConfig, ClientTlsFiles};

use std::net::SocketAddr;

use felix_broker::SubQueuePolicy;
use serde::Serialize;

use defaults::*;

/// A margin over the quorum wait, covering the hop from the publish executor back
/// to the waiter. Small: its only job is to let the inner wait finish first.
const ACK_WAIT_OVER_QUORUM_MS: u64 = 500;

/// Broker service configuration sourced from environment variables.
///
/// `Serialize` is for `--print-config`, and it is *derived* rather than written
/// out by hand so the dump cannot drift from the struct — a listing that quietly
/// stops mentioning a setting is the same class of problem as a documented
/// variable nothing reads.
#[derive(Clone, Serialize)]
pub struct BrokerConfig {
    /// QUIC listener bind address. With `quic_listeners > 1` this is the *first*
    /// of a consecutive run of ports; see [`BrokerConfig::quic_binds`].
    pub quic_bind: SocketAddr,
    /// An explicit `FELIX_IO_RUNTIME_THREADS`, when the operator set one.
    ///
    /// Kept so `validate` can see it: the pool size and the listener count must
    /// hold a relationship, and a pool too small for the listeners silently puts
    /// every listener's driver on one thread. Unset means derived.
    pub io_runtime_threads: Option<usize>,
    /// How many client-facing QUIC listeners to bind, on consecutive ports from
    /// `quic_bind`.
    ///
    /// One socket means one `quinn::Endpoint`, and an endpoint's driver is a
    /// single task that reads every inbound datagram and routes it by connection
    /// id. That task cannot use more than one core, and it is what holds a
    /// broker to ~900 MB/s while the rest of the machine idles (#557). Separate
    /// ports are separate sockets, and separate sockets are separate drivers.
    pub quic_listeners: usize,
    /// Metrics HTTP listener bind address.
    pub metrics_bind: SocketAddr,
    /// Optional control-plane base URL.
    pub controlplane_url: Option<String>,
    /// Credential this broker presents to the control plane; empty when none
    /// was given.
    ///
    /// Every control-plane call carries it: the metadata feeds, and -- for a
    /// cluster member -- registration, heartbeat, the assignment watch and
    /// replica reports. A member cannot start without one, since a broker
    /// that will fail every call on a loop is worse than one that refuses. A
    /// standalone broker may run without it, but then its metadata sync is
    /// refused, and it is told so at startup.
    ///
    /// Never printed. `--print-config` exists to be pasted into an issue.
    #[serde(serialize_with = "print::redacted")]
    pub controlplane_token: String,
    /// Poll interval for control-plane changes.
    pub controlplane_sync_interval_ms: u64,
    /// Extra CA bundle trusted for an `https://` control plane.
    pub controlplane_ca: Option<String>,
    /// The certificate the client-facing listeners present.
    pub client_tls: ClientTlsConfig,
    /// Cluster membership identity, when this broker joins one.
    pub membership: Option<MembershipConfig>,
    /// Broker-internal transport, present only when this broker joins a cluster.
    /// A broker with no peers has nothing to listen for.
    pub peer_transport: Option<crate::peer::PeerTransportConfig>,
    /// If true, publish acks are sent after commit.
    pub ack_on_commit: bool,
    /// Max frame size accepted on QUIC streams.
    pub max_frame_bytes: usize,
    /// Max frame size a stream may send before it has authenticated. Only an
    /// `Auth` has to fit, so this is far below `max_frame_bytes`.
    pub preauth_max_frame_bytes: usize,
    /// Streams one connection may have reading before they authenticate.
    /// More wait their turn rather than being refused.
    pub preauth_max_streams_per_conn: usize,
    /// How long a client connection has to authenticate a stream before it is
    /// closed. `0` disables the deadline.
    pub auth_timeout_ms: u64,
    /// Client QUIC connections this broker holds at once, across all its
    /// client listeners. Attempts beyond it are refused before the handshake.
    pub max_client_connections: usize,
    /// How long a commit-acked publish waits for room in the publish queue
    /// (and the byte budget) before it is answered busy.
    pub publish_queue_wait_timeout_ms: u64,
    /// Max time to wait for ack-on-commit publish completion.
    pub ack_wait_timeout_ms: u64,
    /// How long a consumer group's claim on a record stands before the record
    /// is handed to someone else.
    ///
    /// Too short redelivers work that is still being done; too long leaves a
    /// dead consumer's records stuck for that long. Thirty seconds is a
    /// starting point, not a considered default for any particular workload.
    pub group_visibility_timeout_ms: u64,
    /// Most times a consumer group hands out a record before giving up on it.
    ///
    /// Without a bound a record that always fails is redelivered for ever and
    /// the group never gets past it. A record given up on is recorded as a dead
    /// letter and the group moves on; the record itself stays in the log.
    pub group_max_attempts: u32,
    /// Longest a consumer group's poll may wait for work before answering
    /// empty.
    ///
    /// A cap on what a client asks for, not a default: a client that wants a
    /// shorter wait gets one. It exists so a client cannot hold a broker stream
    /// open indefinitely.
    pub group_max_wait_ms: u64,
    /// Most records one consumer group may have handed out and unsettled at
    /// once, per shard. A poll past it answers empty until acknowledgements or
    /// lapsed claims free room, so one consumer that polls and never answers
    /// cannot pull the whole backlog into memory.
    pub group_max_in_flight: usize,
    /// Disable timing collection for lower overhead.
    pub disable_timings: bool,
    /// Max time to wait for control-stream writer to drain.
    pub control_stream_drain_timeout_ms: u64,
    /// Total budget for draining in-flight work after SIGTERM/SIGINT before
    /// remaining tasks are force-cancelled.
    pub shutdown_drain_timeout_ms: u64,
    /// How long to keep accepting connections after readiness goes false, so a
    /// load balancer polling `/ready` has time to stop routing here.
    pub shutdown_predrain_ms: u64,
    /// How long a clustered broker waits for its shards to move to other
    /// brokers before it stops. `0` skips the handoff.
    pub shutdown_handoff_timeout_ms: u64,
    /// Cache connection flow-control window.
    pub cache_conn_recv_window: u64,
    /// Cache stream flow-control window.
    pub cache_stream_recv_window: u64,
    /// Cache connection send window.
    pub cache_send_window: u64,
    /// Max events per batched subscription frame.
    pub event_batch_max_events: usize,
    /// Max bytes per batched subscription frame.
    pub event_batch_max_bytes: usize,
    /// Max delay before flushing a subscription batch.
    pub event_batch_max_delay_us: u64,
    /// Fanout batch size for subscription sending.
    pub fanout_batch_size: usize,
    /// Executors of the process-wide publish scheduler (per core shard, with
    /// `core_shards`). The name predates the scheduler.
    pub pub_workers_per_conn: usize,
    /// Durable publishes one shard may have awaiting their device flush at
    /// once. Offsets are still claimed serially, so this does not affect the
    /// order records land in -- it decides how many flushes group commit gets
    /// to coalesce. `1` restores the old behaviour of one flush at a time.
    pub pub_flush_concurrency: usize,
    /// Publish queue slots each tenant is guaranteed. The queue holds this
    /// times `pub_workers_per_conn`.
    pub pub_queue_depth: usize,
    /// Shared in-flight publish byte budget across all publishes (process-wide).
    pub pub_inflight_bytes: usize,
    /// Per-connection share of the in-flight publish byte budget. Bounds how much of the
    /// process-wide `pub_inflight_bytes` budget a single connection can occupy at once, so one
    /// connection can't starve every other connection's publishes under load.
    pub pub_conn_inflight_bytes: usize,
    /// If true, un-acked publishes wait (bounded) for ingress capacity instead of shedding.
    /// Off by default: fire-and-forget load should shed visibly under overload.
    pub pub_ingress_wait: bool,
    /// Number of core-pinned shard executors owning stream work (0 = disabled).
    /// When enabled, publish workers and subscription lane feeders run on the
    /// shard owning their stream, keeping the per-message path core-local.
    pub core_shards: usize,
    /// Per-subscriber queue capacity in broker core.
    pub subscriber_queue_capacity: usize,
    /// Max concurrent subscriptions a single QUIC connection may hold. Prevents a single
    /// connection from unboundedly growing broker memory via subscriber queues/writer-lane
    /// registrations.
    pub max_subscriptions_per_conn: usize,
    /// Subscriber queue policy for publish->fanout enqueue.
    #[serde(serialize_with = "print::queue_policy")]
    pub subscriber_queue_policy: SubQueuePolicy,
    /// Number of outbound subscriber writer lanes.
    pub subscriber_writer_lanes: usize,
    /// Bounded queue depth per writer lane.
    pub subscriber_lane_queue_depth: usize,
    /// Queue policy for lane ingress.
    #[serde(serialize_with = "print::queue_policy")]
    pub subscriber_lane_queue_policy: SubQueuePolicy,
    /// Upper bound to prevent over-sharding lane counts that can regress p99/p999 under load.
    pub max_subscriber_writer_lanes: usize,
    /// Deterministic policy for assigning subscribers to writer lanes.
    pub subscriber_lane_shard: SubscriberLaneShard,
    /// If true, route all subscribers on the same QUIC connection to one writer lane.
    pub subscriber_single_writer_per_conn: bool,
    /// Max queued items drained per lane flush.
    pub subscriber_flush_max_items: usize,
    /// Max time spent waiting for a lane flush fill.
    pub subscriber_flush_max_delay_us: u64,
    /// Upper bound for coalesced bytes per write call.
    pub subscriber_max_bytes_per_write: usize,
    /// Number of delivery streams to use per connection in hashed-pool mode.
    pub sub_streams_per_conn: usize,
    /// Strategy for mapping subscribers to streams.
    pub sub_stream_mode: SubStreamMode,
    /// How long a publish to a `Quorum` stream waits for a majority before the
    /// broker says it cannot vouch for the write.
    pub publish_quorum_timeout_ms: u64,
    /// Halted followers this broker rebuilds at once, across every shard it
    /// leads; zero leaves every halt to an operator. A rebuild is a full
    /// transfer, and every follower of a failed broker at once is an outage.
    pub replication_rebuild_max_concurrent: usize,
    /// Bytes per second a rebuilding follower is shipped at; zero is unlimited.
    pub replication_rebuild_bytes_per_sec: u64,
    /// How long a write to a shard that is moving waits for the move to cut
    /// over before it is refused as `moving`; zero refuses at once.
    pub shard_move_hold_ms: u64,
    /// How many writes may wait on moving shards at once. Each holds its
    /// payload, so this bounds the memory a move can pin.
    pub shard_move_hold_max: usize,
    /// Bytes per second this broker ships to move destinations, across every
    /// shard it leads; zero is unlimited. Never applied to a follower the
    /// quorum needs.
    pub shard_move_bytes_per_sec: u64,
    /// Per-address connection caps, per-tenant publish quotas and the
    /// per-tenant metrics guard.
    pub limits: LimitsConfig,
}

/// Rendered through the same `Serialize` as `--print-config`, so the
/// credential is redacted here too and a new field cannot be forgotten.
impl std::fmt::Debug for BrokerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(rendered) => write!(f, "BrokerConfig {rendered}"),
            Err(_) => f.write_str("BrokerConfig { .. }"),
        }
    }
}

impl BrokerConfig {
    /// Every client-facing listener address, in bind order.
    ///
    /// Consecutive ports from `quic_bind`. The first is the one an existing
    /// deployment already knows, so a broker with the default single listener
    /// binds exactly what it always did.
    pub fn quic_binds(&self) -> Vec<SocketAddr> {
        (0..self.quic_listeners)
            .map(|offset| {
                let mut addr = self.quic_bind;
                addr.set_port(self.quic_bind.port() + offset as u16);
                addr
            })
            .collect()
    }

    /// Server endpoints this broker binds: the client listeners, plus the
    /// internal one when it is part of a cluster.
    ///
    /// The internal listener carries replication, so it is a driver like any
    /// other and needs its own runtime rather than sharing a client listener's.
    pub fn server_endpoints(&self) -> usize {
        self.quic_listeners + usize::from(self.peer_transport.is_some())
    }

    /// How long a publish waits for its acknowledgement before the client is
    /// told it timed out.
    ///
    /// Never shorter than the quorum wait it may be sitting on. A publish to a
    /// `Quorum` stream is entitled to `publish_quorum_timeout_ms` to reach a
    /// majority, so a waiter that gives up sooner reports "publish commit
    /// timeout" for a write the broker is still correctly waiting for -- and
    /// replaces the quorum wait's own answer, which says specifically that this
    /// broker cannot vouch for the write, with one that says nothing about why.
    ///
    /// Raising the ceiling does not slow anything down. It is the point at
    /// which a publish that is already stuck is given up on, not a delay any
    /// successful publish pays.
    pub fn ack_wait_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(
            self.ack_wait_timeout_ms.max(
                self.publish_quorum_timeout_ms
                    .saturating_add(ACK_WAIT_OVER_QUORUM_MS),
            ),
        )
    }

    /// How long a forwarded publish may spend before it must answer.
    ///
    /// The same relationship as the quorum wait above, from the other side. A
    /// forward is the other inner wait a publish can be sitting on, and it is
    /// the one with no bound of its own that fits: its budget is up to
    /// `MAX_ATTEMPTS` peer requests, each of which may dial first, so its worst
    /// case runs to tens of seconds while the waiter gives up in a few.
    ///
    /// Raising the ceiling to cover that would make a client wait out the whole
    /// thing, so the forward is bounded to fit under the ceiling instead. What
    /// that buys is the same thing the quorum margin buys: the inner wait
    /// finishes first, so the client is told *why* — the owner was unreachable
    /// and nothing was sent, or the batch went out and its fate is unknown —
    /// rather than getting "publish commit timeout", which says nothing and is
    /// the one answer a client cannot act on.
    ///
    /// Derived rather than configured, so the two cannot be tuned apart.
    pub fn forward_budget(&self) -> std::time::Duration {
        self.ack_wait_timeout()
            .saturating_sub(std::time::Duration::from_millis(ACK_WAIT_OVER_QUORUM_MS))
    }
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            quic_bind: SocketAddr::from(([0, 0, 0, 0], 5000)),
            quic_listeners: 1,
            io_runtime_threads: None,
            metrics_bind: SocketAddr::from(([0, 0, 0, 0], 8080)),
            controlplane_url: None,
            controlplane_token: String::new(),
            controlplane_sync_interval_ms: 2000,
            controlplane_ca: None,
            client_tls: ClientTlsConfig::default(),
            membership: None,
            peer_transport: None,
            ack_on_commit: false,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            preauth_max_frame_bytes: DEFAULT_PREAUTH_MAX_FRAME_BYTES,
            preauth_max_streams_per_conn: DEFAULT_PREAUTH_MAX_STREAMS_PER_CONN,
            auth_timeout_ms: DEFAULT_AUTH_TIMEOUT_MS,
            max_client_connections: DEFAULT_MAX_CLIENT_CONNECTIONS,
            publish_queue_wait_timeout_ms: DEFAULT_PUBLISH_QUEUE_WAIT_TIMEOUT_MS,
            ack_wait_timeout_ms: DEFAULT_ACK_WAIT_TIMEOUT_MS,
            group_visibility_timeout_ms: DEFAULT_GROUP_VISIBILITY_TIMEOUT_MS,
            group_max_attempts: DEFAULT_GROUP_MAX_ATTEMPTS,
            group_max_wait_ms: DEFAULT_GROUP_MAX_WAIT_MS,
            group_max_in_flight: DEFAULT_GROUP_MAX_IN_FLIGHT,
            disable_timings: DEFAULT_DISABLE_TIMINGS,
            control_stream_drain_timeout_ms: DEFAULT_CONTROL_STREAM_DRAIN_TIMEOUT_MS,
            shutdown_drain_timeout_ms: DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS,
            shutdown_predrain_ms: DEFAULT_SHUTDOWN_PREDRAIN_MS,
            shutdown_handoff_timeout_ms: DEFAULT_SHUTDOWN_HANDOFF_TIMEOUT_MS,
            cache_conn_recv_window: DEFAULT_CACHE_CONN_RECV_WINDOW,
            cache_stream_recv_window: DEFAULT_CACHE_STREAM_RECV_WINDOW,
            cache_send_window: DEFAULT_CACHE_SEND_WINDOW,
            event_batch_max_events: 64,
            event_batch_max_bytes: 64 * 1024,
            event_batch_max_delay_us: DEFAULT_EVENT_BATCH_MAX_DELAY_US,
            fanout_batch_size: 64,
            pub_workers_per_conn: DEFAULT_PUB_WORKERS_PER_CONN,
            pub_flush_concurrency: DEFAULT_PUB_FLUSH_CONCURRENCY,
            pub_queue_depth: DEFAULT_PUB_QUEUE_DEPTH,
            pub_inflight_bytes: DEFAULT_PUB_INFLIGHT_BYTES,
            pub_conn_inflight_bytes: DEFAULT_PUB_CONN_INFLIGHT_BYTES,
            pub_ingress_wait: false,
            core_shards: 0,
            subscriber_queue_capacity: DEFAULT_SUBSCRIBER_QUEUE_CAPACITY,
            max_subscriptions_per_conn: DEFAULT_MAX_SUBSCRIPTIONS_PER_CONN,
            subscriber_queue_policy: DEFAULT_SUBSCRIBER_QUEUE_POLICY,
            subscriber_writer_lanes: DEFAULT_SUBSCRIBER_WRITER_LANES,
            subscriber_lane_queue_depth: DEFAULT_SUBSCRIBER_LANE_QUEUE_DEPTH,
            subscriber_lane_queue_policy: DEFAULT_SUBSCRIBER_LANE_QUEUE_POLICY,
            max_subscriber_writer_lanes: DEFAULT_MAX_SUBSCRIBER_WRITER_LANES,
            subscriber_lane_shard: DEFAULT_SUBSCRIBER_LANE_SHARD,
            subscriber_single_writer_per_conn: false,
            subscriber_flush_max_items: DEFAULT_SUBSCRIBER_FLUSH_MAX_ITEMS,
            subscriber_flush_max_delay_us: DEFAULT_SUBSCRIBER_FLUSH_MAX_DELAY_US,
            subscriber_max_bytes_per_write: DEFAULT_SUBSCRIBER_MAX_BYTES_PER_WRITE,
            sub_streams_per_conn: DEFAULT_SUB_STREAMS_PER_CONN,
            sub_stream_mode: DEFAULT_SUB_STREAM_MODE,
            publish_quorum_timeout_ms: DEFAULT_PUBLISH_QUORUM_TIMEOUT_MS,
            replication_rebuild_max_concurrent: DEFAULT_REPLICATION_REBUILD_MAX_CONCURRENT,
            replication_rebuild_bytes_per_sec: DEFAULT_REPLICATION_REBUILD_BYTES_PER_SEC,
            shard_move_hold_ms: DEFAULT_SHARD_MOVE_HOLD_MS,
            shard_move_hold_max: DEFAULT_SHARD_MOVE_HOLD_MAX,
            shard_move_bytes_per_sec: DEFAULT_SHARD_MOVE_BYTES_PER_SEC,
            limits: LimitsConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests;
