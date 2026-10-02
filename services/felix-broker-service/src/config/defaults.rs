//! Default values for every setting the environment and config file may leave
//! unset.

use felix_broker::SubQueuePolicy;

use super::{SubStreamMode, SubscriberLaneShard};

pub(super) const DEFAULT_EVENT_BATCH_MAX_DELAY_US: u64 = 250;
pub(super) const DEFAULT_DISABLE_TIMINGS: bool = false;
pub(super) const DEFAULT_CACHE_SEND_WINDOW: u64 = 256 * 1024 * 1024;
pub(super) const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
/// Room for an `Auth` carrying a token with a long permission list, and no
/// more: until a stream authenticates, anything bigger is an attack.
pub(super) const DEFAULT_PREAUTH_MAX_FRAME_BYTES: usize = 64 * 1024;
/// Clients authenticate their streams one at a time, so this is headroom,
/// not a limit a well-behaved client meets.
pub(super) const DEFAULT_PREAUTH_MAX_STREAMS_PER_CONN: usize = 16;
/// Ten seconds: an `Auth` is one round trip plus, on a cold cache, one JWKS
/// fetch, which has its own shorter timeout.
pub(super) const DEFAULT_AUTH_TIMEOUT_MS: u64 = 10_000;
pub(super) const DEFAULT_MAX_CLIENT_CONNECTIONS: usize = 8_192;
pub(super) const DEFAULT_PUBLISH_QUEUE_WAIT_TIMEOUT_MS: u64 = 2000;
/// How long a publish to a `Quorum` stream waits for a majority.
///
/// Generous next to a healthy replication round trip, because being too short
/// costs a publish the broker cannot vouch for even though the record is on its
/// disk and about to reach a majority.
pub(super) const DEFAULT_PUBLISH_QUORUM_TIMEOUT_MS: u64 = 5_000;
// One at a time: the cautious reading of "under a policy".
pub(super) const DEFAULT_REPLICATION_REBUILD_MAX_CONCURRENT: usize = 1;
pub(super) const DEFAULT_REPLICATION_REBUILD_BYTES_PER_SEC: u64 = 0;
/// Two seconds: a switch-over takes milliseconds, so a write held this long is
/// waiting on a move that is stuck, and the client is better off told.
pub(super) const DEFAULT_SHARD_MOVE_HOLD_MS: u64 = 2_000;
/// A held write is one message or batch on one client stream, so this is
/// room for a thousand streams to be caught by a move at once.
pub(super) const DEFAULT_SHARD_MOVE_HOLD_MAX: usize = 1_024;
/// Unlimited: a move finishes as fast as the network allows unless an
/// operator trades speed for headroom.
pub(super) const DEFAULT_SHARD_MOVE_BYTES_PER_SEC: u64 = 0;
pub(super) const DEFAULT_ACK_WAIT_TIMEOUT_MS: u64 = 2000;
/// Thirty seconds. Long enough that ordinary work finishes inside it, short
/// enough that a dead consumer does not hold its records for minutes.
pub(super) const DEFAULT_GROUP_VISIBILITY_TIMEOUT_MS: u64 = 30_000;

/// Five deliveries. Enough that a transient failure is retried through, few
/// enough that a record that will never succeed is set aside quickly.
pub(super) const DEFAULT_GROUP_MAX_ATTEMPTS: u32 = 5;

/// Thirty seconds. Long enough that an idle consumer wakes rarely, short enough
/// that a client notices a broker that has stopped answering.
pub(super) const DEFAULT_GROUP_MAX_WAIT_MS: u64 = 30_000;

/// Ten thousand. Ten full polls, so several consumers each holding a poll's
/// worth never meet it; one that keeps polling without answering does.
pub(super) const DEFAULT_GROUP_MAX_IN_FLIGHT: usize = 10_000;
pub(super) const DEFAULT_CONTROL_STREAM_DRAIN_TIMEOUT_MS: u64 = 50;
// Total budget for draining in-flight work after a termination signal. Kubernetes
// defaults `terminationGracePeriodSeconds` to 30, and it sends SIGKILL once that
// expires, so the default leaves headroom to finish the drain, log the outcome, and
// exit before being killed. Deployments that raise the grace period should raise
// this to match.
pub(super) const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS: u64 = 25_000;
// Off by default, unlike the control plane's. Kubernetes removes a terminating
// pod from its endpoints without consulting readiness, and the chart's preStop
// sleep covers that; a hold-off here would stack on top of it. Set it for a load
// balancer that learns about draining only by polling `/ready`.
pub(super) const DEFAULT_SHUTDOWN_PREDRAIN_MS: u64 = 0;
// A move to a caught-up follower copies nothing, but moves run one at a time by
// default (`FELIX_SHARD_MOVES_MAX_CONCURRENT`). Spent before the drain deadline,
// so the grace period has to cover both; see deployment/graceful-shutdown.md.
pub(super) const DEFAULT_SHUTDOWN_HANDOFF_TIMEOUT_MS: u64 = 30_000;
pub(super) const DEFAULT_PUB_WORKERS_PER_CONN: usize = 4;
// Enough to keep a device flush busy with company without letting a burst put
// unbounded concurrent callers into shared broker state. `sync_batch_appends`
// is the number to watch: the budget in docs/storage-performance.md is >= 8.
pub(super) const DEFAULT_PUB_FLUSH_CONCURRENCY: usize = 32;
pub(super) const DEFAULT_PUB_QUEUE_DEPTH: usize = 64;
pub(super) const DEFAULT_PUB_INFLIGHT_BYTES: usize = 64 * 1024 * 1024;
pub(super) const DEFAULT_PUB_CONN_INFLIGHT_BYTES: usize = 16 * 1024 * 1024;
pub(super) const DEFAULT_PUBLISH_WINDOW: u32 = 256;
pub(super) const DEFAULT_SUBSCRIBER_QUEUE_CAPACITY: usize = 512;
pub(super) const DEFAULT_MAX_SUBSCRIPTIONS_PER_CONN: usize = 4096;
pub(super) const DEFAULT_SUBSCRIBER_QUEUE_POLICY: SubQueuePolicy = SubQueuePolicy::DropNew;
pub(super) const DEFAULT_SUBSCRIBER_WRITER_LANES: usize = 4;
pub(super) const DEFAULT_SUBSCRIBER_LANE_QUEUE_DEPTH: usize = 64;
pub(super) const DEFAULT_SUBSCRIBER_LANE_QUEUE_POLICY: SubQueuePolicy = SubQueuePolicy::DropNew;
pub(super) const DEFAULT_MAX_SUBSCRIBER_WRITER_LANES: usize = 8;
pub(super) const DEFAULT_SUBSCRIBER_LANE_SHARD: SubscriberLaneShard = SubscriberLaneShard::Auto;
pub(super) const DEFAULT_SUBSCRIBER_FLUSH_MAX_ITEMS: usize = 16;
pub(super) const DEFAULT_SUBSCRIBER_FLUSH_MAX_DELAY_US: u64 = 50;
pub(super) const DEFAULT_SUBSCRIBER_MAX_BYTES_PER_WRITE: usize = 64 * 1024;
pub(super) const DEFAULT_SUB_STREAMS_PER_CONN: usize = 4;
pub(super) const DEFAULT_SUB_STREAM_MODE: SubStreamMode = SubStreamMode::PerSubscriber;
