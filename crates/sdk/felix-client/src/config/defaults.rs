//! Default pool sizes, windows and queue depths, and the hard caps that
//! protect a client from a misbehaving peer.

pub(crate) const DEFAULT_PUBLISH_QUEUE_DEPTH: usize = 64;
pub(crate) const DEFAULT_PUBLISH_INFLIGHT_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const CACHE_WORKER_QUEUE_DEPTH: usize = 1024;
pub(crate) const EVENT_ROUTER_QUEUE_DEPTH: usize = 1024;
pub(crate) const DEFAULT_PUBLISH_CHUNK_BYTES: usize = 16 * 1024;
/// Publish connections per client.
///
/// Four, and measurement says leave it there. An Azure session swept 4 against
/// 16 and the 16-connection runs were **void** -- the generator was ignoring
/// client environment config (#553), so the override never took effect and both
/// runs were really 4. The re-test on a fixed generator put every valid
/// configuration between 842 and 926 MB/s, inside the ~3% run-to-run spread
/// that two identical configurations showed. Raising the *broker's* publish
/// worker pool 4 -> 16 measured slightly worse.
///
/// Worth knowing before raising it: each publisher builds its own client with
/// its own pool, so 32 publishers is 128 connections per generator. At that
/// shape three of four generators failed to connect at all
/// (`docs/perf-investigation-sharding-ceiling.md`, run J). More connections is
/// not free, and on the evidence it is not faster either.
pub(crate) const DEFAULT_PUB_CONN_POOL: usize = 4;
pub(crate) const DEFAULT_PUB_STREAMS_PER_CONN: usize = 2;
/// Publish streams a `ClusterClient` opens per broker for one shard each.
///
/// Sixteen covers every shard of a 16-shard stream, or of a few smaller ones,
/// which is the shape a stall would otherwise spread across. Each costs a
/// writer task, parked while the shard is quiet, and one of the 1024 streams
/// the transport allows a connection by default. Shards past it share the
/// pool as before, so a client publishing to many streams keeps a bounded
/// number of streams open.
pub(crate) const DEFAULT_PUB_SHARD_STREAMS: usize = 16;
pub(crate) const DEFAULT_EVENT_CONN_POOL: usize = 8;
pub(crate) const DEFAULT_CACHE_CONN_POOL: usize = 8;
/// A cluster client's ceiling per broker. It starts at one connection; this
/// only bounds how far saturation can grow it.
pub(crate) const DEFAULT_CLUSTER_CONN_POOL: usize = 8;
/// Matches the transport's default stream limit, so by default the broker's
/// stream credit is what triggers a second connection.
pub(crate) const DEFAULT_CLUSTER_STREAMS_PER_CONN: usize = 1024;
pub(crate) const DEFAULT_CACHE_STREAMS_PER_CONN: usize = 4;
pub(crate) const DEFAULT_EVENT_CONN_RECV_WINDOW: u64 = 256 * 1024 * 1024;
pub(crate) const DEFAULT_EVENT_STREAM_RECV_WINDOW: u64 = 64 * 1024 * 1024;
pub(crate) const DEFAULT_EVENT_SEND_WINDOW: u64 = 256 * 1024 * 1024;
pub(crate) const DEFAULT_CACHE_CONN_RECV_WINDOW: u64 = 256 * 1024 * 1024;
pub(crate) const DEFAULT_CACHE_STREAM_RECV_WINDOW: u64 = 64 * 1024 * 1024;
pub(crate) const DEFAULT_CACHE_SEND_WINDOW: u64 = 256 * 1024 * 1024;

/// Hard safety cap for any single felix-wire frame.
///
/// Rationale:
/// - `read_frame_into` and friends allocate a buffer sized by `header.length`.
/// - Without a cap, a malicious / buggy peer can advertise an enormous length and
///   trigger OOM or allocator churn (DoS).
///
/// Override with `FELIX_MAX_FRAME_BYTES`.
pub(crate) const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// Upper bound on how many pending subscription registrations/streams the event
/// router will hold.
///
/// Rationale:
/// - `pending_waiters` grows when the app registers but the server never opens the uni stream.
/// - `pending_streams` grows when the server opens uni streams for ids the app never registers.
///
/// Either case can happen due to bugs or a malicious peer; we cap memory usage.
/// Override with `FELIX_EVENT_ROUTER_MAX_PENDING`.
pub(crate) const DEFAULT_EVENT_ROUTER_MAX_PENDING: usize = 16 * 1024;
pub(crate) const DEFAULT_CLIENT_SUB_QUEUE_CAPACITY: usize = 256;
