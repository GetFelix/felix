//! Configuration for the broker-internal transport.
//!
//! Separate from the client-facing transport on purpose, and every knob here is
//! read from a `FELIX_INTERNAL_*` variable. Sharing a name with the client-side
//! setting would make it possible to widen a peer limit while believing a client
//! limit had been widened.
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::time::Duration;

/// The ALPN protocol internal endpoints negotiate.
///
/// The client-facing listener uses no ALPN, so this is what separates the two
/// roles at the TLS layer rather than at the first frame. A connection that
/// reaches the internal listener without it is refused before any broker state
/// is touched.
pub const INTERNAL_ALPN: &[u8] = b"felix-internal/1";

/// How many connections the pool may hold to one peer.
///
/// One is enough for correctness and is the default: a QUIC connection
/// multiplexes streams, so a second buys parallelism across congestion-control
/// state rather than across requests. It exists as a knob because a single
/// connection is also a single loss domain.
const DEFAULT_CONNS_PER_PEER: usize = 1;

/// Multiplexed request streams per connection.
///
/// Requests are spread across these round-robin. More than one because a QUIC
/// stream is ordered: a large forwarded batch would otherwise hold up every
/// smaller request queued behind it on the same stream.
const DEFAULT_STREAMS_PER_CONN: usize = 4;

/// Requests allowed in flight to one peer at a time.
///
/// The bound is what stops an unhealthy peer from consuming this broker: a peer
/// that accepts frames and never answers otherwise accumulates one waiter per
/// forwarded publish for as long as the timeout allows.
const DEFAULT_MAX_INFLIGHT_PER_PEER: usize = 1024;

/// Inbound peer connections this broker will hold at once.
///
/// A peer opens `conns_per_peer` (1 by default), so this is room for a cluster
/// far larger than any Felix has been run at, and still a bound: without one, a
/// single caller can make this broker hold 64 MiB × 1024 streams × however many
/// connections it cares to open (#504).
const DEFAULT_MAX_INBOUND_CONNECTIONS: usize = 512;

/// Inbound connections from any one address.
///
/// The total alone does not stop one peer consuming the whole allowance, which
/// is the case that matters: a peer looping on a reconnect bug is more likely
/// than a hostile one, and it starves every other broker in the cluster before
/// anyone notices.
const DEFAULT_MAX_INBOUND_PER_SOURCE: usize = 16;

/// How long a forwarded request waits for its terminal response.
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 5_000;

/// How long a connection may sit unused before it is closed.
///
/// Rebalancing changes which peers this broker talks to, and a connection to a
/// peer it no longer forwards to is a file descriptor and a keepalive with
/// nothing to do.
const DEFAULT_IDLE_TIMEOUT_MS: u64 = 60_000;

/// Reconnect backoff bounds. The first retry is fast because the common cause
/// is a peer restarting, and the ceiling keeps a durably dead peer from being
/// dialled in a tight loop.
const DEFAULT_RECONNECT_BASE_MS: u64 = 50;
const DEFAULT_RECONNECT_MAX_MS: u64 = 5_000;

/// How long the handshake has to complete before the connection is abandoned.
///
/// Deliberately well under the publish quorum timeout, which is also 5s. A peer
/// that is gone does not refuse on every platform -- where the kernel returns no
/// refusal, a dial runs this timeout out instead -- and that dial sits on the
/// critical path of the replication pass, which is what releases a `Quorum`
/// publish. A handshake timeout as long as the publish budget lets one dead
/// replica spend a publish's entire patience before the majority that is up
/// gets to count.
///
/// Still generous for the handshake itself: a broker-internal dial is a round
/// trip on a local network, not seconds.
const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 2_000;

/// Broker-internal transport settings.
/// Durations are printed as milliseconds, matching the variables that set
/// them. Serde's default for `Duration` is `{ secs, nanos }`, which is correct
/// and unreadable next to `FELIX_PEER_REQUEST_TIMEOUT_MS`.
fn as_millis<S: serde::Serializer>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_u64(value.as_millis() as u64)
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PeerTransportConfig {
    /// Where the internal listener binds. Distinct from `quic_bind`, and
    /// startup refuses if they are equal.
    pub bind: SocketAddr,
    /// Certificates for peer connections. `None` is the unauthenticated mode:
    /// encrypted, and anything that can reach the port is a peer.
    pub tls: Option<PeerTlsConfig>,
    /// `FELIX_INTERNAL_ALLOW_UNAUTHENTICATED`: the operator accepts running
    /// without `tls`. Startup refuses a cluster member that has neither.
    pub allow_unauthenticated: bool,
    pub conns_per_peer: usize,
    pub streams_per_conn: usize,
    pub max_inflight_per_peer: usize,
    /// Inbound connections held at once, across all peers.
    pub max_inbound_connections: usize,
    /// Inbound connections held at once from any one address.
    pub max_inbound_per_source: usize,
    #[serde(serialize_with = "as_millis")]
    pub request_timeout: Duration,
    #[serde(serialize_with = "as_millis")]
    pub idle_timeout: Duration,
    #[serde(serialize_with = "as_millis")]
    pub reconnect_base: Duration,
    #[serde(serialize_with = "as_millis")]
    pub reconnect_max: Duration,
    #[serde(serialize_with = "as_millis")]
    pub handshake_timeout: Duration,
    /// Test-only: a file naming peers this broker must not exchange with.
    ///
    /// `None` in every deployment that does not set `FELIX_PEER_PARTITION_FILE`,
    /// which is the only way to turn it on, and the check costs one `Option`
    /// test when it is off.
    ///
    /// A partition is the one failure this harness cannot produce from the
    /// outside: killing, stopping and freezing a broker are all a signal away,
    /// but severing two brokers while both stay alive and both keep heartbeating
    /// to the control plane needs cooperation from the thing being tested. That
    /// combination -- a leader that looks healthy to the control plane and
    /// cannot reach its followers -- is exactly where a replication design is
    /// most likely to be wrong, so it is worth a hook.
    pub partition_file: Option<std::path::PathBuf>,
    /// `FELIX_INTERNAL_FENCE`: whether this broker offers the promotion fence
    /// to its peers and answers it. On by default. Off, it is spoken to as a
    /// broker that predates the fence, and every shard it replicates keeps the
    /// lease alone.
    pub fence: bool,
}

/// Where a broker's peer identity comes from. All three or none: a listener
/// that verifies peers against a CA but presents no certificate of its own
/// would be refused by every peer it verified, and one that presents a
/// certificate but verifies nothing would look secured while accepting anyone.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PeerTlsConfig {
    /// PEM certificate chain, leaf first. Its DNS name is this broker's node id.
    pub cert_path: String,
    /// PEM private key for the leaf.
    pub key_path: String,
    /// PEM bundle every peer's certificate must chain to.
    pub ca_path: String,
}

impl Default for PeerTransportConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:5001".parse().expect("literal address"),
            tls: None,
            allow_unauthenticated: false,
            conns_per_peer: DEFAULT_CONNS_PER_PEER,
            streams_per_conn: DEFAULT_STREAMS_PER_CONN,
            max_inflight_per_peer: DEFAULT_MAX_INFLIGHT_PER_PEER,
            max_inbound_connections: DEFAULT_MAX_INBOUND_CONNECTIONS,
            max_inbound_per_source: DEFAULT_MAX_INBOUND_PER_SOURCE,
            request_timeout: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
            idle_timeout: Duration::from_millis(DEFAULT_IDLE_TIMEOUT_MS),
            reconnect_base: Duration::from_millis(DEFAULT_RECONNECT_BASE_MS),
            reconnect_max: Duration::from_millis(DEFAULT_RECONNECT_MAX_MS),
            handshake_timeout: Duration::from_millis(DEFAULT_HANDSHAKE_TIMEOUT_MS),
            partition_file: None,
            fence: true,
        }
    }
}

/// The three certificate paths, or none. One or two is refused: see
/// [`PeerTlsConfig`].
fn tls_from_env() -> std::io::Result<Option<PeerTlsConfig>> {
    let read = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
    let (cert, key, ca) = (
        read("FELIX_INTERNAL_TLS_CERT"),
        read("FELIX_INTERNAL_TLS_KEY"),
        read("FELIX_INTERNAL_TLS_CA"),
    );
    match (cert, key, ca) {
        (None, None, None) => Ok(None),
        (Some(cert_path), Some(key_path), Some(ca_path)) => Ok(Some(PeerTlsConfig {
            cert_path,
            key_path,
            ca_path,
        })),
        (cert, key, ca) => {
            let missing: Vec<&str> = [
                ("FELIX_INTERNAL_TLS_CERT", cert.is_none()),
                ("FELIX_INTERNAL_TLS_KEY", key.is_none()),
                ("FELIX_INTERNAL_TLS_CA", ca.is_none()),
            ]
            .into_iter()
            .filter_map(|(name, missing)| missing.then_some(name))
            .collect();
            Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "peer mTLS is partly configured: {} not set; set all of \
                     FELIX_INTERNAL_TLS_CERT, FELIX_INTERNAL_TLS_KEY and FELIX_INTERNAL_TLS_CA, \
                     or none of them",
                    missing.join(" and ")
                ),
            ))
        }
    }
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse::<usize>().ok()
}

fn env_millis(name: &str) -> Option<Duration> {
    Some(Duration::from_millis(
        std::env::var(name).ok()?.parse::<u64>().ok()?,
    ))
}

impl PeerTransportConfig {
    /// What this broker offers its peers in the handshake.
    pub fn capabilities(&self) -> felix_wire::internal::PeerCapabilities {
        use felix_wire::internal::PeerCapabilities;
        let labels = PeerCapabilities::GENERATION_LABELS;
        if self.fence {
            labels
                .union(PeerCapabilities::FENCE)
                .union(PeerCapabilities::TAIL_FETCH)
        } else {
            labels
        }
    }

    /// Read the internal transport settings, validated against the
    /// client-facing bind.
    pub fn from_env(client_bind: SocketAddr, client_listeners: usize) -> std::io::Result<Self> {
        let mut config = Self::default();
        if let Some(bind) = std::env::var("FELIX_INTERNAL_BIND")
            .ok()
            .filter(|v| !v.is_empty())
        {
            config.bind = bind.parse().map_err(|_| {
                std::io::Error::new(
                    ErrorKind::InvalidInput,
                    format!("FELIX_INTERNAL_BIND is not a valid host:port address: {bind}"),
                )
            })?;
        }
        config.tls = tls_from_env()?;
        config.allow_unauthenticated = match std::env::var("FELIX_INTERNAL_ALLOW_UNAUTHENTICATED")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("" | "0" | "false" | "no") => false,
            Some("1" | "true" | "yes") => true,
            Some(other) => {
                return Err(std::io::Error::new(
                    ErrorKind::InvalidInput,
                    format!(
                        "FELIX_INTERNAL_ALLOW_UNAUTHENTICATED must be true or false, not {other:?}"
                    ),
                ));
            }
        };
        config.fence = match std::env::var("FELIX_INTERNAL_FENCE")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("" | "1" | "true" | "yes") => true,
            Some("0" | "false" | "no") => false,
            Some(other) => {
                return Err(std::io::Error::new(
                    ErrorKind::InvalidInput,
                    format!("FELIX_INTERNAL_FENCE must be true or false, not {other:?}"),
                ));
            }
        };
        config.partition_file = std::env::var("FELIX_PEER_PARTITION_FILE")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(std::path::PathBuf::from);
        if let Some(value) = env_usize("FELIX_INTERNAL_CONNS_PER_PEER").filter(|v| *v > 0) {
            config.conns_per_peer = value;
        }
        if let Some(value) = env_usize("FELIX_INTERNAL_STREAMS_PER_CONN").filter(|v| *v > 0) {
            config.streams_per_conn = value;
        }
        if let Some(value) = env_usize("FELIX_INTERNAL_MAX_INFLIGHT").filter(|v| *v > 0) {
            config.max_inflight_per_peer = value;
        }
        if let Some(value) = env_usize("FELIX_INTERNAL_MAX_INBOUND_CONNECTIONS").filter(|v| *v > 0)
        {
            config.max_inbound_connections = value;
        }
        if let Some(value) = env_usize("FELIX_INTERNAL_MAX_INBOUND_PER_SOURCE").filter(|v| *v > 0) {
            config.max_inbound_per_source = value;
        }
        if let Some(value) = env_millis("FELIX_INTERNAL_REQUEST_TIMEOUT_MS") {
            config.request_timeout = value;
        }
        if let Some(value) = env_millis("FELIX_INTERNAL_IDLE_TIMEOUT_MS") {
            config.idle_timeout = value;
        }
        if let Some(value) = env_millis("FELIX_INTERNAL_RECONNECT_BASE_MS") {
            config.reconnect_base = value;
        }
        if let Some(value) = env_millis("FELIX_INTERNAL_RECONNECT_MAX_MS") {
            config.reconnect_max = value;
        }
        if let Some(value) = env_millis("FELIX_INTERNAL_HANDSHAKE_TIMEOUT_MS") {
            config.handshake_timeout = value;
        }
        config.validate(client_bind, client_listeners)?;
        Ok(config)
    }

    /// Transport settings for both internal endpoints.
    ///
    /// How long a peer connection may hear nothing before it is declared dead.
    ///
    /// **Shorter than `request_timeout`, and that ordering is the point.** A
    /// broker that is killed leaves its peers holding connections that look
    /// open: nothing is torn down, because nothing is left to tear them down.
    /// Until QUIC gives up on one, every request sent over it waits out
    /// `request_timeout` in full -- and those requests are not idle bookkeeping.
    /// They are a forwarded publish, and a replication pass whose completion is
    /// what releases a `Quorum` publish. With the idle window longer than the
    /// request timeout, a dead peer costs the whole request timeout every time;
    /// with it shorter, the connection fails first and the pool's backoff takes
    /// over.
    ///
    /// Derived from `request_timeout` rather than chosen, so the two cannot be
    /// tuned apart.
    ///
    /// Three quarters, not half: the window has to be long enough that a broker
    /// merely *starved* is not mistaken for one that is gone. A loaded CI runner
    /// under coverage instrumentation can leave a healthy process unscheduled
    /// for seconds, and closing its peer connections on that basis would make
    /// replication churn exactly when the machine can least afford it. Three
    /// quarters still leaves a quarter of the request's patience to spare.
    pub fn peer_idle_timeout(&self) -> Duration {
        self.request_timeout / 4 * 3
    }

    /// Peer connections are long-lived and can be quiet for long stretches
    /// between rebalances, so they need a keep-alive to survive the idle window
    /// above at all. A fifth of it leaves four chances to be heard from before a
    /// healthy but quiet connection would be closed.
    pub fn quic_transport(&self) -> felix_transport::TransportConfig {
        felix_transport::TransportConfig {
            max_idle_timeout: Some(self.peer_idle_timeout()),
            keep_alive_interval: Some(self.peer_idle_timeout() / 5),
            ..Default::default()
        }
    }

    /// Backoff for attempt `attempt` (0-based), capped and jittered.
    ///
    /// Jitter is not decoration here: every broker in a cluster notices the same
    /// peer restart at the same moment, and an unjittered backoff would have all
    /// of them redial it in step.
    pub fn reconnect_delay(&self, attempt: u32) -> Duration {
        let exponential = self
            .reconnect_base
            .saturating_mul(1u32 << attempt.min(16))
            .min(self.reconnect_max);
        let jitter = fastrand_fraction();
        // Full jitter: uniform over [0, exponential]. Decorrelates redials even
        // when every broker starts its backoff in the same millisecond.
        exponential.mul_f64(jitter)
    }

    /// Refuse a configuration in which the two roles could be reached at the
    /// same place.
    ///
    /// Sharing a port is not merely a conflicting bind: it would put client
    /// traffic and peer traffic on one listener, which is the separation the
    /// internal protocol exists to keep.
    fn validate(&self, client_bind: SocketAddr, client_listeners: usize) -> std::io::Result<()> {
        // The client-facing side may occupy a run of consecutive ports
        // (`FELIX_QUIC_LISTENERS`), so the internal listener has to clear the
        // whole range rather than just the first one. Landing inside it is the
        // same fault as sharing the single port -- peer traffic and client
        // traffic on one listener -- and it is easier to do by accident, since
        // the colliding port is one nobody wrote down.
        let first = client_bind.port();
        let last = first.saturating_add(client_listeners.saturating_sub(1) as u16);
        if (first..=last).contains(&self.bind.port()) {
            let clash = if first == last {
                format!("and FELIX_QUIC_BIND ({client_bind}) share a port")
            } else {
                format!("falls inside the FELIX_QUIC_BIND listener range {first}-{last}")
            };
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "FELIX_INTERNAL_BIND ({}) {clash}; \
                     the internal and client-facing listeners must be separate",
                    self.bind
                ),
            ));
        }
        if self.max_inbound_per_source > self.max_inbound_connections {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "FELIX_INTERNAL_MAX_INBOUND_PER_SOURCE ({}) exceeds \
                     FELIX_INTERNAL_MAX_INBOUND_CONNECTIONS ({}); the per-source \
                     limit would never be the one that applies",
                    self.max_inbound_per_source, self.max_inbound_connections
                ),
            ));
        }
        if self.reconnect_base > self.reconnect_max {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "FELIX_INTERNAL_RECONNECT_BASE_MS exceeds FELIX_INTERNAL_RECONNECT_MAX_MS",
            ));
        }
        Ok(())
    }
}

/// A uniform fraction in [0, 1), without pulling in an RNG dependency.
fn fastrand_fraction() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    // Mix so successive calls in the same microsecond do not correlate.
    let mixed = nanos
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .rotate_left(31)
        .wrapping_mul(0xbf58_476d_1ce4_e5b9);
    (mixed >> 11) as f64 / (1u64 << 53) as f64
}
