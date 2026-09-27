//! [`TransportConfig`]: the tuning every endpoint is built from, its
//! defaults, and the environment variables that override them.

mod loopback;
mod quinn_settings;

use std::time::Duration;

// Keep defaults large enough for most dev/test workloads.
const DEFAULT_MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_STREAMS: u16 = 1024;
const DEFAULT_RECEIVE_WINDOW: u64 = 64 * 1024 * 1024;
const DEFAULT_STREAM_RECEIVE_WINDOW: u64 = 16 * 1024 * 1024;
const DEFAULT_SEND_WINDOW: u64 = 64 * 1024 * 1024;
const DEFAULT_INITIAL_MTU: u16 = 1200;
// Bound MTU discovery below Linux's UDP GSO ceiling, for the same reason
// `LOOPBACK_PINNED_MTU_CAP` exists: a batch is one IP datagram, so
// `mtu * segments <= 65535`, and quinn batches up to 10 -- making 6553 the true
// ceiling. Above it the kernel rejects every batch with `EMSGSIZE`, which quinn
// does not treat as a GSO failure (it falls back only on EIO/EINVAL), so
// delivery stalls and stays stalled.
//
// This bound used to be 16384, which is above that ceiling. Loopback was
// already capped, but a *routed* path was not -- and a jumbo-frame network,
// which is what you buy for throughput, is exactly where discovery climbs past
// 6553 and the stall is permanent. Every perf session set
// `FELIX_MTU_UPPER_BOUND=4096` by hand to avoid it; that is now the default.
//
// 4096 rather than 6553: `MAX_TRANSMIT_SEGMENTS` is private to quinn, so the
// ceiling cannot be derived through its API, and 4096 still holds if the batch
// size rises to 15 where 6553 breaks the moment it moves. It is also the
// fastest configuration measured on Linux (round 18) and converges faster than
// a far-away bound, which never finds a larger path anyway.
//
// macOS has no GSO and no such limit, and 16336 is measured good there over
// hundreds of runs -- the same split `LOOPBACK_PINNED_MTU_CAP` makes.
const DEFAULT_MTU_DISCOVERY_UPPER_BOUND: u16 =
    mtu_discovery_upper_bound_for(cfg!(target_os = "macos"));

const DEFAULT_MAX_UDP_PAYLOAD_SIZE: u16 = 65527;
const DEFAULT_UDP_BUFFER_BYTES: usize = 8 * 1024 * 1024;
// The idle timeout is how long a peer that died without closing (SIGKILL, a
// pulled cable) takes to be noticed, and a publish waiting on that connection
// waits that long before it can go elsewhere. So it is short. Three keep-alives
// fit inside it, so a healthy but quiet connection survives two lost packets.
// A WAN deployment that sees multi-second stalls can raise both through
// FELIX_MAX_IDLE_TIMEOUT_MS and FELIX_KEEPALIVE_MS.
const DEFAULT_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(6);
const DEFAULT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(2);

/// Transport-level configuration defaults.
///
/// ```
/// use felix_transport::TransportConfig;
///
/// let config = TransportConfig::default();
/// assert!(config.max_frame_bytes > 0);
/// ```
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// Max payload size enforced by higher layers.
    pub max_frame_bytes: usize,
    /// Max concurrent streams per connection.
    pub max_streams: u16,
    /// Connection-level flow control window.
    pub receive_window: u64,
    /// Per-stream receive window.
    pub stream_receive_window: u64,
    /// Connection-level send window.
    pub send_window: u64,
    /// Starting datagram size before path MTU discovery completes.
    /// RFC-safe default (1200); raise only on known-good paths (loopback, jumbo LAN)
    /// to skip the discovery ramp entirely.
    pub initial_mtu: u16,
    /// Upper bound for path MTU discovery probing. Probes are loss-tolerant, so a
    /// high bound lets loopback (~16 KiB) and jumbo-frame LANs (~9 KiB) converge
    /// to their real MTU instead of quinn's 1452 default. On Linux it must stay
    /// under the UDP GSO ceiling, which is why the default there is 4096.
    pub mtu_discovery_upper_bound: u16,
    /// Largest UDP datagram the endpoint will accept (receive side). Must be at
    /// least as large as the peer's discovered MTU for large datagrams to flow.
    pub max_udp_payload_size: u16,
    /// Requested SO_SNDBUF. Applied best-effort: halved until the OS accepts,
    /// so an unconfigurable host degrades gracefully.
    pub udp_send_buffer_bytes: usize,
    /// Requested SO_RCVBUF, applied the same way.
    pub udp_recv_buffer_bytes: usize,
    /// Optional initial congestion window (bytes). None keeps quinn's RFC default.
    /// Setting this high removes the slow-start ramp on trusted low-loss paths.
    pub initial_congestion_window_bytes: Option<u64>,
    /// How often to send a keep-alive on an otherwise idle connection.
    ///
    /// Load-bearing, not a tuning knob. QUIC closes a connection that has been
    /// idle for `max_idle_timeout`, and a subscription to a quiet stream is
    /// exactly that: the broker sends nothing, the client sends nothing, no
    /// packets flow, and the connection dies underneath a subscriber that is
    /// still perfectly healthy. Without this, a stream with a 30-second gap
    /// between records loses every subscriber.
    ///
    /// Must stay below half of `max_idle_timeout`, so one lost keep-alive
    /// does not end the connection. A larger value is replaced by a third of
    /// the idle timeout when the endpoint is built (see
    /// [`TransportConfig::effective_keep_alive`]). `None` turns keep-alives off.
    /// quinn only sends these when the connection is otherwise silent, so a
    /// busy connection pays nothing. `FELIX_KEEPALIVE_MS`.
    pub keep_alive_interval: Option<std::time::Duration>,
    /// How long a silent connection survives.
    ///
    /// Set explicitly rather than inherited so the relationship with
    /// `keep_alive_interval` is visible in one place: changing this without
    /// changing that is how idle subscriptions start dying again.
    /// `FELIX_MAX_IDLE_TIMEOUT_MS`.
    pub max_idle_timeout: Option<std::time::Duration>,
}

impl TransportConfig {
    /// The keep-alive interval an endpoint is actually built with.
    ///
    /// With an idle timeout set, a keep-alive not below half of it would let
    /// one lost packet close a healthy connection, so it is replaced by a
    /// third of the timeout. A zero interval is treated the same way: quinn
    /// would read it as "send constantly". `None` stays off.
    pub fn effective_keep_alive(&self) -> Option<Duration> {
        let Some(idle) = self.max_idle_timeout else {
            return self
                .keep_alive_interval
                .filter(|interval| !interval.is_zero());
        };
        match self.keep_alive_interval {
            None => None,
            Some(interval) if !interval.is_zero() && interval * 2 < idle => Some(interval),
            configured => {
                let replacement = idle / 3;
                tracing::warn!(
                    configured = ?configured,
                    idle_timeout = ?idle,
                    using = ?replacement,
                    "the QUIC keep-alive must be under half the idle timeout; using a third of it",
                );
                Some(replacement)
            }
        }
    }
}

impl Default for TransportConfig {
    fn default() -> Self {
        // Environment overrides act as process-wide tuning levers so every
        // endpoint (broker, client, demos) picks them up without plumbing.
        let ceiling = mtu_override_ceiling_for(cfg!(target_os = "macos"));
        let initial_mtu = env_u64("FELIX_INITIAL_MTU")
            .map(|value| clamp_mtu_override("FELIX_INITIAL_MTU", value, ceiling))
            .unwrap_or(DEFAULT_INITIAL_MTU);
        let mtu_discovery_upper_bound = env_u64("FELIX_MTU_UPPER_BOUND")
            .map(|value| clamp_mtu_override("FELIX_MTU_UPPER_BOUND", value, ceiling))
            .unwrap_or(DEFAULT_MTU_DISCOVERY_UPPER_BOUND);
        let max_udp_payload_size = env_u64("FELIX_MAX_UDP_PAYLOAD")
            .map(|value| value.clamp(1200, 65527) as u16)
            .unwrap_or(DEFAULT_MAX_UDP_PAYLOAD_SIZE);
        let udp_send_buffer_bytes = env_u64("FELIX_UDP_SEND_BUFFER")
            .map(|value| value as usize)
            .unwrap_or(DEFAULT_UDP_BUFFER_BYTES);
        let udp_recv_buffer_bytes = env_u64("FELIX_UDP_RECV_BUFFER")
            .map(|value| value as usize)
            .unwrap_or(DEFAULT_UDP_BUFFER_BYTES);
        let initial_congestion_window_bytes = env_u64("FELIX_INITIAL_CWND");
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_streams: DEFAULT_MAX_STREAMS,
            receive_window: DEFAULT_RECEIVE_WINDOW,
            stream_receive_window: DEFAULT_STREAM_RECEIVE_WINDOW,
            send_window: DEFAULT_SEND_WINDOW,
            initial_mtu,
            mtu_discovery_upper_bound,
            max_udp_payload_size,
            udp_send_buffer_bytes,
            udp_recv_buffer_bytes,
            initial_congestion_window_bytes,
            keep_alive_interval: env_millis("FELIX_KEEPALIVE_MS")
                .or(Some(DEFAULT_KEEP_ALIVE_INTERVAL)),
            max_idle_timeout: env_millis("FELIX_MAX_IDLE_TIMEOUT_MS")
                .or(Some(DEFAULT_MAX_IDLE_TIMEOUT)),
        }
    }
}

/// The default bound, as a function of the platform, so both branches can be
/// tested from either one.
///
/// A `cfg!` expression would make the Linux value unreachable on a macOS
/// developer machine -- and the value that matters is the Linux one, because
/// Linux is where GSO makes it load-bearing. A test that silently passes on the
/// host doing the editing is worth very little.
const fn mtu_discovery_upper_bound_for(macos: bool) -> u16 {
    if macos { 16384 } else { 4096 }
}

/// The largest MTU an override may set.
///
/// On Linux a GSO batch of quinn's 10 segments is one IP datagram, so an MTU
/// over 6553 gets every batch refused with `EMSGSIZE` and stalls delivery for
/// good (see `DEFAULT_MTU_DISCOVERY_UPPER_BOUND`). An override is clamped to
/// that ceiling rather than trusted to know it. macOS has no GSO limit.
const fn mtu_override_ceiling_for(macos: bool) -> u16 {
    if macos { 65527 } else { 6553 }
}

fn clamp_mtu_override(name: &str, value: u64, ceiling: u16) -> u16 {
    let clamped = value.clamp(1200, u64::from(ceiling)) as u16;
    if u64::from(clamped) != value {
        tracing::warn!(
            name,
            requested = value,
            used = clamped,
            "MTU override out of range; clamped"
        );
    }
    clamped
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse::<u64>().ok()
}

fn env_millis(name: &str) -> Option<Duration> {
    Some(Duration::from_millis(env_u64(name)?))
}

#[cfg(test)]
mod tests;
