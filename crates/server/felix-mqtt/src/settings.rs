//! How the listener behaves, beyond what the broker answers.

use std::time::Duration;

use felix_mqtt_wire::Limits;

/// Until a connection has authenticated, no packet may be larger than this.
/// `CONNECT` with a token is far smaller.
pub(crate) const UNAUTHENTICATED_PACKET_BYTES: u32 = 64 * 1024;

/// What every connection is held to. The defaults are the documented ones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// Serve a connection that sends no credentials as this tenant. A
    /// development switch: without it, `CONNECT` must carry a token.
    pub anonymous_tenant: Option<String>,
    /// Time to a valid `CONNECT`.
    pub auth_timeout: Duration,
    /// Largest packet, in bytes. Advertised to 5.0 clients as `Maximum Packet
    /// Size`.
    pub max_packet_bytes: u32,
    /// Longest topic name or filter, in bytes.
    pub max_topic_bytes: usize,
    /// Most levels in a topic name or filter.
    pub max_topic_levels: usize,
    /// Most filters in one `SUBSCRIBE`.
    pub max_subscriptions: usize,
    /// Longest keepalive honoured, in seconds. A client that asks for more, or
    /// for none, gets this one.
    pub keepalive_max_secs: u16,
    /// Most QoS 1 publishes a client may have unacknowledged. Advertised to
    /// 5.0 clients as `Receive Maximum`; a client past it is disconnected.
    /// Also the most publishes of any QoS held for the broker before the
    /// connection stops reading.
    pub max_inflight: u16,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            anonymous_tenant: None,
            auth_timeout: Duration::from_secs(10),
            max_packet_bytes: 1024 * 1024,
            max_topic_bytes: 1024,
            max_topic_levels: 16,
            max_subscriptions: 64,
            keepalive_max_secs: 600,
            max_inflight: 64,
        }
    }
}

impl Settings {
    /// The decode bounds for an authenticated connection.
    pub(crate) fn limits(&self) -> Limits {
        let mut limits = Limits::default();
        limits.max_packet_size = self.max_packet_bytes;
        limits.max_topic_bytes = self.max_topic_bytes;
        limits.max_topic_levels = self.max_topic_levels;
        limits.max_subscriptions = self.max_subscriptions;
        limits
    }

    /// The decode bounds before `CONNECT` has been accepted.
    pub(crate) fn limits_before_connect(&self) -> Limits {
        let mut limits = self.limits();
        limits.max_packet_size = limits.max_packet_size.min(UNAUTHENTICATED_PACKET_BYTES);
        limits
    }
}
