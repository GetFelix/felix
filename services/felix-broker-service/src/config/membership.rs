//! The cluster identity a broker claims, read from `FELIX_NODE_*`.

use std::io::ErrorKind;
use std::net::SocketAddr;

use serde::Serialize;

/// Identity this broker claims in the cluster.
///
/// Present only when `FELIX_NODE_ID` is set. Membership is opt-in because a
/// single-node broker has no cluster to join, and registering one would put a
/// node in the catalog that placement would then try to use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MembershipConfig {
    /// Stable across restarts. This is the identity, not the process.
    pub node_id: String,
    /// `host:port` peers reach this broker on. Not the bind address: a broker
    /// bound to 0.0.0.0 has to advertise something routable.
    pub advertise_addr: String,
    /// `host:port` *clients* reach this broker on, if it offers itself as one.
    ///
    /// Optional, and left unset by default. A broker that does not advertise
    /// one is not offered to clients looking for somewhere to connect, which is
    /// the right answer for a broker behind a load balancer whose own address
    /// no client should hold, and the only safe answer for one whose operator
    /// has not said where clients reach it.
    pub client_advertise_addr: Option<String>,
    /// `host:port` Kafka clients are told to connect to for this broker.
    ///
    /// Set only when the Kafka listener is on. See [`kafka_advertise_addr`].
    pub kafka_advertise_addr: Option<String>,
    pub region: String,
    /// The failure domain within the region, from `FELIX_NODE_ZONE`. `None`
    /// registers no zone, which placement treats as sharing one with no
    /// other broker.
    pub zone: Option<String>,
    /// `(source, dest)` region pairs traffic may cross, from
    /// `FELIX_REGION_BRIDGES`. This broker forwards to a shard's leader only
    /// in its own region or one it has a bridge to.
    pub region_bridges: Vec<(String, String)>,
    /// The fleet features this broker reports: what this build implements
    /// (`felix_common::fleet::IMPLEMENTED`).
    pub features: std::collections::BTreeSet<String>,
}

/// Read the cluster identity, or `None` when this broker is not joining one.
///
/// Fails rather than defaults on a half-configured identity. A broker that
/// guessed its own advertised address would register something unreachable, and
/// the failure would surface later as peers unable to connect to a node the
/// catalog says is live.
pub(super) fn membership_from_env(
    controlplane_url: &Option<String>,
    controlplane_token: &str,
) -> std::io::Result<Option<MembershipConfig>> {
    let Some(node_id) = std::env::var("FELIX_NODE_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };

    let advertise_addr = std::env::var("FELIX_NODE_ADVERTISE_ADDR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            std::io::Error::new(
                ErrorKind::InvalidInput,
                "FELIX_NODE_ID is set but FELIX_NODE_ADVERTISE_ADDR is not; \
                 a broker cannot advertise an address it has to guess",
            )
        })?;

    // Parsed here so a malformed address fails at startup rather than as a
    // rejected registration once everything else is already running.
    if advertise_addr.parse::<SocketAddr>().is_err() {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            format!("FELIX_NODE_ADVERTISE_ADDR is not a valid host:port address: {advertise_addr}"),
        ));
    }

    if controlplane_url.is_none() {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "FELIX_NODE_ID is set but FELIX_CONTROLPLANE_URL is not; \
             there is nowhere to register",
        ));
    }

    if controlplane_token.is_empty() {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "FELIX_NODE_ID is set but no node credential was provided; \
             set FELIX_NODE_TOKEN or FELIX_NODE_TOKEN_FILE",
        ));
    }

    let region_bridges = match std::env::var("FELIX_REGION_BRIDGES") {
        Ok(spec) => felix_router::parse_bridges(&spec).map_err(|err| {
            std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("FELIX_REGION_BRIDGES: {err}"),
            )
        })?,
        Err(_) => Vec::new(),
    };

    // A name is allowed: clients resolve it. A value that is not even
    // `host:port` would be skipped by every client, so it fails here.
    let client_advertise_addr = std::env::var("FELIX_CLIENT_ADVERTISE_ADDR")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if let Some(addr) = &client_advertise_addr
        && !is_host_port(addr)
    {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            format!("FELIX_CLIENT_ADVERTISE_ADDR is not host:port: {addr}"),
        ));
    }

    Ok(Some(MembershipConfig {
        node_id,
        region_bridges,
        advertise_addr,
        client_advertise_addr,
        kafka_advertise_addr: kafka_advertise_addr(
            std::env::var("FELIX_KAFKA_ADVERTISE_ADDR").ok().as_deref(),
            std::env::var("FELIX_KAFKA_LISTEN").ok().as_deref(),
        ),
        region: std::env::var("FELIX_REGION_ID").unwrap_or_else(|_| "local".to_string()),
        zone: std::env::var("FELIX_NODE_ZONE")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        features: reported_features(),
    }))
}

/// What this build implements, unless a debug build's test harness has
/// asked it to report something else to play an older or newer broker.
fn reported_features() -> std::collections::BTreeSet<String> {
    #[cfg(debug_assertions)]
    if let Ok(names) = std::env::var("FELIX_TEST_FLEET_FEATURES") {
        return names
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .collect();
    }
    // Acknowledging by the followers, reading without the lease, and fencing
    // caches are safe only if every broker fences what it is promoted to, and
    // this one would not.
    let fences = !matches!(
        std::env::var("FELIX_INTERNAL_FENCE")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref(),
        Some("0" | "false" | "no")
    );
    felix_common::fleet::IMPLEMENTED
        .iter()
        .filter(|feature| {
            fences
                || !matches!(
                    **feature,
                    felix_common::fleet::MAJORITY_ACK
                        | felix_common::fleet::LEASE_FREE_READS
                        | felix_common::fleet::FENCED_CACHES
                )
        })
        .map(|feature| feature.name().to_string())
        .collect()
}

/// The Kafka address to register, from `FELIX_KAFKA_ADVERTISE_ADDR` and
/// `FELIX_KAFKA_LISTEN`.
///
/// `None` unless the listener is on, even if an advertised address is set:
/// registering one would send Kafka clients to a port nothing listens on.
/// Without an explicit advertised address the bind address is used. Empty or
/// blank values count as unset.
pub(crate) fn kafka_advertise_addr(
    advertise: Option<&str>,
    listen: Option<&str>,
) -> Option<String> {
    let non_empty = |value: Option<&str>| {
        value
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let listen = non_empty(listen)?;
    Some(non_empty(advertise).unwrap_or(listen))
}

/// Warn when peers would be told to connect somewhere nothing is listening.
///
/// `NodeSpec.advertise_addr` is the *internal* listener's address, so a broker
/// that advertises a port it does not bind is reachable by the catalog and
/// unreachable in fact. Not fatal: a deployment may map ports, and refusing to
/// start on a legitimate NAT would be worse than saying so.
pub(super) fn warn_on_unreachable_advertise(
    membership: &MembershipConfig,
    peer: &felix_replication::peer::PeerTransportConfig,
) {
    // Port 0 is an ephemeral bind, so there is nothing to compare against.
    if peer.bind.port() == 0 {
        return;
    }
    let Ok(advertised) = membership.advertise_addr.parse::<SocketAddr>() else {
        return;
    };
    if advertised.port() != peer.bind.port() {
        tracing::warn!(
            advertise_addr = %membership.advertise_addr,
            internal_bind = %peer.bind,
            "FELIX_NODE_ADVERTISE_ADDR names a different port than the internal \
             listener binds; peers will be told to connect where nothing is listening \
             unless the ports are mapped",
        );
    }
}

/// Whether `addr` is a host, or a bracketed IPv6 address, and a port.
fn is_host_port(addr: &str) -> bool {
    addr.parse::<SocketAddr>().is_ok()
        || addr.rsplit_once(':').is_some_and(|(host, port)| {
            !host.is_empty() && !host.contains(':') && port.parse::<u16>().is_ok()
        })
}

#[cfg(test)]
mod tests;
