//! Limits applied where clients come in: how many connections one source
//! address may hold, and how fast one tenant may publish.
//!
//! Both are enforced at the edge (accept loops and publish admission) rather
//! than in the broker core, so a refusal costs nothing below the handler and
//! the client is told before any work is queued for it.

mod per_ip;
mod tenant_rate;

pub(crate) use per_ip::{PerIpLimiter, PerIpPermit};
pub(crate) use tenant_rate::TenantRates;

use std::sync::Arc;

use crate::config::BrokerConfig;

/// Client QUIC connections refused on arrival, by `reason`; the same series
/// the broker-wide cap in `quic::preauth` counts under `limit`.
pub(crate) const QUIC_CONNECTIONS_REFUSED_TOTAL: &str = "felix_quic_connections_refused_total";

/// The limits one broker's client listeners share.
///
/// Built once per broker and handed to every QUIC accept loop and the Kafka
/// listener, so a broker with several listener sockets still has one
/// per-address count and one bucket per tenant.
pub struct ListenerLimits {
    pub(crate) quic_per_ip: Arc<PerIpLimiter>,
    pub(crate) tenant_rates: Arc<TenantRates>,
}

impl ListenerLimits {
    pub fn from_config(config: &BrokerConfig) -> Arc<Self> {
        Arc::new(Self {
            quic_per_ip: PerIpLimiter::new(config.limits.max_connections_per_ip),
            tenant_rates: Arc::new(TenantRates::new(&config.limits)),
        })
    }
}

/// Take a place for a connection from `peer`, or count the refusal. Called
/// before the handshake, like the broker-wide cap, so a refused host costs one
/// packet; the caller refuses the connection.
pub(crate) fn admit_quic_connection(
    limiter: &Arc<PerIpLimiter>,
    peer: std::net::SocketAddr,
) -> Option<PerIpPermit> {
    let permit = limiter.try_acquire(peer.ip());
    if permit.is_none() {
        // Debug, not warn: under a flood this fires per attempt, and the
        // counter is what to alert on.
        tracing::debug!(
            %peer,
            max = limiter.max(),
            "refusing a client connection: too many from this address (FELIX_MAX_CONNECTIONS_PER_IP)"
        );
        metrics::counter!(QUIC_CONNECTIONS_REFUSED_TOTAL, "reason" => "per_ip_limit").increment(1);
    }
    permit
}

#[cfg(test)]
mod tests;
