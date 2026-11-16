//! Where a client may connect, as this broker last understood the cluster.
//!
//! Kept apart from the node catalog the router uses. That catalog is addressed
//! to other brokers -- it names the internal listener, and it carries placement
//! and liveness detail that is the cluster's business. This is the subset a
//! tenant's client is entitled to and can act on: an identity, and somewhere to
//! dial.
//!
//! Refreshed by the same loop that refreshes the catalog, and read on the
//! control stream, so it is an `ArcSwap`: one writer on a timer, a reader per
//! `Topology` request, and no reason for either to wait on the other.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arc_swap::ArcSwap;
use felix_wire::BrokerEndpoint;

#[derive(Debug, Default)]
pub struct ClientEndpoints {
    /// What a client is told it may connect to: eligible brokers only.
    endpoints: ArcSwap<Vec<BrokerEndpoint>>,
    /// Where a redirect may point: every routable broker, draining ones
    /// included, since a draining broker still leads what it has not handed
    /// off.
    redirects: ArcSwap<Vec<BrokerEndpoint>>,
    /// Kafka listener addresses of every routable broker that runs one.
    kafka: ArcSwap<Vec<BrokerEndpoint>>,
    /// How many brokers the catalog lists, this one included.
    brokers: AtomicUsize,
}

impl ClientEndpoints {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace every list with this refresh of the node catalog.
    ///
    /// Whole-list rather than merged: a broker that has stopped advertising a
    /// client address, or stopped being eligible, has to leave the answer, and
    /// merging would keep handing clients an address the cluster no longer
    /// stands behind.
    pub fn refresh(&self, catalog: &crate::cluster::node_catalog::NodeCatalog) {
        self.endpoints
            .store(Arc::new(catalog.client_endpoints.clone()));
        self.redirects
            .store(Arc::new(catalog.redirect_endpoints.clone()));
        self.kafka.store(Arc::new(catalog.kafka_endpoints.clone()));
        self.brokers.store(catalog.nodes.len(), Ordering::Relaxed);
    }

    /// How many brokers the last refresh listed, live or not; zero before
    /// the first.
    pub fn brokers(&self) -> usize {
        self.brokers.load(Ordering::Relaxed)
    }

    /// The client address to name in a redirect to `node_id`, if the cluster
    /// has published one for a broker still routable.
    pub fn redirect_addr(&self, node_id: &str) -> Option<String> {
        self.redirects
            .load()
            .iter()
            .find(|endpoint| endpoint.node_id == node_id)
            .map(|endpoint| endpoint.addr.clone())
    }

    pub fn snapshot(&self) -> Arc<Vec<BrokerEndpoint>> {
        self.endpoints.load_full()
    }

    /// Where Kafka clients may connect, sorted by node id.
    pub fn kafka_snapshot(&self) -> Arc<Vec<BrokerEndpoint>> {
        self.kafka.load_full()
    }
}

#[cfg(test)]
mod tests;
