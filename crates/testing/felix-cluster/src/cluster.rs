//! The running cluster a test holds.
//!
//! What a test does with it is split by what it touches, one file per concern
//! below; each extends `impl Cluster`. They are children of this module so the
//! fields that are not part of the API can stay private.

mod caches;
mod faults;
mod groups;
mod metrics;
mod ownership;
mod placement;
mod restarts;
mod startup;
mod streams;

pub use ownership::Assignment;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::node::{BrokerNode, FAILURE_LOG_LINES};
use crate::{ClusterConfig, ControlPlane, session};

/// How long any single start-up wait may take before the harness gives up.
const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// A running cluster.
pub struct Cluster {
    pub control_plane: Option<ControlPlane>,
    pub nodes: Vec<BrokerNode>,
    pub tenant_id: String,
    pub namespace: String,
    /// Presented to brokers over QUIC.
    pub client_token: String,
    /// Presented to the control plane's HTTP API for reads.
    pub admin_token: String,
    /// Presented for membership writes, which reads alone cannot do.
    pub operator_token: String,
    /// May subscribe, may not publish.
    pub subscribe_only_token: String,
    /// Presented to brokers to redrive or discard a dead letter.
    pub group_operator_token: String,
    http: reqwest::Client,
    /// Kept so a broker that loses the port race can be started again.
    binary: PathBuf,
    config: ClusterConfig,
    /// Held so the data directories outlive the brokers and are removed with
    /// the cluster.
    _root: tempfile::TempDir,
}

impl Cluster {
    /// The control plane's address. Panics once it has been stopped, which is
    /// deliberate: a caller reading this after `stop_control_plane` is asking
    /// for a service that is gone.
    pub fn control_plane_url(&self) -> &str {
        &self.control_plane().base_url
    }

    /// Everything another process needs to talk to this cluster.
    pub fn session(&self) -> session::Session {
        session::Session {
            control_plane: self.control_plane_url().to_string(),
            tenant_id: self.tenant_id.clone(),
            namespace: self.namespace.clone(),
            client_token: self.client_token.clone(),
            admin_token: self.admin_token.clone(),
            nodes: self
                .nodes
                .iter()
                .map(|node| session::SessionNode {
                    node_id: node.node_id.clone(),
                    client_addr: node.client_addr,
                    metrics_addr: node.metrics_addr,
                })
                .collect(),
        }
    }

    /// Every broker's id, in start order.
    pub fn node_ids(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.node_id.clone()).collect()
    }

    /// The broker named `node_id`, if the cluster has one.
    pub fn node(&self, node_id: &str) -> Option<&BrokerNode> {
        self.nodes.iter().find(|node| node.node_id == node_id)
    }

    /// Every broker's client address, for a seed list.
    pub fn broker_addrs(&self) -> Vec<SocketAddr> {
        self.nodes.iter().map(|node| node.client_addr).collect()
    }

    /// Stop everything. Called by `Drop` too, so an aborted test leaves nothing
    /// behind — this exists for the case where a caller wants to wait for it.
    pub async fn shutdown(mut self) {
        self.kill_brokers();
        if let Some(control_plane) = self.control_plane.take() {
            control_plane.shutdown().await;
        }
    }

    fn control_plane(&self) -> &ControlPlane {
        self.control_plane
            .as_ref()
            .expect("control plane is only taken during shutdown")
    }

    fn kill_brokers(&mut self) {
        for node in &mut self.nodes {
            if let Some(mut process) = node.take_process() {
                let _ = process.kill();
                let _ = process.wait();
            }
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        // On the way out of a failing test, say what the brokers were saying.
        //
        // Otherwise an assertion failure gives the assertion and nothing else:
        // the data root is a `TempDir` and takes every broker log with it. A CI
        // failure like "broker-2 never started forwarding after it moved" would
        // leave no way to ask *why* without reproducing it, which is the one
        // thing a timing-dependent failure will not do on request.
        //
        // Only while panicking, so a passing run stays quiet. Brokers only:
        // the control plane runs in-process, so its tracing is already on this
        // test's stderr.
        if std::thread::panicking() {
            eprintln!("\n--- broker logs (printed because the test failed) ---");
            for node in &self.nodes {
                eprintln!("{}", node.failure_log(FAILURE_LOG_LINES));
            }
            eprintln!("--- end broker logs ---\n");
        }

        // A panicking test must not leave broker processes running. The data
        // root is a `TempDir`, so it goes with this too — but only after the
        // processes holding it are gone.
        self.kill_brokers();
    }
}
