//! A network the harness owns: broker to broker, and each broker to the
//! control plane.
//!
//! Brokers learn each other's addresses from the catalog, which holds what
//! each one advertised, and reach the control plane at the URL they were
//! given. So a broker advertises its peer proxy instead of its internal
//! listener and is handed its own control-plane proxy as the URL, and every
//! link runs through here with no change to the broker. That is what lets a
//! test drop or delay one direction of one link while every process keeps
//! running.
//!
//! The proxies run on a runtime of their own. Most cluster tests run on a
//! single-threaded runtime, and a test blocking it for a moment must not
//! stall the whole cluster's network.

mod owners;
mod rules;
mod tcp;
mod udp;

pub(crate) use rules::Rules;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use crate::fault::Endpoint;
use owners::Processes;
use tcp::TcpProxy;
use udp::UdpProxy;

/// Every proxy in one cluster, and the rules they apply.
pub(crate) struct Links {
    handle: Handle,
    shutdown: CancellationToken,
    rules: Arc<Rules>,
    processes: Arc<Processes>,
    control_plane: SocketAddr,
    nodes: Mutex<HashMap<String, NodeProxies>>,
}

/// The two proxies in front of one broker.
struct NodeProxies {
    /// In front of its internal listener, for the peers that dial it.
    peer: UdpProxy,
    /// In front of the control plane, for its own requests.
    control_plane: TcpProxy,
}

/// Where a broker behind the proxies is told to advertise and to connect.
pub(crate) struct Route {
    pub(crate) advertise: SocketAddr,
    pub(crate) control_plane_url: String,
}

impl Links {
    /// Start the proxy runtime. `control_plane` is where the control plane
    /// really listens; it keeps that address across a restart.
    pub(crate) fn start(control_plane: SocketAddr) -> Result<Self> {
        let shutdown = CancellationToken::new();
        let (handle_tx, handle_rx) = std::sync::mpsc::channel();
        let stopped = shutdown.clone();
        std::thread::Builder::new()
            .name("felix-cluster-links".to_string())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_name("felix-cluster-links")
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        let _ = handle_tx.send(Err(err));
                        return;
                    }
                };
                let _ = handle_tx.send(Ok(runtime.handle().clone()));
                // Dropped on this thread, outside any async context, which
                // is the one place a runtime may be dropped.
                runtime.block_on(stopped.cancelled());
            })
            .context("start the link proxy thread")?;
        let handle = handle_rx
            .recv()
            .context("the link proxy thread exited")?
            .context("build the link proxy runtime")?;
        Ok(Self {
            handle,
            shutdown,
            rules: Arc::new(Rules::new()),
            processes: Arc::new(Processes::default()),
            control_plane,
            nodes: Mutex::new(HashMap::new()),
        })
    }

    /// The addresses `node_id` should use, with its internal listener at
    /// `internal`. The proxies are made on first use and kept, so a broker
    /// started again advertises the same address and only the proxy's
    /// upstream moves.
    pub(crate) fn route(&self, node_id: &str, internal: SocketAddr) -> Result<Route> {
        let mut nodes = self.nodes.lock().expect("links lock");
        if let Some(proxies) = nodes.get(node_id) {
            proxies.peer.set_upstream(internal);
        } else {
            let endpoint = Endpoint::node(node_id);
            let peer = UdpProxy::start(
                &self.handle,
                endpoint.clone(),
                internal,
                Arc::clone(&self.rules),
                self.processes.resolver(),
                self.shutdown.clone(),
            )?;
            let control_plane = TcpProxy::start(
                &self.handle,
                endpoint,
                Endpoint::ControlPlane,
                self.control_plane,
                Arc::clone(&self.rules),
                self.shutdown.clone(),
            )?;
            nodes.insert(
                node_id.to_string(),
                NodeProxies {
                    peer,
                    control_plane,
                },
            );
        }
        let proxies = nodes
            .get(node_id)
            .ok_or_else(|| anyhow!("no proxies for {node_id}"))?;
        Ok(Route {
            advertise: proxies.peer.addr(),
            control_plane_url: format!("http://{}", proxies.control_plane.addr()),
        })
    }

    /// Note which process is `node_id` now, so its datagrams can be told
    /// apart from another broker's.
    pub(crate) fn track(&self, node_id: &str, pid: Option<u32>) {
        self.processes.set(node_id, pid);
    }

    pub(crate) fn rules(&self) -> &Rules {
        &self.rules
    }
}

impl Drop for Links {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[cfg(test)]
mod tests;
