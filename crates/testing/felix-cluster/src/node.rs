//! One broker process: its addresses, its data directory, and the child
//! handle the harness stops it through.

mod logs;
mod spawn;

pub(crate) use logs::FAILURE_LOG_LINES;
pub(crate) use spawn::{broker_binary, spawn_broker};

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, ExitStatus};

/// One broker process.
pub struct BrokerNode {
    pub node_id: String,
    /// Where clients publish and subscribe.
    pub client_addr: SocketAddr,
    /// Where the internal listener binds. Peers dial this, or its proxy in a
    /// cluster started with
    /// [`ClusterConfig::proxy_links`](crate::ClusterConfig::proxy_links).
    pub internal_addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    /// The Kafka listener's advertised `host:port`, when the cluster was
    /// started with [`ClusterConfig::kafka`](crate::ClusterConfig::kafka).
    pub kafka_addr: Option<String>,
    pub data_dir: PathBuf,
    /// `None` once the node has been stopped.
    process: Option<Child>,
}

impl BrokerNode {
    /// This broker's process id, while it is running.
    ///
    /// For measurement: attributing CPU to the broker rather than to the
    /// process driving it is the difference between "the broker is the
    /// bottleneck" and "the generator is".
    pub fn pid(&self) -> Option<u32> {
        self.process.as_ref().map(|child| child.id())
    }

    /// Whether the harness still holds this broker's process, i.e. has not
    /// stopped or killed it.
    pub fn is_running(&self) -> bool {
        self.process.is_some()
    }

    /// The exit status if this broker has already stopped.
    ///
    /// A broker that refuses its configuration exits within milliseconds. Left
    /// unchecked, that becomes a readiness timeout tens of seconds later that
    /// says nothing about why.
    pub(crate) fn exited(&mut self) -> Option<ExitStatus> {
        self.process.as_mut()?.try_wait().ok().flatten()
    }

    /// Take the child handle, leaving the node stopped. `None` if it already was.
    pub(crate) fn take_process(&mut self) -> Option<Child> {
        self.process.take()
    }
}

/// Where a broker's test-only clock skew lives. See `felix_common::clock::fault`.
pub(crate) fn clock_fault_file(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join("clock-fault")
}

/// Where a broker's test-only disk faults live. See `felix_storage::fault`.
pub(crate) fn storage_fault_file(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join("storage-fault")
}

/// Where a broker's test-only partition list lives.
///
/// Under the data directory, so it is cleaned up with the cluster and a broker
/// the harness restarts keeps the same one.
pub(crate) fn partition_file(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join("peer-partition")
}
