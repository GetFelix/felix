//! Which broker a datagram came from.
//!
//! A broker dials its peers from one UDP socket bound to an ephemeral port,
//! and nothing on the wire says whose it is: the node id travels inside the
//! TLS handshake. So the peer proxy asks the OS which of the harness's broker
//! processes holds the source port, once per new source. On Linux that is
//! `/proc`; elsewhere `lsof`.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use crate::fault::Endpoint;

/// Maps a datagram's source to the endpoint that sent it. Pluggable so the
/// proxy's own tests can name sockets without spawning processes.
pub(crate) type Resolver = Arc<dyn Fn(SocketAddr) -> Option<Endpoint> + Send + Sync>;

/// The broker processes the harness is running, by node id.
#[derive(Default)]
pub(crate) struct Processes {
    pids: Mutex<Vec<(String, u32)>>,
}

impl Processes {
    /// Record `node_id`'s current process, or that it has none.
    pub(crate) fn set(&self, node_id: &str, pid: Option<u32>) {
        let mut pids = self.pids.lock().expect("process table lock");
        pids.retain(|(id, _)| id != node_id);
        if let Some(pid) = pid {
            pids.push((node_id.to_string(), pid));
        }
    }

    /// The broker holding the UDP socket `source` was sent from, if any.
    pub(crate) fn owner_of_udp(&self, source: SocketAddr) -> Option<Endpoint> {
        let pids = self.pids.lock().expect("process table lock").clone();
        let port = source.port();
        #[cfg(target_os = "linux")]
        {
            let inodes = linux::udp_inodes(port);
            if inodes.is_empty() {
                return None;
            }
            pids.into_iter()
                .find(|(_, pid)| linux::holds_any(*pid, &inodes))
                .map(|(id, _)| Endpoint::Node(id))
        }
        #[cfg(not(target_os = "linux"))]
        {
            pids.into_iter()
                .find(|(_, pid)| lsof_holds(*pid, port))
                .map(|(id, _)| Endpoint::Node(id))
        }
    }

    /// A [`Resolver`] over this table.
    pub(crate) fn resolver(self: &Arc<Self>) -> Resolver {
        let this = Arc::clone(self);
        Arc::new(move |source| this.owner_of_udp(source))
    }
}

#[cfg(target_os = "linux")]
mod linux {
    /// Socket inodes bound to local UDP `port`, v4 and v6.
    pub(super) fn udp_inodes(port: u16) -> Vec<u64> {
        let mut inodes = Vec::new();
        for table in ["/proc/net/udp", "/proc/net/udp6"] {
            let Ok(body) = std::fs::read_to_string(table) else {
                continue;
            };
            // `sl local_address rem_address st tx:rx tr:when retrnsmt uid timeout inode`
            for line in body.lines().skip(1) {
                let fields: Vec<&str> = line.split_whitespace().collect();
                let (Some(local), Some(inode)) = (fields.get(1), fields.get(9)) else {
                    continue;
                };
                let local_port = local
                    .rsplit(':')
                    .next()
                    .and_then(|hex| u16::from_str_radix(hex, 16).ok());
                if local_port == Some(port)
                    && let Ok(inode) = inode.parse()
                {
                    inodes.push(inode);
                }
            }
        }
        inodes
    }

    /// Whether `pid` has an open descriptor on any of `inodes`.
    pub(super) fn holds_any(pid: u32, inodes: &[u64]) -> bool {
        let Ok(fds) = std::fs::read_dir(format!("/proc/{pid}/fd")) else {
            return false;
        };
        fds.flatten().any(|fd| {
            std::fs::read_link(fd.path())
                .ok()
                .and_then(|target| {
                    let target = target.to_string_lossy().into_owned();
                    target
                        .strip_prefix("socket:[")?
                        .strip_suffix(']')?
                        .parse::<u64>()
                        .ok()
                })
                .is_some_and(|inode| inodes.contains(&inode))
        })
    }
}

#[cfg(not(target_os = "linux"))]
fn lsof_holds(pid: u32, port: u16) -> bool {
    std::process::Command::new("lsof")
        .args([
            "-nP",
            "-a",
            "-p",
            &pid.to_string(),
            &format!("-iUDP:{port}"),
            "-t",
        ])
        .output()
        .is_ok_and(|output| output.status.success() && !output.stdout.is_empty())
}
