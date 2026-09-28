//! Starting a broker process.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow};

use super::{BrokerNode, clock_fault_file, partition_file, storage_fault_file};
use crate::proxy::Links;
use crate::{ClusterConfig, ControlPlane, pki, ports};

/// Start one broker process, behind `links` when the cluster proxies them.
pub(crate) fn spawn_broker(
    binary: &PathBuf,
    control_plane: &ControlPlane,
    config: &ClusterConfig,
    root: &Path,
    index: usize,
    links: Option<&Links>,
) -> Result<BrokerNode> {
    let node_id = format!("broker-{index}");
    // A run, not a single port: with several listeners the broker binds
    // `client_addr.port() + n`, and those have to be free too.
    let client_addr = if config.quic_listeners > 1 {
        ports::free_udp_run(config.quic_listeners)?
    } else {
        ports::free_udp()?
    };
    let internal_addr = ports::free_udp()?;
    let (advertise_addr, control_plane_url) = match links {
        Some(links) => {
            let route = links.route(&node_id, internal_addr)?;
            (route.advertise, route.control_plane_url)
        }
        None => (internal_addr, control_plane.base_url.clone()),
    };
    let metrics_addr = ports::free_tcp()?;
    let data_dir = root.join(&node_id);
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("create data dir {}", data_dir.display()))?;

    let token = control_plane.node_token(&config.tenant_id, &node_id)?;
    // Through a file rather than the environment, which is how a deployment
    // supplies it -- and what the broker requires of an expiring credential,
    // since a file is a seam something can rewrite and a value is not. The
    // harness mints a one-hour token and never rotates it; no test runs that
    // long, and the point here is to exercise the path production uses.
    let node_token_file = data_dir.join("node.token");
    std::fs::write(&node_token_file, &token)
        .with_context(|| format!("write node token to {}", node_token_file.display()))?;
    // Its own certificate, issued to its node id by the cluster's CA, so the
    // peer transport runs authenticated the way a deployment does.
    let cert = pki::issue(root, &node_id)?;
    let mut command = Command::new(binary);
    command
        .env("FELIX_NODE_ID", &node_id)
        .env("FELIX_INTERNAL_TLS_CERT", &cert.cert)
        .env("FELIX_INTERNAL_TLS_KEY", &cert.key)
        .env("FELIX_INTERNAL_TLS_CA", &cert.ca)
        // The advertised address is the internal listener's (or its proxy's):
        // it is what peers forward to, not what clients connect to.
        .env("FELIX_NODE_ADVERTISE_ADDR", advertise_addr.to_string())
        .env("FELIX_NODE_TOKEN_FILE", &node_token_file)
        .env("FELIX_CONTROLPLANE_URL", &control_plane_url)
        .env(
            "FELIX_REGION_ID",
            config.regions.get(index).map_or("local", String::as_str),
        )
        .env("FELIX_QUIC_BIND", client_addr.to_string())
        .env("FELIX_QUIC_LISTENERS", config.quic_listeners.to_string())
        // And where clients reach it, which is what discovery hands out. The
        // harness binds a concrete loopback port rather than 0.0.0.0, so the
        // bind address is also the reachable one.
        .env("FELIX_CLIENT_ADVERTISE_ADDR", client_addr.to_string())
        // Test-only faults, each off until a test writes its file.
        .env("FELIX_PEER_PARTITION_FILE", partition_file(&data_dir))
        .env("FELIX_CLOCK_FAULT_FILE", clock_fault_file(&data_dir))
        .env("FELIX_STORAGE_FAULT_FILE", storage_fault_file(&data_dir))
        // Each broker generates its own certificate, so each needs its own
        // file: one shared path would leave every broker but the last
        // exporting a certificate nobody can read. The client fixture
        // concatenates them into one PEM bundle, which is a thing a trust
        // store is allowed to be.
        .env("FELIX_TLS_CERT_EXPORT", data_dir.join("broker-cert.pem"))
        .env("FELIX_INTERNAL_BIND", internal_addr.to_string())
        .env("FELIX_BROKER_METRICS_BIND", metrics_addr.to_string())
        .env("FELIX_DURABLE_STORAGE_DIR", &data_dir)
        .env(
            "FELIX_CONTROLPLANE_SYNC_INTERVAL_MS",
            config.sync_interval_ms.to_string(),
        )
        .env(
            "RUST_LOG",
            std::env::var("RUST_LOG").as_deref().unwrap_or("info"),
        );
    let kafka_addr = if config.kafka {
        let port = ports::free_tcp()?.port();
        let advertise = format!("{}:{port}", ports::docker_host());
        command
            .env("FELIX_KAFKA_LISTEN", format!("0.0.0.0:{port}"))
            .env("FELIX_KAFKA_ADVERTISE_ADDR", &advertise)
            .env("FELIX_KAFKA_TLS", "false");
        Some(advertise)
    } else {
        None
    };
    if let Some(zone) = config.zones.get(index) {
        command.env("FELIX_NODE_ZONE", zone);
    }
    command.envs(config.broker_env.iter().map(|(key, value)| (key, value)));
    if let Some(env) = config.node_env.get(index) {
        command.envs(env.iter().map(|(key, value)| (key, value)));
    }

    if config.inherit_output {
        command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    } else {
        // To a file rather than discarded: a broker that exits during start-up
        // takes its reason with it otherwise, and "exit status: 1" says nothing
        // about whether it lost a port or was refused by the control plane.
        let log = std::fs::File::create(data_dir.join("broker.log"))
            .with_context(|| format!("create log for {node_id}"))?;
        let errors = log.try_clone().context("clone log handle")?;
        command.stdout(Stdio::from(log)).stderr(Stdio::from(errors));
    }

    let process = command
        .spawn()
        .with_context(|| format!("spawn {}", binary.display()))?;
    if let Some(links) = links {
        links.track(&node_id, Some(process.id()));
    }

    Ok(BrokerNode {
        node_id,
        client_addr,
        internal_addr,
        metrics_addr,
        kafka_addr,
        data_dir,
        process: Some(process),
    })
}

/// Locate the `felix-broker` binary built alongside this harness.
///
/// Taken from this executable's own directory rather than by running cargo: the
/// harness is often already inside a cargo invocation, and a nested one would
/// deadlock on the build lock.
pub(crate) fn broker_binary() -> Result<PathBuf> {
    let mut dir = std::env::current_exe().context("locate the running executable")?;
    dir.pop();
    // Integration test binaries live in `target/<profile>/deps`.
    if dir.ends_with("deps") {
        dir.pop();
    }
    let candidate = dir.join("felix-broker");
    if candidate.exists() {
        return Ok(candidate);
    }
    Err(anyhow!(
        "felix-broker not found at {}; build it first with `cargo build -p felix-broker-service --bin felix-broker`",
        candidate.display()
    ))
}
