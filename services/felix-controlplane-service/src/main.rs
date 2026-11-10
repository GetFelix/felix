//! The `felix-controlplane` binary: the server, configured from the
//! environment and an optional YAML file.
use felix_common::lifecycle;
use felix_controlplane_service::{config, server};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // aws-lc-rs is the only rustls provider linked in. Installing it as the
    // process default means a library that asks for the default gets the same
    // provider as the configs we build explicitly, instead of panicking.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    // The old operator commands: refuse rather than start a second server.
    if args.first().map(String::as_str) == Some("admin") {
        anyhow::bail!("`felix-controlplane admin` moved to `felixctl`; see `felixctl --help`");
    }

    let config = config::ControlPlaneConfig::from_env_or_yaml().expect("control plane config");
    // SIGTERM is what Kubernetes, systemd, and `docker stop` send; SIGINT only
    // covers an interactive Ctrl-C. Evaluated as an argument, so the handlers are
    // installed before `server::run` binds anything — a signal arriving
    // between binding and awaiting would otherwise kill the process outright.
    server::run(config, lifecycle::termination_signal()).await
}
