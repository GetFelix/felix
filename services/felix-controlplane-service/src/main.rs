//! The `felix-controlplane` binary: the server, configured from the
//! environment and an optional YAML file, and the offline `raft` commands
//! that act on a stopped member's data directory.
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
    if args.first().map(String::as_str) == Some("raft") {
        return raft_command(&args[1..]);
    }

    let config = config::ControlPlaneConfig::from_env_or_yaml().expect("control plane config");
    // SIGTERM is what Kubernetes, systemd, and `docker stop` send; SIGINT only
    // covers an interactive Ctrl-C. Evaluated as an argument, so the handlers are
    // installed before `server::run` binds anything — a signal arriving
    // between binding and awaiting would otherwise kill the process outright.
    server::run(config, lifecycle::termination_signal()).await
}

const RAFT_USAGE: &str = "usage: felix-controlplane raft form-without --data-dir <dir> \
                          --cluster-id <id> --slot <n> [--slot <n>...]";

/// `raft form-without`: let this stopped member's first start form the
/// group without the named slots answering.
fn raft_command(args: &[String]) -> anyhow::Result<()> {
    use anyhow::Context;

    if args.first().map(String::as_str) != Some("form-without") {
        anyhow::bail!("{RAFT_USAGE}");
    }
    let mut data_dir = None;
    let mut cluster_id = None;
    let mut slots = std::collections::BTreeSet::new();
    let mut rest = args[1..].iter();
    while let Some(flag) = rest.next() {
        let value = rest
            .next()
            .with_context(|| format!("{flag} needs a value\n{RAFT_USAGE}"))?;
        match flag.as_str() {
            "--data-dir" => data_dir = Some(std::path::PathBuf::from(value)),
            "--cluster-id" => cluster_id = Some(value.clone()),
            "--slot" => {
                let slot = value
                    .parse()
                    .with_context(|| format!("--slot {value} is not a member id"))?;
                slots.insert(slot);
            }
            other => anyhow::bail!("unknown option {other}\n{RAFT_USAGE}"),
        }
    }
    let data_dir = data_dir.with_context(|| format!("--data-dir is required\n{RAFT_USAGE}"))?;
    let cluster_id =
        cluster_id.with_context(|| format!("--cluster-id is required\n{RAFT_USAGE}"))?;
    let written = felix_controlplane_service::raft::form_without(&data_dir, &cluster_id, &slots)?;
    println!("{written}");
    Ok(())
}
