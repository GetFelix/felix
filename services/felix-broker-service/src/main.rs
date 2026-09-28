//! The `felix-broker` binary. Startup and shutdown live in
//! [`felix_broker_service::node`]; this is argument handling around it.

use anyhow::{Context, Result};
use felix_broker_service::config;
use felix_broker_service::node::run_with_shutdown;
use felix_common::lifecycle;

// Tokio async runtime entry point. The broker is primarily I/O-bound (QUIC + HTTP metrics)
// and runs multiple background tasks concurrently.
#[tokio::main]
async fn main() -> Result<()> {
    // `--print-config` before anything is bound, so it can be run against a
    // live deployment's environment without a port conflict.
    //
    // It doubles as a pre-flight check: the config is loaded the same way
    // startup loads it, so a file that will not parse or a key the broker does
    // not know fails here — before a rollout — with the same message it would
    // have produced on the node.
    if std::env::args().any(|arg| arg == "--print-config") {
        return print_config();
    }
    // Offline, over the data directory: no listener is bound and no shard is
    // served while it runs.
    if std::env::args().nth(1).as_deref() == Some("restore-point") {
        return felix_broker_service::restore::run(std::env::args().skip(2).collect()).await;
    }

    // Default shutdown trigger: SIGTERM or SIGINT. SIGTERM is what Kubernetes,
    // systemd, and `docker stop` actually send; SIGINT only covers an interactive
    // Ctrl-C. Evaluated as an argument, so the handlers are installed before
    // `run_with_shutdown` binds anything — a signal arriving between binding and
    // awaiting would otherwise kill the process outright.
    // `run_with_shutdown` is written so we can reuse the same startup logic
    // in tests or alternative hosting environments by passing a different future.
    run_with_shutdown(lifecycle::termination_signal()).await
}

/// Print the configuration this broker would run with, and stop.
///
/// The question an operator actually has is "what is this process running
/// with", and until now the only answer was to read the environment, the config
/// file, and the defaults in the source, then combine them by hand.
///
/// YAML because that is what the config file is, so the shape is one an
/// operator already recognises. The credential is redacted: this output exists
/// to be pasted into an issue.
///
/// Unrecognised variables go to stderr rather than into the document, so the
/// warnings survive a `> config.yml` and the document stays a document.
fn print_config() -> Result<()> {
    for warning in felix_common::env_registry::unrecognised_warnings() {
        eprintln!("warning: {warning}");
    }
    let config = config::BrokerConfig::from_env_or_yaml().context("load broker configuration")?;
    println!(
        "{}",
        serde_yaml_ng::to_string(&config).context("render the configuration")?
    );
    Ok(())
}
