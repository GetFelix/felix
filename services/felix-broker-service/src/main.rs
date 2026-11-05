//! The `felix-broker` binary. Startup and shutdown live in
//! [`felix_broker_service::node`]; this is argument handling around it.

use anyhow::{Context, Result};
use felix_broker_service::config;
use felix_broker_service::node::run_with_shutdown;
use felix_common::lifecycle;

fn main() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_park(name_runtime_worker)
        .build()
        .context("start the async runtime")?
        .block_on(run())
}

/// Rename a runtime worker the first time it parks. Workers and blocking-pool
/// threads share tokio's default name, and telling them apart is how a thread
/// sampler shows whether disk waits land on the threads that run tasks.
fn name_runtime_worker() {
    #[cfg(target_os = "linux")]
    {
        thread_local!(static NAMED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) });
        if !NAMED.with(|named| named.replace(true)) {
            // SAFETY: PR_SET_NAME reads a NUL-terminated string of at most 16
            // bytes, which the literal is.
            unsafe {
                libc::prctl(libc::PR_SET_NAME, c"felix-rt-worker".as_ptr());
            }
        }
    }
}

async fn run() -> Result<()> {
    // aws-lc-rs is the only rustls provider linked in. Installing it as the
    // process default means a library that asks for the default gets the same
    // provider as the configs we build explicitly, instead of panicking.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    // `--print-config` before anything is bound, so it can be run against a
    // live deployment's environment without a port conflict.
    //
    // It doubles as a pre-flight check: the config is loaded the same way
    // startup loads it, so a file that will not parse or a key the broker does
    // not know fails here — before a rollout — with the same message it would
    // have produced on the node.
    // Read-only over a data directory, and before `--print-config` so none of
    // its arguments can be taken for a broker flag. Reads no configuration.
    if std::env::args().nth(1).as_deref() == Some("inspect") {
        std::process::exit(
            felix_broker_service::inspect::run(std::env::args().skip(2).collect()).into(),
        );
    }
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
        serde_saphyr::to_string(&config).context("render the configuration")?
    );
    Ok(())
}
