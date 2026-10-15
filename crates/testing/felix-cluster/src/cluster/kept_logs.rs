//! Keeping a cluster's logs after it is torn down.
//!
//! The data root is a `TempDir`, so every broker log goes with the cluster. A
//! failure that only shows up on CI once in forty runs cannot be reproduced on
//! request, so with [`LOG_DIR_VAR`] set each cluster copies its logs out first,
//! and CI uploads the ones belonging to tests that failed.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, Once};

use super::Cluster;

/// Where to keep the logs. Unset or empty keeps nothing.
const LOG_DIR_VAR: &str = "FELIX_TEST_CLUSTER_LOG_DIR";

fn log_dir() -> Option<PathBuf> {
    std::env::var_os(LOG_DIR_VAR)
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
}

impl Cluster {
    /// Copy each broker's log to `<dir>/<test>/cluster-<n>/<node>.log`.
    ///
    /// The test is named by its thread, which libtest names after the test, so
    /// CI can match a failure in the test output to its folder. Every cluster
    /// is kept, not only a failing one: a test that returns `Err` drops its
    /// cluster before anything has decided it failed.
    pub(super) fn keep_logs(&self) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let Some(root) = log_dir() else {
            return;
        };
        let test = std::thread::current()
            .name()
            .unwrap_or("unnamed")
            .replace("::", "__");
        let dir = root
            .join(test)
            .join(format!("cluster-{}", NEXT.fetch_add(1, Ordering::Relaxed)));
        if let Err(err) = std::fs::create_dir_all(&dir) {
            eprintln!("could not keep cluster logs in {}: {err}", dir.display());
            return;
        }
        for node in &self.nodes {
            // A broker started with inherited output has no file to copy.
            let _ = std::fs::copy(
                node.data_dir.join("broker.log"),
                dir.join(format!("{}.log", node.node_id)),
            );
        }
    }
}

/// Send this process's tracing to `<dir>/controlplane-<test binary>.log`.
///
/// The control plane runs inside the test process, so nothing records what it
/// did unless something here installs a subscriber. One file per test binary,
/// since a subscriber is process-wide; lines are timestamped, so they line up
/// with the broker logs of whichever test failed.
pub(crate) fn capture_control_plane() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let Some(root) = log_dir() else {
            return;
        };
        let binary = std::env::current_exe()
            .ok()
            .and_then(|exe| {
                exe.file_stem()
                    .map(|stem| stem.to_string_lossy().into_owned())
            })
            .unwrap_or_else(|| "tests".to_string());
        let path = root.join(format!("controlplane-{binary}.log"));
        let file = std::fs::create_dir_all(&root).and_then(|()| std::fs::File::create(&path));
        let file = match file {
            Ok(file) => file,
            Err(err) => {
                eprintln!(
                    "could not keep control plane logs in {}: {err}",
                    path.display()
                );
                return;
            }
        };
        let filter = tracing_subscriber::EnvFilter::new(
            "warn,felix_controlplane_service=info,felix_cluster=info",
        );
        // A test that installed its own subscriber keeps it.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .with_writer(Mutex::new(file))
            .try_init();
    });
}
