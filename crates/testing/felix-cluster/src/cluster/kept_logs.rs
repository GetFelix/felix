//! Keeping a cluster's logs after it is torn down.
//!
//! The data root is a `TempDir`, so every broker log goes with the cluster. A
//! failure that only shows up on CI once in forty runs cannot be reproduced on
//! request, so with [`LOG_DIR_VAR`] set each cluster copies its logs out first,
//! and CI uploads the ones belonging to tests that failed.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, Once};

use super::Cluster;

/// Where to keep the logs. Unset or empty keeps nothing.
const LOG_DIR_VAR: &str = "FELIX_TEST_CLUSTER_LOG_DIR";

/// Every run of a broker before the current one, oldest first.
const EARLIER_RUNS: &str = "broker.log.earlier";

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
            copy_broker_logs(&node.data_dir, &dir, &node.node_id);
        }
    }
}

/// Move a stopped broker's log aside before it restarts.
///
/// The last run stays in `broker.log.previous`, where the history checker reads
/// it. Every run is also appended to [`EARLIER_RUNS`] and kept with the cluster
/// logs: a campaign restarts each broker many times, and the run that set up a
/// failure is often several restarts back.
pub(super) fn set_aside_log(data_dir: &Path) {
    let log = data_dir.join("broker.log");
    if let (Ok(mut from), Ok(mut to)) = (
        std::fs::File::open(&log),
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(data_dir.join(EARLIER_RUNS)),
    ) {
        let _ = std::io::copy(&mut from, &mut to);
    }
    let _ = std::fs::rename(&log, log.with_extension("log.previous"));
}

/// A restart moves the old run to `broker.log.previous`, and the run before
/// the kill is usually the one a failure is about, so both are kept.
fn copy_broker_logs(data_dir: &Path, dest: &Path, node_id: &str) {
    // A broker started with inherited output has no file to copy.
    let _ = std::fs::copy(
        data_dir.join("broker.log"),
        dest.join(format!("{node_id}.log")),
    );
    let _ = std::fs::copy(
        data_dir.join("broker.log.previous"),
        dest.join(format!("{node_id}.log.previous")),
    );
    let _ = std::fs::copy(
        data_dir.join(EARLIER_RUNS),
        dest.join(format!("{node_id}.earlier.log")),
    );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_restarted_brokers_previous_log_is_kept_beside_its_current_one() {
        let data = tempfile::tempdir().unwrap();
        let dest = tempfile::tempdir().unwrap();
        std::fs::write(data.path().join("broker.log"), "after").unwrap();
        std::fs::write(data.path().join("broker.log.previous"), "before").unwrap();
        copy_broker_logs(data.path(), dest.path(), "node-1");
        assert_eq!(
            std::fs::read_to_string(dest.path().join("node-1.log")).unwrap(),
            "after"
        );
        assert_eq!(
            std::fs::read_to_string(dest.path().join("node-1.log.previous")).unwrap(),
            "before"
        );
    }
}
