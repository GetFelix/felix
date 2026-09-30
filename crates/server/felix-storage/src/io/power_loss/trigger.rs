//! Running a whole process under the power-loss model, for the cluster
//! harness: arm it on the storage root at startup, then watch the fault file
//! for the directive that turns the power off.
//!
//! The directive is two lines, `power_loss=<seed>` and
//! `power_loss_into=<dir>`. The watcher polls the file itself rather than
//! waiting for the next flush to read it, because an idle broker may not
//! flush at all.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use super::PowerLoss;

/// How often the watcher reads the fault file.
const POLL: Duration = Duration::from_millis(20);

/// Install the model on `root`, taking everything already there as durable,
/// and start the thread that follows `fault_file`.
pub(crate) fn arm(root: &Path, fault_file: PathBuf) -> std::io::Result<()> {
    let observer = PowerLoss::install(root)?;
    tracing::warn!(
        root = %root.display(),
        "simulated power loss is ARMED; this is a test-only facility",
    );
    std::thread::Builder::new()
        .name("power-loss".to_string())
        .spawn(move || watch(observer, &fault_file))?;
    Ok(())
}

/// The seed and image directory the fault file asks for, if it does.
pub(crate) fn directive(body: &str) -> Option<(u64, PathBuf)> {
    let (mut seed, mut into) = (None, None);
    for line in body.lines() {
        match line.split_once('=') {
            Some(("power_loss", value)) => seed = value.trim().parse().ok(),
            Some(("power_loss_into", value)) if !value.trim().is_empty() => {
                into = Some(PathBuf::from(value.trim()));
            }
            _ => {}
        }
    }
    Some((seed?, into?))
}

fn watch(observer: Arc<PowerLoss>, fault_file: &Path) {
    loop {
        std::thread::sleep(POLL);
        let Ok(body) = std::fs::read_to_string(fault_file) else {
            continue;
        };
        // An image already there is one a previous run of this broker built;
        // the harness clears the directive before it restarts anything.
        if let Some((seed, into)) = directive(&body).filter(|(_, into)| !into.exists()) {
            observer.power_off(seed, &into);
        }
    }
}
