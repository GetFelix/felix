//! The disk half of store liveness: one synced write per interval.

use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;

/// A volume that can be asked to prove it still takes synced writes.
#[async_trait::async_trait]
pub trait ProbeDisk: Send + Sync + 'static {
    /// Write and sync once. Returns when the sync has, however long that is.
    async fn probe(&self) -> std::io::Result<()>;
}

/// The probe file in a broker's data root.
#[derive(Debug)]
pub struct VolumeProbe {
    probe: Arc<Mutex<felix_storage::store_liveness::Probe>>,
}

impl VolumeProbe {
    /// Open or create the probe file under `root`.
    pub fn open(root: &Path) -> std::io::Result<Self> {
        Ok(Self {
            probe: Arc::new(Mutex::new(felix_storage::store_liveness::Probe::open(
                root,
            )?)),
        })
    }
}

#[async_trait::async_trait]
impl ProbeDisk for VolumeProbe {
    async fn probe(&self) -> std::io::Result<()> {
        let probe = Arc::clone(&self.probe);
        // Off the runtime: a stalled device must not stall the heartbeats of
        // everything else on a worker thread.
        felix_common::blocking::spawn(move || probe.lock().beat())
            .await
            .map_err(std::io::Error::other)?
    }
}
