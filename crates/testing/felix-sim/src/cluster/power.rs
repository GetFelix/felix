//! The sim's record of power losses, kept for the checker that asks whether
//! recovery repaired each torn append, and for the double-crash exemption.

use std::path::PathBuf;
use std::time::Duration;

use super::kills::Views;

/// How long a power cut waits for an append to land in before it goes
/// without one. Under load a broker appends every few milliseconds; one that
/// is cut off may not append at all.
pub(crate) const APPEND_WITHIN: Duration = Duration::from_secs(2);

/// One power loss.
#[derive(Debug, Clone)]
pub struct PowerLoss {
    pub node: usize,
    /// When the cut was asked for, on the cluster's clock.
    pub at: Duration,
    /// The segment, below the data directory, whose append the cut landed
    /// in and tore. `None` when no append was in flight.
    pub torn: Option<PathBuf>,
    /// How many torn tails the broker's recovery had repaired before the cut.
    pub repaired_before: usize,
    /// The shards as the control plane held them just before.
    pub(crate) view: Views,
    /// When the broker was started again.
    pub restarted_at: Option<Duration>,
}

#[derive(Debug, Default)]
pub(crate) struct PowerLog {
    losses: Vec<PowerLoss>,
    seeds: u64,
}

impl PowerLog {
    /// A different crash seed for every cut in a run.
    pub(crate) fn next_seed(&mut self) -> u64 {
        self.seeds += 1;
        self.seeds.wrapping_mul(0x9E37_79B9_7F4A_7C15)
    }

    pub(crate) fn record(&mut self, loss: PowerLoss) {
        self.losses.push(loss);
    }

    pub(crate) fn losses(&self) -> &[PowerLoss] {
        &self.losses
    }

    /// The broker at `node` starts again at `at`. Returns its power loss if
    /// this is the first start after one.
    pub(crate) fn restart(&mut self, node: usize, at: Duration) -> Option<PowerLoss> {
        let loss = self
            .losses
            .iter_mut()
            .rev()
            .find(|loss| loss.node == node)
            .filter(|loss| loss.restarted_at.is_none())?;
        loss.restarted_at = Some(at);
        Some(loss.clone())
    }
}
