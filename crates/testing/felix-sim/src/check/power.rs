//! Torn-tail repair after a power cut.
//!
//! A power loss in the simulator lands inside an append and leaves that
//! append torn on the device (`felix_storage::sim_fs::arm_power_cut`). When
//! the broker starts again, recovery must find the torn tail and cut it off,
//! or the log would hold half a record. The storage records each segment it
//! repairs under a simulated directory; this checker holds each torn segment
//! to a repair of that segment before the broker's next power loss, or
//! before the end.

use std::path::PathBuf;

use super::{CheckContext, Checker, Finding, Point};
use crate::cluster::PowerLoss;

/// Every power cut that tore an append was followed by a torn-tail repair on
/// the broker's restart.
#[derive(Debug, Default)]
pub struct TornTailRepaired;

#[async_trait::async_trait]
impl Checker for TornTailRepaired {
    fn name(&self) -> &'static str {
        "torn-tail-repaired"
    }

    async fn check(&mut self, ctx: &CheckContext) -> Vec<Finding> {
        if ctx.point != Point::End {
            return Vec::new();
        }
        let cluster = &ctx.cluster;
        let losses = cluster.power_losses();
        let missed = unrepaired(&losses, |node| cluster.torn_tails_repaired(node));
        missed
            .into_iter()
            .map(|loss| {
                Finding::new(
                    self.name(),
                    format!(
                        "b{} lost power at {:?} during an append to {}, but recovery did not \
                         repair that segment's torn tail after it{}",
                        loss.node + 1,
                        loss.at,
                        loss.torn
                            .as_deref()
                            .unwrap_or(std::path::Path::new("?"))
                            .display(),
                        if loss.restarted_at.is_none() {
                            "; it was never started again"
                        } else {
                            ""
                        },
                    ),
                )
            })
            .collect()
    }
}

/// The power losses during an append whose torn segment no repair
/// followed. `repaired(node)` is every segment the broker's recovery has
/// repaired, in order; a loss is followed by a repair of its segment between
/// the loss and the broker's next power loss, or now for the last.
pub(crate) fn unrepaired(
    losses: &[PowerLoss],
    repaired: impl Fn(usize) -> Vec<PathBuf>,
) -> Vec<PowerLoss> {
    let mut missed = Vec::new();
    for (i, loss) in losses.iter().enumerate() {
        let Some(torn) = &loss.torn else {
            continue;
        };
        let all = repaired(loss.node);
        let until = losses[i + 1..]
            .iter()
            .find(|next| next.node == loss.node)
            .map_or(all.len(), |next| next.repaired_before);
        let window = all.get(loss.repaired_before..until).unwrap_or_default();
        if !window.contains(torn) {
            missed.push(loss.clone());
        }
    }
    missed
}

#[cfg(test)]
mod tests;
