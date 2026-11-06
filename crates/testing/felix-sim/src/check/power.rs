//! Torn-tail repair after a power cut.
//!
//! A power loss in the simulator lands inside an append and leaves that
//! append torn on the device (`felix_storage::sim_fs::arm_power_cut`). When
//! the broker starts again, recovery must find the torn tail and cut it off,
//! or the log would hold half a record. The storage counts every repair it
//! makes under a simulated directory; this checker holds each torn append
//! to one repair before the broker's next power loss, or before the end.

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
                        "b{} lost power at {:?} during an append, but recovery repaired no torn tail \
                         after it{}",
                        loss.node + 1,
                        loss.at,
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

/// The power losses during an append that no repair followed. A loss is
/// followed by one when the broker's count of repairs grew between the loss
/// and its next power loss, or `repaired_now` for the last.
pub(crate) fn unrepaired(
    losses: &[PowerLoss],
    repaired_now: impl Fn(usize) -> u64,
) -> Vec<PowerLoss> {
    let mut missed = Vec::new();
    for (i, loss) in losses.iter().enumerate() {
        if !loss.mid_append {
            continue;
        }
        let after = losses[i + 1..]
            .iter()
            .find(|next| next.node == loss.node)
            .map_or_else(|| repaired_now(loss.node), |next| next.repaired_before);
        if after <= loss.repaired_before {
            missed.push(loss.clone());
        }
    }
    missed
}

#[cfg(test)]
mod tests;
