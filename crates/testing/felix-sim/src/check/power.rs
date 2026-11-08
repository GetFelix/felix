//! Torn-tail repair after a power cut.
//!
//! A power loss in the simulator lands inside an append and leaves that
//! append torn on the device (`felix_storage::sim_fs::arm_power_cut`). When
//! the broker starts again, recovery must find the torn tail and cut it off,
//! or the log would hold half a record. The storage records each segment it
//! repairs, and each segment it opens, under a simulated directory; this
//! checker holds each torn segment the broker opened again to a repair of
//! that segment before the broker's next power loss, or before the end.

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
        let missed = unrepaired(
            &losses,
            |node| cluster.torn_tails_repaired(node),
            |node| cluster.segments_opened(node),
        );
        missed
            .into_iter()
            .map(|loss| {
                Finding::new(
                    self.name(),
                    format!(
                        "b{} lost power at {:?} during an append to {}, and the broker opened \
                         that segment again, but recovery did not repair its torn tail",
                        loss.node + 1,
                        loss.at,
                        loss.torn
                            .as_deref()
                            .unwrap_or(std::path::Path::new("?"))
                            .display(),
                    ),
                )
            })
            .collect()
    }
}

/// The power losses during an append whose torn segment was opened again
/// without a repair of it. Recovery repairs a segment when its log is opened,
/// and a broker only opens the logs of the shards it hosts, so a segment that
/// was never opened after the cut (the broker stayed down, or no longer hosts
/// that shard) has nothing to repair yet and is not held to it.
///
/// `repaired(node)` and `opened(node)` are every segment the broker's
/// recovery has repaired and every segment it has opened, in order. Both are
/// read between the loss and the broker's next power loss, or to now for the
/// last.
pub(crate) fn unrepaired(
    losses: &[PowerLoss],
    repaired: impl Fn(usize) -> Vec<PathBuf>,
    opened: impl Fn(usize) -> Vec<PathBuf>,
) -> Vec<PowerLoss> {
    let mut missed = Vec::new();
    for (i, loss) in losses.iter().enumerate() {
        let Some(torn) = &loss.torn else {
            continue;
        };
        let next = losses[i + 1..].iter().find(|next| next.node == loss.node);
        let repaired = repaired(loss.node);
        let repairs = window(
            &repaired,
            loss.repaired_before,
            next.map(|n| n.repaired_before),
        );
        let opened = opened(loss.node);
        let opens = window(&opened, loss.opened_before, next.map(|n| n.opened_before));
        if opens.contains(torn) && !repairs.contains(torn) {
            missed.push(loss.clone());
        }
    }
    missed
}

fn window(all: &[PathBuf], from: usize, until: Option<usize>) -> &[PathBuf] {
    all.get(from..until.unwrap_or(all.len()))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests;
