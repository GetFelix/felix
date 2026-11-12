//! Records acknowledged to a client stay on some disk.
//!
//! A power cut under fsync-on-commit, a power cut of one broker under a
//! relaxed fsync mode and a wipe of one broker must not take an acknowledged
//! record off every disk: a quorum held it, and the broker that lost it is
//! one copy. A fault that changes at least two brokers' disks may take a
//! record that only they held (a majority of its group), under a relaxed
//! fsync mode or a wipe, and the run names what it took. Under
//! fsync-on-commit a cut takes nothing that was acknowledged, however many
//! brokers it cuts.
//!
//! At the end of the run every broker's disk is read once more, and every
//! acknowledged record that no fault took must be on one of them. This also
//! covers shards that could not be read back through a broker.
//!
//! `docs/release-gate.md`, rows 3, 4, 5, 8 and 23.

use std::collections::BTreeSet;

use super::{CheckContext, Checker, Finding, Point};
use crate::cluster::disks::{acked_values, missing};

/// Every fault that changed devices lost only what it was allowed to.
#[derive(Debug, Default)]
pub struct AckedOnSomeDisk;

/// How many records a finding names.
const SHOW: usize = 5;

#[async_trait::async_trait]
impl Checker for AckedOnSomeDisk {
    fn name(&self) -> &'static str {
        "acked-on-some-disk"
    }

    async fn check(&mut self, ctx: &CheckContext) -> Vec<Finding> {
        if ctx.point != Point::End {
            return Vec::new();
        }
        let mut findings = Vec::new();
        let mut allowed = 0;
        let mut gone = BTreeSet::new();
        for event in ctx.cluster.disk_events() {
            gone.extend(event.excused.values().flatten().copied());
            gone.extend(event.unexcused.iter().map(|(_, value, _)| *value));
            allowed += event.excused.values().map(Vec::len).sum::<usize>();
            if event.unexcused.is_empty() {
                continue;
            }
            let shown: Vec<String> = event
                .unexcused
                .iter()
                .take(SHOW)
                .map(|(shard, value, why)| format!("value {value} of shard {shard} ({why})"))
                .collect();
            findings.push(Finding::new(
                self.name(),
                format!(
                    "at {:?}, {}: {} acknowledged records are on no disk and the fault may not \
                     lose them, such as {}. The brokers' disks held {:?} values before it",
                    event.at,
                    event.what,
                    event.unexcused.len(),
                    shown.join(", "),
                    event.held_before
                ),
            ));
        }
        let lost = missing(
            &acked_values(&ctx.history),
            &ctx.cluster.disk_values(),
            &gone,
        );
        if !lost.is_empty() {
            let shown: Vec<String> = lost
                .iter()
                .take(SHOW)
                .map(|(shard, value)| format!("value {value} of shard {shard}"))
                .collect();
            findings.push(Finding::new(
                self.name(),
                format!(
                    "at the end of the run {} acknowledged records are on no disk and no fault \
                     took them, such as {}",
                    lost.len(),
                    shown.join(", ")
                ),
            ));
        }
        if allowed > 0 {
            findings.push(Finding::warning(
                self.name(),
                format!(
                    "{allowed} acknowledged records were on no disk after a fault that may lose \
                     them; they are unknown in the history"
                ),
            ));
        }
        findings
    }
}
