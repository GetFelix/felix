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
//! `docs/release-gate.md`, rows 3, 4, 5, 8 and 23.

use super::{CheckContext, Checker, Finding, Point};

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
        for event in ctx.cluster.disk_events() {
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
        if allowed > 0 {
            findings.push(Finding::warning(
                self.name(),
                format!(
                    "{allowed} acknowledged records were on no disk after a fault that may lose \
                     them; their shards are judged only from the fault on"
                ),
            ));
        }
        findings
    }
}
