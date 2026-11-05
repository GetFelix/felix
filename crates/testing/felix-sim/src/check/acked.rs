//! `AckedHeldByLeader`: whoever serves a shard holds every record a client was
//! told a quorum has.

use std::collections::HashMap;
use std::time::Duration;

use felix_cluster::history::model::{Action, AppendOutcome, Element, History};

use super::{CheckContext, Checker, Digest, Finding, Point};
use crate::client::{list_name, read_log};
use crate::cluster::{SimCluster, describe, stream_key};

/// How many shards print a census; the rest are counted.
const SHOW: usize = 3;

/// An append a client was told is on a quorum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Acked {
    /// Where the broker said it landed, when it said.
    pub(crate) offset: Option<u64>,
    pub(crate) value: u64,
}

/// For each shard, the appends of `history` that were acknowledged.
pub(crate) fn acked_by_shard(history: &History, shards: u32) -> HashMap<u32, Vec<Acked>> {
    let lists: HashMap<String, u32> = (0..shards).map(|shard| (list_name(shard), shard)).collect();
    let mut acked: HashMap<u32, Vec<Acked>> = HashMap::new();
    for op in &history.ops {
        if let Action::Append {
            list,
            value,
            outcome: AppendOutcome::Ok { offset },
        } = &op.action
            && let Some(&shard) = lists.get(list)
        {
            acked.entry(shard).or_default().push(Acked {
                offset: *offset,
                value: *value,
            });
        }
    }
    acked
}

/// The acknowledged appends that `held`, a broker's whole log, does not have
/// at the offset they were acknowledged at (or anywhere, if no offset was
/// given).
pub(crate) fn unheld(acked: &[Acked], held: &[Element]) -> Vec<Acked> {
    let by_offset: HashMap<u64, u64> = held.iter().map(|e| (e.offset, e.value)).collect();
    acked
        .iter()
        .filter(|a| match a.offset {
            Some(offset) => by_offset.get(&offset) != Some(&a.value),
            None => !held.iter().any(|e| e.value == a.value),
        })
        .copied()
        .collect()
}

/// At the end of the run, and once every shard is served after the faults
/// heal, the broker serving each shard has every append a client was
/// acknowledged for, at the offset it was acknowledged at.
///
/// A shard nobody serves, or two serve, is skipped here: `ElectionSettles`
/// reports it. The records are read from the leader's log directly, whether a
/// quorum has committed them or not.
#[derive(Debug)]
pub struct AckedHeldByLeader {
    digest: Digest,
}

impl Default for AckedHeldByLeader {
    fn default() -> Self {
        Self {
            digest: Digest::new(SHOW),
        }
    }
}

impl AckedHeldByLeader {
    /// Check `shard` against its sole server. `None` if it has none or more
    /// than one, or its log could not be read; otherwise the finding, if any.
    async fn look(
        &mut self,
        cluster: &SimCluster,
        shard: u32,
        acked: &[Acked],
    ) -> Option<Option<Finding>> {
        let key = stream_key(shard);
        let [leader] = cluster.servers_of(&key).await[..] else {
            return None;
        };
        let held = read_log(&cluster.handles(leader)?, shard).await?;
        let lost = unheld(acked, &held);
        if lost.is_empty() {
            return Some(None);
        }
        let id = describe(&key);
        if !self.digest.admit(&id) {
            return Some(None);
        }
        let example = lost[0];
        let found = example
            .offset
            .and_then(|offset| held.iter().find(|e| e.offset == offset))
            .map_or("nothing".to_string(), |e| format!("value {}", e.value));
        Some(Some(
            Finding::new(
                self.name(),
                format!(
                    "{id}: {} serves it without {} of {} acknowledged appends, for example value {} at offset {:?}, where it has {found}",
                    cluster.node_id(leader),
                    lost.len(),
                    acked.len(),
                    example.value,
                    example.offset,
                ),
            )
            .with(cluster.census(&key).await),
        ))
    }
}

#[async_trait::async_trait]
impl Checker for AckedHeldByLeader {
    fn name(&self) -> &'static str {
        "acked-held-by-leader"
    }

    async fn check(&mut self, ctx: &CheckContext) -> Vec<Finding> {
        if ctx.point == Point::Quiet {
            return Vec::new();
        }
        let cluster = &ctx.cluster;
        let mut waiting = acked_by_shard(&ctx.history, cluster.config.shards);
        let mut findings = Vec::new();
        let deadline = ctx.healed_at + cluster.config.settle_within;
        // After the heal, give the shards the time `ElectionSettles` does to
        // find a leader; at the end they either have one or are reported.
        loop {
            let mut pending = HashMap::new();
            for (shard, acked) in waiting {
                match self.look(cluster, shard, &acked).await {
                    Some(found) => findings.extend(found),
                    None => {
                        pending.insert(shard, acked);
                    }
                }
            }
            waiting = pending;
            if ctx.point == Point::End || waiting.is_empty() || cluster.elapsed() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        if ctx.point == Point::End {
            findings.extend(self.digest.rest(self.name()));
        }
        findings
    }
}

#[cfg(test)]
mod tests;
