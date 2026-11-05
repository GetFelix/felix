//! `AckedHeldByLeader`: whoever serves a shard holds every record a client was
//! told a quorum has.

use std::collections::HashMap;
use std::time::Duration;

use felix_cluster::history::model::{Action, AppendOutcome, Element, History};

use super::{CheckContext, Checker, Digest, Finding, Point, seen_by_all};
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

/// One serving broker's view of a shard: who, at what generation, and its
/// whole log.
pub(crate) struct Served {
    pub(crate) name: String,
    pub(crate) generation: Option<u64>,
    pub(crate) held: Vec<Element>,
}

/// The acknowledged appends each of `served` is missing.
///
/// A broker at the newest generation must hold every one. One at an older
/// generation is a deposed leader that may not know yet, and the newer
/// leader has acknowledged records it never got, so it is held only to the
/// offsets inside its own log: a hole or a different record there is a lost
/// acknowledgement, a shorter log is not.
pub(crate) fn missing_by_server<'a>(
    acked: &[Acked],
    served: &'a [Served],
    newest: Option<u64>,
) -> Vec<(&'a Served, Vec<Acked>)> {
    served
        .iter()
        .filter_map(|one| {
            let required: Vec<Acked> = if one.generation >= newest {
                acked.to_vec()
            } else {
                let tail = one.held.last().map_or(0, |e| e.offset + 1);
                acked
                    .iter()
                    .filter(|a| a.offset.is_some_and(|offset| offset < tail))
                    .copied()
                    .collect()
            };
            let lost = unheld(&required, &one.held);
            (!lost.is_empty()).then_some((one, lost))
        })
        .collect()
}

/// While the faults run, once the faults have healed, and at the end of the
/// run, every broker serving a shard has every append a client was
/// acknowledged for, at the offset it was acknowledged at.
///
/// Matches the model's `AckedHeldByLeader`: a broker that has the shard open
/// holds what was acknowledged at its generation or before. A broker at the
/// newest generation known to any broker must hold everything acknowledged;
/// a deposed one still serving is held to its own log (see
/// [`missing_by_server`]). A shard nobody serves is skipped here:
/// `ElectionSettles` reports it. The records are read from each broker's log
/// directly, whether a quorum has committed them or not.
#[derive(Debug)]
pub struct AckedHeldByLeader {
    digest: Digest,
    /// The newest generation seen for each shard, so a leader that stops
    /// serving between samples still counts as having superseded the rest.
    newest: HashMap<u32, u64>,
}

impl Default for AckedHeldByLeader {
    fn default() -> Self {
        Self {
            digest: Digest::new(SHOW),
            newest: HashMap::new(),
        }
    }
}

impl AckedHeldByLeader {
    /// Check `shard` against every broker serving it. `None` if none does or
    /// a log could not be read; otherwise the findings, if any.
    async fn look(
        &mut self,
        cluster: &SimCluster,
        shard: u32,
        acked: &[Acked],
    ) -> Option<Vec<Finding>> {
        let key = stream_key(shard);
        let seen = seen_by_all(cluster, &key);
        let newest = seen
            .iter()
            .flat_map(|one| {
                let stand = &one.stand;
                [
                    stand.generation,
                    stand.opened_generation,
                    stand
                        .routed_leader
                        .as_ref()
                        .map(|(_, generation)| *generation),
                ]
            })
            .flatten()
            .chain(self.newest.get(&shard).copied())
            .max();
        if let Some(newest) = newest {
            self.newest.insert(shard, newest);
        }
        let serving: Vec<_> = seen.iter().filter(|one| one.stand.serving).collect();
        if serving.is_empty() {
            return None;
        }
        let mut served = Vec::new();
        for one in serving {
            served.push(Served {
                name: one.name.clone(),
                generation: one.stand.generation,
                held: read_log(&cluster.handles(one.node)?, shard).await?,
            });
        }
        let mut findings = Vec::new();
        for (one, lost) in missing_by_server(acked, &served, newest) {
            let id = format!("{}@{}", describe(&key), one.name);
            if !self.digest.admit(&id) {
                continue;
            }
            let example = lost[0];
            let found = example
                .offset
                .and_then(|offset| one.held.iter().find(|e| e.offset == offset))
                .map_or("nothing".to_string(), |e| format!("value {}", e.value));
            findings.push(
                Finding::new(
                    self.name(),
                    format!(
                        "{}: {} serves it at generation {:?} without {} of {} acknowledged appends, for example value {} at offset {:?}, where it has {found}",
                        describe(&key),
                        one.name,
                        one.generation,
                        lost.len(),
                        acked.len(),
                        example.value,
                        example.offset,
                    ),
                )
                .with(cluster.census(&key).await),
            );
        }
        Some(findings)
    }
}

#[async_trait::async_trait]
impl Checker for AckedHeldByLeader {
    fn name(&self) -> &'static str {
        "acked-held-by-leader"
    }

    async fn check(&mut self, ctx: &CheckContext) -> Vec<Finding> {
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
            if ctx.point != Point::Healed || waiting.is_empty() || cluster.elapsed() >= deadline {
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
