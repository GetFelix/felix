//! `RecordGenerationsAgree`: replicas stamp a record with the same writer.
//!
//! A v9 segment keeps the generation that wrote each record. A follower gets
//! it from the sender, so every replica holding a record should name the same
//! writer. A replica that cannot say (an older segment, or a sender that did
//! not know) stamps [`UNKNOWN_WRITER`], which agrees with anything.

use std::collections::BTreeMap;

use felix_broker::LogKind;
use felix_storage::log::{LogRecord, UNKNOWN_WRITER};

use super::{CheckContext, Checker, Digest, Finding, Point};
use crate::cluster::{NAMESPACE, STREAM, SimCluster, TENANT, describe, stream_key};

/// How many disagreements are printed; the rest are counted.
const SHOW: usize = 5;
/// The most bytes one read of a log asks for.
const PAGE_BYTES: usize = 1 << 20;

/// At the end of a run, every offset that two replicas hold with the same
/// payload has the same known writer on both. Generation 0 is not a writer
/// ([`UNKNOWN_WRITER`]) and a record without a stamp has none; both are left
/// out. Only matters once `record_generations` is finalized
/// ([`SimConfig::finalize_record_generations`](crate::cluster::SimConfig::finalize_record_generations)).
#[derive(Debug)]
pub struct RecordGenerationsAgree {
    digest: Digest,
}

impl Default for RecordGenerationsAgree {
    fn default() -> Self {
        Self {
            digest: Digest::new(SHOW),
        }
    }
}

/// What a run looked at, so a run that compared nothing can be told from one
/// that agreed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Tally {
    /// Records held by two or more replicas.
    pub compared: u64,
    /// Of those, records with two or more known stamps to compare.
    pub stamped: u64,
}

impl Tally {
    /// The summary of the warning that reports it; the seed test reads it
    /// back with [`Tally::parse`].
    pub fn summary(&self) -> String {
        format!(
            "compared {} replicated records, {} with two or more known writers",
            self.compared, self.stamped
        )
    }

    /// The tally in a [`Tally::summary`].
    pub fn parse(summary: &str) -> Option<Tally> {
        let rest = summary.strip_prefix("compared ")?;
        let (compared, rest) = rest.split_once(" replicated records, ")?;
        let (stamped, _) = rest.split_once(' ')?;
        Some(Tally {
            compared: compared.parse().ok()?,
            stamped: stamped.parse().ok()?,
        })
    }
}

/// One replica's stamp for an offset.
type Stamp<'a> = (&'a str, u64);

/// The offsets where `logs` (each a replica's name and records) disagree on
/// a known writer, with the stamps. Records with a different payload at an
/// offset are not compared: that is a divergence other checks report, and
/// the stamps of a record that was replaced mean nothing.
fn disagreements<'a>(
    logs: &'a [(String, Vec<LogRecord>)],
    tally: &mut Tally,
) -> Vec<(u64, Vec<Stamp<'a>>)> {
    let mut by_offset: BTreeMap<u64, Vec<(&str, &LogRecord)>> = BTreeMap::new();
    for (name, records) in logs {
        for record in records {
            by_offset
                .entry(record.offset)
                .or_default()
                .push((name, record));
        }
    }
    let mut found = Vec::new();
    for (offset, held) in by_offset {
        if held.len() < 2 {
            continue;
        }
        // Group the copies that are the same record.
        let mut same: Vec<(&LogRecord, Vec<Stamp<'_>>)> = Vec::new();
        for (name, record) in held {
            let slot = match same.iter().position(|(first, _)| {
                first.checksum == record.checksum && first.payload == record.payload
            }) {
                Some(slot) => slot,
                None => {
                    same.push((record, Vec::new()));
                    same.len() - 1
                }
            };
            if let Some(generation) = record.generation.filter(|g| *g != UNKNOWN_WRITER) {
                same[slot].1.push((name, generation));
            }
        }
        for (_, stamps) in same {
            tally.compared += 1;
            if stamps.len() >= 2 {
                tally.stamped += 1;
            }
            if stamps.windows(2).any(|pair| pair[0].1 != pair[1].1) {
                found.push((offset, stamps));
            }
        }
    }
    found
}

/// Every record of `shard`'s stream log on node `index`, or `None` if the node
/// is down or does not hold it.
async fn read_records(cluster: &SimCluster, index: usize, shard: u32) -> Option<Vec<LogRecord>> {
    let handles = cluster.handles(index)?;
    let log = handles
        .broker
        .shard_log(LogKind::Stream, TENANT, NAMESPACE, STREAM, shard)
        .await?;
    let mut records = Vec::new();
    let mut next = 0;
    loop {
        let page = log.read_from(next, PAGE_BYTES).await.ok()?;
        let Some(last) = page.last() else {
            return Some(records);
        };
        next = last.offset + 1;
        records.extend(page);
    }
}

#[async_trait::async_trait]
impl Checker for RecordGenerationsAgree {
    fn name(&self) -> &'static str {
        "record-generations-agree"
    }

    async fn check(&mut self, ctx: &CheckContext) -> Vec<Finding> {
        if ctx.point != Point::End {
            return Vec::new();
        }
        let cluster = &ctx.cluster;
        let mut findings = Vec::new();
        let mut tally = Tally::default();
        for shard in 0..cluster.config.shards {
            let key = stream_key(shard);
            let mut logs = Vec::new();
            for node in cluster.running().iter().map(|h| h.index) {
                if let Some(records) = read_records(cluster, node, shard).await {
                    logs.push((cluster.node_id(node).to_string(), records));
                }
            }
            for (offset, stamps) in disagreements(&logs, &mut tally) {
                if !self.digest.admit(&format!("{}@{offset}", describe(&key))) {
                    continue;
                }
                let text: Vec<String> = stamps
                    .iter()
                    .map(|(name, generation)| format!("{name} says {generation}"))
                    .collect();
                findings.push(
                    Finding::new(
                        self.name(),
                        format!(
                            "{}: replicas name different writers for offset {offset}: {}",
                            describe(&key),
                            text.join(", "),
                        ),
                    )
                    .with(cluster.census(&key).await),
                );
            }
        }
        findings.extend(self.digest.rest(self.name()));
        findings.push(Finding::warning(self.name(), tally.summary()));
        findings
    }
}

#[cfg(test)]
mod tests;
