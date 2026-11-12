//! What the disks hold, and what a fault that changes them is allowed to cost.
//!
//! A power cut under relaxed fsync, a cut of several brokers at once and a
//! wipe all change a broker's device. The nemesis reads every broker's stream
//! shards before and after, so the checker can say which acknowledged records
//! are on no disk any more and whether that fault may lose them.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use felix_cluster::history::model::{Action, AppendOutcome, FOREIGN_PAYLOAD, History};
use felix_storage::disk_log::inspect::{
    OffsetStatus, RecordsQuery, Store, find_shards, read_records,
};
use felix_storage::log::LogConfig;

use crate::client::decode;

/// What a wipe empties.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WipeScope {
    /// One stream shard's directory. `pick` chooses which, among those the
    /// first broker holds.
    Shard { pick: u64 },
    /// A broker's whole data directory.
    Root,
}

/// Records per read window. Each window reaches this far on both sides.
const WINDOW: u64 = 100;

/// Every client value in the stream shards below `root`.
pub(crate) fn stream_values(root: &Path) -> BTreeSet<u64> {
    let mut values = BTreeSet::new();
    let Ok(shards) = find_shards(root) else {
        return values;
    };
    let config = LogConfig::default();
    for shard in shards.iter().filter(|shard| shard.store == Store::Stream) {
        let mut at = 0;
        for _ in 0..100_000 {
            let query = RecordsQuery {
                offset: at,
                around: WINDOW,
                payload: true,
            };
            let report = match read_records(&shard.path, &query, &config) {
                Ok(report) => report,
                Err(_) => break,
            };
            for record in &report.records {
                if let Some(payload) = &record.payload {
                    let value = decode(payload);
                    if value != FOREIGN_PAYLOAD {
                        values.insert(value);
                    }
                }
            }
            match report.offset {
                OffsetStatus::PastTail { .. } | OffsetStatus::Empty | OffsetStatus::Unreached => {
                    break;
                }
                OffsetStatus::BelowStart { log_start } => at = log_start,
                OffsetStatus::Found | OffsetStatus::Missing => at += WINDOW,
            }
        }
    }
    values
}

/// The stream shard directories below `root`, as paths below it.
pub(crate) fn stream_shard_dirs(root: &Path) -> Vec<PathBuf> {
    let Ok(shards) = find_shards(root) else {
        return Vec::new();
    };
    shards
        .into_iter()
        .filter(|shard| shard.store == Store::Stream)
        .filter_map(|shard| shard.path.strip_prefix(root).ok().map(Path::to_path_buf))
        .collect()
}

/// The values of acknowledged stream appends, by shard.
pub(crate) fn acked_values(history: &History) -> BTreeMap<u32, Vec<u64>> {
    let mut acked: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for op in &history.ops {
        if let Action::Append {
            list,
            value,
            outcome: AppendOutcome::Ok { .. },
        } = &op.action
            && let Some(shard) = list.rsplit('/').next().and_then(|n| n.parse().ok())
        {
            acked.entry(shard).or_default().push(*value);
        }
    }
    acked
}

/// One fault that changed devices, and what it cost.
#[derive(Debug, Clone)]
pub struct DiskEvent {
    pub at: Duration,
    pub what: String,
    /// Acknowledged records that no disk holds now, by shard, that this fault
    /// may lose: every disk that held one was changed, and at least two did.
    pub excused: BTreeMap<u32, Vec<u64>>,
    /// Acknowledged records that no disk holds now and this fault may not
    /// lose, with why.
    pub unexcused: Vec<(u32, u64, &'static str)>,
    /// How many values each broker's disk held before the fault.
    pub held_before: Vec<usize>,
}

/// What a fault is judged from.
pub(crate) struct Observed<'a> {
    pub(crate) at: Duration,
    pub(crate) what: String,
    /// The brokers whose devices the fault changed.
    pub(crate) faulted: &'a [usize],
    /// Whether the fault can take records the broker had synced: a wipe can,
    /// a power cut under fsync-on-commit cannot.
    pub(crate) loses_synced: bool,
    /// Every broker's values before and after.
    pub(crate) before: &'a [BTreeSet<u64>],
    pub(crate) after: &'a [BTreeSet<u64>],
    /// The records acknowledged when the fault began.
    pub(crate) acked: &'a BTreeMap<u32, Vec<u64>>,
    /// Every record an earlier fault took, so one loss is judged once.
    pub(crate) gone: &'a BTreeSet<u64>,
    /// Every record an earlier fault took a copy of, which may be on one disk
    /// now without anything having been acknowledged short of a quorum.
    pub(crate) thinned: &'a BTreeSet<u64>,
}

/// What the fault cost.
pub(crate) fn judge(seen: Observed<'_>) -> DiskEvent {
    let Observed {
        at,
        what,
        faulted,
        loses_synced,
        before,
        after,
        acked,
        gone,
        thinned,
    } = seen;
    let kept: BTreeSet<u64> = after.iter().flatten().copied().collect();
    let mut event = DiskEvent {
        at,
        what,
        excused: BTreeMap::new(),
        unexcused: Vec::new(),
        held_before: before.iter().map(BTreeSet::len).collect(),
    };
    for (&shard, values) in acked {
        for &value in values {
            // A record an earlier fault already took is that fault's.
            if kept.contains(&value) || gone.contains(&value) {
                continue;
            }
            let holders: Vec<usize> = (0..before.len())
                .filter(|&i| before[i].contains(&value))
                .collect();
            if holders.is_empty() {
                event
                    .unexcused
                    .push((shard, value, "no disk held it before the fault"));
            } else if holders.len() < 2 && !thinned.contains(&value) {
                event
                    .unexcused
                    .push((shard, value, "acknowledged with one copy on disk"));
            } else if !holders.iter().all(|holder| faulted.contains(holder)) {
                event
                    .unexcused
                    .push((shard, value, "a disk the fault did not touch lost it"));
            } else if !loses_synced {
                event
                    .unexcused
                    .push((shard, value, "it was synced before it was acknowledged"));
            } else {
                event.excused.entry(shard).or_default().push(value);
            }
        }
    }
    event
}

#[cfg(test)]
mod tests;
