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
///
/// The read opens every segment, torn ones included, and repairs nothing. It
/// is kept out of the opens `torn-tail-repaired` holds a broker to.
pub(crate) fn stream_values(root: &Path) -> BTreeSet<u64> {
    felix_storage::sim_fs::unobserved(|| read_stream_values(root))
}

fn read_stream_values(root: &Path) -> BTreeSet<u64> {
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

/// Acknowledged records a fault took a copy of, each with the brokers whose
/// disks lost it, until two disks hold the record again.
pub(crate) type Thinned = BTreeMap<u64, BTreeSet<usize>>;

/// One fault that changed devices, and what it cost.
#[derive(Debug, Clone)]
pub struct DiskEvent {
    pub at: Duration,
    pub what: String,
    /// Acknowledged records that no disk holds now, by shard, that this fault
    /// may lose: it changed every disk that held one, and at least two disks
    /// lost it, counting a copy an earlier fault took that was not copied
    /// back.
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
    /// Copies earlier faults took, which is how a record acknowledged by a
    /// quorum may be on one disk now.
    pub(crate) thinned: &'a Thinned,
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
            let taken_earlier = thinned.get(&value).map_or(0, |disks| {
                disks.iter().filter(|disk| !holders.contains(disk)).count()
            });
            if holders.is_empty() {
                event
                    .unexcused
                    .push((shard, value, "no disk held it before the fault"));
            } else if holders.len() + taken_earlier < 2 {
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

/// Note which copies of acknowledged records a fault took. A faulted disk
/// that held a record before and not after lost a copy. A record no fault
/// judged (acknowledged while the fault ran, in `late`) counts as lost by
/// every faulted disk that does not hold it after, since what those disks
/// held is not known. A record two disks hold again is dropped.
pub(crate) fn thin(
    thinned: &mut Thinned,
    faulted: &[usize],
    acked: &BTreeMap<u32, Vec<u64>>,
    late: &[u64],
    before: &[BTreeSet<u64>],
    after: &[BTreeSet<u64>],
) {
    let held = |disks: &[BTreeSet<u64>], value: u64| {
        disks.iter().filter(|disk| disk.contains(&value)).count()
    };
    thinned.retain(|&value, _| held(before, value) < 2);
    for &value in acked.values().flatten() {
        for &node in faulted {
            if before[node].contains(&value) && !after[node].contains(&value) {
                thinned.entry(value).or_default().insert(node);
            }
        }
    }
    for &value in late {
        for &node in faulted {
            if !after[node].contains(&value) {
                thinned.entry(value).or_default().insert(node);
            }
        }
    }
    thinned.retain(|&value, _| held(after, value) < 2);
}

/// Acknowledged records, by shard, that no disk in `disks` holds and no
/// fault took (`gone`).
pub(crate) fn missing(
    acked: &BTreeMap<u32, Vec<u64>>,
    disks: &[BTreeSet<u64>],
    gone: &BTreeSet<u64>,
) -> Vec<(u32, u64)> {
    acked
        .iter()
        .flat_map(|(&shard, values)| values.iter().map(move |&value| (shard, value)))
        .filter(|(_, value)| !gone.contains(value) && !disks.iter().any(|d| d.contains(value)))
        .collect()
}

#[cfg(test)]
mod tests;
