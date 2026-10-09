//! Looking at a data directory without changing it: what is on disk, and what
//! startup would do with it.
//!
//! Strictly read-only. Every file is opened for reading; nothing is created,
//! repaired, truncated or re-indexed. The verdict comes from the same plan
//! startup recovery makes before it writes anything, so the two cannot
//! disagree about a torn tail versus corruption.
//!
//! There is no lock on a data directory. Next to a running broker the reads
//! are safe, but the active segment may be mid-write and can show a torn tail
//! that is only an append in flight.

use std::path::{Path, PathBuf};

use super::durable_mark::{self, DurableMark};
use super::offload;
use super::recovery::{self, End, IndexRebuild, Removal, Step};
use crate::log::{LogConfig, Offset, SegmentId, ShardKey};
use crate::segment::{
    ScanStart, SparseIndex, TailRepair, index_file_name, read_segment_header, scan_segment_with,
    segment_file_name,
};
use crate::{Corruption, CorruptionKind, Result, StorageError};

/// Which of the broker's stores a shard directory belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Store {
    /// Stream shards, directly under the data directory.
    Stream,
    /// `caches/`.
    Cache,
    /// `groups/`: consumer-group positions.
    Groups,
    /// `dead-letters/`.
    DeadLetters,
    /// `counters/`.
    Counters,
}

impl Store {
    pub const ALL: [Store; 5] = [
        Store::Stream,
        Store::Cache,
        Store::Groups,
        Store::DeadLetters,
        Store::Counters,
    ];

    /// The name `felixctl` and the docs use for it.
    pub fn name(self) -> &'static str {
        match self {
            Store::Stream => "stream",
            Store::Cache => "cache",
            Store::Groups => "groups",
            Store::DeadLetters => "dead-letters",
            Store::Counters => "counters",
        }
    }

    /// The store's directory under a broker's data directory.
    pub fn root(self, data_dir: &Path) -> PathBuf {
        match self.subdir() {
            Some(subdir) => data_dir.join(subdir),
            None => data_dir.to_path_buf(),
        }
    }

    fn subdir(self) -> Option<&'static str> {
        match self {
            Store::Stream => None,
            Store::Cache => Some("caches"),
            Store::Groups => Some("groups"),
            Store::DeadLetters => Some("dead-letters"),
            Store::Counters => Some("counters"),
        }
    }
}

/// One shard's directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardDir {
    pub store: Store,
    /// The directory's name. A readable rendering of the shard's key plus a
    /// hash; see [`crate::disk_log::layout`].
    pub name: String,
    pub path: PathBuf,
}

/// Every shard directory under a broker's data directory, by store and then
/// name.
pub fn find_shards(data_dir: &Path) -> Result<Vec<ShardDir>> {
    if !data_dir.is_dir() {
        return Err(StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("{} is not a directory", data_dir.display()),
        )));
    }
    let mut shards = Vec::new();
    for store in Store::ALL {
        let root = store.root(data_dir);
        let entries = match std::fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(StorageError::Io(err)),
        };
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            // The other stores' roots sit beside the stream shards, and a
            // dotted name is never a shard (an older build's compaction swap).
            let is_store_root = store == Store::Stream
                && Store::ALL.iter().any(|other| other.subdir() == Some(&name));
            if is_store_root || name.contains('.') {
                continue;
            }
            shards.push(ShardDir {
                store,
                name,
                path: entry.path(),
            });
        }
    }
    shards.sort_by(|a, b| (a.store, &a.name).cmp(&(b.store, &b.name)));
    Ok(shards)
}

/// The directory a shard of `store` would have under `data_dir`.
pub fn shard_dir(data_dir: &Path, store: Store, key: &ShardKey) -> PathBuf {
    super::layout::shard_dir(&store.root(data_dir), key)
}

/// What one shard directory holds and what startup would do with it.
#[derive(Debug, Clone)]
pub struct ShardReport {
    pub segments: Vec<SegmentReport>,
    /// Where the shard's durable mark says synced bytes end, if it has one.
    pub durable_mark: Option<(SegmentId, u64)>,
    pub startup: Startup,
    /// The writes startup would make, in order. Not made here.
    pub actions: Vec<Action>,
}

/// Startup's verdict on a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Startup {
    /// Opens as it is. Rebuilding a derived index does not count as a repair.
    Clean,
    /// Opens after discarding bytes or segments that were never acknowledged:
    /// a torn tail, or a rollover left half done.
    Repair,
    /// Refuses to open: damage in data that may have been acknowledged.
    Refuse(Corruption),
}

/// One write startup would make.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Delete a segment and its index.
    RemoveSegment {
        segment: SegmentId,
        reason: &'static str,
    },
    /// Write a sealed segment's index again from the segment.
    RebuildIndex {
        segment: SegmentId,
        reason: &'static str,
    },
    /// Cut a retired segment whose seal never finished back to `valid_bytes`,
    /// deleting `drops_next` first when it does not continue from there.
    CutRetired {
        segment: SegmentId,
        valid_bytes: u64,
        drops_next: Option<SegmentId>,
    },
    /// Cut the active segment's torn tail.
    TruncateTail {
        segment: SegmentId,
        position: u64,
        discarded_bytes: u64,
        cause: CorruptionKind,
    },
    /// No segments: start the log with an empty segment 0.
    CreateFirstSegment,
}

/// One segment file, checked in full.
#[derive(Debug, Clone)]
pub struct SegmentReport {
    pub id: SegmentId,
    pub size_bytes: u64,
    /// `None` when the header does not decode.
    pub base_offset: Option<Offset>,
    pub created_at_micros: Option<u64>,
    pub version: Option<u16>,
    /// Records that verified, up to a torn tail. `None` when the check
    /// stopped at damage it cannot step past.
    pub records: Option<u64>,
    /// The offset after the last record that verified, under the same rule.
    pub next_offset: Option<Offset>,
    pub index: IndexState,
    /// The first record that does not verify, if any.
    pub damage: Option<Damage>,
}

/// How a segment's index file compares with one rebuilt from the segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    /// Identical.
    Matches,
    /// Its entries are a leading part of the rebuilt index: written by
    /// appends that have not reached a later entry yet, or cut by a crash.
    Behind,
    /// No usable index file: absent, unreadable, or for another base offset.
    Missing,
    /// Present but points somewhere the records are not.
    Stale,
    /// The segment could not be read far enough to rebuild an index.
    Unknown,
}

/// Where a segment stops verifying.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Damage {
    pub position: u64,
    pub cause: CorruptionKind,
    /// The damage runs to the end of the file in a shape only an unfinished
    /// write leaves: a record the file ends inside, or zeros to the end.
    /// Otherwise it could be a complete record that rotted.
    pub tail: bool,
}

/// Inspect the shard in `dir`, with startup's verdict for `config`. Only
/// `index_spacing_bytes`, `repair_checksum_tail` and `verify_all_on_open`
/// affect the result.
pub fn inspect_shard(dir: &Path, config: &LogConfig) -> Result<ShardReport> {
    let label = dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let offloaded = offload::manifest::load(dir)?;
    let plan = recovery::plan_recovery(dir, &label, config, &offloaded)?;

    let mut actions: Vec<Action> = plan.steps.iter().filter_map(describe).collect();
    let startup = match &plan.end {
        End::Refuse(detail) => Startup::Refuse(detail.clone()),
        End::Fresh => {
            actions.push(Action::CreateFirstSegment);
            Startup::Clean
        }
        End::Resume(resume) => {
            if let Some(tail) = &resume.active.torn_tail {
                actions.push(Action::TruncateTail {
                    segment: resume.active_id,
                    position: tail.position,
                    discarded_bytes: tail.discarded_bytes,
                    cause: tail.cause.clone(),
                });
            }
            Startup::Clean
        }
    };
    let startup = match startup {
        Startup::Clean
            if actions.iter().any(|action| {
                !matches!(
                    action,
                    Action::RebuildIndex { .. } | Action::CreateFirstSegment
                )
            }) =>
        {
            Startup::Repair
        }
        other => other,
    };

    let mut segments = Vec::new();
    if dir.exists() {
        for id in recovery::discover_segment_ids(dir)? {
            segments.push(inspect_segment(dir, &label, config, id)?);
        }
    }
    Ok(ShardReport {
        segments,
        durable_mark: durable_mark::load(dir).map(
            |DurableMark {
                 segment,
                 synced_bytes,
             }| { (segment, synced_bytes) },
        ),
        startup,
        actions,
    })
}

fn describe(step: &Step) -> Option<Action> {
    Some(match step {
        Step::Remove { id, why } => Action::RemoveSegment {
            segment: *id,
            reason: match why {
                Removal::LostRollRace => "empty, left by a rollover that lost its race",
                Removal::HeaderlessPreparation => "no header, left by an uninstalled rollover",
                Removal::EmptyPreparation { .. } => {
                    "empty and based away from the log's end, left by an uninstalled rollover"
                }
                Removal::BlankFirst => "the only segment, and blank: its creation never finished",
                Removal::BelowOffloadedGap => "below a gap the offload manifest covers",
            },
        },
        Step::RebuildIndex { id, why, .. } => Action::RebuildIndex {
            segment: *id,
            reason: match why {
                IndexRebuild::Missing => "missing",
                IndexRebuild::Mismatch => "does not match its segment",
                IndexRebuild::VerifyAll => "verify-all-on-open walks every segment",
            },
        },
        Step::CutRetired {
            id,
            valid_bytes,
            drop_next,
        } => Action::CutRetired {
            segment: *id,
            valid_bytes: *valid_bytes,
            drops_next: *drop_next,
        },
        Step::SyncDir => return None,
    })
}

fn inspect_segment(
    dir: &Path,
    label: &str,
    config: &LogConfig,
    id: SegmentId,
) -> Result<SegmentReport> {
    let path = dir.join(segment_file_name(id));
    let size_bytes = std::fs::metadata(&path)?.len();
    let header = read_segment_header(&path, id, label);
    let mut report = SegmentReport {
        id,
        size_bytes,
        base_offset: header.as_ref().ok().map(|header| header.base_offset),
        created_at_micros: header.as_ref().ok().map(|header| header.created_at_micros),
        version: header.as_ref().ok().map(|header| header.version),
        records: None,
        next_offset: None,
        index: IndexState::Unknown,
        damage: None,
    };
    let header = match header {
        Ok(header) => header,
        Err(StorageError::Corruption(detail)) => {
            report.damage = Some(Damage {
                position: 0,
                cause: detail.kind,
                tail: false,
            });
            return Ok(report);
        }
        Err(err) => return Err(err),
    };
    // Nothing repaired: any damage is reported where it starts.
    let strict = TailRepair::default();
    match scan_segment_with(
        &path,
        id,
        label,
        config.index_spacing_bytes,
        ScanStart::Full,
        strict,
    ) {
        Ok(outcome) => {
            report.records = Some(outcome.record_count);
            report.next_offset = Some(outcome.next_offset);
            report.index = compare_index(dir, id, header.base_offset, &outcome.index);
            report.damage = outcome.torn_tail.map(|tail| Damage {
                position: tail.position,
                cause: tail.cause,
                tail: true,
            });
        }
        Err(StorageError::Corruption(detail)) => {
            report.damage = Some(Damage {
                position: detail.site.position.unwrap_or(0),
                cause: detail.kind,
                tail: false,
            });
        }
        Err(err) => return Err(err),
    }
    Ok(report)
}

fn compare_index(
    dir: &Path,
    id: SegmentId,
    base_offset: Offset,
    rebuilt: &SparseIndex,
) -> IndexState {
    let Some(on_disk) = SparseIndex::load(&dir.join(index_file_name(id)), base_offset) else {
        return IndexState::Missing;
    };
    let (on_disk, rebuilt) = (on_disk.entries(), rebuilt.entries());
    if on_disk == rebuilt {
        IndexState::Matches
    } else if on_disk.len() < rebuilt.len() && rebuilt.starts_with(on_disk) {
        IndexState::Behind
    } else if on_disk.is_empty() {
        IndexState::Missing
    } else {
        IndexState::Stale
    }
}

#[cfg(test)]
mod tests;
