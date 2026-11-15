//! Starting a copy in a re-formed group's new epoch.
//!
//! A voter keeps its base records and gets a purged id, a snapshot id and a
//! saved commit at `first_index - 1` in the base's term, so the first leader
//! appends at the epoch's first index. A learner holding records past the
//! base empties its log and catches up by snapshot. Both get `raft-epoch`
//! and a reservation through `first_index - 1`.
//!
//! The steps run under a `reform-building` marker. [`super::CopyRoot::open_copy`]
//! refuses a directory that holds one, and a start run again over a crashed
//! one redoes every step, so a half-built copy is never served. See
//! `docs/per-shard-raft.md`, "Starting the new epoch".

use std::io;
use std::path::Path;

use bytes::Bytes;

use super::epoch_file::{self, EpochFile};
use crate::disk_log::crash_point;
use crate::raft_log::snapshot::{SnapshotId, StoredSnapshot};
use crate::raft_log::{held, reserved, state_file};

/// Present while a start is under way.
pub(crate) const BUILDING: &str = "reform-building";

/// What the copy is in the new epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    Voter,
    Learner,
}

/// One copy's start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Start {
    /// The copy's `raft-epoch` once started: its last base is the new epoch's.
    pub(crate) epoch: EpochFile,
    pub(crate) role: Role,
    /// The meta and data of a voter's snapshot at the end of the gap.
    pub(crate) meta: Bytes,
    pub(crate) data: Bytes,
}

/// Whether a start is under way in `dir`.
pub(crate) fn building(dir: &Path) -> io::Result<bool> {
    Ok(state_file::load(dir, BUILDING)?.is_some())
}

/// Start the copy in `dir`, which no log has open. A voter's records must
/// end exactly at the base's last log id; anything past it is refused.
pub(crate) fn start_copy(dir: &Path, start: &Start, index_spacing_bytes: u64) -> io::Result<()> {
    let base = *start
        .epoch
        .bases
        .last()
        .ok_or_else(|| invalid("a start needs the new epoch's base".into()))?;
    let label = dir.display().to_string();
    let step = || crash_point::reached(dir);
    let gap_end = base.first_index.checked_sub(1);

    state_file::store(dir, BUILDING, &[])?;
    step()?;
    // Before the first append, so an offset below the first index is never
    // given out again (#2640).
    if let Some(top) = gap_end
        && reserved::load(dir)? < Some(top)
    {
        reserved::store(dir, top)?;
        step()?;
    }
    let held = held::read(dir, &label, index_spacing_bytes)?;
    let record_last = held.last_record.map(|(index, _)| index);
    let base_last = base.base_last.map(|last| last.index);
    match start.role {
        Role::Learner if record_last > base_last => {
            empty_log(dir)?;
            step()?;
        }
        Role::Learner => {}
        Role::Voter => {
            if record_last != base_last {
                return Err(invalid(format!(
                    "{label}: the log ends at {record_last:?}, the base at {base_last:?}"
                )));
            }
        }
    }
    // Before the purged id: a purged id at the gap's end reads as the base
    // only under the epoch that made the gap.
    epoch_file::store(dir, &start.epoch)?;
    step()?;
    if start.role == Role::Voter
        && let Some(index) = gap_end
    {
        let term = base.base_last.map_or(0, |last| last.term);
        let generation = crate::raft_log::generation_of(term)?;
        crate::raft_log::snapshot::store_in(
            dir,
            &StoredSnapshot {
                last: Some(SnapshotId { term, index }),
                meta: start.meta.clone(),
                data: start.data.clone(),
            },
        )?;
        step()?;
        crate::raft_log::store_purged(dir, index, generation)?;
        step()?;
        crate::disk_log::store_saved_commit(dir, base.first_index, generation)
            .map_err(io::Error::other)?;
        step()?;
    }
    check_started(dir, start)?;
    state_file::remove(dir, BUILDING)
}

/// The two promises the vouch rule depends on (#2640), checked before the
/// marker goes: a voter's saved commit is at or above the base's end, and
/// every copy is reserved through `first_index - 1`.
pub(super) fn check_started(dir: &Path, start: &Start) -> io::Result<()> {
    let Some(base) = start.epoch.bases.last() else {
        return Ok(());
    };
    let label = dir.display();
    if let Some(top) = base.first_index.checked_sub(1)
        && reserved::load(dir)? < Some(top)
    {
        return Err(invalid(format!(
            "{label}: started without a reservation through {top}"
        )));
    }
    if start.role == Role::Voter {
        let (commit, _) = crate::disk_log::saved_commit(dir).map_err(io::Error::other)?;
        if commit < base.base_end() {
            return Err(invalid(format!(
                "{label}: started with a saved commit of {commit}, below the base's end {}",
                base.base_end()
            )));
        }
    }
    Ok(())
}

/// Remove every segment, index, purged id and snapshot in `dir`. The copy
/// file, votes, reservation and markers stay.
fn empty_log(dir: &Path) -> io::Result<()> {
    for entry in crate::fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let log_file = crate::segment::parse_segment_file_name(name).is_some()
            || name.ends_with(".index")
            || name.starts_with(crate::raft_log::purged::FILE_NAME)
            || name.starts_with(crate::raft_log::snapshot::FILE_NAME);
        if log_file {
            crate::fs::remove_file(&path)?;
        }
    }
    crate::io::sync_dir(dir)
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
