//! A learner's staging log for a re-formed stream's base: the entries
//! `floor..=base` it pulls from a voter, checked one by one with
//! [`super::BaseCheck`], kept apart from its copy until all of them are in.
//! [`Staging::finish`] syncs the log and then writes a marker naming the
//! base; [`super::start::adopt_staged`] moves the log into the copy only
//! when that marker names the base it is asked for. Issue #3005.

use std::io;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use super::epoch_file::EpochLogId;
use crate::DiskLog;
use crate::log::{Epoch, LogConfig, WrittenBy};
use crate::raft_log::{CommittedLeaderIdOf, EntryCodec, LogEntry, LogTypes, record_from_entry};

/// The staging log's directory, inside the copy's.
pub(crate) const STAGING_DIR: &str = "base-staging";

/// Written in the staging directory once every entry of the base is in it
/// and synced: the base's last index and term.
pub(crate) const STAGED: &str = "base-staged";

/// A staging log being filled.
pub struct Staging {
    dir: PathBuf,
    log: DiskLog,
    next: u64,
}

impl Staging {
    /// An empty staging log in `copy` starting at `floor`. One left by an
    /// earlier pull, finished or not, is discarded first.
    pub async fn begin(copy: &Path, floor: u64, config: &LogConfig) -> io::Result<Self> {
        discard(copy)?;
        let dir = copy.join(STAGING_DIR);
        crate::fs::create_dir_all(&dir)?;
        crate::io::sync_dir(copy)?;
        let config = LogConfig {
            raft_log: true,
            ..config.clone()
        };
        let label = format!("{} (staging)", copy.display());
        let log = DiskLog::open(&dir, label, config).map_err(io::Error::other)?;
        let mut staging = Self { dir, log, next: 0 };
        staging.restart(floor).await?;
        Ok(staging)
    }

    /// Empty the log and start again at `floor`, for a pull whose voters'
    /// floor rose under it.
    pub async fn restart(&mut self, floor: u64) -> io::Result<()> {
        self.log.reset_to(floor).await.map_err(io::Error::other)?;
        self.next = floor;
        Ok(())
    }

    /// The index the next entry must have.
    pub fn next(&self) -> u64 {
        self.next
    }

    /// Write `entries`, which must start at [`Self::next`] and run on
    /// without a hole, each stamped with the generation that wrote it, and
    /// sync them. A blank or membership entry's record carries `now`, as on
    /// any follower; a command's carries its own time.
    pub async fn append<C, K>(
        &mut self,
        codec: &K,
        entries: &[LogEntry<C>],
        now: u64,
    ) -> io::Result<()>
    where
        C: LogTypes,
        CommittedLeaderIdOf<C>: From<u64> + Deref<Target = u64>,
        K: EntryCodec<C>,
    {
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let start = first.log_id.index;
        if start != self.next {
            return Err(invalid(format!(
                "{}: staged entries start at {start}, not {}",
                self.dir.display(),
                self.next
            )));
        }
        let mut records = Vec::with_capacity(entries.len());
        let mut writers: Vec<Epoch> = Vec::new();
        for (at, entry) in entries.iter().enumerate() {
            let index = start + at as u64;
            if entry.log_id.index != index {
                return Err(invalid(format!(
                    "{}: staged entries skip from {index} to {}",
                    self.dir.display(),
                    entry.log_id.index
                )));
            }
            let (record, generation) = record_from_entry::<C, K>(codec, entry, now)?;
            if writers
                .last()
                .is_none_or(|run| run.generation != generation)
            {
                writers.push(Epoch {
                    generation,
                    start_offset: index,
                });
            }
            records.push(record);
        }
        let written = self
            .log
            .append_pending_at_keeping(start, records, WrittenBy::Writers(writers))
            .await
            .map_err(|refused| io::Error::other(refused.error))?;
        if written.is_none() {
            return Err(invalid(format!(
                "{}: the staging log moved under the pull",
                self.dir.display()
            )));
        }
        self.log.sync().await.map_err(io::Error::other)?;
        self.next = start + entries.len() as u64;
        Ok(())
    }

    /// Close the log, synced, and mark it the whole of `base`. Called only
    /// once [`super::BaseCheck::complete`] holds.
    pub async fn finish(self, base: EpochLogId) -> io::Result<()> {
        if self.next != base.index + 1 {
            return Err(invalid(format!(
                "{}: staged through {}, not the base's {}",
                self.dir.display(),
                self.next.saturating_sub(1),
                base.index
            )));
        }
        self.log.sync().await.map_err(io::Error::other)?;
        self.log.close().await.map_err(io::Error::other)?;
        let marker = self.dir.join(STAGED);
        let mut file = crate::fs::File::create(&marker)?;
        io::Write::write_all(
            &mut file,
            format!("{} {}", base.index, base.term).as_bytes(),
        )?;
        crate::io::sync_all(&file)?;
        crate::io::sync_dir(&self.dir)
    }
}

/// Remove `copy`'s staging log, if it has one.
pub fn discard(copy: &Path) -> io::Result<()> {
    let dir = copy.join(STAGING_DIR);
    if !crate::fs::try_exists(&dir)? {
        return Ok(());
    }
    crate::fs::remove_dir_all(&dir)?;
    crate::io::sync_dir(copy)
}

/// The base a finished staging log in `copy` holds, as (index, term).
pub(crate) fn staged(copy: &Path) -> io::Result<Option<(u64, u64)>> {
    let marker = copy.join(STAGING_DIR).join(STAGED);
    if !crate::fs::try_exists(&marker)? {
        return Ok(None);
    }
    let text = crate::fs::read(&marker)?;
    let text = String::from_utf8_lossy(&text);
    let mut parts = text.split(' ').map(str::parse::<u64>);
    match (parts.next(), parts.next(), parts.next()) {
        (Some(Ok(index)), Some(Ok(term)), None) => Ok(Some((index, term))),
        _ => Err(invalid(format!("{}: unreadable", marker.display()))),
    }
}

pub(crate) fn staging_dir(copy: &Path) -> PathBuf {
    copy.join(STAGING_DIR)
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests;
