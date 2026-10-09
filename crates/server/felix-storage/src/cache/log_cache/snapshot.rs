//! The key index a compaction pass leaves on disk, and loading it back.
//!
//! `index_snapshot` owns the file. This is the cache's half: what goes in,
//! and the checks that the snapshot still describes this shard's log before
//! the replay starts from it. A snapshot that fails any of them is ignored
//! and the log is replayed whole.

use super::shard::{CacheShard, Index, SCAN_CHUNK_BYTES, ShardState};
use crate::disk_log::DiskLog;
use crate::index_snapshot::{self, Header, Reader, Rejected, STORE_CACHE};
use crate::log::{AppendOnlyLog, ReadRange};
use crate::{Result, StorageError};

impl CacheShard {
    /// Write the index to disk for the next open to start from. True when a
    /// snapshot was installed.
    ///
    /// Run at the end of a compaction pass, when the log is at its smallest
    /// next to the index. The file is written and flushed under a temporary
    /// name, then renamed over the old one, so a crash leaves the old
    /// snapshot or the new one.
    pub(super) async fn write_snapshot(&self) -> Result<bool> {
        let (header, mut entries, epoch, log) = {
            let mut state = self.state.lock().await;
            if state.closed {
                return Ok(false);
            }
            self.ensure_index(&mut state).await?;
            let Some(covered) = state.index.covered_through else {
                return Ok(false);
            };
            let last_checksum = if covered > state.log.base_offset() {
                record_checksum(&state.log, covered - 1)
                    .await?
                    .ok_or(StorageError::NotFound)?
            } else {
                0
            };
            let entries: Vec<_> = state
                .index
                .entries
                .iter()
                .map(|(key, entry)| (index_snapshot::cache_key(key), *entry))
                .collect();
            let header = Header {
                store: STORE_CACHE,
                covered_through: covered,
                log_bytes: state.index.log_bytes,
                last_checksum,
            };
            (header, entries, state.index_epoch, state.log.clone())
        };

        // A snapshot that outlives the records it covers is caught on load,
        // but costs a full replay; flushing them first avoids that.
        log.sync().await?;
        let dir = self.dir.clone();
        tokio::task::spawn_blocking(move || {
            index_snapshot::write_temporary(&dir, &header, &mut entries)
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
        .map_err(StorageError::Io)?;

        {
            // Installed under the lock `forget_index` and `close_shard` take,
            // so a snapshot of records replication has since cut, or of a
            // shard closed and reopened meanwhile, never lands.
            let state = self.state.lock().await;
            if state.closed || state.index_epoch != epoch {
                return Ok(false);
            }
            index_snapshot::install(&self.dir).map_err(StorageError::Io)?;
        }
        crate::io::sync_dir(&self.dir).map_err(StorageError::Io)?;
        Ok(true)
    }

    /// The index as the shard's snapshot has it, or `None` when there is no
    /// snapshot that matches the log and the whole log has to be replayed.
    /// `stop` is where this replay will end.
    pub(super) async fn restore(&self, state: &ShardState, stop: u64) -> Option<Index> {
        let base = state.log.base_offset();
        let dir = self.dir.clone();
        let loaded = tokio::task::spawn_blocking(move || load(&dir, base, stop))
            .await
            .unwrap_or_else(|err| Err(Rejected(format!("the load failed: {err}"))));
        let checked = match loaded {
            Ok(None) => return None,
            Ok(Some((header, index))) => match same_log(&state.log, base, &header).await {
                Ok(()) => Ok(index),
                Err(rejected) => Err(rejected),
            },
            Err(rejected) => Err(rejected),
        };
        match checked {
            Ok(index) => {
                tracing::debug!(
                    shard = %self.label,
                    covered_through = ?index.covered_through,
                    keys = index.entries.len(),
                    "cache index loaded from its snapshot",
                );
                Some(index)
            }
            Err(rejected) => {
                tracing::warn!(
                    shard = %self.label, reason = %rejected,
                    "ignoring the cache's index snapshot; replaying the whole log",
                );
                None
            }
        }
    }
}

/// Read the snapshot in `dir` into an index, checking it against what the
/// log holds now: records from `base` up to `stop`.
fn load(
    dir: &std::path::Path,
    base: u64,
    stop: u64,
) -> std::result::Result<Option<(Header, Index)>, Rejected> {
    let Some(mut reader) = Reader::open(dir)? else {
        return Ok(None);
    };
    let header = reader.header();
    let covered = header.covered_through;
    if header.store != STORE_CACHE {
        return Err(Rejected(format!("store {} is not a cache", header.store)));
    }
    // Below the base, records the snapshot never saw were trimmed; past the
    // end, records it did see are gone.
    if covered < base || covered > stop {
        return Err(Rejected(format!(
            "it covers up to {covered}, but the log holds {base}..{stop}"
        )));
    }
    let mut index = Index {
        log_bytes: header.log_bytes,
        covered_through: Some(covered),
        restored_through: Some(covered),
        ..Index::default()
    };
    // Bounded by the file's length, which `Reader::open` checked the count against.
    index.entries.reserve(reader.entry_count() as usize);
    while let Some((key, entry)) = reader.next_entry()? {
        let key = index_snapshot::parse_cache_key(&key)
            .ok_or_else(|| Rejected("an entry is not a cache key".into()))?
            .to_string();
        if entry.offset < base || entry.offset >= covered {
            return Err(Rejected(format!(
                "an entry points at {}, outside {base}..{covered}",
                entry.offset
            )));
        }
        index.live_bytes += u64::from(entry.bytes);
        if index.entries.insert(key, entry).is_some() {
            return Err(Rejected("a key appears twice".into()));
        }
    }
    reader.finish()?;
    Ok(Some((header, index)))
}

/// Whether the last record the snapshot covers is the one in the log now.
///
/// The offsets alone cannot tell: replication can cut a log back and append
/// other records until it is as long again.
async fn same_log(log: &DiskLog, base: u64, header: &Header) -> std::result::Result<(), Rejected> {
    let covered = header.covered_through;
    if covered == base {
        return Ok(());
    }
    match record_checksum(log, covered - 1).await {
        Ok(Some(checksum)) if checksum == header.last_checksum => Ok(()),
        Ok(Some(_)) => Err(Rejected(format!(
            "the record at {} is not the one it was taken from",
            covered - 1
        ))),
        Ok(None) => Err(Rejected(format!("there is no record at {}", covered - 1))),
        Err(err) => Err(Rejected(format!(
            "could not read the record at {}: {err}",
            covered - 1
        ))),
    }
}

/// The checksum of the record at `offset`, or `None` when the log has none
/// there.
async fn record_checksum(log: &DiskLog, offset: u64) -> Result<Option<u32>> {
    let records = log
        .read_range(ReadRange {
            start: offset,
            max_bytes: SCAN_CHUNK_BYTES,
        })
        .await?;
    Ok(records
        .first()
        .filter(|record| record.offset == offset)
        .map(|record| record.checksum))
}
