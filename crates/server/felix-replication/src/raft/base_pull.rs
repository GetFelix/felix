//! A voter's side of a learner's pull of a re-formed stream's base: the
//! committed entries below the gap, which the gap snapshot does not carry.
//! The learner asks in chunks and checks each entry with
//! [`felix_storage::raft_copy::BaseCheck`]; this only reads and bounds.
//! Issue #3005.

use std::io;

use felix_storage::raft_copy::EpochBase;
use felix_storage::raft_log::LogReader;
use openraft::storage::RaftLogReader;
use openraft::type_config::alias::EntryOf;

use super::codec::ShardCodec;
use super::types::ShardTypes;
use super::wire::entry_bytes;

/// The most entries one chunk carries, whatever the learner asks.
pub(crate) const MAX_BASE_ENTRIES: u32 = 4096;
/// The most record bytes one chunk carries, whatever the learner asks. A
/// chunk always carries its first entry, which may be a whole
/// `MAX_RECORD_BYTES` record.
pub(crate) const MAX_BASE_BYTES: u32 = 8 * 1024 * 1024;

/// One chunk of the base, starting at the asked index.
#[derive(Debug)]
pub(crate) struct BaseChunk {
    /// The voter's first index. Above the asked start, the chunk is empty
    /// and the learner restarts its pull here.
    pub(crate) floor: u64,
    pub(crate) entries: Vec<EntryOf<ShardTypes>>,
}

/// Read the base entries from `start` that fit `max_bytes` and `max_count`,
/// each held to its own cap. Refused when the copy has no base or `start`
/// is past it: nothing past the base is ever served, since the gap holds
/// nothing and the epoch's entries come by append.
pub(crate) async fn serve(
    reader: &mut LogReader<ShardTypes, ShardCodec>,
    first_index: u64,
    base: Option<EpochBase>,
    start: u64,
    max_bytes: u32,
    max_count: u32,
) -> Result<BaseChunk, String> {
    let Some(last) = base.and_then(|base| base.base_last) else {
        return Err("this copy holds no re-form base".into());
    };
    if start > last.index {
        return Err(format!(
            "{start} is past the base, which ends at {}",
            last.index
        ));
    }
    let empty = |floor| BaseChunk {
        floor,
        entries: Vec::new(),
    };
    if start < first_index {
        return Ok(empty(first_index));
    }
    let count = u64::from(max_count.clamp(1, MAX_BASE_ENTRIES));
    let budget = max_bytes.clamp(1, MAX_BASE_BYTES) as usize;
    let end = last.index.min(start + count - 1);
    let mut entries: Vec<EntryOf<ShardTypes>> = Vec::new();
    let mut bytes = 0;
    let mut next = start;
    'pages: while next <= end {
        // One page a read, so a chunk never holds much past its budget.
        let page = reader
            .limited_get_log_entries(next, end + 1)
            .await
            .map_err(|err: io::Error| err.to_string())?;
        if page.is_empty() {
            return Err(format!("the log holds no entry at {next}, inside the base"));
        }
        for entry in page {
            let index = entry.log_id.index;
            if index != next {
                // A purge moved the first index under the read.
                if entries.is_empty() && index > next {
                    return Ok(empty(index));
                }
                return Err(format!("the log skips from {next} to {index}"));
            }
            let size = entry_bytes(&entry);
            if !entries.is_empty() && bytes + size > budget {
                break 'pages;
            }
            bytes += size;
            entries.push(entry);
            next += 1;
            if bytes >= budget {
                break 'pages;
            }
        }
    }
    Ok(BaseChunk {
        floor: first_index,
        entries,
    })
}

#[cfg(test)]
mod tests;
