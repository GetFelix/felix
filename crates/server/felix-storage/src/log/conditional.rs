//! What a reader may see of a log that holds conditional batches.
//!
//! A conditional batch of `len` records on a Raft log is `len` entries at
//! consecutive offsets, each a [`RecordMark::CondPiece`] naming its position
//! and the batch's length. Every member keeps the whole batch at its expected
//! offsets or blanks all of it, but the commit is not atomic: a leader change
//! can commit a strict prefix and write other entries after it. That prefix
//! is a *cut* batch. It stays in the log and no reader may see it.
//!
//! The rule, from each member's committed log alone:
//!
//! - A piece is a record only once the batch's later pieces follow it up to
//!   its last, all committed. Raft appends a batch as one run and a log only
//!   loses a suffix, so a committed last piece of the same length at the
//!   right offset is this batch's own, and the batch is whole.
//! - A committed entry where the next piece belongs that is anything else
//!   shows the batch was cut. It can never become whole, so its pieces are
//!   skipped like blanks and reads go on past them.
//! - A batch whose run reaches the end of the committed log is undecided:
//!   the read bound stops before its first piece.
//!
//! So a reader never sees part of a batch, and what it sees below the bound
//! never changes as the commit grows. `docs/formal/FelixRaftCond.tla` checks
//! the rule (`WholeBatchesOnly`, `ReadsStable`); see `docs/storage-format.md`,
//! "Conditional batches".

#[cfg(test)]
mod tests;

use super::{AppendOnlyLog, LogRecord, Offset, ReadPage, ReadRange, RecordMark};
use crate::Result;

/// A page of records after [`resolve`].
#[derive(Debug, Default)]
pub struct Resolved {
    /// The page's records without the pieces of a cut or undecided batch.
    pub records: Vec<LogRecord>,
    /// The pieces of the batch the page ends inside, which the records read
    /// so far cannot decide. Empty when no batch is left open.
    pub pending: Vec<LogRecord>,
    /// Where the next read starts: the first pending piece, or the end of
    /// what the page covered.
    pub next: Offset,
}

/// Resolve the conditional pieces of `records`, a read in offset order that
/// covered every offset from its first record up to `covered`. Offsets the
/// read skipped (trim records) count as entries that are not pieces. Only
/// offsets below `end`, the end of the committed log, are taken as
/// committed: a record at or past it is dropped.
pub fn resolve(records: Vec<LogRecord>, covered: Offset, end: Offset) -> Resolved {
    let covered = covered.min(end);
    let mut out = Vec::with_capacity(records.len());
    // The batch being followed: where its pieces start in `out`, and the
    // offset and position its next piece must have.
    let mut open: Option<(usize, Offset, u32, u32)> = None;
    for record in records {
        if record.offset >= end {
            break;
        }
        if let Some((start, next, position, len)) = open {
            match record.mark {
                RecordMark::CondPiece(piece)
                    if record.offset == next && piece.position == position && piece.len == len =>
                {
                    open = (!piece.is_last()).then_some((start, next + 1, position + 1, len));
                    out.push(record);
                    continue;
                }
                _ => {
                    out.truncate(start);
                    open = None;
                }
            }
        }
        if let RecordMark::CondPiece(piece) = record.mark {
            if piece.position >= piece.len {
                // No batch has this piece, so nothing can finish it.
                continue;
            }
            if !piece.is_last() {
                open = Some((out.len(), record.offset + 1, piece.position + 1, piece.len));
            }
        }
        out.push(record);
    }
    match open {
        // The read skipped the offset the next piece needed.
        Some((start, next, ..)) if next < covered => {
            out.truncate(start);
            Resolved {
                records: out,
                pending: Vec::new(),
                next: covered,
            }
        }
        Some((start, ..)) => {
            let pending = out.split_off(start);
            Resolved {
                next: pending[0].offset,
                records: out,
                pending,
            }
        }
        None => Resolved {
            records: out,
            pending: Vec::new(),
            next: covered,
        },
    }
}

/// [`AppendOnlyLog::read_range`] for a log that may hold conditional
/// batches, with `end` the end of its committed log. The page holds no piece
/// of a cut or undecided batch. A batch the read ends inside is read on to
/// its end, so the page may run past `max_bytes` by one batch, and a page
/// that stops before an undecided batch has `next` at its first piece.
pub async fn read_range<L: AppendOnlyLog + ?Sized>(
    log: &L,
    range: ReadRange,
    end: Offset,
) -> Result<ReadPage> {
    let max_bytes = range.max_bytes;
    let page = log.read_range(range).await?;
    let mut covered = page.next.min(end);
    let mut resolved = resolve(page.records, page.next, end);
    while !resolved.pending.is_empty() && covered < end {
        let more = log
            .read_range(ReadRange {
                start: covered,
                max_bytes,
            })
            .await?;
        if more.next <= covered {
            break;
        }
        let mut records = std::mem::take(&mut resolved.pending);
        records.extend(more.records);
        covered = more.next.min(end);
        let rest = resolve(records, more.next, end);
        resolved.records.extend(rest.records);
        resolved.pending = rest.pending;
        resolved.next = rest.next;
    }
    Ok(ReadPage {
        records: resolved.records,
        next: resolved.next,
    })
}

/// How far below `end`, the end of the committed log, a reader may go: `end`,
/// or the first piece of the batch the committed log ends inside. Never
/// falls as `end` grows. `last` is the mark of the record at `end - 1`.
pub fn bound(end: Offset, last: RecordMark) -> Offset {
    match last {
        RecordMark::CondPiece(piece) if piece.position < piece.len && !piece.is_last() => {
            end.saturating_sub(1 + u64::from(piece.position))
        }
        _ => end,
    }
}

/// [`bound`] read from `log`.
pub async fn read_bound<L: AppendOnlyLog + ?Sized>(log: &L, end: Offset) -> Result<Offset> {
    let Some(at) = end.checked_sub(1) else {
        return Ok(end);
    };
    // One byte asks for a single record; the log returns one whatever its
    // size. Trims are included, so the record found is the one at `at`.
    let records = log
        .read_range_with_trims(ReadRange {
            start: at,
            max_bytes: 1,
        })
        .await?;
    Ok(match records.first() {
        Some(record) if record.offset == at => bound(end, record.mark),
        _ => end,
    })
}
