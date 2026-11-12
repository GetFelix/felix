//! The newest entries, as the records they were appended as.
//!
//! Replication reads an entry right after it is appended. Serving it from
//! here hands back the appender's own bytes with no read of the segment.

use std::collections::VecDeque;

use crate::log::{LogRecord, Offset};

/// Record bytes kept per log. A read below them goes to the segments.
/// Small under test, so the adapter's tests read from both.
const CAPACITY_BYTES: u64 = if cfg!(test) { 2 << 10 } else { 4 << 20 };

#[derive(Default)]
pub(super) struct TailCache {
    /// `DiskLog::cuts` as read before the oldest record here was written.
    /// While the log still reports it, nothing has been cut since, so the
    /// log still holds every record here.
    cuts: u64,
    records: VecDeque<LogRecord>,
    bytes: u64,
}

impl TailCache {
    /// Take records just written. `cuts` is the log's count from before
    /// the write. Anything held from before a cut, or not directly below
    /// these, is dropped.
    pub(super) fn extend(&mut self, cuts: u64, records: impl IntoIterator<Item = LogRecord>) {
        for record in records {
            let follows = self
                .records
                .back()
                .is_some_and(|last| last.offset + 1 == record.offset);
            if self.cuts != cuts || !follows {
                self.clear();
                self.cuts = cuts;
            }
            self.bytes += size_of_record(&record);
            self.records.push_back(record);
        }
        while self.bytes > CAPACITY_BYTES {
            let Some(oldest) = self.records.pop_front() else {
                break;
            };
            self.bytes -= size_of_record(&oldest);
        }
    }

    pub(super) fn clear(&mut self) {
        self.records.clear();
        self.bytes = 0;
    }

    /// The records from `start` up to `end`, or as many of them as are
    /// held. `None` unless the one at `start` is held and the log's count
    /// of cuts is still `cuts`.
    pub(super) fn range(&self, cuts: u64, start: Offset, end: Offset) -> Option<Vec<LogRecord>> {
        if self.cuts != cuts {
            return None;
        }
        let skip = start.checked_sub(self.records.front()?.offset)?;
        let skip = usize::try_from(skip).ok()?;
        if skip >= self.records.len() {
            return None;
        }
        let take = usize::try_from(end.saturating_sub(start)).unwrap_or(usize::MAX);
        Some(self.records.iter().skip(skip).take(take).cloned().collect())
    }
}

fn size_of_record(record: &LogRecord) -> u64 {
    let headers = record.headers.as_ref().map_or(0, |headers| headers.len());
    let publisher = record
        .publisher
        .as_ref()
        .map_or(0, |publisher| publisher.len());
    (std::mem::size_of::<LogRecord>() + record.payload.len() + headers + publisher) as u64
}

#[cfg(test)]
mod tests;
