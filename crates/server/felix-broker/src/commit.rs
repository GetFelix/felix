//! Atomic commits: an event and the state updates that go with it, written
//! to one stream shard's log as one record.
//!
//! One record is the whole trick. Replication ships by bytes, so a batch of
//! several records can reach a follower in part, and a follower promoted with
//! half a batch keeps that half. A single record is shipped, truncated and
//! counted by the commit mark whole. `docs/atomic-commit.md` has the
//! semantics and `docs/formal/FelixAtomicCommit.tla` the model.
//!
//! Every reader outside replication sees the record as its event: the
//! payload is unwrapped on the way out ([`client_record`]), and the state
//! updates go to the shard's [`StateView`] under the same lock that puts the
//! event in the replay ring.

mod record;
mod view;

pub use record::{CommitRecord, StateOp};
pub(crate) use view::{Entry as StateEntry, StateView};

use felix_storage::log::LogRecord;

/// `record` as a reader outside replication sees it: a commit record's
/// payload is replaced by the event it carries. Any other record is returned
/// as it is.
///
/// A commit record whose payload does not decode is returned unchanged
/// rather than dropped: the log's checksums passed, so the bytes are what
/// was written, and hiding the offset would read as a drop.
pub fn client_record(mut record: LogRecord) -> LogRecord {
    if record.mark.is_commit()
        && let Ok(commit) = CommitRecord::decode(&record.payload)
    {
        record.payload = commit.event;
    }
    record
}

#[cfg(test)]
mod tests;
