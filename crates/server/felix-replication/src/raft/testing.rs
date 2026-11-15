//! Test-only writes that skip the checks a proposal goes through. Not an API.

use super::runtime::GroupRuntime;
use super::types::{Command, ProducerMark};
use crate::ShardKey;
use bytes::Bytes;

/// One record for [`GroupRuntime::write_unchecked`]: its payload and, for
/// a record of an idempotent producer's batch, `(producer_id, sequence,
/// position, len)`.
pub type UncheckedRecord = (Bytes, Option<(u64, u64, u32, u32)>);

impl GroupRuntime {
    /// Write `records` to `shard`'s group as entries of their own, at
    /// whatever term this member leads, with no check against the log or
    /// the batch. What a leader that stops partway through a batch leaves,
    /// for tests above this crate that cannot stop one there. Returns each
    /// record's offset, or `None` if the member does not lead or a write
    /// failed.
    pub async fn write_unchecked(
        &self,
        shard: &ShardKey,
        records: Vec<UncheckedRecord>,
    ) -> Option<Vec<u64>> {
        let group = self.group(shard)?;
        let mut offsets = Vec::with_capacity(records.len());
        for (payload, mark) in records {
            let producer = mark.map(|(producer_id, sequence, position, len)| ProducerMark {
                producer_id,
                sequence,
                position,
                len,
            });
            let command = Command {
                timestamp_micros: 1_700_000_000,
                payload,
                producer,
                expected: None,
                trim: None,
                commit: false,
                publisher: None,
                headers: None,
                written: None,
            };
            let written = group.raft().client_write(command).await.ok()?;
            offsets.push(written.log_id.index);
        }
        Some(offsets)
    }
}
