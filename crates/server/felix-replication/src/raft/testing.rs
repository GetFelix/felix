//! Test-only writes that skip the checks a proposal goes through. Not an API.

use super::runtime::GroupRuntime;
use super::types::{Command, CondPlace, ProducerMark};
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
        let commands = records.into_iter().map(|(payload, mark)| {
            let producer = mark.map(|(producer_id, sequence, position, len)| ProducerMark {
                producer_id,
                sequence,
                position,
                len,
            });
            command(payload, producer, None)
        });
        self.write_commands(shard, commands).await
    }

    /// Write `payloads` as pieces `position..` of a conditional batch of
    /// `len` records, each an entry of its own with no check, like
    /// [`Self::write_unchecked`]. Stopping short of the last piece and
    /// writing anything else after it leaves the batch cut.
    pub async fn write_cond_unchecked(
        &self,
        shard: &ShardKey,
        position: u32,
        len: u32,
        payloads: Vec<Bytes>,
    ) -> Option<Vec<u64>> {
        let commands = payloads.into_iter().zip(position..).map(|(payload, at)| {
            let cond = CondPlace { position: at, len };
            command(payload, None, Some(cond))
        });
        self.write_commands(shard, commands).await
    }

    async fn write_commands(
        &self,
        shard: &ShardKey,
        commands: impl Iterator<Item = Command>,
    ) -> Option<Vec<u64>> {
        let group = self.group(shard)?;
        let mut offsets = Vec::new();
        for command in commands {
            let written = group.raft().client_write(command).await.ok()?;
            offsets.push(written.log_id.index);
        }
        Some(offsets)
    }
}

fn command(payload: Bytes, producer: Option<ProducerMark>, cond: Option<CondPlace>) -> Command {
    Command {
        timestamp_micros: 1_700_000_000,
        payload,
        producer,
        expected: None,
        trim: None,
        commit: false,
        group: false,
        publisher: None,
        headers: None,
        written: None,
        cond,
    }
}
