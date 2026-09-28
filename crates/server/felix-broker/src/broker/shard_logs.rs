//! A shard's logs as replication sees them: finding each one, and telling a
//! stream that its log grew or was rebuilt underneath it.

use std::sync::Arc;

use super::Broker;
use crate::durable::StreamLog;
use crate::error::Result;

/// Which of a shard's logs a request is about.
///
/// A stream shard has two: the records themselves, and the consumer-group
/// cursors kept beside them. Both have to reach a replica, or a promoted leader
/// serves the records and has no idea where any group had got to — it starts
/// them at the beginning and redelivers everything already finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogKind {
    /// A stream's records.
    Stream,
    /// A cache's records.
    Cache,
    /// The consumer-group cursors belonging to a stream shard.
    GroupCursors,
    /// The dead-letter list belonging to a stream shard: the offsets its
    /// groups gave up on. Beside the cursors for the same reason the cursors
    /// are beside the records — a promoted leader that serves the stream but
    /// has lost which records its groups abandoned would silently redrive
    /// nothing and list nothing.
    GroupDeadLetters,
    /// The counter log belonging to a cache shard: signed deltas folded into
    /// running sums. Rides the cache shard's replica set the way group state
    /// rides a stream shard's, so a promoted replica resumes the true sum.
    Counters,
}

impl Broker {
    /// Signalled after every durable append.
    ///
    /// Replication waits on it so shipping starts when a record lands rather
    /// than on its next tick — which is most of a `Quorum` publish's latency
    /// when the tick is seconds and the shipping is milliseconds.
    pub fn appended(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.appended)
    }

    /// The log for one shard, whichever kind of thing it belongs to.
    ///
    /// The single place replication resolves a shard's log. A stream's lives in
    /// the durable-storage provider and a cache's belongs to the cache store,
    /// and getting that wrong writes a stream log for a cache — an empty
    /// directory nothing reads, while the real records go unreplicated.
    ///
    /// Not cached by the caller: a cache shard's log is replaced when the shard
    /// is closed and reopened, so a handle held across that goes stale.
    pub async fn shard_log(
        &self,
        kind: LogKind,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        shard: u32,
    ) -> Option<StreamLog> {
        match kind {
            LogKind::Cache => self
                .cache
                .shard_log(tenant_id, namespace, name, shard)
                .await
                .map(StreamLog::from_log),
            LogKind::GroupCursors => self
                .consumer_groups
                .as_ref()?
                .shard_log(tenant_id, namespace, name, shard)
                .await
                .ok()
                .map(StreamLog::from_log),
            LogKind::GroupDeadLetters => self
                .group_reader
                .as_ref()?
                .dead_letters()
                .shard_log(tenant_id, namespace, name, shard)
                .await
                .ok()
                .map(StreamLog::from_log),
            LogKind::Counters => self
                .counters
                .as_ref()?
                .shard_log(tenant_id, namespace, name, shard)
                .await
                .ok()
                .map(StreamLog::from_log),
            LogKind::Stream => self
                .durable_storage
                .as_ref()?
                .open_stream(tenant_id, namespace, name, shard)
                .ok(),
        }
    }

    /// [`Broker::shard_log`], creating the log at `base_offset` when this broker
    /// has never held the shard. The base it comes back with is the authority.
    pub async fn shard_log_at(
        &self,
        kind: LogKind,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        shard: u32,
        base_offset: u64,
    ) -> Option<StreamLog> {
        match kind {
            LogKind::Cache => self
                .cache
                .shard_log_at(tenant_id, namespace, name, shard, base_offset)
                .await
                .map(StreamLog::from_log),
            LogKind::GroupCursors => self
                .consumer_groups
                .as_ref()?
                .shard_log_at(tenant_id, namespace, name, shard, base_offset)
                .await
                .ok()
                .map(StreamLog::from_log),
            LogKind::GroupDeadLetters => self
                .group_reader
                .as_ref()?
                .dead_letters()
                .shard_log_at(tenant_id, namespace, name, shard, base_offset)
                .await
                .ok()
                .map(StreamLog::from_log),
            LogKind::Counters => self
                .counters
                .as_ref()?
                .shard_log_at(tenant_id, namespace, name, shard, base_offset)
                .await
                .ok()
                .map(StreamLog::from_log),
            LogKind::Stream => self
                .durable_storage
                .as_ref()?
                .open_stream_at(tenant_id, namespace, name, shard, base_offset)
                .ok(),
        }
    }

    /// Tell a stream that records reached its log without passing through it.
    ///
    /// Replication writes the shard's log directly, so a follower's in-memory
    /// view of the stream stays at zero while its disk fills. Left that way, the
    /// first publish this broker accepts after being promoted waits on commit
    /// turns that were never taken, and never returns.
    pub async fn adopt_replicated(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        durable_offset: u64,
    ) -> Result<()> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        handle.state.advance_to(durable_offset);
        Ok(())
    }

    /// Append a generation-start record for `generation` to a durable
    /// stream's log and move the stream past it. Returns its offset.
    ///
    /// For a promoted leader, before it serves. Writing through the
    /// [`StreamLog`] alone leaves the stream's commit order waiting on the
    /// record's offset, and the first publish after it would report a drop to
    /// every subscriber reading offsets. This keeps both straight, counting any
    /// generation-start records inherited just before this one into the skip.
    pub async fn append_generation_start(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        generation: u64,
    ) -> Result<u64> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        let Some(log) = &handle.state.durable else {
            return Err(crate::error::BrokerError::StreamNotDurable {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            });
        };
        let offset = log.append_generation_start(generation).await?;
        let mut run = 1;
        let mut at = offset;
        // Walks back one record at a time, but only over generation starts
        // with nothing between them, so a handful at most.
        while at > log.base_offset() {
            at -= 1;
            let before = log.read_log_from(at, 1).await?;
            match before.first() {
                Some(record) if record.offset == at && record.mark.is_generation_start() => {
                    run += 1;
                }
                _ => break,
            }
        }
        handle.state.skip_generation_start(offset, run);
        self.appended.notify_one();
        Ok(offset)
    }

    /// The stream's log was rebuilt from `base_offset`; forget the tail the
    /// old copy had. The counterpart of [`Self::adopt_replicated`] for a
    /// follower that discarded its records rather than storing more.
    pub async fn reset_replicated(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        base_offset: u64,
    ) -> Result<()> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        handle.state.reset_to(base_offset);
        Ok(())
    }
}
