//! Reading a bounded range of a durable stream shard, without subscribing.

use felix_storage::log::LogRecord;

use super::Broker;
use super::subscribe::committed_tail;
use crate::error::{BrokerError, Result};

/// One page of [`Broker::read_range`].
#[derive(Debug, Clone)]
pub struct ReadPage {
    /// Committed records, in offset order.
    pub records: Vec<LogRecord>,
    /// Where the next page starts. Offsets that hold no record a client is
    /// given are passed over, so this is not always the last offset plus one.
    /// At or past the requested end once the range is done.
    pub next_offset: u64,
}

impl Broker {
    /// Up to `max_records` committed records of a durable stream shard, from
    /// `from` and stopping before `end`, about `max_bytes` of payload at most
    /// (a single larger record is still returned, alone).
    ///
    /// Registers no subscriber and never waits: a page that reaches what is
    /// committed comes back short, or empty, with `next_offset` where to ask
    /// again. Only records that will stay are returned: on a `Quorum` shard
    /// nothing at or past the committed mark, and under `FsyncMode::OnCommit`
    /// nothing not yet synced.
    ///
    /// [`BrokerError::CursorTooOld`] when `from` is below what retention kept,
    /// [`BrokerError::CursorInFuture`] when it is past the log's tail,
    /// [`BrokerError::StreamNotDurable`] for an in-memory stream, and
    /// [`BrokerError::NotReadable`] while this broker cannot say what is
    /// committed.
    #[allow(clippy::too_many_arguments)]
    pub async fn read_range(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        from: u64,
        end: Option<u64>,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<ReadPage> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        let Some(log) = &handle.state.durable else {
            return Err(BrokerError::StreamNotDurable {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            });
        };
        let oldest = log.base_offset();
        if from < oldest {
            return Err(BrokerError::CursorTooOld {
                oldest,
                requested: from,
            });
        }
        // Settled, so an append already under way is either counted in the
        // tail or not yet written; its bytes are never half-read past it.
        let tail = log.settled_tail_offset().await?;
        if from > tail {
            return Err(BrokerError::CursorInFuture {
                requested: from,
                tail,
            });
        }
        let readable = log.readable_end(committed_tail(
            handle.state.read_bound(),
            tail,
            stream,
            shard,
        )?);
        let stop = end.map_or(readable, |end| end.min(readable));
        if from >= stop || max_records == 0 {
            return Ok(ReadPage {
                records: Vec::new(),
                next_offset: from,
            });
        }
        let mut records = log.read_from(from, max_bytes).await?;
        // Every offset in `[from, read_to)` was either returned or held a
        // generation-start record, which no client is given.
        let read_to = records.last().map_or(stop, |record| record.offset + 1);
        records.retain(|record| record.offset < stop);
        let next_offset = if records.len() > max_records {
            records.truncate(max_records);
            records.last().map_or(from, |record| record.offset + 1)
        } else {
            read_to.min(stop)
        };
        Ok(ReadPage {
            records,
            next_offset,
        })
    }
}
