//! What the listener needs of the broker to write a PUBLISH.
//!
//! The broker service implements [`Writes`] over the broker's own publish
//! path, the way the Kafka listener's `Cluster` does. The listener decides
//! what to write and when to answer; the implementation decides whether this
//! broker may write the shard and when the write counts as acknowledged.
//! `docs/mqtt-design.md`, "Publishing".

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::mount::MountedTopic;

/// One MQTT message as a Felix record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// The application payload, untouched.
    pub payload: Bytes,
    /// The record's key and headers block (`felix_wire::RecordHeaders`): the
    /// topic as the key, and the message's fields as headers.
    pub headers: Bytes,
}

/// Records for one shard, written as one idempotent producer batch.
#[derive(Debug, Clone)]
pub struct WriteBatch {
    pub tenant_id: Arc<str>,
    pub target: MountedTopic,
    /// The connection's producer id.
    pub producer_id: u64,
    /// This producer's count of batches written to this shard, from zero.
    pub sequence: u64,
    pub records: Vec<Record>,
    /// Whether to wait for the stream's consistency level after the write: a
    /// majority on a `Quorum` stream. Off for a batch with nothing to
    /// acknowledge.
    pub acknowledged: bool,
}

/// Why nothing was written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// Another broker owns the shard, or this one does not hold it right now.
    NotOwner,
    /// This broker does not accept record headers, which every MQTT message
    /// carries.
    HeadersDisabled,
    /// The mounted stream is gone, not durable, or closing.
    StreamUnavailable,
}

/// Why a batch has no acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteError {
    /// Nothing was written, so the client may be told.
    Refused(Refusal),
    /// The batch may or may not be in the log: leadership moved, a majority
    /// did not answer in time, or the write failed partway. The client must
    /// not be told either way; the connection is closed and the client sends
    /// the message again. The text is the reason counted.
    OutcomeUnknown(&'static str),
}

/// The broker's side of a PUBLISH.
#[async_trait]
pub trait Writes: Send + Sync {
    /// A producer id no other producer holds, for a new connection.
    fn new_producer_id(&self) -> u64;

    /// Count `records` records of `bytes` payload against the tenant's publish
    /// quota and say how long to stop reading the connection to stay within
    /// it. MQTT has no throttle field, so the delay is the only answer.
    fn admit_produce(&self, tenant_id: &str, records: u64, bytes: u64) -> Duration;

    /// Write the batch and return once it has what the stream's consistency
    /// asks for, when `batch.acknowledged`: the leader's durable write under
    /// the fsync policy, and on a `Quorum` stream a majority as well. Never
    /// earlier, because the caller sends `PUBACK` when this returns.
    async fn write(&self, batch: WriteBatch) -> Result<(), WriteError>;
}

/// Writes nothing: every batch is refused as owned elsewhere.
pub struct NoWrites;

#[async_trait]
impl Writes for NoWrites {
    fn new_producer_id(&self) -> u64 {
        1
    }

    fn admit_produce(&self, _tenant_id: &str, _records: u64, _bytes: u64) -> Duration {
        Duration::ZERO
    }

    async fn write(&self, _batch: WriteBatch) -> Result<(), WriteError> {
        Err(WriteError::Refused(Refusal::NotOwner))
    }
}
