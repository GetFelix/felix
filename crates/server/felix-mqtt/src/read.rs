//! What the listener needs of the broker to serve a SUBSCRIBE.
//!
//! A subscription reads the mounted stream's shards by offset, bounded by
//! what the shard has committed, so a failover cannot take back a message a
//! device was sent. `docs/mqtt-design.md`, "Subscribing".

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

/// One committed record of a mounted stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMessage {
    pub offset: u64,
    pub payload: Bytes,
    /// The record's key and headers block (`felix_wire::RecordHeaders`), when
    /// it was stored with one.
    pub headers: Option<Bytes>,
}

/// Records read from one shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPage {
    pub messages: Vec<StoredMessage>,
    /// Where to ask next. Offsets below it were returned or hold no record a
    /// client sees.
    pub next: u64,
}

/// Why a read returned nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadError {
    /// This broker cannot say what is committed right now. Ask again.
    Unavailable,
    /// Retention removed the offset asked for; `oldest` is the first one kept.
    TooOld { oldest: u64 },
    /// The stream is gone, or not durable.
    Gone,
}

/// The broker's side of a SUBSCRIBE.
#[async_trait]
pub trait Reads: Send + Sync {
    /// Whether this broker serves subscriptions at all.
    fn available(&self) -> bool {
        true
    }

    /// How many shards the stream has. A filter with a wildcard reads all of
    /// them.
    fn shard_count(&self, tenant_id: &str, namespace: &str, stream: &str) -> u32;

    /// The end of what `shard` has committed: where a clean session starts.
    async fn tail(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<u64, ReadError>;

    /// Committed records from `from`, about `max_bytes` of them. When none
    /// are ready it waits up to `wait` for one, then returns an empty page.
    #[allow(clippy::too_many_arguments)]
    async fn read(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        from: u64,
        max_bytes: usize,
        wait: Duration,
    ) -> Result<ReadPage, ReadError>;
}

/// Serves nothing: every SUBSCRIBE is refused.
pub struct NoReads;

#[async_trait]
impl Reads for NoReads {
    fn available(&self) -> bool {
        false
    }

    fn shard_count(&self, _tenant_id: &str, _namespace: &str, _stream: &str) -> u32 {
        1
    }

    async fn tail(&self, _: &str, _: &str, _: &str, _: u32) -> Result<u64, ReadError> {
        Err(ReadError::Unavailable)
    }

    async fn read(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: u32,
        _: u64,
        _: usize,
        _: Duration,
    ) -> Result<ReadPage, ReadError> {
        Err(ReadError::Unavailable)
    }
}
