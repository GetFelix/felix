//! One consumer group across every shard of a stream.

use std::sync::Arc;

use felix_client::{ShardedGroup, ShardedGroupRecord as Pending};
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::args::{duration_ms, u64_of};
use crate::errors::classify;

/// A record from `ShardedGroupHandle.poll`.
#[napi(object)]
pub struct ShardedGroupRecord {
    /// The shard it came from, which settling it has to reach.
    pub shard: u32,
    pub offset: BigInt,
    pub payload: Buffer,
    /// How many times this record has been handed out, this delivery included.
    pub attempts: u32,
    pub skipped_before: BigInt,
}

/// What settling a `ShardedGroupRecord` needs: the shard, the offset and which
/// delivery it was. A `ShardedGroupRecord` is one.
#[napi(object)]
pub struct ShardedGroupDelivery {
    pub shard: u32,
    pub offset: BigInt,
    pub attempts: u32,
}

impl ShardedGroupDelivery {
    fn pending(self) -> Result<Pending> {
        Ok(Pending {
            shard: self.shard,
            record: felix_wire::GroupRecord {
                offset: u64_of(self.offset)?,
                payload: Default::default(),
                attempts: self.attempts,
                skipped_before: 0,
                publisher: None,
                timestamp_micros: None,
            },
        })
    }
}

/// A consumer group read across every shard of a stream.
#[napi]
pub struct ShardedGroupHandle {
    inner: Arc<ShardedGroup>,
}

impl ShardedGroupHandle {
    pub(crate) fn new(group: ShardedGroup) -> Self {
        Self {
            inner: Arc::new(group),
        }
    }
}

#[napi]
impl ShardedGroupHandle {
    /// How many shards the group spans.
    #[napi(getter)]
    pub fn shards(&self) -> u32 {
        self.inner.shards()
    }

    /// Take up to `maxRecords` from the next shard that has any. The shards
    /// are not merged into one order; each record says which shard it is from.
    #[napi]
    pub async fn poll(&self, max_records: Option<u32>) -> Result<Vec<ShardedGroupRecord>> {
        let records = self
            .inner
            .poll(max_records.unwrap_or(32))
            .await
            .map_err(classify)?;
        Ok(records
            .into_iter()
            .map(|r| ShardedGroupRecord {
                shard: r.shard,
                offset: BigInt::from(r.record.offset),
                payload: r.record.payload.to_vec().into(),
                attempts: r.record.attempts,
                skipped_before: BigInt::from(r.record.skipped_before),
            })
            .collect())
    }

    /// Finish a record on its shard.
    #[napi]
    pub async fn ack(&self, record: ShardedGroupDelivery) -> Result<()> {
        self.inner.ack(&record.pending()?).await.map_err(classify)
    }

    /// Hand a record back for redelivery at once.
    #[napi]
    pub async fn nack(&self, record: ShardedGroupDelivery) -> Result<()> {
        self.inner.nack(&record.pending()?).await.map_err(classify)
    }

    /// Hand a record back to be redelivered after `delayMs`.
    #[napi]
    pub async fn nack_after(&self, record: ShardedGroupDelivery, delay_ms: f64) -> Result<()> {
        let delay = duration_ms(delay_ms, 0.0, "delayMs")?;
        self.inner
            .nack_after(&record.pending()?, delay)
            .await
            .map_err(classify)
    }

    /// Keep a record's claim standing for `extendMs` from now. Resolves with
    /// how many milliseconds it now stands.
    #[napi]
    pub async fn extend(&self, record: ShardedGroupDelivery, extend_ms: f64) -> Result<f64> {
        let extend = duration_ms(extend_ms, 1.0, "extendMs")?;
        let stands = self
            .inner
            .extend(&record.pending()?, extend)
            .await
            .map_err(classify)?;
        Ok(stands.as_millis() as f64)
    }

    /// Give up on a record: list it as a dead letter of the group.
    #[napi]
    pub async fn dead_letter(&self, record: ShardedGroupDelivery) -> Result<()> {
        self.inner
            .dead_letter(&record.pending()?)
            .await
            .map_err(classify)
    }
}
