//! `Client` methods for reading a stream: subscriptions, shard listing, paged reads, trimming and time lookups.

use std::collections::HashMap;

use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::args::{parse_positions, parse_start, u64_of};
use crate::errors::classify;
use crate::sharded_subscription::ShardedSubscriptionHandle;
use crate::stream_read::StreamPage;
use crate::subscription::SubscriptionHandle;

use super::Client;

#[napi]
impl Client {
    /// Subscribe to a stream.
    ///
    /// `start` is `"latest"` (default), `"earliest"`, or an offset — the first
    /// record you have *not* seen, so a resuming client passes the offset it
    /// last handled plus one.
    ///
    /// A subscription reads **one shard**. For a multi-shard stream this is
    /// shard 0; `subscribeSharded` is what reads the whole stream.
    #[napi]
    pub async fn subscribe(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        start: Option<Either<String, BigInt>>,
        shard: Option<u32>,
    ) -> Result<SubscriptionHandle> {
        let start = parse_start(start)?;
        let inner = self.cluster()?;
        let subscription = inner
            .subscribe_shard(
                &tenant_id,
                &namespace,
                &stream,
                shard.unwrap_or(0),
                Some(start),
            )
            .await
            .map_err(classify)?;
        Ok(SubscriptionHandle::new(subscription))
    }

    /// Subscribe to **every** shard of a stream and merge them.
    ///
    /// Per-shard ordering only — that is all a sharded stream has. A lost
    /// shard does not disturb the others, and resumes from its own offset.
    ///
    /// `resume` is a `positions()` result: each listed shard continues at its
    /// own `offset + 1`, and a shard not listed starts wherever `start` says.
    /// Offsets are per shard, so resuming is a map rather than a number — one
    /// number carried across shards replays on all but one of them.
    #[napi]
    pub async fn subscribe_sharded(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        start: Option<Either<String, BigInt>>,
        resume: Option<HashMap<String, BigInt>>,
    ) -> Result<ShardedSubscriptionHandle> {
        let start = parse_start(start)?;
        let resume = parse_positions(resume)?;
        let inner = self.cluster()?;
        let sub = match resume {
            Some(offsets) => {
                inner
                    .resubscribe_sharded(&tenant_id, &namespace, &stream, offsets, Some(start))
                    .await
            }
            None => {
                inner
                    .subscribe_sharded(&tenant_id, &namespace, &stream, Some(start))
                    .await
            }
        }
        .map_err(classify)?;
        Ok(ShardedSubscriptionHandle::new(sub))
    }

    /// How many shards a stream was placed with.
    ///
    /// A subscription reads one shard, so a client that wants the whole stream
    /// needs this to know how many there are — nothing else on the wire says.
    #[napi]
    pub async fn stream_shards(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
    ) -> Result<u32> {
        self.cluster()?
            .stream_shards(&tenant_id, &namespace, &stream)
            .await
            .map_err(classify)
    }

    /// Hide the records of `shard` below `before`, only if the shard's next
    /// offset is exactly `expectedOffset`. `headers`, if given, are kept with
    /// the trim. A stale tail resolves with `trimmed: false` and the shard's
    /// `tail`.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub async fn trim_before(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        before: BigInt,
        expected_offset: BigInt,
        headers: Option<Buffer>,
    ) -> Result<crate::conditional::ConditionalTrim> {
        let before = u64_of(before)?;
        let expected = u64_of(expected_offset)?;
        let answer = self
            .cluster()?
            .trim_before(
                &tenant_id,
                &namespace,
                &stream,
                shard,
                before,
                expected,
                headers.as_deref(),
            )
            .await
            .map_err(classify)?;
        Ok(answer.into())
    }

    /// The first offset on one shard of a durable stream whose record was
    /// appended at or after `atMicros` (microseconds since the Unix epoch), or
    /// `null` when no committed record is that recent yet. Subscribe at it to
    /// replay from a time.
    #[napi]
    pub async fn offset_for_time(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        at_micros: BigInt,
    ) -> Result<Option<BigInt>> {
        let at = u64_of(at_micros)?;
        Ok(self
            .cluster()?
            .offset_for_time(&tenant_id, &namespace, &stream, shard, at)
            .await
            .map_err(classify)?
            .map(BigInt::from))
    }

    /// One page of a durable stream shard's committed records, from `from` and
    /// stopping before `end`, without subscribing. Read the next page from
    /// `nextOffset`; the range is done once it reaches `end`. `maxRecords` of
    /// `0` takes the broker's cap, which also caps a page's size. A page that
    /// reaches what is committed comes back short or empty rather than
    /// waiting. Needs the `stream.subscribe` permission.
    #[napi]
    pub async fn read(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        from: BigInt,
        end: Option<BigInt>,
        max_records: Option<u32>,
    ) -> Result<StreamPage> {
        let from = u64_of(from)?;
        let end = end.map(u64_of).transpose()?;
        self.cluster()?
            .read(
                &tenant_id,
                &namespace,
                &stream,
                shard,
                from,
                end,
                max_records.unwrap_or(0),
            )
            .await
            .map(StreamPage::from)
            .map_err(classify)
    }
}
