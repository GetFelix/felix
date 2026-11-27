//! `Client` methods for caches, counters and commits: reads, writes, conditional writes and cache watches.

use std::collections::HashMap;

use bytes::Bytes;
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::args::{parse_positions, u64_of};
use crate::cache_watch::CacheWatchHandle;
use crate::errors::{classify, invalid};
use crate::sharded_cache_watch::ShardedCacheWatchHandle;

use super::Client;

#[napi]
impl Client {
    /// Store a value, optionally with a time-to-live in seconds.
    #[napi]
    pub async fn cache_put(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        value: Buffer,
        ttl_seconds: Option<f64>,
    ) -> Result<()> {
        // The wire carries a TTL in milliseconds; seconds is what a caller
        // reaches for, so the conversion happens here rather than leaking the
        // unit into the signature.
        let ttl_ms = ttl_seconds.map(|s| (s.max(0.0) * 1000.0) as u64);
        let value = Bytes::copy_from_slice(&value);
        self.cluster()?
            .cache_put(&tenant_id, &namespace, &cache, &key, value, ttl_ms)
            .await
            .map_err(classify)
    }

    /// Read a value, or `null` if the key is absent or expired.
    #[napi]
    pub async fn cache_get(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
    ) -> Result<Option<Buffer>> {
        let value = self
            .cluster()?
            .cache_get(&tenant_id, &namespace, &cache, &key)
            .await
            .map_err(classify)?;
        Ok(value.map(|v| v.to_vec().into()))
    }

    /// Remove a key, returning what it held, or `null` if it held nothing.
    ///
    /// The previous value is the return rather than a discard: it is what lets
    /// a caller tell "I deleted something" from "it was already gone" without
    /// a second round trip.
    #[napi]
    pub async fn cache_delete(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
    ) -> Result<Option<Buffer>> {
        let previous = self
            .cluster()?
            .cache_delete(&tenant_id, &namespace, &cache, &key)
            .await
            .map_err(classify)?;
        Ok(previous.map(|v| v.to_vec().into()))
    }

    /// Commit `ops` as one record on the shard `entityKey` routes to.
    ///
    /// Every reader sees all of it or none of it. `ops` must carry exactly one
    /// event (`publish` or `enqueue`) and name one stream; otherwise the call
    /// rejects with `EventCountError` or `NotOnOwningShardError` before
    /// anything is sent.
    #[napi]
    pub async fn commit(
        &self,
        tenant_id: String,
        namespace: String,
        entity_key: Buffer,
        ops: Vec<crate::commit::CommitOp>,
    ) -> Result<crate::commit::CommitReceipt> {
        let ops = ops
            .into_iter()
            .map(crate::commit::to_client)
            .collect::<Result<Vec<_>>>()?;
        let receipt = self
            .cluster()?
            .commit(&tenant_id, &namespace, &entity_key, ops)
            .await
            .map_err(classify)?;
        Ok(crate::commit::CommitReceipt {
            offset: BigInt::from(receipt.offset),
        })
    }

    /// `key` in the state of `stream`'s shard that `entityKey` routes to.
    #[napi]
    pub async fn state_get(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        entity_key: Buffer,
        key: String,
    ) -> Result<crate::commit::StateValue> {
        let state = self
            .cluster()?
            .state_get(&tenant_id, &namespace, &stream, &entity_key, &key)
            .await
            .map_err(classify)?;
        Ok(state.into())
    }

    /// Read a value with its version, or `null` if the key is absent or
    /// expired. Pass the version to `cachePutIf` or `cacheDeleteIf`.
    #[napi]
    pub async fn cache_get_versioned(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
    ) -> Result<Option<crate::conditional::VersionedValue>> {
        let entry = self
            .cluster()?
            .cache_get_versioned(&tenant_id, &namespace, &cache, &key)
            .await
            .map_err(classify)?;
        Ok(entry.map(Into::into))
    }

    /// Store a value only if the key's entry meets `condition`. A refusal
    /// resolves with `applied: false` and the key's current version.
    ///
    /// Never sent twice: a retry of a put that applied would be refused by the
    /// version it wrote, so a lost answer rejects instead.
    #[napi]
    pub async fn cache_put_if(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        value: Buffer,
        condition: crate::conditional::CacheCondition,
        ttl_seconds: Option<f64>,
    ) -> Result<crate::conditional::CacheConditionResult> {
        let condition = condition.into_client()?;
        let ttl_ms = ttl_seconds.map(|s| (s.max(0.0) * 1000.0) as u64);
        let value = Bytes::copy_from_slice(&value);
        let result = self
            .cluster()?
            .cache_put_if(
                &tenant_id, &namespace, &cache, &key, value, ttl_ms, condition,
            )
            .await
            .map_err(classify)?;
        Ok(result.into())
    }

    /// `cachePutIf` that also asks for a lease token. When the write applies,
    /// the broker may mint a token for whoever holds the lease, granting what
    /// the caller's lease templates name for the key; pass it to
    /// `withIdentity` to act under it. `token` is null when the write did not
    /// apply, and `tokenRefused` says why an applied write came back without
    /// one: the write stands either way. The token is a bearer secret.
    #[napi]
    pub async fn cache_put_if_lease(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        value: Buffer,
        condition: crate::conditional::CacheCondition,
        ttl_seconds: Option<f64>,
    ) -> Result<crate::conditional::LeaseWrite> {
        let condition = condition.into_client()?;
        let ttl_ms = ttl_seconds.map(|s| (s.max(0.0) * 1000.0) as u64);
        let value = Bytes::copy_from_slice(&value);
        let written = self
            .cluster()?
            .cache_put_if_lease(
                &tenant_id, &namespace, &cache, &key, value, ttl_ms, condition,
            )
            .await
            .map_err(classify)?;
        Ok(written.into())
    }

    /// Remove a key only if its version is `version`. A refusal resolves with
    /// `applied: false` and the key's current version.
    #[napi]
    pub async fn cache_delete_if(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        version: BigInt,
    ) -> Result<crate::conditional::CacheConditionResult> {
        let version = u64_of(version)?;
        let result = self
            .cluster()?
            .cache_delete_if(&tenant_id, &namespace, &cache, &key, version)
            .await
            .map_err(classify)?;
        Ok(result.into())
    }

    /// Commit `ops` only if the shard's next offset is `expectedOffset`. A
    /// refusal resolves with `written: false` and the shard's `tail`.
    #[napi]
    pub async fn commit_if(
        &self,
        tenant_id: String,
        namespace: String,
        entity_key: Buffer,
        ops: Vec<crate::commit::CommitOp>,
        expected_offset: BigInt,
    ) -> Result<crate::conditional::ConditionalWrite> {
        let ops = ops
            .into_iter()
            .map(crate::commit::to_client)
            .collect::<Result<Vec<_>>>()?;
        let expected = u64_of(expected_offset)?;
        let answer = self
            .cluster()?
            .commit_if(&tenant_id, &namespace, &entity_key, ops, expected)
            .await
            .map_err(classify)?;
        Ok(answer.into())
    }

    /// `commit`, to the named shard instead of the one `entityKey` hashes to.
    #[napi]
    pub async fn commit_on_shard(
        &self,
        tenant_id: String,
        namespace: String,
        shard: u32,
        entity_key: Buffer,
        ops: Vec<crate::commit::CommitOp>,
    ) -> Result<crate::commit::CommitReceipt> {
        let ops = ops
            .into_iter()
            .map(crate::commit::to_client)
            .collect::<Result<Vec<_>>>()?;
        let receipt = self
            .cluster()?
            .commit_on_shard(&tenant_id, &namespace, shard, &entity_key, ops)
            .await
            .map_err(classify)?;
        Ok(crate::commit::CommitReceipt {
            offset: BigInt::from(receipt.offset),
        })
    }

    /// `commitIf`, to the named shard. `expectedOffset` is that shard's next
    /// offset.
    #[napi]
    pub async fn commit_if_on_shard(
        &self,
        tenant_id: String,
        namespace: String,
        shard: u32,
        entity_key: Buffer,
        ops: Vec<crate::commit::CommitOp>,
        expected_offset: BigInt,
    ) -> Result<crate::conditional::ConditionalWrite> {
        let ops = ops
            .into_iter()
            .map(crate::commit::to_client)
            .collect::<Result<Vec<_>>>()?;
        let expected = u64_of(expected_offset)?;
        let answer = self
            .cluster()?
            .commit_if_on_shard(&tenant_id, &namespace, shard, &entity_key, ops, expected)
            .await
            .map_err(classify)?;
        Ok(answer.into())
    }

    /// `key` in the state of the named shard of `stream`, which takes no
    /// entity key.
    #[napi]
    pub async fn state_get_on_shard(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        key: String,
    ) -> Result<crate::commit::StateValue> {
        let state = self
            .cluster()?
            .state_get_on_shard(&tenant_id, &namespace, &stream, shard, &key)
            .await
            .map_err(classify)?;
        Ok(state.into())
    }

    /// Add to a counter and return its new value. `delta` may be negative.
    #[napi]
    pub async fn counter_add(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        delta: i64,
    ) -> Result<i64> {
        self.cluster()?
            .counter_add(&tenant_id, &namespace, &cache, &key, delta)
            .await
            .map_err(classify)
    }

    /// Read a counter's current value, or `null` if it does not exist.
    ///
    /// Absent is not zero: a counter nobody has added to has never been
    /// written, and the distinction is the caller's to make.
    #[napi]
    pub async fn counter_get(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
    ) -> Result<Option<i64>> {
        self.cluster()?
            .counter_get(&tenant_id, &namespace, &cache, &key)
            .await
            .map_err(classify)
    }

    /// Watch one cache for changes.
    ///
    /// Exactly one of `key` or `prefix` selects what to watch; a `prefix` of
    /// `""` is every key in the shard.
    ///
    /// `start` is the first cache-log offset you have *not* seen, so a
    /// resuming watcher passes the offset it last handled plus one. `null`
    /// means from now: live changes only.
    ///
    /// `retained` instead delivers each matching key's current value first and
    /// then live changes — join a room and immediately hold the roster.
    /// Mutually exclusive with `start`, whose replay already reconstructs the
    /// state that shortcuts.
    #[napi]
    pub async fn watch_cache(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        key: Option<String>,
        prefix: Option<String>,
        start: Option<BigInt>,
        retained: Option<bool>,
    ) -> Result<CacheWatchHandle> {
        let filter = match (key, prefix) {
            (Some(key), None) => felix_client::CacheWatchFilter::Key(key),
            (None, Some(prefix)) => felix_client::CacheWatchFilter::Prefix(prefix),
            (None, None) => {
                return Err(invalid("watchCache needs either key or prefix"));
            }
            (Some(_), Some(_)) => {
                return Err(invalid("watchCache takes key or prefix, not both"));
            }
        };
        let retained = retained.unwrap_or(false);
        if retained && start.is_some() {
            return Err(invalid(
                "retained and start are mutually exclusive: a resume already \
                 replays the state a retained start shortcuts",
            ));
        }
        let from = match start {
            Some(offset) => Some(u64_of(offset)?),
            None => None,
        };
        let inner = self.cluster()?;
        let opened = if retained {
            inner
                .watch_cache_retained(&tenant_id, &namespace, &cache, filter)
                .await
        } else {
            inner
                .watch_cache(&tenant_id, &namespace, &cache, filter, from)
                .await
        }
        .map_err(classify)?;
        Ok(CacheWatchHandle::new(opened))
    }

    /// Watch a key prefix across **every** shard of a cache, merged into one
    /// handle. Keys sharing a prefix hash to different shards and a watch reads
    /// one, so this opens one prefix watch per shard.
    ///
    /// `resume` is a `resumeOffsets()` result: a listed shard resumes at that
    /// offset and the rest watch from now. `retained` instead delivers each
    /// matching key's current value first; the two are mutually exclusive.
    #[napi]
    pub async fn watch_cache_sharded(
        &self,
        tenant_id: String,
        namespace: String,
        cache: String,
        prefix: String,
        resume: Option<HashMap<String, BigInt>>,
        retained: Option<bool>,
    ) -> Result<ShardedCacheWatchHandle> {
        let resume = parse_positions(resume)?;
        let inner = self.cluster()?;
        let opened = if retained.unwrap_or(false) {
            if resume.is_some() {
                return Err(invalid(
                    "retained and resume are mutually exclusive: a resume already \
                     replays the state a retained start shortcuts",
                ));
            }
            inner
                .watch_cache_sharded_retained(&tenant_id, &namespace, &cache, &prefix)
                .await
        } else {
            inner
                .watch_cache_sharded(&tenant_id, &namespace, &cache, &prefix, resume)
                .await
        }
        .map_err(classify)?;
        Ok(ShardedCacheWatchHandle::new(opened))
    }
}
