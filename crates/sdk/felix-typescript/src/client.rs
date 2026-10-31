//! `Client`, the connected cluster client every other handle comes from.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use felix_client::ClientConfig;
use napi::JsFunction;
use napi::JsObject;
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::args::{
    duration_ms, parse_ack, parse_positions, parse_start, reconnect_policy, seeds, u64_of,
};
use crate::cache_watch::CacheWatchHandle;
use crate::errors::{classify, invalid};
use crate::group_admin::{GroupInfo, GroupPosition, delivery};
use crate::producer::Producer;
use crate::sharded_cache_watch::ShardedCacheWatchHandle;
use crate::sharded_group::ShardedGroupHandle;
use crate::sharded_subscription::ShardedSubscriptionHandle;
use crate::stream_read::{BatchReport, ConnectionCount, KeyedRecord, StreamPage};
use crate::subscription::SubscriptionHandle;
use crate::tls;
use crate::token;
use crate::types::{GroupDelivery, GroupRecord};

/// A connected Felix client.
#[napi]
pub struct Client {
    /// `None` once closed. A plain `std::sync::Mutex` and not the async one:
    /// the lock is only ever held long enough to clone the `Arc`, so callers
    /// sharing a client never queue behind each other's in-flight calls.
    inner: std::sync::Mutex<Option<Arc<felix_client::Client>>>,
}

impl Client {
    /// The cluster client, or an error if this one has been closed.
    fn cluster(&self) -> Result<Arc<felix_client::Client>> {
        self.inner
            .lock()
            .expect("the client lock is only ever held across a clone")
            .clone()
            .ok_or_else(|| invalid("this client is closed"))
    }
}

#[napi]
impl Client {
    /// Connect to a cluster.
    ///
    /// `addrs` is one `"host:port"` or several; any reachable one is enough
    /// and the client discovers the rest. A name is resolved again whenever
    /// no broker the client knows answers, so prefer one to an IP.
    /// `serverName` is the name the broker's certificate is expected to
    /// carry.
    ///
    /// TLS is not optional — QUIC has no unencrypted mode. Supply `caFile` to
    /// trust a specific CA (what a self-signed development broker needs); omit
    /// it to use the operating system's trust store.
    ///
    /// `offerAlpn` offers the `felix/1` ALPN. A broker with
    /// `FELIX_TLS_REQUIRE_ALPN=true` serves only clients that do; a broker
    /// older than ALPN support refuses them, which is why it is off by default.
    ///
    /// Pass `token`, a `tokenProvider`, or both. `tokenProvider` is called on
    /// the JS thread, lazily and one call at a time, and `token` is what the
    /// client starts with.
    ///
    /// `reconnectAttempts` (default 5, a whole number of at least 1) is how
    /// many times a call tries to reach a broker before it gives up.
    /// `reconnectDeadline` (milliseconds, default none) bounds one call across
    /// every attempt and wait. A deadline shorter than one attempt's own
    /// timeout leaves the call a single attempt. `reconnectBackoff` (default
    /// 200) is the ceiling of the first wait in milliseconds, and it doubles
    /// each attempt up to `reconnectMaxBackoff` (default 2000), which must not
    /// be below it. Each wait is a random fraction of its ceiling. All apply to
    /// `withIdentity` clients too.
    #[napi(
        ts_args_type = "addrs: string | string[], tenantId: string, token?: string | null, serverName?: string, caFile?: string, offerAlpn?: boolean, tokenProvider?: (signal: AbortSignal) => Promise<string>, reconnectAttempts?: number, reconnectDeadline?: number, reconnectBackoff?: number, reconnectMaxBackoff?: number",
        ts_return_type = "Promise<Client>"
    )]
    pub fn connect(
        env: Env,
        addrs: Either<String, Vec<String>>,
        tenant_id: String,
        token: Option<String>,
        server_name: Option<String>,
        ca_file: Option<String>,
        offer_alpn: Option<bool>,
        token_provider: Option<JsFunction>,
        reconnect_attempts: Option<f64>,
        reconnect_deadline: Option<f64>,
        reconnect_backoff: Option<f64>,
        reconnect_max_backoff: Option<f64>,
    ) -> Result<JsObject> {
        let policy = reconnect_policy(
            reconnect_attempts,
            reconnect_deadline,
            reconnect_backoff,
            reconnect_max_backoff,
        )?;
        let addrs = match addrs {
            Either::A(one) => vec![one],
            Either::B(many) => many,
        };
        let seeds = seeds(addrs)?;
        if seeds.is_empty() {
            return Err(invalid("at least one broker address is required"));
        }
        let quinn = tls::client_config(ca_file.as_deref(), offer_alpn.unwrap_or(false))?;
        let mut config = ClientConfig::optimized_defaults(quinn);
        config.auth_tenant_id = Some(tenant_id);
        match token_provider {
            Some(callback) => {
                let fetch = token::fetch_from(&env, &callback)?;
                config.token_provider = Some(token::provider(&fetch, token));
            }
            None => {
                config.auth_token = Some(
                    token
                        .filter(|token| !token.is_empty())
                        .ok_or_else(|| invalid("a token or a tokenProvider is required"))?,
                );
            }
        }

        let server_name = server_name.unwrap_or_else(|| "localhost".to_string());
        env.execute_tokio_future(
            async move {
                let inner =
                    felix_client::Client::connect_seeds(&seeds, &server_name, config, policy)
                        .await
                        .map_err(classify)?;
                Ok(inner)
            },
            |_, inner| {
                Ok(Client {
                    inner: std::sync::Mutex::new(Some(Arc::new(inner))),
                })
            },
        )
    }

    /// A client that acts as another user over this client's connections.
    ///
    /// Every stream it opens authenticates with `tenantId` and a token: the
    /// fixed `token`, or what `tokenProvider` returns (called as for
    /// `connect`; `token` then only seeds it). The broker checks its requests
    /// against that token's grants alone. It follows shards and failovers as
    /// this client does, reaching each broker over this client's connection to
    /// it. A refused token fails here.
    #[napi(
        ts_args_type = "tenantId: string, token?: string | null, tokenProvider?: (signal: AbortSignal) => Promise<string>",
        ts_return_type = "Promise<Client>"
    )]
    pub fn with_identity(
        &self,
        env: Env,
        tenant_id: String,
        token: Option<String>,
        token_provider: Option<JsFunction>,
    ) -> Result<JsObject> {
        let cluster = self.cluster()?;
        let tokens = match token_provider {
            Some(callback) => {
                let fetch = token::fetch_from(&env, &callback)?;
                Ok(token::provider(&fetch, token))
            }
            None => Err(token
                .filter(|token| !token.is_empty())
                .ok_or_else(|| invalid("a token or a tokenProvider is required"))?),
        };
        env.execute_tokio_future(
            async move {
                match tokens {
                    Ok(provider) => cluster.with_identity(tenant_id, provider).await,
                    Err(fixed) => cluster.with_identity_token(tenant_id, fixed).await,
                }
                .map_err(classify)
            },
            |_, identity| {
                Ok(Client {
                    inner: std::sync::Mutex::new(Some(Arc::new(identity))),
                })
            },
        )
    }

    /// Release the client. Idempotent.
    ///
    /// Later calls fail rather than quietly using a connection that was meant
    /// to be gone. Subscriptions already handed out hold their own connection
    /// and keep running — closing the client is not a way to stop them, and
    /// `close` on the subscription is.
    #[napi]
    pub fn close(&self) {
        self.inner
            .lock()
            .expect("the client lock is only ever held across a clone")
            .take();
    }

    #[napi(getter)]
    pub fn closed(&self) -> bool {
        self.inner
            .lock()
            .expect("the client lock is only ever held across a clone")
            .is_none()
    }

    /// Publish one record.
    ///
    /// `key` is the routing key, and it decides the shard. Without one every
    /// record lands on shard 0, so a multi-shard stream behaves like a
    /// single-shard one. Records sharing a key share a shard and stay ordered
    /// with respect to each other; records with different keys do not.
    ///
    /// `atLeastOnce` **may duplicate the record.** By default a publish that
    /// fails after the broker may already have written it is reported, not
    /// re-sent, because nothing downstream can tell the copies apart. With
    /// `atLeastOnce` it is re-sent to another broker instead: the record is
    /// then certain to land, and may land twice. That is a delivery guarantee
    /// the caller chooses, never one this client assumes.
    ///
    /// Resolves to the record's log offset, or `null` when the broker
    /// acknowledged before writing it, the stream has no log, the broker is
    /// too old to say, or `ack` is `"none"`.
    #[napi]
    pub async fn publish(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        payload: Buffer,
        key: Option<Buffer>,
        ack: Option<String>,
        at_least_once: Option<bool>,
    ) -> Result<Option<BigInt>> {
        let ack = parse_ack(ack.as_deref().unwrap_or("per_message"))?;
        let at_least_once = at_least_once.unwrap_or(false);
        let payload = payload.to_vec();
        let key = key.map(|k| Bytes::copy_from_slice(&k));
        let inner = self.cluster()?;
        match (key, at_least_once) {
            (Some(key), false) => {
                inner
                    .publish_keyed(&tenant_id, &namespace, &stream, payload, key, ack)
                    .await
            }
            (Some(key), true) => {
                inner
                    .publish_keyed_at_least_once(&tenant_id, &namespace, &stream, key, payload, ack)
                    .await
            }
            (None, true) => {
                inner
                    .publish_at_least_once(&tenant_id, &namespace, &stream, payload, ack)
                    .await
            }
            (None, false) => {
                inner
                    .publish(&tenant_id, &namespace, &stream, payload, ack)
                    .await
            }
        }
        .map(|offset| offset.map(BigInt::from))
        .map_err(classify)
    }

    /// Publish `payloads` (at least one) as one batch to the named shard,
    /// rather than the shard a key would hash to. For a producer that does its
    /// own partitioning. A shard the stream does not have is refused with
    /// `invalid_request` and nothing is written; a broker that does not
    /// advertise explicit shards rejects the call without sending anything.
    /// Resolves to the batch's first log offset, or `null` as `publish` does.
    #[napi]
    pub async fn publish_on_shard(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        payloads: Vec<Buffer>,
        ack: Option<String>,
    ) -> Result<Option<BigInt>> {
        let ack = parse_ack(ack.as_deref().unwrap_or("per_message"))?;
        if payloads.is_empty() {
            return Err(invalid("a batch needs at least one payload"));
        }
        let payloads = payloads.into_iter().map(|p| p.to_vec()).collect();
        self.cluster()?
            .publish_batch_on_shard(&tenant_id, &namespace, &stream, shard, payloads, ack)
            .await
            .map(|offset| offset.map(BigInt::from))
            .map_err(classify)
    }

    /// Ask the broker for a producer id and return a `Producer` that sends
    /// under it. Its publishes land once, however many times they are sent.
    /// Close it with `close()` or `await using`.
    #[napi]
    pub async fn producer(&self) -> Result<Producer> {
        let producer = self
            .cluster()?
            .idempotent_producer()
            .await
            .map_err(classify)?;
        Ok(Producer::new(producer))
    }

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

    /// Every broker this client would try, seeds included.
    #[napi]
    pub async fn endpoints(&self) -> Result<Vec<String>> {
        Ok(self
            .cluster()?
            .endpoints()
            .await
            .into_iter()
            .map(|addr| addr.to_string())
            .collect())
    }

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

    /// Publish `payloads` only if the shard's next offset is `expectedOffset`.
    /// `key` picks the shard as a plain publish does (none is shard 0). A
    /// refusal resolves with `written: false` and the shard's `tail`.
    #[napi]
    pub async fn publish_if(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        payloads: Vec<Buffer>,
        expected_offset: BigInt,
        key: Option<Buffer>,
    ) -> Result<crate::conditional::ConditionalWrite> {
        let payloads = payloads.into_iter().map(|p| p.to_vec()).collect();
        let expected = u64_of(expected_offset)?;
        let answer = self
            .cluster()?
            .publish_if(
                &tenant_id,
                &namespace,
                &stream,
                key.as_deref(),
                payloads,
                expected,
            )
            .await
            .map_err(classify)?;
        Ok(answer.into())
    }

    /// `publishIf`, to the named shard.
    #[napi]
    pub async fn publish_if_on_shard(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        payloads: Vec<Buffer>,
        expected_offset: BigInt,
    ) -> Result<crate::conditional::ConditionalWrite> {
        let payloads = payloads.into_iter().map(|p| p.to_vec()).collect();
        let expected = u64_of(expected_offset)?;
        let answer = self
            .cluster()?
            .publish_if_on_shard(&tenant_id, &namespace, &stream, shard, payloads, expected)
            .await
            .map_err(classify)?;
        Ok(answer.into())
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

    /// Claim up to `maxRecords` from a consumer group, waiting up to `waitMs`
    /// for one to appear.
    ///
    /// Each record stays claimed until acked or the visibility timeout lapses,
    /// at which point it is handed to someone else — which is why `attempts`
    /// is worth reading. An empty array is an answer, not a failure: the group
    /// is owed nothing right now.
    ///
    /// The wait is a long poll rather than a spin, because the caller would
    /// otherwise write the spin: a busy loop measures the loop, and a bare
    /// `setTimeout` between polls adds latency to a record that has arrived.
    ///
    /// `visibilityMs` sets how long this poll's claims stand, instead of the
    /// broker's visibility timeout. The broker caps it, and a broker that
    /// cannot honour it refuses the poll.
    #[napi]
    pub async fn group_poll(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        max_records: Option<u32>,
        wait_ms: Option<u32>,
        consumer: Option<String>,
        reclaim: Option<bool>,
        visibility_ms: Option<f64>,
    ) -> Result<Vec<GroupRecord>> {
        let max_records = max_records.unwrap_or(32);
        let wait_ms = wait_ms.unwrap_or(0);
        let options = felix_client::GroupPollOptions {
            member: consumer.map(|consumer| felix_client::GroupMember {
                consumer,
                reclaim: reclaim.unwrap_or(false),
            }),
            wait: std::time::Duration::from_millis(u64::from(wait_ms)),
            visibility: visibility_ms
                .map(|ms| duration_ms(ms, 1.0, "visibilityMs"))
                .transpose()?,
        };
        let records = self
            .cluster()?
            .group_poll_with(
                &tenant_id,
                &namespace,
                &stream,
                shard,
                &group,
                max_records,
                &options,
            )
            .await
            .map_err(classify)?;
        Ok(records
            .into_iter()
            .map(|r| GroupRecord {
                offset: BigInt::from(r.offset),
                payload: r.payload.to_vec().into(),
                attempts: r.attempts,
                skipped_before: BigInt::from(r.skipped_before),
            })
            .collect())
    }

    /// Finish a record: it will not be handed out again.
    #[napi]
    pub async fn group_ack(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: BigInt,
    ) -> Result<()> {
        let offset = u64_of(offset)?;
        self.cluster()?
            .group_ack(&tenant_id, &namespace, &stream, shard, &group, offset)
            .await
            .map_err(classify)
    }

    /// Return a record for redelivery without waiting out its timeout.
    /// Refused once its claim has lapsed or the record has gone out again.
    #[napi]
    pub async fn group_nack(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        record: GroupDelivery,
    ) -> Result<()> {
        let record = delivery(record)?;
        self.cluster()?
            .group_nack(&tenant_id, &namespace, &stream, shard, &group, &record)
            .await
            .map_err(classify)
    }

    /// Offsets this group gave up on after exhausting their attempts.
    #[napi]
    pub async fn group_dead_letters(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
    ) -> Result<Vec<BigInt>> {
        let offsets = self
            .cluster()?
            .group_dead_letters(&tenant_id, &namespace, &stream, shard, &group)
            .await
            .map_err(classify)?;
        Ok(offsets.into_iter().map(BigInt::from).collect())
    }

    /// Drop a dead-lettered record permanently.
    #[napi]
    pub async fn group_discard(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: BigInt,
    ) -> Result<()> {
        let offset = u64_of(offset)?;
        self.cluster()?
            .group_discard(&tenant_id, &namespace, &stream, shard, &group, offset)
            .await
            .map_err(classify)
    }

    /// Put a dead-lettered record back into the group for another attempt.
    #[napi]
    pub async fn group_redrive(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: BigInt,
    ) -> Result<()> {
        let offset = u64_of(offset)?;
        self.cluster()?
            .group_redrive(&tenant_id, &namespace, &stream, shard, &group, offset)
            .await
            .map_err(classify)
    }

    /// Keep the claim on `record` standing for `extendMs` from now. Resolves
    /// with how many milliseconds it now stands, which is less than asked when
    /// the broker capped it. Pass the record `groupPoll` returned: it names
    /// that delivery, and is refused once the claim has lapsed or the record
    /// has gone out again, even to this consumer.
    #[napi]
    pub async fn group_extend(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        record: GroupDelivery,
        extend_ms: f64,
    ) -> Result<f64> {
        let extend = duration_ms(extend_ms, 1.0, "extendMs")?;
        let record = delivery(record)?;
        let stands = self
            .cluster()?
            .group_extend(
                &tenant_id, &namespace, &stream, shard, &group, &record, extend,
            )
            .await
            .map_err(classify)?;
        Ok(stands.as_millis() as f64)
    }

    /// Move a group's cursor on one shard to `start`: `"latest"`,
    /// `"earliest"` or an offset. Records the group had handed out are void
    /// and delivered again from the new position; dead letters are kept.
    #[napi]
    pub async fn group_seek(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        start: Either<String, BigInt>,
    ) -> Result<GroupPosition> {
        let start = parse_start(Some(start))?;
        self.cluster()?
            .group_seek(&tenant_id, &namespace, &stream, shard, &group, start)
            .await
            .map(GroupPosition::from)
            .map_err(classify)
    }

    /// Create a group on one shard at `start`, so its first poll begins
    /// there. A group that already exists is left where it is and `moved` is
    /// false, so this is safe to call on every start.
    #[napi]
    pub async fn group_create(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        start: Either<String, BigInt>,
    ) -> Result<GroupPosition> {
        let start = parse_start(Some(start))?;
        self.cluster()?
            .group_create(&tenant_id, &namespace, &stream, shard, &group, start)
            .await
            .map(GroupPosition::from)
            .map_err(classify)
    }

    /// Where a group stands on one shard.
    #[napi]
    pub async fn group_describe(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
    ) -> Result<GroupInfo> {
        self.cluster()?
            .group_describe(&tenant_id, &namespace, &stream, shard, &group)
            .await
            .map(GroupInfo::from)
            .map_err(classify)
    }

    /// Delete a group on one shard: its cursor, its dead letters and the
    /// records it has handed out. Resolves with whether there was anything to
    /// delete.
    #[napi]
    pub async fn group_delete(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
    ) -> Result<bool> {
        self.cluster()?
            .group_delete(&tenant_id, &namespace, &stream, shard, &group)
            .await
            .map_err(classify)
    }

    /// Hand `record` back to be redelivered once `delayMs` has passed rather
    /// than at once, for a consumer that backs off before retrying. A delay
    /// of `0` is `groupNack`. The broker caps the delay.
    #[napi]
    pub async fn group_nack_after(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        record: GroupDelivery,
        delay_ms: f64,
    ) -> Result<()> {
        let delay = duration_ms(delay_ms, 0.0, "delayMs")?;
        let record = delivery(record)?;
        self.cluster()?
            .group_nack_after(
                &tenant_id, &namespace, &stream, shard, &group, &record, delay,
            )
            .await
            .map_err(classify)
    }

    /// Give up on one record: list it as a dead letter of the group and finish
    /// it, as the group does once a record runs out of attempts.
    /// `groupRedrive` puts it back.
    #[napi]
    pub async fn group_dead_letter(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: BigInt,
    ) -> Result<()> {
        let offset = u64_of(offset)?;
        self.cluster()?
            .group_dead_letter(&tenant_id, &namespace, &stream, shard, &group, offset)
            .await
            .map_err(classify)
    }

    /// `groupSeek` on every shard of the stream, in shard order. Each entry is
    /// that shard's position.
    #[napi]
    pub async fn group_seek_stream(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        group: String,
        start: Either<String, BigInt>,
    ) -> Result<Vec<GroupPosition>> {
        let start = parse_start(Some(start))?;
        let positions = self
            .cluster()?
            .group_seek_stream(&tenant_id, &namespace, &stream, &group, start)
            .await
            .map_err(classify)?;
        Ok(positions.into_iter().map(GroupPosition::from).collect())
    }

    /// `groupCreate` on every shard of the stream. A shard where the group
    /// already exists is left alone.
    #[napi]
    pub async fn group_create_stream(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        group: String,
        start: Either<String, BigInt>,
    ) -> Result<Vec<GroupPosition>> {
        let start = parse_start(Some(start))?;
        let positions = self
            .cluster()?
            .group_create_stream(&tenant_id, &namespace, &stream, &group, start)
            .await
            .map_err(classify)?;
        Ok(positions.into_iter().map(GroupPosition::from).collect())
    }

    /// `groupDescribe` for every shard of the stream, indexed by shard.
    #[napi]
    pub async fn group_describe_stream(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        group: String,
    ) -> Result<Vec<GroupInfo>> {
        let infos = self
            .cluster()?
            .group_describe_stream(&tenant_id, &namespace, &stream, &group)
            .await
            .map_err(classify)?;
        Ok(infos.into_iter().map(GroupInfo::from).collect())
    }

    /// `groupDelete` on every shard of the stream. Resolves with whether any
    /// shard had anything to delete.
    #[napi]
    pub async fn group_delete_stream(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        group: String,
    ) -> Result<bool> {
        self.cluster()?
            .group_delete_stream(&tenant_id, &namespace, &stream, &group)
            .await
            .map_err(classify)
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

    /// Publish `payloads` as one batch, to the shard `key` hashes to or to
    /// shard 0 without one. Records sharing a key share a shard, so the batch
    /// is one frame and keeps its order. Retries and failures are those of
    /// `publish`. Resolves to the first record's log offset, or `null` as
    /// `publish` does.
    #[napi]
    pub async fn publish_batch(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        payloads: Vec<Buffer>,
        key: Option<Buffer>,
        ack: Option<String>,
    ) -> Result<Option<BigInt>> {
        let ack = parse_ack(ack.as_deref().unwrap_or("per_message"))?;
        let payloads: Vec<Vec<u8>> = payloads.iter().map(|p| p.to_vec()).collect();
        let inner = self.cluster()?;
        match key {
            Some(key) => {
                inner
                    .publish_batch_keyed(
                        &tenant_id,
                        &namespace,
                        &stream,
                        Bytes::copy_from_slice(&key),
                        payloads,
                        ack,
                    )
                    .await
            }
            None => {
                inner
                    .publish_batch(&tenant_id, &namespace, &stream, payloads, ack)
                    .await
            }
        }
        .map(|offset| offset.map(BigInt::from))
        .map_err(classify)
    }

    /// Publish records that each carry a routing key, split by shard and sent
    /// to each shard's owner. A shard's records go as one batch, in the order
    /// given, accepted or refused as a unit; there is no order between shards.
    ///
    /// Failure is per shard and does not reject: the report says which
    /// records each piece held and why a piece failed, and a failed piece does
    /// not stop the others. Without `atLeastOnce` a piece that may have been
    /// written is not sent again. With it, such a piece is re-sent after the
    /// reconnect policy's backoff, so a record may land twice.
    #[napi]
    pub async fn publish_records(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        records: Vec<KeyedRecord>,
        ack: Option<String>,
        at_least_once: Option<bool>,
    ) -> Result<BatchReport> {
        let ack = parse_ack(ack.as_deref().unwrap_or("per_message"))?;
        let records: Vec<(Bytes, Vec<u8>)> = records
            .iter()
            .map(|r| (Bytes::copy_from_slice(&r.key), r.payload.to_vec()))
            .collect();
        let inner = self.cluster()?;
        let report = if at_least_once.unwrap_or(false) {
            inner
                .publish_records_at_least_once(&tenant_id, &namespace, &stream, records, ack)
                .await
        } else {
            inner
                .publish_records(&tenant_id, &namespace, &stream, records, ack)
                .await
        };
        Ok(BatchReport::from_report(report))
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

    /// A consumer group across every shard of a stream, for a consumer that
    /// wants the whole stream through one handle.
    #[napi]
    pub async fn group_sharded(
        &self,
        tenant_id: String,
        namespace: String,
        stream: String,
        group: String,
    ) -> Result<ShardedGroupHandle> {
        self.cluster()?
            .group_sharded(&tenant_id, &namespace, &stream, &group)
            .await
            .map(ShardedGroupHandle::new)
            .map_err(classify)
    }

    /// Ask the broker in use which brokers a client may connect to, and add
    /// them to the endpoints this client will try. Resolves with how many the
    /// cluster named, which may be zero. Rejects when the broker predates
    /// discovery.
    #[napi]
    pub async fn refresh_topology(&self) -> Result<u32> {
        let named = self.cluster()?.refresh_topology().await.map_err(classify)?;
        Ok(named as u32)
    }

    /// Live connections to each broker this client holds one for, as
    /// `[address, count]` pairs. A broker starts at one connection and gains
    /// more only when its streams are saturated.
    #[napi]
    pub async fn connections_per_node(&self) -> Result<Vec<ConnectionCount>> {
        Ok(self
            .cluster()?
            .connections_per_node()
            .await
            .into_iter()
            .map(|(addr, connections)| ConnectionCount {
                addr: addr.to_string(),
                connections: connections as u32,
            })
            .collect())
    }
}
