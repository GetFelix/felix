//! `Client`, the connected cluster client every other handle comes from.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use felix_client::{ClientConfig, ClusterClient};
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::args::{parse_ack, parse_positions, parse_start, resolve, u64_of};
use crate::cache_watch::CacheWatchHandle;
use crate::errors::{classify, invalid};
use crate::sharded_subscription::ShardedSubscriptionHandle;
use crate::subscription::SubscriptionHandle;
use crate::tls;
use crate::types::GroupRecord;

/// A connected Felix client.
#[napi]
pub struct Client {
    /// `None` once closed. A plain `std::sync::Mutex` and not the async one:
    /// the lock is only ever held long enough to clone the `Arc`, so callers
    /// sharing a client never queue behind each other's in-flight calls.
    inner: std::sync::Mutex<Option<Arc<ClusterClient>>>,
}

impl Client {
    /// The cluster client, or an error if this one has been closed.
    fn cluster(&self) -> Result<Arc<ClusterClient>> {
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
    /// and the client discovers the rest. `serverName` is the name the
    /// broker's certificate is expected to carry.
    ///
    /// TLS is not optional — QUIC has no unencrypted mode. Supply `caFile` to
    /// trust a specific CA (what a self-signed development broker needs); omit
    /// it to use the operating system's trust store.
    ///
    /// `offerAlpn` offers the `felix/1` ALPN. A broker with
    /// `FELIX_TLS_REQUIRE_ALPN=true` serves only clients that do; a broker
    /// older than ALPN support refuses them, which is why it is off by default.
    #[napi(factory)]
    pub async fn connect(
        addrs: Either<String, Vec<String>>,
        tenant_id: String,
        token: String,
        server_name: Option<String>,
        ca_file: Option<String>,
        offer_alpn: Option<bool>,
    ) -> Result<Client> {
        let addrs = match addrs {
            Either::A(one) => vec![one],
            Either::B(many) => many,
        };
        let seeds = resolve(addrs)?;
        if seeds.is_empty() {
            return Err(invalid("at least one broker address is required"));
        }
        let quinn = tls::client_config(ca_file.as_deref(), offer_alpn.unwrap_or(false))?;
        let mut config = ClientConfig::optimized_defaults(quinn);
        config.auth_tenant_id = Some(tenant_id);
        config.auth_token = Some(token);

        let server_name = server_name.unwrap_or_else(|| "localhost".to_string());
        let inner = ClusterClient::connect(&seeds, &server_name, config)
            .await
            .map_err(classify)?;
        Ok(Client {
            inner: std::sync::Mutex::new(Some(Arc::new(inner))),
        })
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
        if key.is_some() && at_least_once {
            return Err(invalid(
                "atLeastOnce does not carry a routing key yet; publish the \
                 keyed record without it, or drop the key",
            ));
        }
        let inner = self.cluster()?;
        match (key, at_least_once) {
            (Some(key), _) => {
                inner
                    .publish_keyed(&tenant_id, &namespace, &stream, payload, key, ack)
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
    ) -> Result<SubscriptionHandle> {
        let start = parse_start(start)?;
        let inner = self.cluster()?;
        let subscription = inner
            .subscribe_from(&tenant_id, &namespace, &stream, Some(start))
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
        let client = self.cluster()?.client().await;
        client
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
        let client = self.cluster()?.client().await;
        client
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
        let client = self.cluster()?.client().await;
        let value = client
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
        let client = self.cluster()?.client().await;
        let previous = client
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
        let client = self.cluster()?.client().await;
        client
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
        let client = self.cluster()?.client().await;
        client
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
    ) -> Result<Vec<GroupRecord>> {
        let max_records = max_records.unwrap_or(32);
        let wait_ms = wait_ms.unwrap_or(0);
        let records = self
            .cluster()?
            .group_poll_wait(
                &tenant_id,
                &namespace,
                &stream,
                shard,
                &group,
                max_records,
                std::time::Duration::from_millis(u64::from(wait_ms)),
            )
            .await
            .map_err(classify)?;
        Ok(records
            .into_iter()
            .map(|r| GroupRecord {
                offset: BigInt::from(r.offset),
                payload: r.payload.to_vec().into(),
                attempts: r.attempts,
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
    #[napi]
    pub async fn group_nack(
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
            .group_nack(&tenant_id, &namespace, &stream, shard, &group, offset)
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
}
