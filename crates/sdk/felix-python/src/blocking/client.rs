//! The synchronous `Client`.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use felix_client::{ClientConfig, ClusterClient};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::{CacheWatchHandle, ShardedSubscriptionHandle, SubscriptionHandle};
use crate::args::{owned4, parse_ack, parse_addrs, parse_start};
use crate::errors::to_py_err;
use crate::runtime::block_on;
use crate::tls;
use crate::types::{CacheWatchFilter, GroupRecord, OwnedGroupRecord};

/// A connection to a Felix cluster.
///
/// Holds the seed addresses it was given plus every broker the cluster has
/// since advertised, and moves itself to a healthy broker when the one in use
/// goes away — the reconnect and redirect logic is the Rust client's, not a
/// reimplementation.
#[pyclass(module = "felix")]
pub struct Client {
    inner: Arc<ClusterClient>,
}

#[pymethods]
impl Client {
    /// Connect to a cluster.
    ///
    /// `addrs` is one `"host:port"` or a list of them; any reachable one is
    /// enough, and the client discovers the rest. `server_name` is the name
    /// the broker's certificate is expected to carry.
    ///
    /// TLS is not optional — QUIC has no unencrypted mode. Supply `ca_file`
    /// to trust a specific CA (what the demos and a self-signed development
    /// broker need); omit it to use the operating system's trust store.
    ///
    /// `offer_alpn=True` offers the `felix/1` ALPN. A broker with
    /// `FELIX_TLS_REQUIRE_ALPN=true` serves only clients that do; a broker
    /// older than ALPN support refuses them, which is why it is off by default.
    #[new]
    #[pyo3(signature = (
        addrs,
        *,
        tenant_id,
        token,
        server_name="localhost",
        ca_file=None,
        offer_alpn=false,
    ))]
    fn new(
        py: Python<'_>,
        addrs: Py<PyAny>,
        tenant_id: &str,
        token: &str,
        server_name: &str,
        ca_file: Option<&str>,
        offer_alpn: bool,
    ) -> PyResult<Self> {
        let seeds = parse_addrs(py, &addrs)?;
        if seeds.is_empty() {
            return Err(PyValueError::new_err(
                "at least one broker address is required",
            ));
        }
        let quinn = tls::client_config(ca_file, offer_alpn)?;
        let mut config = ClientConfig::optimized_defaults(quinn);
        config.auth_tenant_id = Some(tenant_id.to_string());
        config.auth_token = Some(token.to_string());

        let server_name = server_name.to_string();
        let inner = block_on(py, async move {
            ClusterClient::connect(&seeds, &server_name, config)
                .await
                .map_err(to_py_err)
        })?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Publish one record.
    ///
    /// `ack` selects what the broker must have done before this returns:
    /// `"none"` (fire and forget), `"per_message"`, or `"per_batch"`. With
    /// `at_least_once=True` a publish that fails because the broker went away
    /// is **re-sent** to its replacement, which can duplicate the record — use
    /// it only where consumers tolerate that.
    ///
    /// `key` is the routing key, and it decides the shard. Without one every
    /// record lands on shard 0, so a multi-shard stream behaves like a
    /// single-shard one. Records sharing a key share a shard and stay ordered
    /// with respect to each other; records with different keys do not.
    ///
    /// Returns the record's log offset, or `None` when the broker acknowledged
    /// before writing it, the stream has no log, the broker is too old to say,
    /// or `ack` is `"none"`.
    #[pyo3(signature = (
        tenant_id,
        namespace,
        stream,
        payload,
        *,
        key=None,
        ack="per_message",
        at_least_once=false,
    ))]
    fn publish(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: &[u8],
        key: Option<&[u8]>,
        ack: &str,
        at_least_once: bool,
    ) -> PyResult<Option<u64>> {
        let ack = parse_ack(ack)?;
        if key.is_some() && at_least_once {
            return Err(PyValueError::new_err(
                "at_least_once does not carry a routing key yet; publish the \
                 keyed record without it, or drop the key",
            ));
        }
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let payload = payload.to_vec();
        let key = key.map(Bytes::copy_from_slice);
        block_on(py, async move {
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
            .map_err(to_py_err)
        })
    }

    /// Subscribe to a stream.
    ///
    /// `start` is `"latest"` (default), `"earliest"`, or an integer offset —
    /// the first record you have *not* seen, so a resuming client passes the
    /// offset it last handled plus one.
    ///
    /// A subscription reads **one shard**. For a multi-shard stream this is
    /// shard 0; consuming every shard needs one subscription per shard, which
    /// this binding does not yet wrap.
    #[pyo3(signature = (tenant_id, namespace, stream, *, start=None))]
    fn subscribe(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<Py<PyAny>>,
    ) -> PyResult<SubscriptionHandle> {
        let start = parse_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let subscription = block_on(py, async move {
            inner
                .subscribe_from(&tenant_id, &namespace, &stream, start)
                .await
                .map_err(to_py_err)
        })?;
        Ok(SubscriptionHandle::new(subscription))
    }

    /// Store a value, optionally with a time-to-live in seconds.
    #[pyo3(signature = (tenant_id, namespace, cache, key, value, *, ttl=None))]
    fn cache_put(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: &[u8],
        ttl: Option<f64>,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        let value = Bytes::copy_from_slice(value);
        // The wire carries a TTL in milliseconds; seconds is what a Python
        // caller reaches for, so the conversion happens here rather than
        // leaking the unit into the signature.
        let ttl_ms = ttl.map(|seconds| (seconds.max(0.0) * 1000.0) as u64);
        block_on(py, async move {
            let client = inner.client().await;
            client
                .cache_put(&tenant_id, &namespace, &cache, &key, value, ttl_ms)
                .await
                .map_err(to_py_err)
        })
    }

    /// Read a value, or `None` if the key is absent or expired.
    fn cache_get(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> PyResult<Option<Py<PyBytes>>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        let value = block_on(py, async move {
            let client = inner.client().await;
            client
                .cache_get(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)
        })?;
        Ok(value.map(|bytes| PyBytes::new(py, &bytes).unbind()))
    }

    /// Commit `ops` as one record on the shard `entity_key` routes to.
    ///
    /// Every reader sees all of it or none of it. `ops` must carry exactly
    /// one event (`CommitOp.publish` or `CommitOp.enqueue`) and name one
    /// stream; otherwise `EventCountError` or `NotOnOwningShardError` is
    /// raised before anything is sent. Returns a `CommitReceipt` whose
    /// `offset` is the event's offset and the version of every key written.
    fn commit(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        entity_key: &[u8],
        ops: Vec<PyRef<'_, crate::commit::CommitOp>>,
    ) -> PyResult<crate::commit::CommitReceipt> {
        let inner = Arc::clone(&self.inner);
        let ops = crate::commit::ops(ops);
        let (tenant_id, namespace, entity_key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            entity_key.to_vec(),
        );
        let receipt = block_on(py, async move {
            inner
                .commit(&tenant_id, &namespace, &entity_key, ops)
                .await
                .map_err(to_py_err)
        })?;
        Ok(crate::commit::CommitReceipt {
            offset: receipt.offset,
        })
    }

    /// `key` in the state of `stream`'s shard that `entity_key` routes to,
    /// as a `StateValue` (value, version, as_of).
    fn state_get(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        entity_key: &[u8],
        key: &str,
    ) -> PyResult<Py<crate::commit::StateValue>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, key) = owned4(tenant_id, namespace, stream, key);
        let entity_key = entity_key.to_vec();
        let state = block_on(py, async move {
            inner
                .state_get(&tenant_id, &namespace, &stream, &entity_key, &key)
                .await
                .map_err(to_py_err)
        })?;
        Ok(crate::commit::OwnedStateValue(state)
            .into_pyobject(py)?
            .unbind())
    }

    /// Remove a key, returning the value it held, or `None` if it held none.
    fn cache_delete(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> PyResult<Option<Py<PyBytes>>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        let value = block_on(py, async move {
            let client = inner.client().await;
            client
                .cache_delete(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)
        })?;
        Ok(value.map(|bytes| PyBytes::new(py, &bytes).unbind()))
    }

    /// Apply a signed delta to a counter and return the sum *including* it.
    ///
    /// One round trip: incrementing and learning where you stand are the same
    /// call. Delivery is at least once — a retried add after a lost
    /// acknowledgement counts twice, so an application that cannot tolerate a
    /// double count keeps its own idempotency key.
    fn counter_add(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        delta: i64,
    ) -> PyResult<i64> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        block_on(py, async move {
            let client = inner.client().await;
            client
                .counter_add(&tenant_id, &namespace, &cache, &key, delta)
                .await
                .map_err(to_py_err)
        })
    }

    /// Read a counter. `None` means never written, which is not zero.
    fn counter_get(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> PyResult<Option<i64>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        block_on(py, async move {
            let client = inner.client().await;
            client
                .counter_get(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)
        })
    }

    // ---- consumer groups -------------------------------------------------
    //
    // A queue rather than a stream: records are *pulled*, because only the
    // consumer knows when it has capacity, and each one is claimed by one
    // member until it is settled or its visibility timeout lapses.

    /// Take up to `max_records` for this group, waiting up to `wait` seconds
    /// for work to appear.
    ///
    /// An empty list is an answer, not an error: the log has nothing unclaimed
    /// for this group right now. `wait=0` polls without blocking, which spins
    /// if you loop on it — prefer a few seconds so the broker holds the
    /// request open instead.
    #[pyo3(signature = (tenant_id, namespace, stream, shard, group, *, max_records=32, wait=5.0, consumer=None, reclaim=false))]
    fn group_poll(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        max_records: u32,
        wait: f64,
        consumer: Option<String>,
        reclaim: bool,
    ) -> PyResult<Vec<Py<GroupRecord>>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        let wait = std::time::Duration::from_secs_f64(wait.max(0.0));
        let member = consumer.map(|consumer| felix_client::GroupMember { consumer, reclaim });
        let records = block_on(py, async move {
            inner
                .group_poll_as(
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    &group,
                    member.as_ref(),
                    max_records,
                    wait,
                )
                .await
                .map_err(to_py_err)
        })?;
        records
            .into_iter()
            .map(|record| {
                OwnedGroupRecord::from(record)
                    .into_pyobject(py)
                    .map(Bound::unbind)
            })
            .collect()
    }

    /// Finish one record. Everything below the group's cursor stays finished.
    fn group_ack(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_ack(&tenant_id, &namespace, &stream, shard, &group, offset)
                .await
                .map_err(to_py_err)
        })
    }

    /// Hand one record back for immediate redelivery, rather than waiting out
    /// the visibility timeout.
    fn group_nack(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_nack(&tenant_id, &namespace, &stream, shard, &group, offset)
                .await
                .map_err(to_py_err)
        })
    }

    /// Offsets this group gave up on, lowest first.
    ///
    /// The records are still in the log at these offsets: this is a list of
    /// what to look at, not a copy of it.
    fn group_dead_letters(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> PyResult<Vec<u64>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_dead_letters(&tenant_id, &namespace, &stream, shard, &group)
                .await
                .map_err(to_py_err)
        })
    }

    /// Drop one dead letter, having decided the record is not worth
    /// reprocessing. Does not touch the record itself.
    fn group_discard(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_discard(&tenant_id, &namespace, &stream, shard, &group, offset)
                .await
                .map_err(to_py_err)
        })
    }

    /// Put one dead letter back in the queue with its attempt count reset.
    ///
    /// For when the reason it failed has been fixed. The group's cursor does
    /// not move backwards — the record is owed again, which is a different
    /// thing: everything the group finished stays finished.
    fn group_redrive(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_redrive(&tenant_id, &namespace, &stream, shard, &group, offset)
                .await
                .map_err(to_py_err)
        })
    }

    // ---- cache watch -----------------------------------------------------

    /// Watch a key or prefix for changes.
    ///
    /// `start` is the first cache-log offset you have *not* seen, so a
    /// resuming watcher passes the offset it last handled plus one. `None`
    /// means from now: live changes only.
    ///
    /// With `retained=True` the watch delivers each matching key's *current*
    /// value first and then live changes — join a room and immediately hold
    /// the roster. Mutually exclusive with `start`, whose replay already
    /// reconstructs the state that shortcuts.
    #[pyo3(signature = (tenant_id, namespace, cache, filter, *, start=None, retained=false))]
    fn watch_cache(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        filter: &CacheWatchFilter,
        start: Option<u64>,
        retained: bool,
    ) -> PyResult<CacheWatchHandle> {
        if retained && start.is_some() {
            return Err(PyValueError::new_err(
                "retained and start are mutually exclusive: a resume already \
                 replays the state a retained start shortcuts",
            ));
        }
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
        );
        let filter = filter.to_client();
        let watch = block_on(py, async move {
            // Through the cluster client, so the watch follows the redirect to
            // whichever broker owns the key's shard.
            if retained {
                inner
                    .watch_cache_retained(&tenant_id, &namespace, &cache, filter)
                    .await
            } else {
                inner
                    .watch_cache(&tenant_id, &namespace, &cache, filter, start)
                    .await
            }
            .map_err(to_py_err)
        })?;
        Ok(CacheWatchHandle::new(watch))
    }

    // ---- multi-shard subscribe -------------------------------------------

    /// How many shards this stream was placed with.
    ///
    /// A subscription reads one shard, so consuming a whole stream means
    /// knowing how many there are. `0` means the broker knows nothing of the
    /// stream.
    fn stream_shards(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> PyResult<u32> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        block_on(py, async move {
            let client = inner.client().await;
            client
                .stream_shards(&tenant_id, &namespace, &stream)
                .await
                .map_err(to_py_err)
        })
    }

    /// Subscribe to **every** shard of a stream and merge them.
    ///
    /// One subscription per shard underneath, each following its own shard's
    /// owner, because two shards of one stream can live on two brokers and an
    /// answer about one says nothing about the other.
    ///
    /// Ordering holds *within* a shard, not across them — records with the
    /// same routing key share a shard and stay ordered; unrelated records do
    /// not. A consumer that needs total order wants a single-shard stream.
    ///
    /// `resume` is a mapping of shard to the offset last handled; each listed
    /// shard resumes at `offset + 1`, and a shard not listed starts wherever
    /// `start` says.
    #[pyo3(signature = (tenant_id, namespace, stream, *, start=None, resume=None))]
    fn subscribe_sharded(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<Py<PyAny>>,
        resume: Option<std::collections::BTreeMap<u32, u64>>,
    ) -> PyResult<ShardedSubscriptionHandle> {
        let start = parse_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let subscription = block_on(py, async move {
            match resume {
                Some(positions) => {
                    inner
                        .resubscribe_sharded(&tenant_id, &namespace, &stream, positions, start)
                        .await
                }
                None => {
                    inner
                        .subscribe_sharded(&tenant_id, &namespace, &stream, start)
                        .await
                }
            }
            .map_err(to_py_err)
        })?;
        Ok(ShardedSubscriptionHandle::new(subscription))
    }

    /// Every broker this client would try, seeds included.
    fn endpoints(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let inner = Arc::clone(&self.inner);
        let addrs = block_on(py, async move { Ok(inner.endpoints().await) })?;
        Ok(addrs.iter().map(SocketAddr::to_string).collect())
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        "Client(...)".to_string()
    }
}
