//! `AsyncClient`, the asyncio counterpart of the synchronous `Client`.

use std::sync::Arc;

use bytes::Bytes;
use felix_client::{ClientConfig, ClusterClient};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::{AsyncCacheWatch, AsyncProducer, AsyncShardedSubscription, AsyncSubscription};
use crate::args::{owned4, parse_ack, parse_addrs, parse_start};
use crate::conditional::{
    CacheCondition, CacheConditionResult, ConditionalWrite, OwnedVersionedValue,
};
use crate::errors::to_py_err;
use crate::producer::{Core, check_payloads};
use crate::token::{Flavor, Tokens, resolve};
use crate::types::{CacheWatchFilter, GroupRecord, OwnedGroupRecord};

/// A connection to a Felix cluster, for asyncio.
///
/// Construct with `await AsyncClient.connect(...)` rather than `__init__`:
/// connecting reaches the network, and a constructor that blocked an event
/// loop to do it would be the wrong shape.
#[pyclass(module = "felix", name = "AsyncClient")]
pub struct AsyncClient {
    inner: Arc<ClusterClient>,
}

#[pymethods]
impl AsyncClient {
    /// Connect to a cluster. Arguments are those of `Client(...)`, except that
    /// `token_provider` must be an `async def` function, or any callable
    /// returning an awaitable; one that returns a `str` fails with `AuthError`
    /// when first called. It is awaited on the event loop running this call,
    /// which must stay open for the life of the client and every
    /// `with_identity` handle; once it is closed the provider fails with "the
    /// event loop the token provider was bound to is closed".
    ///
    /// `reconnect_attempts` and `reconnect_deadline` are as for the
    /// synchronous `Client`.
    #[staticmethod]
    #[pyo3(signature = (
        addrs,
        *,
        tenant_id,
        token=None,
        token_provider=None,
        server_name="localhost",
        ca_file=None,
        offer_alpn=false,
        reconnect_attempts=None,
        reconnect_deadline=None,
    ))]
    fn connect<'py>(
        py: Python<'py>,
        addrs: Py<PyAny>,
        tenant_id: &str,
        token: Option<&str>,
        token_provider: Option<Py<PyAny>>,
        server_name: &str,
        ca_file: Option<&str>,
        offer_alpn: bool,
        reconnect_attempts: Option<Bound<'_, PyAny>>,
        reconnect_deadline: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let seeds = parse_addrs(py, &addrs)?;
        if seeds.is_empty() {
            return Err(PyValueError::new_err(
                "at least one broker address is required",
            ));
        }
        let policy =
            crate::args::reconnect_policy(reconnect_attempts.as_ref(), reconnect_deadline)?;
        let quinn = crate::tls::client_config(ca_file, offer_alpn)?;
        let mut config = ClientConfig::optimized_defaults(quinn);
        config.auth_tenant_id = Some(tenant_id.to_string());
        let flavor = Flavor::asyncio(py, token_provider.is_some())?;
        match resolve(py, token, token_provider, flavor)? {
            Tokens::Fixed(token) => config.auth_token = Some(token),
            Tokens::Provider(provider) => config.token_provider = Some(provider),
        }
        let server_name = server_name.to_string();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = ClusterClient::connect_seeds(&seeds, &server_name, config, policy)
                .await
                .map_err(to_py_err)?;
            Ok(AsyncClient {
                inner: Arc::new(inner),
            })
        })
    }

    /// Resolve to a client that acts as another user over this client's
    /// connections. See the synchronous `Client.with_identity`.
    #[pyo3(signature = (tenant_id, token=None, *, token_provider=None))]
    fn with_identity<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        token: Option<&str>,
        token_provider: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let flavor = Flavor::asyncio(py, token_provider.is_some())?;
        let tokens = resolve(py, token, token_provider, flavor)?;
        let inner = Arc::clone(&self.inner);
        let tenant_id = tenant_id.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let identity = match tokens {
                Tokens::Fixed(token) => inner.with_identity_token(tenant_id, token).await,
                Tokens::Provider(provider) => inner.with_identity(tenant_id, provider).await,
            }
            .map_err(to_py_err)?;
            Ok(AsyncClient {
                inner: Arc::new(identity),
            })
        })
    }

    /// Publish one record, resolving to its log offset or `None`. See the
    /// synchronous `Client.publish` for what `ack` and `at_least_once` mean,
    /// and when there is no offset.
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
    fn publish<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: &[u8],
        key: Option<&[u8]>,
        ack: &str,
        at_least_once: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ack = parse_ack(ack)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let payload = payload.to_vec();
        let key = key.map(Bytes::copy_from_slice);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match (key, at_least_once) {
                (Some(key), false) => {
                    inner
                        .publish_keyed(&tenant_id, &namespace, &stream, payload, key, ack)
                        .await
                }
                (Some(key), true) => {
                    inner
                        .publish_keyed_at_least_once(
                            &tenant_id, &namespace, &stream, key, payload, ack,
                        )
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

    /// Publish `payloads` (at least one) as one batch to the named shard. See
    /// the synchronous `Client.publish_on_shard`.
    #[pyo3(signature = (tenant_id, namespace, stream, shard, payloads, *, ack="per_message"))]
    fn publish_on_shard<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        payloads: Vec<Vec<u8>>,
        ack: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ack = parse_ack(ack)?;
        check_payloads(&payloads)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .publish_batch_on_shard(&tenant_id, &namespace, &stream, shard, payloads, ack)
                .await
                .map_err(to_py_err)
        })
    }

    /// Ask the broker for a producer id, awaiting an `AsyncProducer`. See the
    /// synchronous `Client.producer`.
    fn producer<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let producer = inner.idempotent_producer().await.map_err(to_py_err)?;
            Ok(AsyncProducer::new(Core::new(producer)))
        })
    }

    /// Subscribe to a stream, awaiting an `AsyncSubscription`.
    #[pyo3(signature = (tenant_id, namespace, stream, *, start=None))]
    fn subscribe<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let start = parse_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let subscription = inner
                .subscribe_from(&tenant_id, &namespace, &stream, start)
                .await
                .map_err(to_py_err)?;
            Ok(AsyncSubscription::new(subscription))
        })
    }

    #[pyo3(signature = (tenant_id, namespace, cache, key, value, *, ttl=None))]
    fn cache_put<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: &[u8],
        ttl: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        let value = Bytes::copy_from_slice(value);
        let ttl_ms = ttl.map(|seconds| (seconds.max(0.0) * 1000.0) as u64);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .cache_put(&tenant_id, &namespace, &cache, &key, value, ttl_ms)
                .await
                .map_err(to_py_err)
        })
    }

    fn cache_get<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let value = inner
                .cache_get(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)?;
            Ok(value.map(|bytes| bytes.to_vec()))
        })
    }

    /// Commit `ops` as one record on the shard `entity_key` routes to. See
    /// the synchronous `Client.commit`.
    fn commit<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        entity_key: &[u8],
        ops: Vec<PyRef<'py, crate::commit::CommitOp>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let ops = crate::commit::ops(ops);
        let (tenant_id, namespace, entity_key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            entity_key.to_vec(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let receipt = inner
                .commit(&tenant_id, &namespace, &entity_key, ops)
                .await
                .map_err(to_py_err)?;
            Ok(crate::commit::CommitReceipt {
                offset: receipt.offset,
            })
        })
    }

    /// `key` in the state of `stream`'s shard that `entity_key` routes to.
    fn state_get<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        entity_key: &[u8],
        key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            key.to_string(),
        );
        let entity_key = entity_key.to_vec();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let state = inner
                .state_get(&tenant_id, &namespace, &stream, &entity_key, &key)
                .await
                .map_err(to_py_err)?;
            Ok(crate::commit::OwnedStateValue(state))
        })
    }

    fn cache_delete<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let value = inner
                .cache_delete(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)?;
            Ok(value.map(|bytes| bytes.to_vec()))
        })
    }

    /// Read a value with its version, or `None`. See the synchronous
    /// `Client.cache_get_versioned`.
    fn cache_get_versioned<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = owned4(tenant_id, namespace, cache, key);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let entry = inner
                .cache_get_versioned(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)?;
            Ok(entry.map(OwnedVersionedValue))
        })
    }

    /// Store a value only if the key's entry meets `condition`. See the
    /// synchronous `Client.cache_put_if`; a lost answer is never resent.
    #[pyo3(signature = (tenant_id, namespace, cache, key, value, condition, *, ttl=None))]
    fn cache_put_if<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: &[u8],
        condition: PyRef<'py, CacheCondition>,
        ttl: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = owned4(tenant_id, namespace, cache, key);
        let value = Bytes::copy_from_slice(value);
        let condition = condition.inner;
        let ttl_ms = ttl.map(|seconds| (seconds.max(0.0) * 1000.0) as u64);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = inner
                .cache_put_if(
                    &tenant_id, &namespace, &cache, &key, value, ttl_ms, condition,
                )
                .await
                .map_err(to_py_err)?;
            Ok(CacheConditionResult::from(result))
        })
    }

    /// Remove a key only if its version is `version`.
    fn cache_delete_if<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        version: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = owned4(tenant_id, namespace, cache, key);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = inner
                .cache_delete_if(&tenant_id, &namespace, &cache, &key, version)
                .await
                .map_err(to_py_err)?;
            Ok(CacheConditionResult::from(result))
        })
    }

    /// Commit `ops` only if the shard's next offset is `expected_offset`.
    fn commit_if<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        entity_key: &[u8],
        ops: Vec<PyRef<'py, crate::commit::CommitOp>>,
        expected_offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let ops = crate::commit::ops(ops);
        let (tenant_id, namespace, entity_key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            entity_key.to_vec(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let answer = inner
                .commit_if(&tenant_id, &namespace, &entity_key, ops, expected_offset)
                .await
                .map_err(to_py_err)?;
            Ok(ConditionalWrite::from(answer))
        })
    }

    /// `commit`, to the named shard.
    fn commit_on_shard<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        shard: u32,
        entity_key: &[u8],
        ops: Vec<PyRef<'py, crate::commit::CommitOp>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let ops = crate::commit::ops(ops);
        let (tenant_id, namespace, entity_key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            entity_key.to_vec(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let receipt = inner
                .commit_on_shard(&tenant_id, &namespace, shard, &entity_key, ops)
                .await
                .map_err(to_py_err)?;
            Ok(crate::commit::CommitReceipt {
                offset: receipt.offset,
            })
        })
    }

    /// `commit_if`, to the named shard.
    fn commit_if_on_shard<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        shard: u32,
        entity_key: &[u8],
        ops: Vec<PyRef<'py, crate::commit::CommitOp>>,
        expected_offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let ops = crate::commit::ops(ops);
        let (tenant_id, namespace, entity_key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            entity_key.to_vec(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let answer = inner
                .commit_if_on_shard(
                    &tenant_id,
                    &namespace,
                    shard,
                    &entity_key,
                    ops,
                    expected_offset,
                )
                .await
                .map_err(to_py_err)?;
            Ok(ConditionalWrite::from(answer))
        })
    }

    /// `key` in the state of the named shard of `stream`, with no entity key.
    fn state_get_on_shard<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, key) = owned4(tenant_id, namespace, stream, key);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let state = inner
                .state_get_on_shard(&tenant_id, &namespace, &stream, shard, &key)
                .await
                .map_err(to_py_err)?;
            Ok(crate::commit::OwnedStateValue(state))
        })
    }

    /// Publish `payloads` only if the shard's next offset is
    /// `expected_offset`. See the synchronous `Client.publish_if`.
    #[pyo3(signature = (tenant_id, namespace, stream, payloads, expected_offset, *, key=None))]
    fn publish_if<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        expected_offset: u64,
        key: Option<&[u8]>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let key = key.map(<[u8]>::to_vec);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let answer = inner
                .publish_if(
                    &tenant_id,
                    &namespace,
                    &stream,
                    key.as_deref(),
                    payloads,
                    expected_offset,
                )
                .await
                .map_err(to_py_err)?;
            Ok(ConditionalWrite::from(answer))
        })
    }

    /// `publish_if`, to the named shard.
    fn publish_if_on_shard<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        payloads: Vec<Vec<u8>>,
        expected_offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let answer = inner
                .publish_if_on_shard(
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    payloads,
                    expected_offset,
                )
                .await
                .map_err(to_py_err)?;
            Ok(ConditionalWrite::from(answer))
        })
    }

    fn counter_add<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        delta: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .counter_add(&tenant_id, &namespace, &cache, &key, delta)
                .await
                .map_err(to_py_err)
        })
    }

    fn counter_get<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            key.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .counter_get(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)
        })
    }

    // ---- consumer groups -------------------------------------------------

    /// Take up to `max_records` for this group, waiting up to `wait` seconds.
    /// See the synchronous `Client.group_poll` for what the options mean.
    #[pyo3(signature = (tenant_id, namespace, stream, shard, group, *, max_records=32, wait=5.0, consumer=None, reclaim=false))]
    fn group_poll<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        max_records: u32,
        wait: f64,
        consumer: Option<String>,
        reclaim: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            group.to_string(),
        );
        let wait = std::time::Duration::from_secs_f64(wait.max(0.0));
        let member = consumer.map(|consumer| felix_client::GroupMember { consumer, reclaim });
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let records = inner
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
                .map_err(to_py_err)?;
            Ok(records
                .into_iter()
                .map(OwnedGroupRecord::from)
                .collect::<Vec<_>>())
        })
    }

    /// Finish one record.
    fn group_ack<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.settle(
            py,
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            offset,
            Settle::Ack,
        )
    }

    /// Hand one record back for immediate redelivery. Refused once its claim
    /// has lapsed or the record has gone out again.
    fn group_nack<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        record: PyRef<'_, GroupRecord>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let record = record.delivery();
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            group.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .group_nack(&tenant_id, &namespace, &stream, shard, &group, &record)
                .await
                .map_err(to_py_err)
        })
    }

    /// Offsets this group gave up on, lowest first.
    fn group_dead_letters<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            group.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .group_dead_letters(&tenant_id, &namespace, &stream, shard, &group)
                .await
                .map_err(to_py_err)
        })
    }

    /// Drop one dead letter without touching the record.
    fn group_discard<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.settle(
            py,
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            offset,
            Settle::Discard,
        )
    }

    /// Put one dead letter back in the queue, attempts reset.
    fn group_redrive<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.settle(
            py,
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            offset,
            Settle::Redrive,
        )
    }

    // ---- cache watch -----------------------------------------------------

    /// Watch a key or prefix for changes. See the synchronous
    /// `Client.watch_cache` for what `start` and `retained` mean.
    #[pyo3(signature = (tenant_id, namespace, cache, filter, *, start=None, retained=false))]
    fn watch_cache<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        filter: &CacheWatchFilter,
        start: Option<u64>,
        retained: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
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
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Through the cluster client, so the watch follows the redirect to
            // whichever broker owns the key's shard.
            let watch = if retained {
                inner
                    .watch_cache_retained(&tenant_id, &namespace, &cache, filter)
                    .await
            } else {
                inner
                    .watch_cache(&tenant_id, &namespace, &cache, filter, start)
                    .await
            }
            .map_err(to_py_err)?;
            Ok(AsyncCacheWatch::new(watch))
        })
    }

    // ---- multi-shard subscribe -------------------------------------------

    /// How many shards this stream was placed with.
    fn stream_shards<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .stream_shards(&tenant_id, &namespace, &stream)
                .await
                .map_err(to_py_err)
        })
    }

    /// Subscribe to every shard of a stream and merge them. See the
    /// synchronous `Client.subscribe_sharded` for the ordering caveat.
    #[pyo3(signature = (tenant_id, namespace, stream, *, start=None, resume=None))]
    fn subscribe_sharded<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<Py<PyAny>>,
        resume: Option<std::collections::BTreeMap<u32, u64>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let start = parse_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let subscription = match resume {
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
            .map_err(to_py_err)?;
            Ok(AsyncShardedSubscription::new(subscription))
        })
    }

    fn endpoints<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(inner
                .endpoints()
                .await
                .iter()
                .map(std::net::SocketAddr::to_string)
                .collect::<Vec<_>>())
        })
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let handle: Py<Self> = slf.into();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(handle) })
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(false) })
    }
}

impl AsyncClient {
    /// The four group settles differ only in which client method they call.
    #[allow(clippy::too_many_arguments)]
    fn settle<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
        what: Settle,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            group.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = &inner;
            match what {
                Settle::Ack => {
                    client
                        .group_ack(&tenant_id, &namespace, &stream, shard, &group, offset)
                        .await
                }
                Settle::Discard => {
                    client
                        .group_discard(&tenant_id, &namespace, &stream, shard, &group, offset)
                        .await
                }
                Settle::Redrive => {
                    client
                        .group_redrive(&tenant_id, &namespace, &stream, shard, &group, offset)
                        .await
                }
            }
            .map_err(to_py_err)
        })
    }
}

/// Which settle a group call is making. One helper serves all three because
/// they differ only in the client method they reach.
#[derive(Clone, Copy)]
enum Settle {
    Ack,
    Discard,
    Redrive,
}
