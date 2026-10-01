//! `AsyncClient`, the asyncio counterpart of the synchronous `Client`.

use std::sync::Arc;

use bytes::Bytes;
use felix_client::{ClientConfig, ClusterClient};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::{AsyncCacheWatch, AsyncShardedSubscription, AsyncSubscription};
use crate::args::{parse_ack, parse_addrs, parse_start};
use crate::errors::to_py_err;
use crate::types::{CacheWatchFilter, OwnedGroupRecord};

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
    #[staticmethod]
    #[pyo3(signature = (
        addrs,
        *,
        tenant_id,
        token,
        server_name="localhost",
        ca_file=None,
        offer_alpn=false,
    ))]
    fn connect<'py>(
        py: Python<'py>,
        addrs: Py<PyAny>,
        tenant_id: &str,
        token: &str,
        server_name: &str,
        ca_file: Option<&str>,
        offer_alpn: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let seeds = parse_addrs(py, &addrs)?;
        if seeds.is_empty() {
            return Err(PyValueError::new_err(
                "at least one broker address is required",
            ));
        }
        let quinn = crate::tls::client_config(ca_file, offer_alpn)?;
        let mut config = ClientConfig::optimized_defaults(quinn);
        config.auth_tenant_id = Some(tenant_id.to_string());
        config.auth_token = Some(token.to_string());
        let server_name = server_name.to_string();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = ClusterClient::connect(&seeds, &server_name, config)
                .await
                .map_err(to_py_err)?;
            Ok(AsyncClient {
                inner: Arc::new(inner),
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
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
            let client = inner.client().await;
            client
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
            let client = inner.client().await;
            let value = client
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
            let client = inner.client().await;
            let value = client
                .cache_delete(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)?;
            Ok(value.map(|bytes| bytes.to_vec()))
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
            let client = inner.client().await;
            client
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
            let client = inner.client().await;
            client
                .counter_get(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)
        })
    }

    // ---- consumer groups -------------------------------------------------

    /// Take up to `max_records` for this group, waiting up to `wait` seconds.
    /// See the synchronous `Client.group_poll` for what the options mean.
    #[pyo3(signature = (tenant_id, namespace, stream, shard, group, *, max_records=32, wait=5.0))]
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            group.to_string(),
        );
        let wait = std::time::Duration::from_secs_f64(wait.max(0.0));
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let records = inner
                .group_poll_wait(
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    &group,
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

    /// Hand one record back for immediate redelivery.
    fn group_nack<'py>(
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
            Settle::Nack,
        )
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
            let client = inner.client().await;
            client
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
                Settle::Nack => {
                    client
                        .group_nack(&tenant_id, &namespace, &stream, shard, &group, offset)
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

/// Which settle a group call is making. One helper serves all four because
/// they differ only in the client method they reach.
#[derive(Clone, Copy)]
enum Settle {
    Ack,
    Nack,
    Discard,
    Redrive,
}
