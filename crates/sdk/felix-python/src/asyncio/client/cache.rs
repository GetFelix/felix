//! The cache: keyed reads and writes, conditional variants, watches and counters.

use std::sync::Arc;

use bytes::Bytes;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::AsyncClient;
use crate::args::{check_sharded_watch, owned4};
use crate::asyncio::{AsyncCacheWatch, AsyncShardedCacheWatch};
use crate::conditional::{
    CacheCondition, CacheConditionResult, OwnedLeaseWrite, OwnedVersionedValue,
};
use crate::errors::to_py_err;
use crate::inspect;
use crate::types::CacheWatchFilter;

#[pymethods]
impl AsyncClient {
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

    /// `cache_put_if` that also asks for a lease token. See the synchronous
    /// `Client.cache_put_if_lease`.
    #[pyo3(signature = (tenant_id, namespace, cache, key, value, condition, *, ttl=None))]
    fn cache_put_if_lease<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: &[u8],
        condition: PyRef<'_, CacheCondition>,
        ttl: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = owned4(tenant_id, namespace, cache, key);
        let value = Bytes::copy_from_slice(value);
        let condition = condition.inner;
        let ttl_ms = crate::args::ttl_millis(ttl)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .cache_put_if_lease(
                    &tenant_id, &namespace, &cache, &key, value, ttl_ms, condition,
                )
                .await
                .map(OwnedLeaseWrite)
                .map_err(to_py_err)
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

    /// How many shards a cache has.
    fn cache_shards<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inspect::cache_shards(&inner, &tenant_id, &namespace, &cache).await
        })
    }

    /// Watch a key prefix across every shard of a cache, awaiting an
    /// `AsyncShardedCacheWatch`. See the synchronous
    /// `Client.watch_cache_sharded`.
    #[pyo3(signature = (tenant_id, namespace, cache, prefix, *, resume=None, retained=false))]
    fn watch_cache_sharded<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        prefix: &str,
        resume: Option<std::collections::BTreeMap<u32, u64>>,
        retained: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        check_sharded_watch(retained, &resume)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, prefix) = owned4(tenant_id, namespace, cache, prefix);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let watch = if retained {
                inner
                    .watch_cache_sharded_retained(&tenant_id, &namespace, &cache, &prefix)
                    .await
            } else {
                inner
                    .watch_cache_sharded(&tenant_id, &namespace, &cache, &prefix, resume)
                    .await
            }
            .map_err(to_py_err)?;
            Ok(AsyncShardedCacheWatch::new(watch))
        })
    }

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
}
