//! The cache: keyed reads and writes, conditional variants, watches and counters.

use std::sync::Arc;

use bytes::Bytes;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::Client;
use crate::args::{check_sharded_watch, owned4};
use crate::blocking::{CacheWatchHandle, ShardedCacheWatchHandle};
use crate::conditional::{
    CacheCondition, CacheConditionResult, LeaseWrite, OwnedLeaseWrite, OwnedVersionedValue,
    VersionedValue,
};
use crate::errors::to_py_err;
use crate::inspect;
use crate::runtime::block_on;
use crate::types::CacheWatchFilter;

#[pymethods]
impl Client {
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
            inner
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
            inner
                .cache_get(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)
        })?;
        Ok(value.map(|bytes| PyBytes::new(py, &bytes).unbind()))
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
            inner
                .cache_delete(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)
        })?;
        Ok(value.map(|bytes| PyBytes::new(py, &bytes).unbind()))
    }

    /// Read a value with its version, or `None` if the key is absent or
    /// expired. Pass the version to `cache_put_if` or `cache_delete_if`.
    fn cache_get_versioned(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> PyResult<Option<Py<VersionedValue>>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = owned4(tenant_id, namespace, cache, key);
        let entry = block_on(py, async move {
            inner
                .cache_get_versioned(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)
        })?;
        entry
            .map(|entry| Ok(OwnedVersionedValue(entry).into_pyobject(py)?.unbind()))
            .transpose()
    }

    /// Store a value only if the key's entry meets `condition`. A refusal is
    /// a `CacheConditionResult` with `applied` false and the key's current
    /// version, not an error.
    ///
    /// Never sent twice: a retry of a put that applied would be refused by the
    /// version it wrote, so a lost answer raises instead.
    #[pyo3(signature = (tenant_id, namespace, cache, key, value, condition, *, ttl=None))]
    fn cache_put_if(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: &[u8],
        condition: PyRef<'_, CacheCondition>,
        ttl: Option<f64>,
    ) -> PyResult<CacheConditionResult> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = owned4(tenant_id, namespace, cache, key);
        let value = Bytes::copy_from_slice(value);
        let condition = condition.inner;
        let ttl_ms = ttl.map(|seconds| (seconds.max(0.0) * 1000.0) as u64);
        block_on(py, async move {
            inner
                .cache_put_if(
                    &tenant_id, &namespace, &cache, &key, value, ttl_ms, condition,
                )
                .await
                .map(Into::into)
                .map_err(to_py_err)
        })
    }

    /// `cache_put_if` that also asks the broker for a lease token for the
    /// writer. Use it to take a lease: `CacheCondition.absent()` claims a key
    /// nobody holds, and the `LeaseWrite` carries the token to present with
    /// `with_identity(tenant_id, token=lease.token.token)` for what the
    /// broker's lease templates grant on that key. `token` is `None` when the
    /// condition was refused or the broker minted none; `token_refused` says
    /// why for an applied write. Never sent twice, like `cache_put_if`.
    #[pyo3(signature = (tenant_id, namespace, cache, key, value, condition, *, ttl=None))]
    fn cache_put_if_lease(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: &[u8],
        condition: PyRef<'_, CacheCondition>,
        ttl: Option<f64>,
    ) -> PyResult<Py<LeaseWrite>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = owned4(tenant_id, namespace, cache, key);
        let value = Bytes::copy_from_slice(value);
        let condition = condition.inner;
        let ttl_ms = crate::args::ttl_millis(ttl)?;
        let write = block_on(py, async move {
            inner
                .cache_put_if_lease(
                    &tenant_id, &namespace, &cache, &key, value, ttl_ms, condition,
                )
                .await
                .map_err(to_py_err)
        })?;
        Ok(OwnedLeaseWrite(write).into_pyobject(py)?.unbind())
    }

    /// Remove a key only if its version is `version`. A refusal is a
    /// `CacheConditionResult` with `applied` false and the key's current
    /// version.
    fn cache_delete_if(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        version: u64,
    ) -> PyResult<CacheConditionResult> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, key) = owned4(tenant_id, namespace, cache, key);
        block_on(py, async move {
            inner
                .cache_delete_if(&tenant_id, &namespace, &cache, &key, version)
                .await
                .map(Into::into)
                .map_err(to_py_err)
        })
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
            inner
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
            inner
                .counter_get(&tenant_id, &namespace, &cache, &key)
                .await
                .map_err(to_py_err)
        })
    }

    /// How many shards a cache has.
    fn cache_shards(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
    ) -> PyResult<u32> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
        );
        block_on(py, async move {
            inspect::cache_shards(&inner, &tenant_id, &namespace, &cache).await
        })
    }

    /// Watch a key prefix across **every** shard of a cache, merged into one
    /// handle. Keys sharing a prefix hash to different shards and a watch
    /// reads one, so this opens one prefix watch per shard.
    ///
    /// `resume` is a `resume_offsets()` result: a listed shard resumes at that
    /// offset and the rest watch from now. `retained=True` instead delivers
    /// each matching key's current value first, then `CacheStateComplete`; it
    /// is mutually exclusive with `resume`. Fails if any shard cannot be
    /// opened.
    #[pyo3(signature = (tenant_id, namespace, cache, prefix, *, resume=None, retained=false))]
    fn watch_cache_sharded(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        prefix: &str,
        resume: Option<std::collections::BTreeMap<u32, u64>>,
        retained: bool,
    ) -> PyResult<ShardedCacheWatchHandle> {
        check_sharded_watch(retained, &resume)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, cache, prefix) = owned4(tenant_id, namespace, cache, prefix);
        let watch = block_on(py, async move {
            if retained {
                inner
                    .watch_cache_sharded_retained(&tenant_id, &namespace, &cache, &prefix)
                    .await
            } else {
                inner
                    .watch_cache_sharded(&tenant_id, &namespace, &cache, &prefix, resume)
                    .await
            }
            .map_err(to_py_err)
        })?;
        Ok(ShardedCacheWatchHandle::new(watch))
    }

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
}
