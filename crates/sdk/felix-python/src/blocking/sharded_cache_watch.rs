//! A prefix watch across every shard of a cache, read from a thread.

use std::sync::Arc;

use pyo3::prelude::*;

use crate::closable::Closable;
use crate::producer::timeout_of;
use crate::runtime::block_on;
use crate::sharded_cache_watch::OwnedShardedWatchItem;

/// A prefix watch across every shard of a cache, merged into one iterator.
///
/// Yields `ShardedCacheChange` and, on a retained watch, `CacheStateComplete`
/// once every shard has delivered its state. A shard that falls behind, moves
/// or ends is an item (`CacheShardLagged`, `ShardMoved`, `CacheShardClosed`)
/// and not silence: the others carry on, and ignoring these means watching
/// part of the prefix. Order holds per key only.
#[pyclass(module = "felix")]
pub struct ShardedCacheWatchHandle {
    inner: Arc<Closable<felix_client::ShardedCacheWatch>>,
    #[pyo3(get)]
    shards: u32,
    #[pyo3(get)]
    retained_count: Option<u64>,
}

impl ShardedCacheWatchHandle {
    pub(crate) fn new(watch: felix_client::ShardedCacheWatch) -> Self {
        Self {
            shards: watch.shards(),
            retained_count: watch.retained_count(),
            inner: Arc::new(Closable::new(watch)),
        }
    }
}

#[pymethods]
impl ShardedCacheWatchHandle {
    /// The next item from any shard, or `None` once every shard has ended or
    /// `timeout` seconds passed.
    #[pyo3(signature = (timeout=None))]
    fn recv(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Py<PyAny>>> {
        let inner = Arc::clone(&self.inner);
        let timeout = timeout_of(timeout)?;
        let item = block_on(py, async move {
            let Some(mut guard) = inner.lock().await else {
                return Ok(None);
            };
            let Some(watch) = guard.as_mut() else {
                return Ok(None);
            };
            let read = async {
                match timeout {
                    Some(duration) => tokio::time::timeout(duration, watch.recv()).await.ok(),
                    None => Some(watch.recv().await),
                }
            };
            // `None` here is a timeout or a close: both read as nothing.
            let next = inner.until_closed(read).await.flatten().flatten();
            Ok(next.map(OwnedShardedWatchItem::from))
        })?;
        item.map(|item| item.into_pyobject(py).map(|bound| bound.unbind()))
            .transpose()
    }

    /// Where to resume each shard: pass it back as `resume=`. Every shard is
    /// listed, and one still delivering its state resumes at 0 so no key is
    /// missed.
    fn resume_offsets(&self, py: Python<'_>) -> PyResult<std::collections::BTreeMap<u32, u64>> {
        let inner = Arc::clone(&self.inner);
        block_on(py, async move {
            let Some(guard) = inner.lock().await else {
                return Ok(Default::default());
            };
            Ok(guard
                .as_ref()
                .map(felix_client::ShardedCacheWatch::resume_offsets)
                .unwrap_or_default())
        })
    }

    /// Stop watching. Idempotent, and returns even while another thread is
    /// blocked in `recv`, which then returns `None`.
    fn close(&self) {
        self.inner.close();
    }

    #[getter]
    fn closed(&self) -> bool {
        self.inner.is_closed()
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.recv(py, None)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        _py: Python<'_>,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        self.close();
        Ok(false)
    }
}
