//! A prefix watch across every shard of a cache, consumed with `async for`.

use std::sync::Arc;

use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use tokio::sync::Mutex;

use crate::sharded_cache_watch::OwnedShardedWatchItem;

/// A prefix watch across every shard of a cache, consumed with `async for`.
/// See `ShardedCacheWatchHandle` for the items.
#[pyclass(module = "felix", name = "AsyncShardedCacheWatch")]
pub struct AsyncShardedCacheWatch {
    inner: Arc<Mutex<Option<felix_client::ShardedCacheWatch>>>,
    #[pyo3(get)]
    shards: u32,
    #[pyo3(get)]
    retained_count: Option<u64>,
}

impl AsyncShardedCacheWatch {
    pub(crate) fn new(watch: felix_client::ShardedCacheWatch) -> Self {
        Self {
            shards: watch.shards(),
            retained_count: watch.retained_count(),
            inner: Arc::new(Mutex::new(Some(watch))),
        }
    }
}

#[pymethods]
impl AsyncShardedCacheWatch {
    #[pyo3(signature = (timeout=None))]
    fn recv<'py>(&self, py: Python<'py>, timeout: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let Some(watch) = guard.as_mut() else {
                return Ok(None);
            };
            let next = match timeout {
                Some(seconds) => {
                    let duration = std::time::Duration::from_secs_f64(seconds.max(0.0));
                    match tokio::time::timeout(duration, watch.recv()).await {
                        Ok(item) => item,
                        Err(_elapsed) => return Ok(None),
                    }
                }
                None => watch.recv().await,
            };
            Ok(next.map(OwnedShardedWatchItem::from))
        })
    }

    /// Where to resume each shard: pass it back as `resume=`.
    fn resume_offsets<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            Ok(guard
                .as_ref()
                .map(felix_client::ShardedCacheWatch::resume_offsets)
                .unwrap_or_default())
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.take();
            Ok(())
        })
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let Some(watch) = guard.as_mut() else {
                return Err(PyStopAsyncIteration::new_err("watch closed"));
            };
            match watch.recv().await {
                Some(item) => Ok(OwnedShardedWatchItem::from(item)),
                None => Err(PyStopAsyncIteration::new_err("watch ended")),
            }
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
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.take();
            Ok(false)
        })
    }
}
