//! A cache watch consumed with `async for`.

use std::sync::Arc;

use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;

use crate::closable::Closable;
use crate::producer::timeout_of;
use crate::types::OwnedWatchItem;
use crate::watch_progress::WatchProgress;

/// A cache watch consumed with `async for`.
#[pyclass(module = "felix", name = "AsyncCacheWatch")]
pub struct AsyncCacheWatch {
    inner: Arc<Closable<felix_client::ClusterCacheWatch>>,
    progress: Arc<WatchProgress>,
    #[pyo3(get)]
    resume_offset: u64,
    #[pyo3(get)]
    resnapshot: bool,
    #[pyo3(get)]
    retained_count: Option<u64>,
}

impl AsyncCacheWatch {
    pub(crate) fn new(watch: felix_client::ClusterCacheWatch) -> Self {
        Self {
            resume_offset: watch.resume_offset(),
            resnapshot: watch.resnapshot(),
            retained_count: watch.retained_count(),
            progress: Arc::new(WatchProgress::of(&watch)),
            inner: Arc::new(Closable::new(watch)),
        }
    }
}

#[pymethods]
impl AsyncCacheWatch {
    #[pyo3(signature = (timeout=None))]
    fn recv<'py>(&self, py: Python<'py>, timeout: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let progress = Arc::clone(&self.progress);
        let timeout = timeout_of(timeout)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
            progress.record(watch);
            Ok(next.map(OwnedWatchItem::from))
        })
    }

    /// The `from_offset` that resumes this watch without a gap, counting
    /// everything handed out so far. Readable while a read is in flight.
    #[getter]
    fn resume_from(&self) -> u64 {
        self.progress.resume_from()
    }

    /// How many times this watch has followed its shard to a new owner.
    #[getter]
    fn moves(&self) -> u64 {
        self.progress.moves()
    }

    /// How many times this watch was reopened after losing its connection.
    #[getter]
    fn reconnects(&self) -> u64 {
        self.progress.reconnects()
    }

    /// Why the watch ended, when it could not be reopened on another broker
    /// within the reconnect deadline; otherwise `None`.
    #[getter]
    fn error(&self) -> Option<String> {
        self.progress.error()
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.close();
            Ok(())
        })
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let progress = Arc::clone(&self.progress);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let Some(mut guard) = inner.lock().await else {
                return Err(PyStopAsyncIteration::new_err("watch closed"));
            };
            let Some(watch) = guard.as_mut() else {
                return Err(PyStopAsyncIteration::new_err("watch closed"));
            };
            let next = inner.until_closed(watch.recv()).await;
            progress.record(watch);
            match next {
                Some(Some(item)) => Ok(OwnedWatchItem::from(item)),
                Some(None) => Err(PyStopAsyncIteration::new_err("watch ended")),
                None => Err(PyStopAsyncIteration::new_err("watch closed")),
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
            inner.close();
            Ok(false)
        })
    }
}
