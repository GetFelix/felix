//! A cache watch read from a thread.

use std::sync::Arc;

use pyo3::prelude::*;

use crate::closable::Closable;
use crate::producer::timeout_of;
use crate::runtime::block_on;
use crate::types::OwnedWatchItem;
use crate::watch_progress::WatchProgress;

/// A live cache watch. Iterate it, or call `recv()`.
///
/// Yields `CacheChange` for each applied write and, if the watch falls behind,
/// a single `CacheWatchLagged` before ending. The lag is a value rather than
/// an exception because it is not a failure: the watch did its job by telling
/// you, and re-watching from `resume_from` is gapless.
#[pyclass(module = "felix")]
pub struct CacheWatchHandle {
    inner: Arc<Closable<felix_client::ClusterCacheWatch>>,
    progress: Arc<WatchProgress>,
    #[pyo3(get)]
    resume_offset: u64,
    #[pyo3(get)]
    resnapshot: bool,
    #[pyo3(get)]
    retained_count: Option<u64>,
}

impl CacheWatchHandle {
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
impl CacheWatchHandle {
    /// The next change, or `None` once the watch ends.
    #[pyo3(signature = (timeout=None))]
    fn recv(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Py<PyAny>>> {
        let inner = Arc::clone(&self.inner);
        let progress = Arc::clone(&self.progress);
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
            progress.record(watch);
            Ok(next.map(OwnedWatchItem::from))
        })?;
        item.map(|item| item.into_pyobject(py).map(|bound| bound.unbind()))
            .transpose()
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
