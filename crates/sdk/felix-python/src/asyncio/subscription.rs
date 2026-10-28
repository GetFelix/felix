//! A subscription consumed with `async for`.

use std::sync::Arc;

use felix_client::ClusterSubscription;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;

use crate::closable::Closable;
use crate::errors::to_py_err;
use crate::producer::timeout_of;
use crate::types::OwnedEvent;

/// A subscription consumed with `async for`.
#[pyclass(module = "felix", name = "AsyncSubscription")]
pub struct AsyncSubscription {
    inner: Arc<Closable<ClusterSubscription>>,
    /// Where the subscription began, as of when it opened.
    #[pyo3(get)]
    start_offset: Option<u64>,
    /// The offset live delivery began at, as of when it opened.
    #[pyo3(get)]
    live_offset: Option<u64>,
}

impl AsyncSubscription {
    pub(crate) fn new(subscription: ClusterSubscription) -> Self {
        Self {
            start_offset: subscription.start_offset(),
            live_offset: subscription.live_offset(),
            inner: Arc::new(Closable::new(subscription)),
        }
    }
}

#[pymethods]
impl AsyncSubscription {
    /// The next event, or `None` once the broker closes the stream.
    ///
    /// `timeout` (seconds) bounds the wait and yields `None` on expiry, which
    /// is *not* the same as the stream ending — check `closed` to tell them
    /// apart.
    #[pyo3(signature = (timeout=None))]
    fn next_event<'py>(
        &self,
        py: Python<'py>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let timeout = timeout_of(timeout)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let Some(mut guard) = inner.lock().await else {
                return Ok(None);
            };
            let Some(subscription) = guard.as_mut() else {
                return Ok(None);
            };
            let read = async {
                match timeout {
                    Some(duration) => {
                        tokio::time::timeout(duration, subscription.next_event()).await
                    }
                    None => Ok(subscription.next_event().await),
                }
            };
            let next = match inner.until_closed(read).await {
                Some(Ok(result)) => result.map_err(to_py_err)?,
                Some(Err(_)) | None => return Ok(None),
            };
            // The broker ended the stream. Released here so `closed` reads
            // true, which is how a caller tells this `None` from a timeout.
            if next.is_none() {
                guard.end();
            }
            Ok(next.map(OwnedEvent::from))
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.close();
            Ok(())
        })
    }

    #[getter]
    fn closed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(inner.is_closed()) })
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// `async for` ends when the broker closes the stream: the iterator raises
    /// `StopAsyncIteration` rather than yielding `None`, so a loop terminates
    /// the way Python expects instead of the caller testing for a sentinel.
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let Some(mut guard) = inner.lock().await else {
                return Err(PyStopAsyncIteration::new_err("subscription closed"));
            };
            let Some(subscription) = guard.as_mut() else {
                return Err(PyStopAsyncIteration::new_err("subscription closed"));
            };
            match inner.until_closed(subscription.next_event()).await {
                Some(result) => match result.map_err(to_py_err)? {
                    Some(event) => Ok(OwnedEvent::from(event)),
                    None => {
                        guard.end();
                        Err(PyStopAsyncIteration::new_err("stream ended"))
                    }
                },
                None => Err(PyStopAsyncIteration::new_err("subscription closed")),
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
