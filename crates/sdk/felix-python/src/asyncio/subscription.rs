//! A subscription consumed with `async for`.

use std::sync::Arc;

use felix_client::ClusterSubscription;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use tokio::sync::Mutex;

use crate::errors::to_py_err;
use crate::types::OwnedEvent;

/// A subscription consumed with `async for`.
#[pyclass(module = "felix", name = "AsyncSubscription")]
pub struct AsyncSubscription {
    inner: Arc<Mutex<Option<ClusterSubscription>>>,
}

impl AsyncSubscription {
    pub(crate) fn new(subscription: ClusterSubscription) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(subscription))),
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
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let Some(subscription) = guard.as_mut() else {
                return Ok(None);
            };
            let next = match timeout {
                Some(seconds) => {
                    let duration = std::time::Duration::from_secs_f64(seconds.max(0.0));
                    match tokio::time::timeout(duration, subscription.next_event()).await {
                        Ok(result) => result.map_err(to_py_err)?,
                        Err(_elapsed) => return Ok(None),
                    }
                }
                None => subscription.next_event().await.map_err(to_py_err)?,
            };
            // The broker ended the stream. Released here so `closed` reads
            // true, which is how a caller tells this `None` from a timeout.
            if next.is_none() {
                guard.take();
            }
            Ok(next.map(OwnedEvent::from))
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.take();
            Ok(())
        })
    }

    #[getter]
    fn closed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            async move { Ok(inner.lock().await.is_none()) },
        )
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
            let mut guard = inner.lock().await;
            let Some(subscription) = guard.as_mut() else {
                return Err(PyStopAsyncIteration::new_err("subscription closed"));
            };
            match subscription.next_event().await.map_err(to_py_err)? {
                Some(event) => Ok(OwnedEvent::from(event)),
                None => {
                    guard.take();
                    Err(PyStopAsyncIteration::new_err("stream ended"))
                }
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
