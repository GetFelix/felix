//! A subscription read from a thread.

use std::sync::Arc;

use felix_client::ClusterSubscription;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::Mutex;

use crate::errors::to_py_err;
use crate::runtime::block_on;
use crate::types::Event;

/// A live subscription. Iterate it, or call `next_event()`.
#[pyclass(module = "felix")]
pub struct SubscriptionHandle {
    inner: Arc<Mutex<Option<ClusterSubscription>>>,
}

impl SubscriptionHandle {
    pub(crate) fn new(subscription: ClusterSubscription) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(subscription))),
        }
    }
}

#[pymethods]
impl SubscriptionHandle {
    /// The next event, or `None` once the broker closes the stream.
    ///
    /// Blocks until one arrives. Pass `timeout` (seconds) to bound the wait;
    /// a timeout returns `None`, which is *not* the same as the stream ending
    /// — check `closed` to tell them apart.
    #[pyo3(signature = (timeout=None))]
    fn next_event(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Event>> {
        let inner = Arc::clone(&self.inner);
        let event = block_on(py, async move {
            let mut guard = inner.lock().await;
            let Some(subscription) = guard.as_mut() else {
                return Ok(None);
            };
            let next = match timeout {
                Some(seconds) => {
                    let duration = std::time::Duration::from_secs_f64(seconds.max(0.0));
                    match tokio::time::timeout(duration, subscription.next_event()).await {
                        Ok(result) => result.map_err(to_py_err)?,
                        // A timeout is a "nothing yet", not an end: the
                        // subscription stays usable and the caller may ask
                        // again.
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
            Ok(next)
        })?;

        Ok(event.map(|event| Event {
            tenant_id: event.tenant_id.to_string(),
            namespace: event.namespace.to_string(),
            stream: event.stream.to_string(),
            payload: PyBytes::new(py, &event.payload).unbind(),
            offset: event.offset,
            skipped_before: event.skipped_before,
        }))
    }

    /// Stop receiving. Idempotent.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        block_on(py, async move {
            inner.lock().await.take();
            Ok(())
        })
    }

    #[getter]
    fn closed(&self, py: Python<'_>) -> PyResult<bool> {
        let inner = Arc::clone(&self.inner);
        block_on(py, async move { Ok(inner.lock().await.is_none()) })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Event>> {
        self.next_event(py, None)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }
}
