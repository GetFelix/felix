//! A subscription read from a thread.

use std::sync::Arc;

use felix_client::ClusterSubscription;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::closable::Closable;
use crate::errors::to_py_err;
use crate::producer::timeout_of;
use crate::runtime::block_on;
use crate::types::Event;

/// A live subscription. Iterate it, or call `next_event()`.
#[pyclass(module = "felix")]
pub struct SubscriptionHandle {
    inner: Arc<Closable<ClusterSubscription>>,
    /// Where the subscription began, as of when it opened.
    #[pyo3(get)]
    start_offset: Option<u64>,
    /// The offset live delivery began at, as of when it opened.
    #[pyo3(get)]
    live_offset: Option<u64>,
}

impl SubscriptionHandle {
    pub(crate) fn new(subscription: ClusterSubscription) -> Self {
        Self {
            start_offset: subscription.start_offset(),
            live_offset: subscription.live_offset(),
            inner: Arc::new(Closable::new(subscription)),
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
        let timeout = timeout_of(timeout)?;
        let event = block_on(py, async move {
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
                    // Never expires.
                    None => Ok(subscription.next_event().await),
                }
            };
            let next = match inner.until_closed(read).await {
                Some(Ok(result)) => result.map_err(to_py_err)?,
                // A timeout is a "nothing yet", not an end: the subscription
                // stays usable and the caller may ask again. A close is the
                // reader's own end.
                Some(Err(_)) | None => return Ok(None),
            };
            // The broker ended the stream. Released here so `closed` reads
            // true, which is how a caller tells this `None` from a timeout.
            if next.is_none() {
                guard.end();
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

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Event>> {
        self.next_event(py, None)
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
