//! A subscription read from a thread.

use std::sync::Arc;

use felix_client::ClusterSubscription;
use pyo3::prelude::*;

use crate::closable::Closable;
use crate::errors::to_py_err;
use crate::producer::timeout_of;
use crate::runtime::block_on;
use crate::types::OwnedEvent;
use crate::watch_progress::SubscriptionProgress;

/// A live subscription. Iterate it, or call `next_event()`.
#[pyclass(module = "felix")]
pub struct SubscriptionHandle {
    inner: Arc<Closable<ClusterSubscription>>,
    progress: Arc<SubscriptionProgress>,
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
            progress: Arc::new(SubscriptionProgress::of(&subscription)),
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
    fn next_event(
        &self,
        py: Python<'_>,
        timeout: Option<f64>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let inner = Arc::clone(&self.inner);
        let progress = Arc::clone(&self.progress);
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
            let outcome = inner.until_closed(read).await;
            progress.record(subscription);
            let next = match outcome {
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

        event
            .map(|event| OwnedEvent::from(event).into_pyobject(py).map(|b| b.into_any().unbind()))
            .transpose()
    }

    /// How many times this subscription has followed its shard to a new
    /// owner. Readable while a read is in flight.
    #[getter]
    fn moves(&self) -> u64 {
        self.progress.moves()
    }

    /// How many times this subscription was reopened after losing its
    /// connection.
    #[getter]
    fn reconnects(&self) -> u64 {
        self.progress.reconnects()
    }

    /// True once the broker said the stream was deleted. The subscription
    /// then ends rather than following it.
    #[getter]
    fn stream_deleted(&self) -> bool {
        self.progress.stream_deleted()
    }

    /// How many published batches the broker queues for this subscription,
    /// as granted; `None` when it did not say.
    #[getter]
    fn queue_capacity(&self) -> Option<u32> {
        self.progress.queue_capacity()
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

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
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
