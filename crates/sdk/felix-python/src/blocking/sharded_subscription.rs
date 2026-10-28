//! Every shard of a stream, read from a thread.

use std::sync::Arc;

use pyo3::prelude::*;

use crate::closable::Closable;
use crate::producer::timeout_of;
use crate::runtime::block_on;
use crate::types::next_shard_event;

/// Every shard of a stream, merged into one iterator.
///
/// Yields `ShardRecord`, and also `ShardLost` / `ShardRecovered` /
/// `ShardMoved` — a shard going away is surfaced rather than swallowed,
/// because the other shards carry on and a consumer that ignored it would be
/// reading part of the stream while believing it read all of it.
#[pyclass(module = "felix")]
pub struct ShardedSubscriptionHandle {
    inner: Arc<Closable<felix_client::ShardedSubscription>>,
    #[pyo3(get)]
    shards: u32,
}

impl ShardedSubscriptionHandle {
    pub(crate) fn new(subscription: felix_client::ShardedSubscription) -> Self {
        Self {
            shards: subscription.shards(),
            inner: Arc::new(Closable::new(subscription)),
        }
    }
}

#[pymethods]
impl ShardedSubscriptionHandle {
    /// The next event from any shard, or `None` once every shard has ended.
    #[pyo3(signature = (timeout=None))]
    fn next_event(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Py<PyAny>>> {
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
                        tokio::time::timeout(duration, next_shard_event(subscription))
                            .await
                            .ok()
                    }
                    None => Some(next_shard_event(subscription).await),
                }
            };
            // `None` here is a timeout or a close: both read as nothing.
            Ok(inner.until_closed(read).await.flatten().flatten())
        })?;
        event
            .map(|event| event.into_pyobject(py).map(|bound| bound.unbind()))
            .transpose()
    }

    /// The highest offset handled per shard, for resuming.
    ///
    /// Pass it back as `resume=` and each listed shard continues at
    /// `offset + 1`.
    fn positions(&self, py: Python<'_>) -> PyResult<std::collections::BTreeMap<u32, u64>> {
        let inner = Arc::clone(&self.inner);
        block_on(py, async move {
            let Some(guard) = inner.lock().await else {
                return Ok(Default::default());
            };
            Ok(guard
                .as_ref()
                .map(felix_client::ShardedSubscription::positions)
                .unwrap_or_default())
        })
    }

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
