//! Every shard of a stream, merged, consumed with `async for`.

use std::sync::Arc;

use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;

use crate::closable::Closable;
use crate::producer::timeout_of;
use crate::types::next_shard_event;

/// Every shard of a stream, merged, consumed with `async for`.
#[pyclass(module = "felix", name = "AsyncShardedSubscription")]
pub struct AsyncShardedSubscription {
    inner: Arc<Closable<felix_client::ShardedSubscription>>,
    #[pyo3(get)]
    shards: u32,
}

impl AsyncShardedSubscription {
    pub(crate) fn new(subscription: felix_client::ShardedSubscription) -> Self {
        Self {
            shards: subscription.shards(),
            inner: Arc::new(Closable::new(subscription)),
        }
    }
}

#[pymethods]
impl AsyncShardedSubscription {
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
                        tokio::time::timeout(duration, next_shard_event(subscription))
                            .await
                            .ok()
                    }
                    None => Some(next_shard_event(subscription).await),
                }
            };
            // `None` here is a timeout or a close: both read as nothing.
            Ok(inner.until_closed(read).await.flatten().flatten())
        })
    }

    /// The highest offset handled per shard, for resuming.
    fn positions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let Some(guard) = inner.lock().await else {
                return Ok(Default::default());
            };
            Ok(guard
                .as_ref()
                .map(felix_client::ShardedSubscription::positions)
                .unwrap_or_default())
        })
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
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let Some(mut guard) = inner.lock().await else {
                return Err(PyStopAsyncIteration::new_err("subscription closed"));
            };
            let Some(subscription) = guard.as_mut() else {
                return Err(PyStopAsyncIteration::new_err("subscription closed"));
            };
            match inner.until_closed(next_shard_event(subscription)).await {
                Some(Some(event)) => Ok(event),
                Some(None) => Err(PyStopAsyncIteration::new_err("every shard ended")),
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
