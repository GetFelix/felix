//! The asyncio idempotent producer.
//!
//! Cancelling an awaited call after the batch was handed over does not stop
//! it: the batch is still sent, so it is in doubt. Settle it by calling again
//! with the same payloads and key.

use std::sync::Arc;

use bytes::Bytes;
use pyo3::prelude::*;

use crate::errors::to_py_err;
use crate::producer::{Core, check_batches, check_payloads, timeout_of};

/// A producer whose publishes land once, however many times they are sent.
/// Made by `await client.producer()`; the methods are those of the
/// synchronous `Producer`, awaitable.
#[pyclass(module = "felix", name = "AsyncProducer")]
pub struct AsyncProducer {
    core: Arc<Core>,
}

impl AsyncProducer {
    pub(crate) fn new(core: Core) -> Self {
        Self {
            core: Arc::new(core),
        }
    }
}

#[pymethods]
impl AsyncProducer {
    #[pyo3(signature = (tenant_id, namespace, stream, payload, *, key=None))]
    fn publish<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: &[u8],
        key: Option<&[u8]>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.publish_batch(
            py,
            tenant_id,
            namespace,
            stream,
            vec![payload.to_vec()],
            key,
        )
    }

    #[pyo3(signature = (tenant_id, namespace, stream, payloads, *, key=None))]
    fn publish_batch<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        key: Option<&[u8]>,
    ) -> PyResult<Bound<'py, PyAny>> {
        check_payloads(&payloads)?;
        let core = Arc::clone(&self.core);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let key = key.map(Bytes::copy_from_slice);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core.publish_batch(&tenant_id, &namespace, &stream, key, payloads)
                .await
                .map_err(to_py_err)
        })
    }

    fn publish_batches<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        batches: Vec<Vec<Vec<u8>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        check_batches(&batches)?;
        let core = Arc::clone(&self.core);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core.publish_batches(&tenant_id, &namespace, &stream, batches)
                .await
                .map_err(to_py_err)
        })
    }

    fn abandon_in_doubt<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = Arc::clone(&self.core);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core.abandon_in_doubt(&tenant_id, &namespace, &stream)
                .await
                .map_err(to_py_err)
        })
    }

    /// The id the broker assigned this producer.
    #[getter]
    fn id(&self) -> u64 {
        self.core.id()
    }

    fn id_for<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = Arc::clone(&self.core);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core.id_for(&tenant_id, &namespace, &stream)
                .await
                .map_err(to_py_err)
        })
    }

    #[pyo3(signature = (timeout=None))]
    fn close<'py>(&self, py: Python<'py>, timeout: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
        let timeout = timeout_of(timeout)?;
        let core = Arc::clone(&self.core);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core.close(timeout).await.map_err(to_py_err)
        })
    }

    #[getter]
    fn closed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = Arc::clone(&self.core);
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(core.is_closed().await) })
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
        let core = Arc::clone(&self.core);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core.close(None).await.map_err(to_py_err)?;
            Ok(false)
        })
    }
}
