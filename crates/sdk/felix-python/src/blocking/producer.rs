//! The synchronous idempotent producer.

use std::sync::Arc;

use bytes::Bytes;
use pyo3::prelude::*;

use crate::errors::to_py_err;
use crate::producer::{Core, check_batches, check_payloads, timeout_of};
use crate::runtime::block_on;

/// A producer whose publishes land once, however many times they are sent.
/// Made by `Client.producer()`; see that method for the rules.
#[pyclass(module = "felix", name = "Producer")]
pub struct ProducerHandle {
    core: Arc<Core>,
}

impl ProducerHandle {
    pub(crate) fn new(core: Core) -> Self {
        Self {
            core: Arc::new(core),
        }
    }
}

#[pymethods]
impl ProducerHandle {
    /// Publish one payload, once. Returns its log offset, or `None` when the
    /// leader reported none. See `publish_batch`.
    #[pyo3(signature = (tenant_id, namespace, stream, payload, *, key=None))]
    fn publish(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: &[u8],
        key: Option<&[u8]>,
    ) -> PyResult<Option<u64>> {
        self.publish_batch(
            py,
            tenant_id,
            namespace,
            stream,
            vec![payload.to_vec()],
            key,
        )
    }

    /// Publish a batch, once. The batch takes one sequence whatever its size.
    ///
    /// Returns the log offset of the first record, from the ack that settled
    /// it. `key` picks the shard as it does for `Client.publish`; without one
    /// the batch goes to shard 0.
    ///
    /// Any error but `PublishRefusedError` leaves the batch in doubt: the
    /// sequence has not advanced and the batch may have landed. Call again with
    /// the same payloads and key to settle it; the leader answers a batch it
    /// holds without appending it. A different batch raises without being sent
    /// until then.
    #[pyo3(signature = (tenant_id, namespace, stream, payloads, *, key=None))]
    fn publish_batch(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        key: Option<&[u8]>,
    ) -> PyResult<Option<u64>> {
        check_payloads(&payloads)?;
        let core = Arc::clone(&self.core);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let key = key.map(Bytes::copy_from_slice);
        block_on(py, async move {
            core.publish_batch(&tenant_id, &namespace, &stream, key, payloads)
                .await
                .map_err(to_py_err)
        })
    }

    /// Publish several batches to shard 0, once each, under consecutive
    /// sequences. On a failure the batches already acknowledged stay
    /// acknowledged and the rest are in doubt together: call again with the
    /// same batches to send only those.
    fn publish_batches(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        batches: Vec<Vec<Vec<u8>>>,
    ) -> PyResult<()> {
        check_batches(&batches)?;
        let core = Arc::clone(&self.core);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        block_on(py, async move {
            core.publish_batches(&tenant_id, &namespace, &stream, batches)
                .await
                .map_err(to_py_err)
        })
    }

    /// Give up on the stream's batches in doubt and number the stream from 0
    /// under a fresh producer id. **A batch given up may land twice.**
    fn abandon_in_doubt(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> PyResult<()> {
        let core = Arc::clone(&self.core);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        block_on(py, async move {
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

    /// The id this producer's batches to `stream` go out under: `id`, unless
    /// the stream was given up with `abandon_in_doubt` or created again at
    /// another width.
    fn id_for(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> PyResult<u64> {
        let core = Arc::clone(&self.core);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        block_on(py, async move {
            core.id_for(&tenant_id, &namespace, &stream)
                .await
                .map_err(to_py_err)
        })
    }

    /// Wait for the batches already handed over, then stop. `timeout` is in
    /// seconds; without one it waits as long as it takes. Idempotent. On a
    /// timeout it raises and the producer stays closed, and a batch already
    /// sent may still land.
    #[pyo3(signature = (timeout=None))]
    fn close(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<()> {
        let timeout = timeout_of(timeout)?;
        let core = Arc::clone(&self.core);
        block_on(
            py,
            async move { core.close(timeout).await.map_err(to_py_err) },
        )
    }

    #[getter]
    fn closed(&self, py: Python<'_>) -> PyResult<bool> {
        let core = Arc::clone(&self.core);
        block_on(py, async move { Ok(core.is_closed().await) })
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
        self.close(py, None)?;
        Ok(false)
    }
}
