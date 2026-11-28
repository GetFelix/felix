//! Publishing: single records, batches, conditional publishes and the idempotent producer.

use std::sync::Arc;

use bytes::Bytes;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::AsyncClient;
use crate::args::parse_ack;
use crate::asyncio::AsyncProducer;
use crate::batch::OwnedBatchReport;
use crate::conditional::ConditionalWrite;
use crate::errors::to_py_err;
use crate::producer::{Core, check_payloads};

#[pymethods]
impl AsyncClient {
    /// Publish one record, resolving to its log offset or `None`. See the
    /// synchronous `Client.publish` for what `ack` and `at_least_once` mean,
    /// and when there is no offset.
    #[pyo3(signature = (
        tenant_id,
        namespace,
        stream,
        payload,
        *,
        key=None,
        ack="per_message",
        at_least_once=false,
        headers=None,
    ))]
    fn publish<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: &[u8],
        key: Option<&[u8]>,
        ack: &str,
        at_least_once: bool,
        headers: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ack = parse_ack(ack)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let payload = payload.to_vec();
        let key = key.map(Bytes::copy_from_slice);
        let headers = headers
            .filter(|h| !h.is_none())
            .map(|h| {
                if key.is_some() || at_least_once {
                    return Err(PyValueError::new_err(
                        "headers cannot be combined with key or at_least_once: put the \
                         record's own key in RecordHeaders(key=...)",
                    ));
                }
                crate::headers::extract(h)
            })
            .transpose()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(headers) = headers {
                return inner
                    .publish_with_headers(&tenant_id, &namespace, &stream, headers, payload, ack)
                    .await
                    .map_err(to_py_err);
            }
            match (key, at_least_once) {
                (Some(key), false) => {
                    inner
                        .publish_keyed(&tenant_id, &namespace, &stream, payload, key, ack)
                        .await
                }
                (Some(key), true) => {
                    inner
                        .publish_keyed_at_least_once(
                            &tenant_id, &namespace, &stream, key, payload, ack,
                        )
                        .await
                }
                (None, true) => {
                    inner
                        .publish_at_least_once(&tenant_id, &namespace, &stream, payload, ack)
                        .await
                }
                (None, false) => {
                    inner
                        .publish(&tenant_id, &namespace, &stream, payload, ack)
                        .await
                }
            }
            .map_err(to_py_err)
        })
    }

    /// Publish `payloads` (at least one) as one batch to the named shard. See
    /// the synchronous `Client.publish_on_shard`.
    #[pyo3(signature = (tenant_id, namespace, stream, shard, payloads, *, ack="per_message"))]
    fn publish_on_shard<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        payloads: Vec<Vec<u8>>,
        ack: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ack = parse_ack(ack)?;
        check_payloads(&payloads)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .publish_batch_on_shard(&tenant_id, &namespace, &stream, shard, payloads, ack)
                .await
                .map_err(to_py_err)
        })
    }

    /// Ask the broker for a producer id, awaiting an `AsyncProducer`. See the
    /// synchronous `Client.producer`.
    fn producer<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let producer = inner.idempotent_producer().await.map_err(to_py_err)?;
            Ok(AsyncProducer::new(Core::new(producer)))
        })
    }

    /// Publish `payloads` only if the shard's next offset is
    /// `expected_offset`. See the synchronous `Client.publish_if`.
    #[pyo3(signature = (tenant_id, namespace, stream, payloads, expected_offset, *, key=None))]
    fn publish_if<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        expected_offset: u64,
        key: Option<&[u8]>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let key = key.map(<[u8]>::to_vec);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let answer = inner
                .publish_if(
                    &tenant_id,
                    &namespace,
                    &stream,
                    key.as_deref(),
                    payloads,
                    expected_offset,
                )
                .await
                .map_err(to_py_err)?;
            Ok(ConditionalWrite::from(answer))
        })
    }

    /// `publish_if`, to the named shard.
    fn publish_if_on_shard<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        payloads: Vec<Vec<u8>>,
        expected_offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let answer = inner
                .publish_if_on_shard(
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    payloads,
                    expected_offset,
                )
                .await
                .map_err(to_py_err)?;
            Ok(ConditionalWrite::from(answer))
        })
    }

    /// Publish `payloads` as one batch. See the synchronous
    /// `Client.publish_batch`.
    #[pyo3(signature = (tenant_id, namespace, stream, payloads, *, key=None, ack="per_message", headers=None))]
    fn publish_batch<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        key: Option<&[u8]>,
        ack: &str,
        headers: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        check_payloads(&payloads)?;
        let ack = parse_ack(ack)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let key = key.map(Bytes::copy_from_slice);
        let records = headers
            .filter(|h| !h.is_none())
            .map(|h| {
                if key.is_some() {
                    return Err(PyValueError::new_err(
                        "headers cannot be combined with key on a batch: the batch's key routes it, \
                         and a record's own key goes in RecordHeaders(key=...)",
                    ));
                }
                crate::headers::extract_batch(h, payloads.len())
            })
            .transpose()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(blocks) = records {
                let records = blocks.into_iter().zip(payloads).collect();
                return inner
                    .publish_batch_with_headers(&tenant_id, &namespace, &stream, records, ack)
                    .await
                    .map_err(to_py_err);
            }
            match key {
                Some(key) => {
                    inner
                        .publish_batch_keyed(&tenant_id, &namespace, &stream, key, payloads, ack)
                        .await
                }
                None => {
                    inner
                        .publish_batch(&tenant_id, &namespace, &stream, payloads, ack)
                        .await
                }
            }
            .map_err(to_py_err)
        })
    }

    /// Publish `(key, payload)` records split by shard. See the synchronous
    /// `Client.publish_records`; resolves to a `BatchReport`.
    #[pyo3(signature = (tenant_id, namespace, stream, records, *, ack="per_message", at_least_once=false))]
    fn publish_records<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        records: Vec<(Vec<u8>, Vec<u8>)>,
        ack: &str,
        at_least_once: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ack = parse_ack(ack)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let records: Vec<(Bytes, Vec<u8>)> = records
            .into_iter()
            .map(|(key, payload)| (Bytes::from(key), payload))
            .collect();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let report = if at_least_once {
                inner
                    .publish_records_at_least_once(&tenant_id, &namespace, &stream, records, ack)
                    .await
            } else {
                inner
                    .publish_records(&tenant_id, &namespace, &stream, records, ack)
                    .await
            };
            Ok(OwnedBatchReport(report))
        })
    }
}
