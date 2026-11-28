//! Publishing: single records, batches, conditional publishes and the idempotent producer.

use std::sync::Arc;

use bytes::Bytes;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::Client;
use crate::args::parse_ack;
use crate::batch::{BatchReport, OwnedBatchReport};
use crate::blocking::ProducerHandle;
use crate::conditional::ConditionalWrite;
use crate::errors::to_py_err;
use crate::producer::{Core, check_payloads};
use crate::runtime::block_on;

#[pymethods]
impl Client {
    /// Publish one record.
    ///
    /// `ack` selects what the broker must have done before this returns:
    /// `"none"` (fire and forget), `"per_message"`, or `"per_batch"`. With
    /// `at_least_once=True` a publish that fails because the broker went away
    /// is **re-sent** to its replacement, which can duplicate the record — use
    /// it only where consumers tolerate that.
    ///
    /// `key` is the routing key, and it decides the shard. Without one every
    /// record lands on shard 0, so a multi-shard stream behaves like a
    /// single-shard one. Records sharing a key share a shard and stay ordered
    /// with respect to each other; records with different keys do not.
    ///
    /// Returns the record's log offset, or `None` when the broker acknowledged
    /// before writing it, the stream has no log, the broker is too old to say,
    /// or `ack` is `"none"`.
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
    fn publish(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: &[u8],
        key: Option<&[u8]>,
        ack: &str,
        at_least_once: bool,
        headers: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Option<u64>> {
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
        block_on(py, async move {
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

    /// Publish `payloads` (at least one) as one batch to the named shard,
    /// rather than the shard a key would hash to. For a producer that does its
    /// own partitioning. A shard the stream does not have is refused with
    /// `invalid_request` and nothing is written; a broker that does not
    /// advertise explicit shards fails the call without sending anything.
    /// Returns the batch's first log offset, or `None` as `publish` does.
    #[pyo3(signature = (tenant_id, namespace, stream, shard, payloads, *, ack="per_message"))]
    fn publish_on_shard(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        payloads: Vec<Vec<u8>>,
        ack: &str,
    ) -> PyResult<Option<u64>> {
        let ack = parse_ack(ack)?;
        check_payloads(&payloads)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        block_on(py, async move {
            inner
                .publish_batch_on_shard(&tenant_id, &namespace, &stream, shard, payloads, ack)
                .await
                .map_err(to_py_err)
        })
    }

    /// Ask the broker for a producer id and return a `Producer` that sends
    /// under it. Its publishes land once, however many times they are sent.
    /// Close it with `close()` or a `with` block.
    fn producer(&self, py: Python<'_>) -> PyResult<ProducerHandle> {
        let inner = Arc::clone(&self.inner);
        let producer = block_on(py, async move {
            inner.idempotent_producer().await.map_err(to_py_err)
        })?;
        Ok(ProducerHandle::new(Core::new(producer)))
    }

    /// Publish `payloads` (at least one) only if the shard's next offset is
    /// `expected_offset`. `key` picks the shard as a plain publish does;
    /// without one the shard is 0. A refusal is a `ConditionalWrite` with
    /// `written` false and the shard's `tail`.
    #[pyo3(signature = (tenant_id, namespace, stream, payloads, expected_offset, *, key=None))]
    fn publish_if(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        expected_offset: u64,
        key: Option<&[u8]>,
    ) -> PyResult<ConditionalWrite> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let key = key.map(<[u8]>::to_vec);
        block_on(py, async move {
            inner
                .publish_if(
                    &tenant_id,
                    &namespace,
                    &stream,
                    key.as_deref(),
                    payloads,
                    expected_offset,
                )
                .await
                .map(Into::into)
                .map_err(to_py_err)
        })
    }

    /// `publish_if`, to the named shard.
    fn publish_if_on_shard(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        payloads: Vec<Vec<u8>>,
        expected_offset: u64,
    ) -> PyResult<ConditionalWrite> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        block_on(py, async move {
            inner
                .publish_if_on_shard(
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    payloads,
                    expected_offset,
                )
                .await
                .map(Into::into)
                .map_err(to_py_err)
        })
    }

    /// Publish `payloads` as one batch, all under `key` when given, else all
    /// to shard 0 as `publish` does. Records sharing a key share a shard, so
    /// the batch is one frame to its owner and keeps its order. Returns the
    /// first record's log offset, or `None` as `publish` does. An empty list raises
    /// `ValueError`.
    #[pyo3(signature = (tenant_id, namespace, stream, payloads, *, key=None, ack="per_message", headers=None))]
    fn publish_batch(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        key: Option<&[u8]>,
        ack: &str,
        headers: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Option<u64>> {
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
        block_on(py, async move {
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

    /// Publish `(key, payload)` records, splitting them by the shard each key
    /// hashes to and sending each shard's records to its owner as one batch.
    ///
    /// Order holds within a shard, not between shards. Failure is per shard:
    /// the `BatchReport` says which records each piece held, and a failed
    /// piece does not stop the others. A piece the broker may have written is
    /// not sent again unless `at_least_once` is set, which re-sends it and so
    /// can duplicate. This call does not raise for a failed piece; check
    /// `report.ok`.
    #[pyo3(signature = (tenant_id, namespace, stream, records, *, ack="per_message", at_least_once=false))]
    fn publish_records(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        records: Vec<(Vec<u8>, Vec<u8>)>,
        ack: &str,
        at_least_once: bool,
    ) -> PyResult<Py<BatchReport>> {
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
        let report = block_on(py, async move {
            Ok(if at_least_once {
                inner
                    .publish_records_at_least_once(&tenant_id, &namespace, &stream, records, ack)
                    .await
            } else {
                inner
                    .publish_records(&tenant_id, &namespace, &stream, records, ack)
                    .await
            })
        })?;
        Ok(OwnedBatchReport(report).into_pyobject(py)?.unbind())
    }
}
