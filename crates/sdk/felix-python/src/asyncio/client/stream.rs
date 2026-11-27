//! Reading a stream: subscriptions, ranged reads, offset lookup and retention.

use std::sync::Arc;

use pyo3::prelude::*;

use super::AsyncClient;
use crate::args::parse_start;
use crate::asyncio::{AsyncShardedSubscription, AsyncSubscription};
use crate::conditional::ConditionalTrim;
use crate::errors::to_py_err;
use crate::stream_page::OwnedStreamPage;

#[pymethods]
impl AsyncClient {
    /// Subscribe to a stream, awaiting an `AsyncSubscription`.
    #[pyo3(signature = (tenant_id, namespace, stream, *, start=None, shard=0))]
    fn subscribe<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<Py<PyAny>>,
        shard: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let start = parse_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let subscription = inner
                .subscribe_shard(&tenant_id, &namespace, &stream, shard, start)
                .await
                .map_err(to_py_err)?;
            Ok(AsyncSubscription::new(subscription))
        })
    }

    /// Hide the records of `shard` below `before`, only if the shard's next
    /// offset is `expected_offset`. See the synchronous `Client.trim_before`.
    #[pyo3(signature = (tenant_id, namespace, stream, shard, before, expected_offset, *, headers=None))]
    #[allow(clippy::too_many_arguments)]
    fn trim_before<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        before: u64,
        expected_offset: u64,
        headers: Option<&[u8]>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let headers = headers.map(<[u8]>::to_vec);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let answer = inner
                .trim_before(
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    before,
                    expected_offset,
                    headers.as_deref(),
                )
                .await
                .map_err(to_py_err)?;
            Ok(ConditionalTrim::from(answer))
        })
    }

    /// The first offset on one shard whose record was appended at or after
    /// `at_micros`, or `None`. See the synchronous `Client.offset_for_time`.
    fn offset_for_time<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        at_micros: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .offset_for_time(&tenant_id, &namespace, &stream, shard, at_micros)
                .await
                .map_err(to_py_err)
        })
    }

    /// One page of committed records from one shard. See the synchronous
    /// `Client.read`.
    #[pyo3(signature = (tenant_id, namespace, stream, shard, from_offset, *, end=None, max_records=0))]
    fn read<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        from_offset: u64,
        end: Option<u64>,
        max_records: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let page = inner
                .read(
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    from_offset,
                    end,
                    max_records,
                )
                .await
                .map_err(to_py_err)?;
            Ok(OwnedStreamPage(page))
        })
    }

    /// How many shards this stream was placed with.
    fn stream_shards<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .stream_shards(&tenant_id, &namespace, &stream)
                .await
                .map_err(to_py_err)
        })
    }

    /// Subscribe to every shard of a stream and merge them. See the
    /// synchronous `Client.subscribe_sharded` for the ordering caveat.
    #[pyo3(signature = (tenant_id, namespace, stream, *, start=None, resume=None))]
    fn subscribe_sharded<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<Py<PyAny>>,
        resume: Option<std::collections::BTreeMap<u32, u64>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let start = parse_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let subscription = match resume {
                Some(positions) => {
                    inner
                        .resubscribe_sharded(&tenant_id, &namespace, &stream, positions, start)
                        .await
                }
                None => {
                    inner
                        .subscribe_sharded(&tenant_id, &namespace, &stream, start)
                        .await
                }
            }
            .map_err(to_py_err)?;
            Ok(AsyncShardedSubscription::new(subscription))
        })
    }
}
