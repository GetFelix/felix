//! Consumer groups. A queue rather than a stream: records are *pulled*, because only
//! the consumer knows when it has capacity, and each one is claimed by one member
//! until it is settled or its visibility timeout lapses.

use std::sync::Arc;

use pyo3::prelude::*;

use super::AsyncClient;
use crate::args::{non_negative_duration, owned4, parse_required_start, positive_duration};
use crate::asyncio::AsyncShardedGroup;
use crate::errors::to_py_err;
use crate::group_admin::{GroupInfo, GroupPosition};
use crate::types::{GroupRecord, OwnedGroupRecord};

#[pymethods]
impl AsyncClient {
    /// Take up to `max_records` for this group, waiting up to `wait` seconds.
    /// See the synchronous `Client.group_poll` for what the options mean.
    #[pyo3(signature = (tenant_id, namespace, stream, shard, group, *, max_records=32, wait=5.0, consumer=None, reclaim=false, visibility=None))]
    fn group_poll<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        max_records: u32,
        wait: f64,
        consumer: Option<String>,
        reclaim: bool,
        visibility: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            group.to_string(),
        );
        let wait = non_negative_duration(wait, "wait")?;
        let options = felix_client::GroupPollOptions {
            member: consumer.map(|consumer| felix_client::GroupMember { consumer, reclaim }),
            wait,
            visibility: visibility
                .map(|seconds| positive_duration(seconds, "visibility"))
                .transpose()?,
        };
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let records = inner
                .group_poll_with(
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    &group,
                    max_records,
                    &options,
                )
                .await
                .map_err(to_py_err)?;
            Ok(records
                .into_iter()
                .map(OwnedGroupRecord::from)
                .collect::<Vec<_>>())
        })
    }

    /// Finish one record.
    fn group_ack<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.settle(
            py,
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            offset,
            Settle::Ack,
        )
    }

    /// Hand one record back for immediate redelivery. Refused once its claim
    /// has lapsed or the record has gone out again.
    fn group_nack<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        record: PyRef<'_, GroupRecord>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let record = record.delivery();
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            group.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .group_nack(&tenant_id, &namespace, &stream, shard, &group, &record)
                .await
                .map_err(to_py_err)
        })
    }

    /// Offsets this group gave up on, lowest first.
    fn group_dead_letters<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            group.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .group_dead_letters(&tenant_id, &namespace, &stream, shard, &group)
                .await
                .map_err(to_py_err)
        })
    }

    /// Drop one dead letter without touching the record.
    fn group_discard<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.settle(
            py,
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            offset,
            Settle::Discard,
        )
    }

    /// Put one dead letter back in the queue, attempts reset.
    fn group_redrive<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.settle(
            py,
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            offset,
            Settle::Redrive,
        )
    }

    /// Keep the claim on `record` standing for `extend` seconds from now. See
    /// the synchronous `Client.group_extend`; resolves to how many seconds it
    /// now stands.
    fn group_extend<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        record: PyRef<'_, GroupRecord>,
        extend: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let record = record.delivery();
        let extend = positive_duration(extend, "extend")?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stands = inner
                .group_extend(
                    &tenant_id, &namespace, &stream, shard, &group, &record, extend,
                )
                .await
                .map_err(to_py_err)?;
            Ok(stands.as_secs_f64())
        })
    }

    /// Move a group's cursor to `start`. See the synchronous
    /// `Client.group_seek`.
    fn group_seek<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        start: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let start = parse_required_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let position = inner
                .group_seek(&tenant_id, &namespace, &stream, shard, &group, start)
                .await
                .map_err(to_py_err)?;
            Ok(GroupPosition::from(position))
        })
    }

    /// Create a group at `start`, leaving an existing one where it is. See the
    /// synchronous `Client.group_create`.
    fn group_create<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        start: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let start = parse_required_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let position = inner
                .group_create(&tenant_id, &namespace, &stream, shard, &group, start)
                .await
                .map_err(to_py_err)?;
            Ok(GroupPosition::from(position))
        })
    }

    /// Where a group stands on one shard.
    fn group_describe<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let info = inner
                .group_describe(&tenant_id, &namespace, &stream, shard, &group)
                .await
                .map_err(to_py_err)?;
            Ok(GroupInfo::from(info))
        })
    }

    /// Delete a group on one shard. Resolves to whether there was anything to
    /// delete.
    fn group_delete<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .group_delete(&tenant_id, &namespace, &stream, shard, &group)
                .await
                .map_err(to_py_err)
        })
    }

    /// Give up on one record. See the synchronous `Client.group_dead_letter`.
    fn group_dead_letter<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .group_dead_letter(&tenant_id, &namespace, &stream, shard, &group, offset)
                .await
                .map_err(to_py_err)
        })
    }

    /// Hand `record` back to be redelivered after `delay` seconds. See the
    /// synchronous `Client.group_nack_after`.
    fn group_nack_after<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        record: PyRef<'_, GroupRecord>,
        delay: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let delay = non_negative_duration(delay, "delay")?;
        let record = record.delivery();
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .group_nack_after(
                    &tenant_id, &namespace, &stream, shard, &group, &record, delay,
                )
                .await
                .map_err(to_py_err)
        })
    }

    /// `group_seek` on every shard of the stream. See the synchronous
    /// `Client.group_seek_stream`.
    fn group_seek_stream<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        group: &str,
        start: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let start = parse_required_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let positions = inner
                .group_seek_stream(&tenant_id, &namespace, &stream, &group, start)
                .await
                .map_err(to_py_err)?;
            Ok(positions
                .into_iter()
                .map(GroupPosition::from)
                .collect::<Vec<_>>())
        })
    }

    /// `group_create` on every shard of the stream. See the synchronous
    /// `Client.group_create_stream`.
    fn group_create_stream<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        group: &str,
        start: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let start = parse_required_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let positions = inner
                .group_create_stream(&tenant_id, &namespace, &stream, &group, start)
                .await
                .map_err(to_py_err)?;
            Ok(positions
                .into_iter()
                .map(GroupPosition::from)
                .collect::<Vec<_>>())
        })
    }

    /// `group_describe` for every shard of the stream, indexed by shard.
    fn group_describe_stream<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        group: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let infos = inner
                .group_describe_stream(&tenant_id, &namespace, &stream, &group)
                .await
                .map_err(to_py_err)?;
            Ok(infos.into_iter().map(GroupInfo::from).collect::<Vec<_>>())
        })
    }

    /// `group_delete` on every shard of the stream. Resolves to whether any
    /// shard had anything to delete.
    fn group_delete_stream<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        group: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .group_delete_stream(&tenant_id, &namespace, &stream, &group)
                .await
                .map_err(to_py_err)
        })
    }

    /// One consumer group across every shard of a stream, awaiting an
    /// `AsyncShardedGroup`.
    fn group_sharded<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        group: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let sharded = inner
                .group_sharded(&tenant_id, &namespace, &stream, &group)
                .await
                .map_err(to_py_err)?;
            Ok(AsyncShardedGroup::new(sharded))
        })
    }
}

impl AsyncClient {
    /// The four group settles differ only in which client method they call.
    #[allow(clippy::too_many_arguments)]
    fn settle<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
        what: Settle,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            group.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = &inner;
            match what {
                Settle::Ack => {
                    client
                        .group_ack(&tenant_id, &namespace, &stream, shard, &group, offset)
                        .await
                }
                Settle::Discard => {
                    client
                        .group_discard(&tenant_id, &namespace, &stream, shard, &group, offset)
                        .await
                }
                Settle::Redrive => {
                    client
                        .group_redrive(&tenant_id, &namespace, &stream, shard, &group, offset)
                        .await
                }
            }
            .map_err(to_py_err)
        })
    }
}

/// Which settle a group call is making. One helper serves all three because
/// they differ only in the client method they reach.
#[derive(Clone, Copy)]
enum Settle {
    Ack,
    Discard,
    Redrive,
}
