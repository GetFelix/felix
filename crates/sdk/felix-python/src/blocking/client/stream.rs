//! Reading a stream: subscriptions, ranged reads, offset lookup and retention.

use std::sync::Arc;

use pyo3::prelude::*;

use super::Client;
use crate::args::parse_start;
use crate::blocking::{ShardedSubscriptionHandle, SubscriptionHandle};
use crate::conditional::ConditionalTrim;
use crate::errors::to_py_err;
use crate::runtime::block_on;
use crate::stream_page::{OwnedStreamPage, StreamPage};

#[pymethods]
impl Client {
    /// Subscribe to a stream.
    ///
    /// `start` is `"latest"` (default), `"earliest"`, or an integer offset —
    /// the first record you have *not* seen, so a resuming client passes the
    /// offset it last handled plus one.
    ///
    /// A subscription reads **one shard**: `shard` (default 0). Use
    /// `subscribe_sharded` to read every shard of a multi-shard stream.
    #[pyo3(signature = (tenant_id, namespace, stream, *, start=None, shard=0))]
    fn subscribe(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<Py<PyAny>>,
        shard: u32,
    ) -> PyResult<SubscriptionHandle> {
        let start = parse_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let subscription = block_on(py, async move {
            inner
                .subscribe_shard(&tenant_id, &namespace, &stream, shard, start)
                .await
                .map_err(to_py_err)
        })?;
        Ok(SubscriptionHandle::new(subscription))
    }

    /// Hide the records of `shard` below `before`, only if the shard's next
    /// offset is exactly `expected_offset`.
    ///
    /// Afterwards a read below the floor raises `CursorError`. The trim is a
    /// record of its own and lands at `expected_offset`, so `before` can be at
    /// most that. `headers`, if given, are kept with the trim. A stale tail is
    /// a `ConditionalTrim` with `trimmed` false and the shard's `tail`.
    /// Needs `stream.trim`, and a durable stream without atomic commits.
    ///
    /// A broker that does not support head trims raises `TrimError` and is
    /// sent nothing. A trim that timed out raises `OutcomeUnknownError`: it
    /// may have landed, and sending it again then answers with the tail.
    #[pyo3(signature = (tenant_id, namespace, stream, shard, before, expected_offset, *, headers=None))]
    #[allow(clippy::too_many_arguments)]
    fn trim_before(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        before: u64,
        expected_offset: u64,
        headers: Option<&[u8]>,
    ) -> PyResult<ConditionalTrim> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let headers = headers.map(<[u8]>::to_vec);
        block_on(py, async move {
            inner
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
                .map(Into::into)
                .map_err(to_py_err)
        })
    }

    /// The first offset on one shard of a durable stream whose record was
    /// appended at or after `at_micros` (microseconds since the Unix epoch),
    /// or `None` when no committed record is that recent yet.
    ///
    /// Subscribe at the answer to replay from a time. A time older than every
    /// record answers with the oldest. Needs the `stream.subscribe`
    /// permission.
    fn offset_for_time(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        at_micros: u64,
    ) -> PyResult<Option<u64>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        block_on(py, async move {
            inner
                .offset_for_time(&tenant_id, &namespace, &stream, shard, at_micros)
                .await
                .map_err(to_py_err)
        })
    }

    /// One page of a durable stream shard's committed records, from
    /// `from_offset` and stopping before `end`, without subscribing.
    ///
    /// Read the next page from `next_offset`. `max_records` of 0 takes the
    /// broker's cap. A page that reaches what is committed comes back short
    /// or empty rather than waiting. A `from_offset` below what retention
    /// kept, or past the tail, raises `CursorError`. Needs the
    /// `stream.subscribe` permission.
    #[pyo3(signature = (tenant_id, namespace, stream, shard, from_offset, *, end=None, max_records=0))]
    fn read(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        from_offset: u64,
        end: Option<u64>,
        max_records: u32,
    ) -> PyResult<Py<StreamPage>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let page = block_on(py, async move {
            inner
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
                .map_err(to_py_err)
        })?;
        Ok(OwnedStreamPage(page).into_pyobject(py)?.unbind())
    }

    /// How many shards this stream was placed with.
    ///
    /// A subscription reads one shard, so consuming a whole stream means
    /// knowing how many there are. `0` means the broker knows nothing of the
    /// stream.
    fn stream_shards(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> PyResult<u32> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        block_on(py, async move {
            inner
                .stream_shards(&tenant_id, &namespace, &stream)
                .await
                .map_err(to_py_err)
        })
    }

    /// Subscribe to **every** shard of a stream and merge them.
    ///
    /// One subscription per shard underneath, each following its own shard's
    /// owner, because two shards of one stream can live on two brokers and an
    /// answer about one says nothing about the other.
    ///
    /// Ordering holds *within* a shard, not across them — records with the
    /// same routing key share a shard and stay ordered; unrelated records do
    /// not. A consumer that needs total order wants a single-shard stream.
    ///
    /// `resume` is a mapping of shard to the offset last handled; each listed
    /// shard resumes at `offset + 1`, and a shard not listed starts wherever
    /// `start` says.
    #[pyo3(signature = (tenant_id, namespace, stream, *, start=None, resume=None))]
    fn subscribe_sharded(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<Py<PyAny>>,
        resume: Option<std::collections::BTreeMap<u32, u64>>,
    ) -> PyResult<ShardedSubscriptionHandle> {
        let start = parse_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let subscription = block_on(py, async move {
            match resume {
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
            .map_err(to_py_err)
        })?;
        Ok(ShardedSubscriptionHandle::new(subscription))
    }
}
