//! A consumer group over every shard of a stream, used from a thread.

use std::sync::Arc;

use pyo3::prelude::*;

use crate::args::{non_negative_duration, positive_duration};
use crate::errors::to_py_err;
use crate::runtime::block_on;
use crate::sharded_group::{OwnedShardedGroupRecord, ShardedGroupRecord};

/// One consumer group across every shard of a stream.
///
/// `poll` visits the shards in turn and returns the first non-empty batch, so
/// none starves. Order holds per shard only. Settle a record through the
/// handle that polled it: the record carries its shard.
#[pyclass(module = "felix")]
pub struct ShardedGroupHandle {
    inner: Arc<felix_client::ShardedGroup>,
    #[pyo3(get)]
    shards: u32,
}

impl ShardedGroupHandle {
    pub(crate) fn new(group: felix_client::ShardedGroup) -> Self {
        Self {
            shards: group.shards(),
            inner: Arc::new(group),
        }
    }
}

#[pymethods]
impl ShardedGroupHandle {
    /// Take up to `max_records` from one shard, visiting shards in turn. An
    /// empty list means no shard had anything.
    #[pyo3(signature = (*, max_records=32))]
    fn poll(&self, py: Python<'_>, max_records: u32) -> PyResult<Vec<Py<ShardedGroupRecord>>> {
        let inner = Arc::clone(&self.inner);
        let records = block_on(py, async move {
            inner.poll(max_records).await.map_err(to_py_err)
        })?;
        records
            .into_iter()
            .map(|record| {
                OwnedShardedGroupRecord(record)
                    .into_pyobject(py)
                    .map(Bound::unbind)
            })
            .collect()
    }

    /// Finish a record, on the shard it was claimed from.
    fn ack(&self, py: Python<'_>, record: PyRef<'_, ShardedGroupRecord>) -> PyResult<()> {
        let record = record.to_client();
        let inner = Arc::clone(&self.inner);
        block_on(
            py,
            async move { inner.ack(&record).await.map_err(to_py_err) },
        )
    }

    /// Hand a record back to be redelivered at once.
    fn nack(&self, py: Python<'_>, record: PyRef<'_, ShardedGroupRecord>) -> PyResult<()> {
        let record = record.to_client();
        let inner = Arc::clone(&self.inner);
        block_on(
            py,
            async move { inner.nack(&record).await.map_err(to_py_err) },
        )
    }

    /// Hand a record back to be redelivered after `delay` seconds.
    fn nack_after(
        &self,
        py: Python<'_>,
        record: PyRef<'_, ShardedGroupRecord>,
        delay: f64,
    ) -> PyResult<()> {
        let delay = non_negative_duration(delay, "delay")?;
        let record = record.to_client();
        let inner = Arc::clone(&self.inner);
        block_on(py, async move {
            inner.nack_after(&record, delay).await.map_err(to_py_err)
        })
    }

    /// Keep a record's claim standing for `extend` seconds from now. Returns
    /// how many seconds it now stands.
    fn extend(
        &self,
        py: Python<'_>,
        record: PyRef<'_, ShardedGroupRecord>,
        extend: f64,
    ) -> PyResult<f64> {
        let extend = positive_duration(extend, "extend")?;
        let record = record.to_client();
        let inner = Arc::clone(&self.inner);
        let stands = block_on(py, async move {
            inner.extend(&record, extend).await.map_err(to_py_err)
        })?;
        Ok(stands.as_secs_f64())
    }

    /// Give up on a record: list it as a dead letter and finish it.
    fn dead_letter(&self, py: Python<'_>, record: PyRef<'_, ShardedGroupRecord>) -> PyResult<()> {
        let record = record.to_client();
        let inner = Arc::clone(&self.inner);
        block_on(py, async move {
            inner.dead_letter(&record).await.map_err(to_py_err)
        })
    }
}
