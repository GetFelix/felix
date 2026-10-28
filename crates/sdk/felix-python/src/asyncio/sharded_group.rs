//! A consumer group over every shard of a stream, used with `await`.

use std::sync::Arc;

use pyo3::prelude::*;

use crate::args::{non_negative_duration, positive_duration};
use crate::errors::to_py_err;
use crate::sharded_group::{OwnedShardedGroupRecord, ShardedGroupRecord};

/// One consumer group across every shard of a stream. See
/// `ShardedGroupHandle` for what `poll` and the settles do.
#[pyclass(module = "felix", name = "AsyncShardedGroup")]
pub struct AsyncShardedGroup {
    inner: Arc<felix_client::ShardedGroup>,
    #[pyo3(get)]
    shards: u32,
}

impl AsyncShardedGroup {
    pub(crate) fn new(group: felix_client::ShardedGroup) -> Self {
        Self {
            shards: group.shards(),
            inner: Arc::new(group),
        }
    }
}

#[pymethods]
impl AsyncShardedGroup {
    #[pyo3(signature = (*, max_records=32))]
    fn poll<'py>(&self, py: Python<'py>, max_records: u32) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let records = inner.poll(max_records).await.map_err(to_py_err)?;
            Ok(records
                .into_iter()
                .map(OwnedShardedGroupRecord)
                .collect::<Vec<_>>())
        })
    }

    fn ack<'py>(
        &self,
        py: Python<'py>,
        record: PyRef<'_, ShardedGroupRecord>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let record = record.to_client();
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.ack(&record).await.map_err(to_py_err)
        })
    }

    fn nack<'py>(
        &self,
        py: Python<'py>,
        record: PyRef<'_, ShardedGroupRecord>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let record = record.to_client();
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.nack(&record).await.map_err(to_py_err)
        })
    }

    fn nack_after<'py>(
        &self,
        py: Python<'py>,
        record: PyRef<'_, ShardedGroupRecord>,
        delay: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let delay = non_negative_duration(delay, "delay")?;
        let record = record.to_client();
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.nack_after(&record, delay).await.map_err(to_py_err)
        })
    }

    fn extend<'py>(
        &self,
        py: Python<'py>,
        record: PyRef<'_, ShardedGroupRecord>,
        extend: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let extend = positive_duration(extend, "extend")?;
        let record = record.to_client();
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stands = inner.extend(&record, extend).await.map_err(to_py_err)?;
            Ok(stands.as_secs_f64())
        })
    }

    fn dead_letter<'py>(
        &self,
        py: Python<'py>,
        record: PyRef<'_, ShardedGroupRecord>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let record = record.to_client();
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.dead_letter(&record).await.map_err(to_py_err)
        })
    }
}
