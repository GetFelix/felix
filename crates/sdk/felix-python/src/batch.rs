//! `publish_records`' answer: how each shard's piece of a batch fared.

use pyo3::prelude::*;

use crate::errors::to_py_err;

/// How one shard's piece of a batch fared. A piece is all or nothing.
#[pyclass(module = "felix", frozen, get_all)]
pub struct ShardPublish {
    /// The shard the records resolve to.
    pub shard: u32,
    /// Positions, in the order passed, of the records in this piece.
    pub records: Vec<usize>,
    /// The log offset of the piece's first record when the broker reported
    /// one. `None` for a piece that failed, or on a stream without offsets.
    pub offset: Option<u64>,
    /// Why the piece was not acknowledged, or `None` when it was.
    pub error: Option<Py<PyAny>>,
}

#[pymethods]
impl ShardPublish {
    fn __repr__(&self) -> String {
        format!(
            "ShardPublish(shard={}, {} records, {})",
            self.shard,
            self.records.len(),
            if self.error.is_some() {
                "failed"
            } else {
                "accepted"
            }
        )
    }
}

/// What a batch publish did, shard by shard. Every input record is in exactly
/// one piece.
#[pyclass(module = "felix", frozen, get_all)]
pub struct BatchReport {
    /// One entry per shard, ascending by shard.
    pub shards: Vec<Py<ShardPublish>>,
}

#[pymethods]
impl BatchReport {
    /// Whether every piece was accepted.
    #[getter]
    fn ok(&self) -> bool {
        self.shards.iter().all(|piece| piece.get().error.is_none())
    }

    /// Positions of the records in failed pieces, ascending. Without
    /// `at_least_once` a failed record may still have landed.
    fn failed_records(&self, py: Python<'_>) -> Vec<usize> {
        let mut failed: Vec<usize> = self
            .shards
            .iter()
            .map(|piece| piece.bind(py).get())
            .filter(|piece| piece.error.is_some())
            .flat_map(|piece| piece.records.iter().copied())
            .collect();
        failed.sort_unstable();
        failed
    }

    fn __repr__(&self) -> String {
        format!("BatchReport({} pieces)", self.shards.len())
    }
}

/// A report carried off the runtime thread before there is a `Python<'_>`.
pub(crate) struct OwnedBatchReport(pub felix_client::BatchReport);

impl<'py> IntoPyObject<'py> for OwnedBatchReport {
    type Target = BatchReport;
    type Output = Bound<'py, BatchReport>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let shards = self
            .0
            .shards
            .into_iter()
            .map(|piece| {
                let (offset, error) = match piece.outcome {
                    Ok(offset) => (offset, None),
                    Err(err) => (None, Some(to_py_err(err).into_value(py).into_any())),
                };
                Bound::new(
                    py,
                    ShardPublish {
                        shard: piece.shard,
                        records: piece.records,
                        offset,
                        error,
                    },
                )
                .map(Bound::unbind)
            })
            .collect::<PyResult<Vec<_>>>()?;
        Bound::new(py, BatchReport { shards })
    }
}
