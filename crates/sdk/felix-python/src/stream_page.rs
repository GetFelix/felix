//! `read`'s typed answer: a page of committed records from one shard.

use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// One committed record from `read`.
#[pyclass(module = "felix", frozen, get_all)]
pub struct StreamRecord {
    pub offset: u64,
    pub payload: Py<PyBytes>,
    /// The principal that published the record, when the broker recorded one.
    pub publisher: Option<String>,
    /// When the record was appended, in microseconds since the Unix epoch.
    pub timestamp_micros: u64,
}

#[pymethods]
impl StreamRecord {
    fn __repr__(&self, py: Python<'_>) -> String {
        format!(
            "StreamRecord(offset={}, {} bytes)",
            self.offset,
            self.payload.bind(py).as_bytes().len()
        )
    }
}

/// One page from `read`.
#[pyclass(module = "felix", frozen, get_all)]
pub struct StreamPage {
    /// Committed records, in offset order.
    pub records: Vec<Py<StreamRecord>>,
    /// Where the next page starts. Offsets that hold no record are passed
    /// over, so this is not always the last offset plus one.
    pub next_offset: u64,
}

#[pymethods]
impl StreamPage {
    fn __repr__(&self) -> String {
        format!(
            "StreamPage({} records, next_offset={})",
            self.records.len(),
            self.next_offset
        )
    }
}

/// A page carried off the runtime thread before there is a `Python<'_>`.
pub(crate) struct OwnedStreamPage(pub felix_client::StreamPage);

impl<'py> IntoPyObject<'py> for OwnedStreamPage {
    type Target = StreamPage;
    type Output = Bound<'py, StreamPage>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let records = self
            .0
            .records
            .into_iter()
            .map(|record| {
                Bound::new(
                    py,
                    StreamRecord {
                        offset: record.offset,
                        payload: PyBytes::new(py, &record.payload).unbind(),
                        publisher: record.publisher,
                        timestamp_micros: record.timestamp_micros,
                    },
                )
                .map(Bound::unbind)
            })
            .collect::<PyResult<Vec<_>>>()?;
        Bound::new(
            py,
            StreamPage {
                records,
                next_offset: self.0.next_offset,
            },
        )
    }
}
