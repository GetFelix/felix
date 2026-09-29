//! Atomic commits: the op builder and the results both surfaces return.
//!
//! The checks (one event, one stream) are the Rust client's; this only
//! carries its types across. See `docs/atomic-commit.md`.

use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// One part of an atomic commit. Build with `CommitOp.publish`,
/// `CommitOp.enqueue`, `CommitOp.put` or `CommitOp.delete`.
#[pyclass(module = "felix", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct CommitOp {
    pub(crate) inner: felix_client::CommitOp,
}

#[pymethods]
impl CommitOp {
    /// The commit's event, for the stream's subscribers.
    #[staticmethod]
    fn publish(stream: &str, payload: &[u8]) -> Self {
        Self {
            inner: felix_client::CommitOp::publish(stream, payload.to_vec()),
        }
    }

    /// The commit's event, for the stream's consumer groups. The same record
    /// as `publish`: a queue is a stream read through a group.
    #[staticmethod]
    fn enqueue(queue: &str, payload: &[u8]) -> Self {
        Self {
            inner: felix_client::CommitOp::enqueue(queue, payload.to_vec()),
        }
    }

    /// Set `key` in the stream's state.
    #[staticmethod]
    fn put(stream: &str, key: &str, value: &[u8]) -> Self {
        Self {
            inner: felix_client::CommitOp::put(stream, key, value.to_vec()),
        }
    }

    /// Remove `key` from the stream's state.
    #[staticmethod]
    fn delete(stream: &str, key: &str) -> Self {
        Self {
            inner: felix_client::CommitOp::delete(stream, key),
        }
    }

    fn __repr__(&self) -> String {
        format!("CommitOp({:?})", self.inner)
    }
}

/// A commit the broker made durable.
#[pyclass(module = "felix", frozen, get_all)]
pub struct CommitReceipt {
    /// Where the commit's event is read, and the version of every key it
    /// wrote.
    pub offset: u64,
}

#[pymethods]
impl CommitReceipt {
    fn __repr__(&self) -> String {
        format!("CommitReceipt(offset={})", self.offset)
    }
}

/// A key in a stream shard's state.
#[pyclass(module = "felix", frozen, get_all)]
pub struct StateValue {
    /// `None` when the key was never written or was deleted.
    pub value: Option<Py<PyBytes>>,
    /// Offset of the commit that wrote `value`.
    pub version: Option<u64>,
    /// Offset of the last commit the answer reflects.
    pub as_of: Option<u64>,
}

#[pymethods]
impl StateValue {
    fn __repr__(&self) -> String {
        format!(
            "StateValue(version={:?}, as_of={:?}, present={})",
            self.version,
            self.as_of,
            self.value.is_some()
        )
    }
}

/// `felix_client::StateValue`, carried off the runtime thread.
pub(crate) struct OwnedStateValue(pub(crate) felix_client::StateValue);

impl<'py> IntoPyObject<'py> for OwnedStateValue {
    type Target = StateValue;
    type Output = Bound<'py, StateValue>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Bound::new(
            py,
            StateValue {
                value: self.0.value.map(|bytes| PyBytes::new(py, &bytes).unbind()),
                version: self.0.version,
                as_of: self.0.as_of,
            },
        )
    }
}

/// The ops a Python list names, in order.
pub(crate) fn ops(ops: Vec<PyRef<'_, CommitOp>>) -> Vec<felix_client::CommitOp> {
    ops.iter().map(|op| op.inner.clone()).collect()
}
