//! Conditional writes: the condition Python passes in and the typed answers
//! it gets back. A refusal is an answer, not an error; only a call the broker
//! could not take raises. See `docs/atomic-commit.md` and the cache page's
//! conditional writes section.

use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// What a conditional cache put requires of the key's current entry. Build
/// with `CacheCondition.absent()` or `CacheCondition.version(n)`.
#[pyclass(module = "felix", frozen, skip_from_py_object)]
#[derive(Clone, Copy)]
pub struct CacheCondition {
    pub(crate) inner: felix_client::CacheCondition,
}

#[pymethods]
impl CacheCondition {
    /// The key has no live entry: never written, deleted or expired. Takes a
    /// lease or claims an idempotency key.
    #[staticmethod]
    fn absent() -> Self {
        Self {
            inner: felix_client::CacheCondition::Absent,
        }
    }

    /// The key's live entry is at exactly this version, as
    /// `cache_get_versioned` or an earlier answer reported it.
    #[staticmethod]
    fn version(version: u64) -> Self {
        Self {
            inner: felix_client::CacheCondition::Version(version),
        }
    }

    fn __repr__(&self) -> String {
        match self.inner {
            felix_client::CacheCondition::Absent => "CacheCondition.absent()".to_string(),
            felix_client::CacheCondition::Version(v) => format!("CacheCondition.version({v})"),
        }
    }
}

/// The answer to `cache_put_if` or `cache_delete_if`.
#[pyclass(module = "felix", frozen, get_all)]
pub struct CacheConditionResult {
    /// The condition held and the write was made.
    pub applied: bool,
    /// After an applied put, the version it wrote. Otherwise the key's
    /// current version, `None` when it has no live entry.
    pub version: Option<u64>,
}

#[pymethods]
impl CacheConditionResult {
    fn __repr__(&self) -> String {
        format!(
            "CacheConditionResult(applied={}, version={:?})",
            self.applied, self.version
        )
    }
}

impl From<felix_client::CacheConditionResult> for CacheConditionResult {
    fn from(result: felix_client::CacheConditionResult) -> Self {
        Self {
            applied: result.applied,
            version: result.version,
        }
    }
}

/// A cache value and the version a conditional write compares against.
#[pyclass(module = "felix", frozen, get_all)]
pub struct VersionedValue {
    pub value: Py<PyBytes>,
    pub version: u64,
}

#[pymethods]
impl VersionedValue {
    fn __repr__(&self) -> String {
        format!("VersionedValue(version={})", self.version)
    }
}

/// `felix_client::VersionedValue`, carried off the runtime thread.
pub(crate) struct OwnedVersionedValue(pub(crate) felix_client::VersionedValue);

impl<'py> IntoPyObject<'py> for OwnedVersionedValue {
    type Target = VersionedValue;
    type Output = Bound<'py, VersionedValue>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Bound::new(
            py,
            VersionedValue {
                value: PyBytes::new(py, &self.0.value).unbind(),
                version: self.0.version,
            },
        )
    }
}

/// The answer to a conditional publish or commit. Nothing is written, and no
/// offset consumed, when `written` is false.
#[pyclass(module = "felix", frozen, get_all)]
pub struct ConditionalWrite {
    /// The write was made at the expected offset.
    pub written: bool,
    /// Where it was written; `None` when refused.
    pub offset: Option<u64>,
    /// The shard's next offset when refused; `None` when written.
    pub tail: Option<u64>,
}

#[pymethods]
impl ConditionalWrite {
    fn __repr__(&self) -> String {
        format!(
            "ConditionalWrite(written={}, offset={:?}, tail={:?})",
            self.written, self.offset, self.tail
        )
    }
}

impl From<felix_client::ConditionalWrite> for ConditionalWrite {
    fn from(answer: felix_client::ConditionalWrite) -> Self {
        match answer {
            felix_client::ConditionalWrite::Written { offset } => Self {
                written: true,
                offset: Some(offset),
                tail: None,
            },
            felix_client::ConditionalWrite::Refused { tail } => Self {
                written: false,
                offset: None,
                tail: Some(tail),
            },
        }
    }
}
