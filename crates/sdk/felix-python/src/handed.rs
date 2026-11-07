//! A producer call that is queued but not yet answered.
//!
//! `hand_off_*` returns once the call is queued for the producer's task, so a
//! caller can issue calls in order without waiting for each answer.
//! Concurrent `publish_batch` calls do not promise that order. Dropping a
//! `Handed` does not stop the call: it is sent either way.

use std::sync::{Arc, Mutex};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::errors::to_py_err;
use crate::runtime::block_on;

/// The queued call, taken by the one read that consumes it.
pub(crate) struct Slot {
    handed: Mutex<Option<felix_client::Handed>>,
}

impl Slot {
    pub(crate) fn new(handed: felix_client::Handed) -> Arc<Self> {
        Arc::new(Self {
            handed: Mutex::new(Some(handed)),
        })
    }

    fn take(&self) -> PyResult<felix_client::Handed> {
        self.handed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| {
                PyRuntimeError::new_err(
                    "this call's answer was already read; offset() and done() work once between them",
                )
            })
    }

    async fn offset(self: Arc<Self>) -> PyResult<Option<u64>> {
        self.take()?.offset().await.map_err(to_py_err)
    }

    async fn done(self: Arc<Self>) -> PyResult<()> {
        self.take()?.done().await.map_err(to_py_err)
    }
}

/// A call handed to the producer and not yet answered. `offset()` returns the
/// first record's log offset (`None` when the leader reported none) and
/// `done()` only waits for success; either raises what the call raised. One
/// of them works, once.
#[pyclass(module = "felix", name = "Handed")]
pub struct Handed {
    slot: Arc<Slot>,
}

impl Handed {
    pub(crate) fn new(slot: Arc<Slot>) -> Self {
        Self { slot }
    }
}

#[pymethods]
impl Handed {
    fn offset(&self, py: Python<'_>) -> PyResult<Option<u64>> {
        let slot = Arc::clone(&self.slot);
        block_on(py, slot.offset())
    }

    fn done(&self, py: Python<'_>) -> PyResult<()> {
        let slot = Arc::clone(&self.slot);
        block_on(py, slot.done())
    }

    fn __repr__(&self) -> String {
        "Handed(...)".to_string()
    }
}

/// The asyncio form of [`Handed`]: `await handed.offset()` or `await handed.done()`.
#[pyclass(module = "felix", name = "AsyncHanded")]
pub struct AsyncHanded {
    slot: Arc<Slot>,
}

impl AsyncHanded {
    pub(crate) fn new(slot: Arc<Slot>) -> Self {
        Self { slot }
    }
}

#[pymethods]
impl AsyncHanded {
    fn offset<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.slot);
        pyo3_async_runtimes::tokio::future_into_py(py, slot.offset())
    }

    fn done<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.slot);
        pyo3_async_runtimes::tokio::future_into_py(py, slot.done())
    }

    fn __repr__(&self) -> String {
        "AsyncHanded(...)".to_string()
    }
}
