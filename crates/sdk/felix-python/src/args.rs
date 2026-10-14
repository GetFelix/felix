//! Turning Python arguments into the Rust client's types, with a
//! `ValueError` that names the argument when they do not fit.

use felix_client::Seed;

use felix_wire::{AckMode, StartPosition};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

pub(crate) fn parse_addrs(py: Python<'_>, addrs: &Py<PyAny>) -> PyResult<Vec<Seed>> {
    let bound = addrs.bind(py);
    let items: Vec<String> = if let Ok(single) = bound.extract::<String>() {
        vec![single]
    } else {
        bound.extract::<Vec<String>>().map_err(|_| {
            PyValueError::new_err("addrs must be a 'host:port' string or a list of them")
        })?
    };

    // Names are kept rather than resolved here: the client resolves them
    // again when every address it holds stops answering, which is how it
    // survives a rollout that moves every broker.
    items
        .iter()
        .map(|item| {
            item.parse::<Seed>()
                .map_err(|err| PyValueError::new_err(format!("{err:#}")))
        })
        .collect()
}

pub(crate) fn parse_ack(ack: &str) -> PyResult<AckMode> {
    match ack {
        "none" => Ok(AckMode::None),
        "per_message" => Ok(AckMode::PerMessage),
        "per_batch" => Ok(AckMode::PerBatch),
        other => Err(PyValueError::new_err(format!(
            "ack must be 'none', 'per_message', or 'per_batch', not {other:?}"
        ))),
    }
}

pub(crate) fn parse_start(
    py: Python<'_>,
    start: Option<Py<PyAny>>,
) -> PyResult<Option<StartPosition>> {
    let Some(start) = start else {
        return Ok(None);
    };
    let bound = start.bind(py);
    if let Ok(offset) = bound.extract::<u64>() {
        return Ok(Some(StartPosition::Offset(offset)));
    }
    match bound.extract::<String>()?.as_str() {
        "latest" => Ok(Some(StartPosition::Latest)),
        "earliest" => Ok(Some(StartPosition::Earliest)),
        other => Err(PyValueError::new_err(format!(
            "start must be 'latest', 'earliest', or an integer offset, not {other:?}"
        ))),
    }
}

/// Four scope strings at once — the shape every group call starts with.
pub(crate) fn owned4(a: &str, b: &str, c: &str, d: &str) -> (String, String, String, String) {
    (a.to_string(), b.to_string(), c.to_string(), d.to_string())
}
