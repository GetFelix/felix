//! Turning Python arguments into the Rust client's types, with a
//! `ValueError` that names the argument when they do not fit.

use std::net::ToSocketAddrs;

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

    // A name is handed to the client as a name, which resolves it again when
    // every address it holds stops answering. It is resolved here too, so a
    // name that does not resolve now is still a ValueError.
    let mut seeds = Vec::with_capacity(items.len());
    for item in items {
        let seed = item
            .parse::<Seed>()
            .map_err(|err| PyValueError::new_err(format!("{err:#}")))?;
        if let Seed::Name(name) = &seed {
            let mut found = name.to_socket_addrs().map_err(|err| {
                PyValueError::new_err(format!("could not resolve {item:?}: {err}"))
            })?;
            if found.next().is_none() {
                return Err(PyValueError::new_err(format!(
                    "{item:?} resolved to no addresses"
                )));
            }
        }
        seeds.push(seed);
    }
    Ok(seeds)
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
