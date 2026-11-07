//! Record headers: the key and named byte values a record carries beside its
//! payload, and the arguments that publish them.

use bytes::Bytes;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

/// A record's own key and headers. Build one to publish with, or read one off
/// `Event.headers`.
///
/// The key belongs to the record and does not route it; `publish(key=...)` is
/// the routing key. Names may repeat and keep their order. A name must not be
/// empty or start with `felix.`, which the broker reserves.
#[pyclass(module = "felix", frozen)]
#[derive(Clone)]
pub struct RecordHeaders {
    key: Option<Vec<u8>>,
    headers: Vec<(String, Vec<u8>)>,
}

#[pymethods]
impl RecordHeaders {
    #[new]
    #[pyo3(signature = (headers=None, *, key=None))]
    fn new(headers: Option<&Bound<'_, PyAny>>, key: Option<&[u8]>) -> PyResult<Self> {
        let headers = match headers {
            Some(headers) => pairs(headers)?,
            None => Vec::new(),
        };
        Ok(Self {
            key: key.map(<[u8]>::to_vec),
            headers,
        })
    }

    /// The record's own key, or `None`.
    #[getter]
    fn key<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.key.as_deref().map(|key| PyBytes::new(py, key))
    }

    /// The headers as `(name, value)` pairs in send order.
    #[getter]
    fn headers<'py>(&self, py: Python<'py>) -> Vec<(String, Bound<'py, PyBytes>)> {
        self.headers
            .iter()
            .map(|(name, value)| (name.clone(), PyBytes::new(py, value)))
            .collect()
    }

    /// The first value under `name`, or `None`.
    fn get<'py>(&self, py: Python<'py>, name: &str) -> Option<Bound<'py, PyBytes>> {
        self.headers
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, value)| PyBytes::new(py, value))
    }

    fn __len__(&self) -> usize {
        self.headers.len()
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.key == other.key && self.headers == other.headers
    }

    fn __repr__(&self) -> String {
        let names: Vec<&str> = self.headers.iter().map(|(name, _)| name.as_str()).collect();
        format!(
            "RecordHeaders(names={names:?}, key={})",
            if self.key.is_some() { "set" } else { "none" }
        )
    }
}

impl RecordHeaders {
    pub(crate) fn to_client(&self) -> felix_client::RecordHeaders {
        felix_client::RecordHeaders {
            key: self.key.as_deref().map(Bytes::copy_from_slice),
            headers: self
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), Bytes::copy_from_slice(value)))
                .collect(),
        }
    }
}

impl From<felix_client::RecordHeaders> for RecordHeaders {
    fn from(block: felix_client::RecordHeaders) -> Self {
        Self {
            key: block.key.map(|key| key.to_vec()),
            headers: block
                .headers
                .into_iter()
                .map(|(name, value)| (name, value.to_vec()))
                .collect(),
        }
    }
}

/// `(name, value)` pairs from a dict or any iterable of 2-tuples.
fn pairs(source: &Bound<'_, PyAny>) -> PyResult<Vec<(String, Vec<u8>)>> {
    let bad = || PyTypeError::new_err("headers must be a dict or a list of (str, bytes) pairs");
    if let Ok(dict) = source.cast::<PyDict>() {
        return dict
            .iter()
            .map(|(name, value)| Ok((name.extract().map_err(|_| bad())?, value.extract().map_err(|_| bad())?)))
            .collect();
    }
    source
        .try_iter()
        .map_err(|_| bad())?
        .map(|item| item.and_then(|pair| pair.extract::<(String, Vec<u8>)>()).map_err(|_| bad()))
        .collect()
}

/// One record's headers argument: a `RecordHeaders`, or the dict or pairs it
/// is built from. Empty headers publish a plain record.
pub(crate) fn extract(source: &Bound<'_, PyAny>) -> PyResult<felix_client::RecordHeaders> {
    if let Ok(block) = source.extract::<PyRef<'_, RecordHeaders>>() {
        return Ok(block.to_client());
    }
    Ok(RecordHeaders {
        key: None,
        headers: pairs(source)?,
    }
    .to_client())
}

/// One `RecordHeaders` per payload for a batch; `None` entries are plain
/// records.
pub(crate) fn extract_batch(
    sources: &Bound<'_, PyAny>,
    payloads: usize,
) -> PyResult<Vec<felix_client::RecordHeaders>> {
    let items: Vec<Bound<'_, PyAny>> = sources
        .try_iter()
        .map_err(|_| PyTypeError::new_err("headers must be a list with one entry per payload"))?
        .collect::<PyResult<_>>()?;
    if items.len() != payloads {
        return Err(PyValueError::new_err(format!(
            "headers has {} entries for {payloads} payloads; give one per payload, or None",
            items.len()
        )));
    }
    items
        .iter()
        .map(|item| {
            if item.is_none() {
                Ok(felix_client::RecordHeaders::default())
            } else {
                extract(item)
            }
        })
        .collect()
}
