//! Turning Python arguments into the Rust client's types, with a
//! `ValueError` that names the argument when they do not fit.

use std::net::ToSocketAddrs;

use std::time::Duration;

use felix_client::{ReconnectPolicy, Seed};
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

/// The longest reconnect deadline accepted, in seconds. Longer is no deadline
/// in practice, and a huge one would overflow the clock.
const MAX_DEADLINE_SECS: f64 = 365.0 * 24.0 * 3600.0;

/// The default reconnect policy with the caller's attempts and deadline.
pub(crate) fn reconnect_policy(
    attempts: Option<&Bound<'_, PyAny>>,
    deadline: Option<f64>,
) -> PyResult<ReconnectPolicy> {
    let mut policy = ReconnectPolicy::default();
    if let Some(attempts) = attempts {
        // A Python int of any size is a ValueError here, not an OverflowError.
        let wanted = match attempts.extract::<i64>() {
            Ok(n) => usize::try_from(n).ok().filter(|&n| n >= 1),
            Err(err) if err.is_instance_of::<pyo3::exceptions::PyOverflowError>(attempts.py()) => {
                None
            }
            Err(err) => return Err(err),
        };
        policy.attempts = wanted.ok_or_else(|| {
            PyValueError::new_err(format!(
                "reconnect_attempts must be at least 1, not {}",
                attempts
                    .str()
                    .map_or_else(|_| "that".into(), |s| s.to_string())
            ))
        })?;
    }
    if let Some(seconds) = deadline {
        if !(seconds > 0.0 && seconds <= MAX_DEADLINE_SECS) {
            return Err(PyValueError::new_err(format!(
                "reconnect_deadline must be a number of seconds above 0 and at most one year, not {seconds}"
            )));
        }
        // Positive, finite and bounded above, so this cannot fail.
        policy.deadline = Some(Duration::from_secs_f64(seconds));
    }
    Ok(policy)
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

/// A start position that must be given, as `group_create` and `group_seek`
/// require: unlike a subscription, a group has no default to fall back on.
pub(crate) fn parse_required_start(py: Python<'_>, start: Py<PyAny>) -> PyResult<StartPosition> {
    Ok(parse_start(py, Some(start))?.expect("a given start parses to a position"))
}

/// A length of time in seconds that must be positive, finite and at most a
/// year, so `from_secs_f64` cannot panic on what Python passed.
pub(crate) fn positive_duration(seconds: f64, name: &str) -> PyResult<std::time::Duration> {
    if seconds.is_finite() && seconds > 0.0 && seconds <= 365.0 * 24.0 * 3600.0 {
        Ok(std::time::Duration::from_secs_f64(seconds))
    } else {
        Err(PyValueError::new_err(format!(
            "{name} must be a positive number of seconds, not {seconds}"
        )))
    }
}

/// Like [`positive_duration`], but zero is allowed: a delay of nothing is a
/// plain nack.
pub(crate) fn non_negative_duration(seconds: f64, name: &str) -> PyResult<std::time::Duration> {
    if seconds.is_finite() && (0.0..=365.0 * 24.0 * 3600.0).contains(&seconds) {
        Ok(std::time::Duration::from_secs_f64(seconds))
    } else {
        Err(PyValueError::new_err(format!(
            "{name} must be a number of seconds from 0 to one year, not {seconds}"
        )))
    }
}

/// Four scope strings at once — the shape every group call starts with.
pub(crate) fn owned4(a: &str, b: &str, c: &str, d: &str) -> (String, String, String, String) {
    (a.to_string(), b.to_string(), c.to_string(), d.to_string())
}

/// A sharded cache watch resumes or delivers retained state, not both: a
/// resume already replays the state a retained start shortcuts.
pub(crate) fn check_sharded_watch<T>(retained: bool, resume: &Option<T>) -> PyResult<()> {
    if retained && resume.is_some() {
        return Err(PyValueError::new_err(
            "retained and resume are mutually exclusive: a resume already \
             replays the state a retained start shortcuts",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
