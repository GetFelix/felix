//! Turning JavaScript arguments into the Rust client's types, with an
//! invalid-argument error when they do not fit.

use std::collections::{BTreeMap, HashMap};
use std::net::ToSocketAddrs;

use std::time::Duration;

use felix_client::{ReconnectPolicy, Seed};
use felix_wire::{AckMode, StartPosition};
use napi::bindgen_prelude::*;

use crate::errors::invalid;

pub(crate) fn seeds(addrs: Vec<String>) -> Result<Vec<Seed>> {
    // A name is handed to the client as a name, which resolves it again when
    // every address it holds stops answering. It is resolved here too, so a
    // name that does not resolve now is still an invalid argument.
    let mut seeds = Vec::with_capacity(addrs.len());
    for addr in addrs {
        let seed = addr
            .parse::<Seed>()
            .map_err(|err| invalid(format!("{err:#}")))?;
        if let Seed::Name(name) = &seed {
            let mut found = name
                .to_socket_addrs()
                .map_err(|err| invalid(format!("could not resolve {addr:?}: {err}")))?;
            if found.next().is_none() {
                return Err(invalid(format!("{addr:?} resolved to no addresses")));
            }
        }
        seeds.push(seed);
    }
    Ok(seeds)
}

/// The longest reconnect deadline accepted, in milliseconds. Longer is no
/// deadline in practice, and a huge one would overflow the clock.
const MAX_DEADLINE_MS: f64 = 365.0 * 24.0 * 3600.0 * 1000.0;

/// The default reconnect policy with the caller's attempts, deadline and
/// backoff (ms).
///
/// Numbers arrive as `f64` so a fraction or `NaN` is refused instead of being
/// truncated into something the caller did not ask for.
pub(crate) fn reconnect_policy(
    attempts: Option<f64>,
    deadline_ms: Option<f64>,
    backoff_ms: Option<f64>,
    max_backoff_ms: Option<f64>,
) -> Result<ReconnectPolicy> {
    let mut policy = ReconnectPolicy::default();
    if let Some(attempts) = attempts {
        if !(attempts.is_finite() && attempts.fract() == 0.0 && (1.0..=1e9).contains(&attempts)) {
            return Err(invalid(format!(
                "reconnectAttempts must be a whole number of at least 1, not {attempts}"
            )));
        }
        policy.attempts = attempts as usize;
    }
    if let Some(ms) = deadline_ms {
        // NaN fails both comparisons. The cap keeps the clock from overflowing.
        if !(1.0..=MAX_DEADLINE_MS).contains(&ms) {
            return Err(invalid(format!(
                "reconnectDeadline must be between 1 millisecond and one year, not {ms}"
            )));
        }
        policy.deadline = Some(Duration::from_secs_f64(ms / 1000.0));
    }
    for (name, wanted, slot) in [
        ("reconnectBackoff", backoff_ms, &mut policy.backoff),
        (
            "reconnectMaxBackoff",
            max_backoff_ms,
            &mut policy.max_backoff,
        ),
    ] {
        if let Some(ms) = wanted {
            if !(1.0..=MAX_DEADLINE_MS).contains(&ms) {
                return Err(invalid(format!(
                    "{name} must be between 1 millisecond and one year, not {ms}"
                )));
            }
            *slot = Duration::from_secs_f64(ms / 1000.0);
        }
    }
    if policy.max_backoff < policy.backoff {
        return Err(invalid(format!(
            "reconnectMaxBackoff ({}ms) must be at least reconnectBackoff ({}ms)",
            policy.max_backoff.as_millis(),
            policy.backoff.as_millis()
        )));
    }
    Ok(policy)
}

/// How much the broker must have done before a publish resolves.
pub(crate) fn parse_ack(ack: &str) -> Result<AckMode> {
    match ack {
        "none" => Ok(AckMode::None),
        "per_message" => Ok(AckMode::PerMessage),
        "per_batch" => Ok(AckMode::PerBatch),
        other => Err(invalid(format!(
            r#"ack must be "none", "per_message" or "per_batch", got {other:?}"#
        ))),
    }
}

/// A length of time in milliseconds from `min` to one year, so
/// `from_secs_f64` cannot panic on what JavaScript passed.
pub(crate) fn duration_ms(ms: f64, min: f64, name: &str) -> Result<Duration> {
    if !(min..=MAX_DEADLINE_MS).contains(&ms) {
        return Err(invalid(format!(
            "{name} must be between {min} milliseconds and one year, not {ms}"
        )));
    }
    Ok(Duration::from_secs_f64(ms / 1000.0))
}

/// Where a new subscription begins.
pub(crate) fn parse_start(start: Option<Either<String, BigInt>>) -> Result<StartPosition> {
    match start {
        None => Ok(StartPosition::Latest),
        Some(Either::A(name)) => match name.as_str() {
            "latest" => Ok(StartPosition::Latest),
            "earliest" => Ok(StartPosition::Earliest),
            other => Err(invalid(format!(
                r#"start must be "latest", "earliest" or an offset, got {other:?}"#
            ))),
        },
        Some(Either::B(offset)) => {
            let (_, value, lossless) = offset.get_u64();
            if !lossless {
                return Err(invalid("start offset does not fit in a u64"));
            }
            Ok(StartPosition::Offset(value))
        }
    }
}

/// Per-shard resume offsets, in the shape `positions()` hands back.
///
/// Keyed by shard number as a string because that is what a JavaScript object
/// key is, so a `positions()` result can be passed straight back with no
/// translation on the caller's part.
pub(crate) fn parse_positions(
    resume: Option<HashMap<String, BigInt>>,
) -> Result<Option<BTreeMap<u32, u64>>> {
    let Some(resume) = resume else {
        return Ok(None);
    };
    let mut out = BTreeMap::new();
    for (shard, offset) in resume {
        let parsed: u32 = shard
            .parse()
            .map_err(|_| invalid(format!("resume keys are shard numbers, got {shard:?}")))?;
        out.insert(parsed, u64_of(offset)?);
    }
    Ok(Some(out))
}

pub(crate) fn u64_of(value: BigInt) -> Result<u64> {
    let (_, out, lossless) = value.get_u64();
    if !lossless {
        return Err(invalid("offset does not fit in a u64"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
