//! Turning JavaScript arguments into the Rust client's types, with an
//! invalid-argument error when they do not fit.

use felix_client::Seed;
use std::collections::{BTreeMap, HashMap};

use felix_wire::{AckMode, StartPosition};
use napi::bindgen_prelude::*;

use crate::errors::invalid;

pub(crate) fn seeds(addrs: Vec<String>) -> Result<Vec<Seed>> {
    // Names are kept rather than resolved here: the client resolves them
    // again when every address it holds stops answering.
    addrs
        .iter()
        .map(|addr| {
            addr.parse::<Seed>()
                .map_err(|err| invalid(format!("{err:#}")))
        })
        .collect()
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
