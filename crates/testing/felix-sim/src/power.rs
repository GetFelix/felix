//! What a simulated power cut leaves behind, for the runs that cut power.

use std::time::Duration;

/// How long a power cut waits for an append to land in before it goes
/// without one. Under load a broker appends every few milliseconds; one that
/// is cut off may not append at all.
pub(crate) const APPEND_WITHIN: Duration = Duration::from_secs(2);

/// Whether a segment the cut tore leaves recovery a tail to repair: it came
/// back, at `len`, with bytes past `from`, where the whole records of the
/// torn append end.
pub(crate) fn needs_repair(len: Option<u64>, from: u64) -> bool {
    len.is_some_and(|len| len > from)
}

#[cfg(test)]
mod tests;
