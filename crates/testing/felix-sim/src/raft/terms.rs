//! What the run's terms say about a shard: no two members pass the leader
//! lease at once, copies agree on the term of every entry they hold, and a
//! shard does not go through more terms than its faults explain.

use std::collections::BTreeMap;
use std::time::Duration;

use super::{list, name};

/// Terms a shard may go through with no fault: its first election and the
/// hand-over to the broker placement names.
const BASE: u64 = 2;
/// Terms one fault that hits a shard's leader may cost: the election it
/// forces, and a second if the old leader comes back and is moved again.
const PER_FAULT: u64 = 2;

/// Whether a run's timer rates keep the lease inside a follower's promise:
/// a lease on the slowest clock ends, in real time, before `election_max`
/// on the fastest. The simulator's skewed clocks can draw rates further
/// apart than the drift the engine is configured for, and there two leaders
/// passing the lease is the clocks breaking the assumption, not a bug.
pub(super) fn lease_judged(rates_ppm: &[u64], lease: Duration, election_max: Duration) -> bool {
    let (Some(&slowest), Some(&fastest)) = (rates_ppm.iter().min(), rates_ppm.iter().max()) else {
        return false;
    };
    lease.as_nanos() * u128::from(fastest) <= election_max.as_nanos() * u128::from(slowest)
}

/// The finding when more than one of `serving`, the members that passed
/// the lease check for `shard` at one instant, did.
pub(super) fn two_serving(shard: u32, serving: &[usize]) -> Option<String> {
    let [first, second, ..] = serving else {
        return None;
    };
    Some(format!(
        "{}: {} and {} both passed the lease check at once",
        list(shard),
        name(*first),
        name(*second)
    ))
}

/// The first offset two members' logs of `shard` hold entries of different
/// terms at. Each log is the generation of every entry below the member's
/// read bound; an entry with none was put back below a copy's base, where
/// terms are not compared.
pub(super) fn disagreement(
    shard: u32,
    logs: &[(usize, BTreeMap<u64, Option<u64>>)],
) -> Option<String> {
    let mut first: BTreeMap<u64, (usize, u64)> = BTreeMap::new();
    for (member, log) in logs {
        for (&offset, &generation) in log {
            let Some(generation) = generation else {
                continue;
            };
            let (other, seen) = *first.entry(offset).or_insert((*member, generation));
            if seen != generation {
                return Some(format!(
                    "{}: {} holds offset {offset} at generation {seen} and {} at {generation}",
                    list(shard),
                    name(other),
                    name(*member)
                ));
            }
        }
    }
    None
}

/// The lowest and highest term the run saw a shard at.
#[derive(Debug, Clone, Copy)]
pub(super) struct Span {
    pub(super) first: u64,
    pub(super) last: u64,
}

impl Span {
    /// Widen `spans`' span of `shard` to take in `term`.
    pub(super) fn saw(spans: &mut BTreeMap<u32, Self>, shard: u32, term: u64) {
        let span = spans.entry(shard).or_insert(Self {
            first: term,
            last: term,
        });
        span.first = span.first.min(term);
        span.last = span.last.max(term);
    }
}

/// A warning for each shard that went through more terms than `faults`
/// explain. Elections that keep failing burn terms, so this catches an
/// election storm, which no safety check does. A warning for a person to
/// read, never a failure.
pub(super) fn burned(spans: &BTreeMap<u32, Span>, faults: usize) -> Vec<String> {
    let allowed = PER_FAULT * faults as u64 + BASE;
    spans
        .iter()
        .filter_map(|(shard, span)| {
            let advanced = span.last - span.first;
            (advanced > allowed).then(|| {
                format!(
                    "{}: went through {advanced} terms, {allowed} explained by {faults} faults",
                    list(*shard)
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests;
