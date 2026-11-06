//! Generation burn: how many generations a shard went through, against how
//! many its faults could explain.
//!
//! Every leader change takes a new generation, so a shard that loses its
//! leader once advances by one or two. A shard that advances by dozens under a
//! handful of faults is electing again and again: candidates deposing each
//! other, or a leader that cannot hold what it won. The model checks that
//! elections settle (`ElectionSettles`), not how many it took, so this is a
//! warning for a person to read, never a failure.

use std::collections::{BTreeSet, HashMap};

use super::{CheckContext, Checker, Finding, Point, Seen, seen_by_all};
use crate::cluster::describe;
use crate::nemesis::Plan;

/// Generations a shard may advance without any fault: placement moves and the
/// first fence.
const BASE: u64 = 2;
/// Generations one fault that hits a shard's leader may cost: the election it
/// forces, and a second if the old leader comes back and is put out again.
const PER_FAULT: u64 = 2;
/// How many shards the warning names.
const SHOW: usize = 5;

/// What the run saw of one shard.
#[derive(Debug, Default)]
struct Course {
    /// The generation at the first sample.
    first: Option<u64>,
    /// The highest generation any broker routed the shard at.
    latest: u64,
    /// Brokers that led it at some point.
    led: BTreeSet<usize>,
}

/// Warns when a shard went through more generations than its faults explain:
/// more than `2 * faults + 2`, counting the faults of the plan that could have
/// cost its leader.
#[derive(Debug, Default)]
pub struct GenerationBurn {
    shards: HashMap<String, Course>,
}

impl GenerationBurn {
    fn observe(&mut self, shard: &str, seen: &[Seen]) {
        let course = self.shards.entry(shard.to_string()).or_default();
        for one in seen {
            for generation in one
                .stand
                .routed_leader
                .iter()
                .map(|(_, generation)| *generation)
                .chain(one.stand.opened_generation)
            {
                course.first.get_or_insert(generation);
                course.latest = course.latest.max(generation);
            }
            if one.stand.opened_generation.is_some() {
                course.led.insert(one.node);
            }
        }
    }

    /// The shards that burned more than their faults explain, worst first,
    /// as `(shard, generations advanced, allowed, faults)`.
    fn burned(&self, plan: &Plan) -> Vec<(String, u64, u64, usize)> {
        let mut burned: Vec<_> = self
            .shards
            .iter()
            .filter_map(|(shard, course)| {
                let advanced = course.latest.saturating_sub(course.first?);
                let faults = plan
                    .steps
                    .iter()
                    .filter(|step| step.fault.can_unseat(&course.led))
                    .count();
                let allowed = PER_FAULT * faults as u64 + BASE;
                (advanced > allowed).then(|| (shard.clone(), advanced, allowed, faults))
            })
            .collect();
        burned.sort_by_key(|(shard, advanced, ..)| (std::cmp::Reverse(*advanced), shard.clone()));
        burned
    }
}

#[async_trait::async_trait]
impl Checker for GenerationBurn {
    fn name(&self) -> &'static str {
        "generation-burn"
    }

    async fn check(&mut self, ctx: &CheckContext) -> Vec<Finding> {
        for key in ctx.cluster.shard_keys() {
            self.observe(&describe(&key), &seen_by_all(&ctx.cluster, &key));
        }
        if ctx.point != Point::End {
            return Vec::new();
        }
        let burned = self.burned(&ctx.plan);
        if burned.is_empty() {
            return Vec::new();
        }
        let list: Vec<String> = burned
            .iter()
            .take(SHOW)
            .map(|(shard, advanced, allowed, faults)| {
                format!("{shard}: {advanced} generations, {allowed} explained by {faults} faults")
            })
            .collect();
        vec![Finding::warning(
            self.name(),
            format!(
                "{} of {} shards went through more generations than the faults that hit them explain ({})",
                burned.len(),
                self.shards.len(),
                list.join("; ")
            ),
        )]
    }
}

#[cfg(test)]
mod tests;
