//! Checkers that read what each broker believes about each shard, at one
//! instant, and compare brokers with each other or a broker with itself.
//!
//! Each is named after an invariant of `docs/formal/FelixShard.tla`:
//!
//! | Checker | TLA+ invariant |
//! |---|---|
//! | [`OneLeaderPerGeneration`] | `OneLeaderPerGeneration` |
//! | [`AtMostOneServing`] | `AtMostOneServing` |
//! | [`AdoptedMatchesOpened`] | `AdoptedMatchesOpened`, for the generation |
//!
//! They run at every [`Point`], so a state that lasts only seconds during the
//! faults is seen. A violation is reported once, with a census of the shard.

use std::collections::{BTreeSet, HashMap};

use super::{CheckContext, Checker, Digest, Finding, Point, Seen, seen_by_all};
use crate::cluster::{SimCluster, describe};

/// How many violations print a census; the rest are counted.
const SHOW: usize = 3;

/// One violation: an id so it is reported once, and what to say.
type Violation = (String, String);

/// No two brokers lead one shard in the same generation.
///
/// "Lead" is a broker whose lifecycle has the shard open for writes at that
/// generation. The record spans the whole run, so a broker that led and then
/// crashed still counts when another opens the same generation later. The
/// same broker opening a generation again after a restart is not two leaders.
#[derive(Debug)]
pub struct OneLeaderPerGeneration {
    led: HashMap<(String, u64), BTreeSet<String>>,
    digest: Digest,
}

impl Default for OneLeaderPerGeneration {
    fn default() -> Self {
        Self {
            led: HashMap::new(),
            digest: Digest::new(SHOW),
        }
    }
}

impl Sampled for OneLeaderPerGeneration {
    fn observe(&mut self, shard: &str, seen: &[Seen]) -> Vec<Violation> {
        let mut found = Vec::new();
        for one in seen {
            let Some(generation) = one.stand.opened_generation else {
                continue;
            };
            let leaders = self.led.entry((shard.to_string(), generation)).or_default();
            if leaders.insert(one.name.clone()) && leaders.len() > 1 {
                let names: Vec<&str> = leaders.iter().map(String::as_str).collect();
                found.push((
                    format!("{shard}@{generation}"),
                    format!(
                        "{shard} was led by {} in generation {generation}",
                        names.join(" and ")
                    ),
                ));
            }
        }
        found
    }

    fn digest(&mut self) -> &mut Digest {
        &mut self.digest
    }
}

#[async_trait::async_trait]
impl Checker for OneLeaderPerGeneration {
    fn name(&self) -> &'static str {
        "one-leader-per-generation"
    }

    async fn check(&mut self, ctx: &CheckContext) -> Vec<Finding> {
        sweep(self, ctx).await
    }
}

/// At most one broker would take a write for a shard at any instant.
///
/// Sampled at [`Point::Quiet`] and the points after, so it sees two brokers
/// serving for as long as a second.
///
/// It applies to shards whose writes are protected by the lease. The model
/// asserts it for those only. A shard acknowledged by a majority of followers
/// at the leader's generation (`AckByFollowers`) lets a deposed leader keep
/// serving until its lease ends, because it cannot collect a majority once its
/// successor has fenced one; there the model asserts `AckedHeldByLeader`
/// instead, and so does this crate. The streams and caches of a run that has
/// finalized election are in that design.
#[derive(Debug)]
pub struct AtMostOneServing {
    digest: Digest,
}

impl Default for AtMostOneServing {
    fn default() -> Self {
        Self {
            digest: Digest::new(SHOW),
        }
    }
}

impl Sampled for AtMostOneServing {
    fn observe(&mut self, shard: &str, seen: &[Seen]) -> Vec<Violation> {
        let serving: Vec<String> = seen
            .iter()
            .filter(|one| one.stand.serving && !one.stand.acks_by_followers)
            .map(|one| match one.stand.generation {
                Some(generation) => format!("{} at generation {generation}", one.name),
                None => one.name.clone(),
            })
            .collect();
        if serving.len() < 2 {
            return Vec::new();
        }
        vec![(
            shard.to_string(),
            format!("{shard} is served by {} at once", serving.join(" and ")),
        )]
    }

    fn digest(&mut self) -> &mut Digest {
        &mut self.digest
    }
}

#[async_trait::async_trait]
impl Checker for AtMostOneServing {
    fn name(&self) -> &'static str {
        "at-most-one-serving"
    }

    async fn check(&mut self, ctx: &CheckContext) -> Vec<Finding> {
        sweep(self, ctx).await
    }
}

/// A broker that serves a shard at generation `g` opened generation `g`: the
/// generation the router says it leads, the one its fence admits writes at
/// and the one its lifecycle opened are the same.
///
/// The model's `AdoptedMatchesOpened` also compares the replica set the leader
/// leads with the one the control plane records; the simulator does not read
/// the set.
#[derive(Debug)]
pub struct AdoptedMatchesOpened {
    digest: Digest,
}

impl Default for AdoptedMatchesOpened {
    fn default() -> Self {
        Self {
            digest: Digest::new(SHOW),
        }
    }
}

impl Sampled for AdoptedMatchesOpened {
    fn observe(&mut self, shard: &str, seen: &[Seen]) -> Vec<Violation> {
        let mut found = Vec::new();
        for one in seen.iter().filter(|one| one.stand.serving) {
            let stand = &one.stand;
            let routed = stand
                .routed_leader
                .as_ref()
                .filter(|(leader, _)| *leader == one.name)
                .map(|(_, generation)| *generation);
            // `serving` already means the fence is open at the routed
            // generation, so the third reading is the lifecycle's.
            if stand.generation.is_some()
                && stand.generation == routed
                && stand.generation == stand.opened_generation
            {
                continue;
            }
            found.push((
                format!("{shard}@{}", one.name),
                format!(
                    "{} serves {shard} at generation {:?}, but routes it to {:?} and its lifecycle opened {:?}",
                    one.name, stand.generation, stand.routed_leader, stand.opened_generation
                ),
            ));
        }
        found
    }

    fn digest(&mut self) -> &mut Digest {
        &mut self.digest
    }
}

#[async_trait::async_trait]
impl Checker for AdoptedMatchesOpened {
    fn name(&self) -> &'static str {
        "adopted-matches-opened"
    }

    async fn check(&mut self, ctx: &CheckContext) -> Vec<Finding> {
        sweep(self, ctx).await
    }
}

/// What [`sweep`] needs of a checker.
trait Sampled: Checker {
    fn observe(&mut self, shard: &str, seen: &[Seen]) -> Vec<Violation>;
    fn digest(&mut self) -> &mut Digest;
}

/// Read every shard once, hand each to `observe`, and turn what it returns
/// into findings with a census.
async fn sweep<C: Sampled>(checker: &mut C, ctx: &CheckContext) -> Vec<Finding> {
    let mut findings = Vec::new();
    let cluster: &SimCluster = &ctx.cluster;
    let name = checker.name();
    for key in cluster.shard_keys() {
        let shard = describe(&key);
        for (id, summary) in checker.observe(&shard, &seen_by_all(cluster, &key)) {
            if checker.digest().admit(&id) {
                let at = format!("at {:?}, {:?}", cluster.elapsed(), ctx.point);
                findings.push(
                    Finding::new(name, format!("{summary} ({at})"))
                        .with(cluster.census(&key).await),
                );
            }
        }
    }
    if ctx.point == Point::End {
        findings.extend(checker.digest().rest(name));
    }
    findings
}

#[cfg(test)]
mod tests;
