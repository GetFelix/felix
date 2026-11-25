//! [`EveryFamily`]: goes round fault kinds in order until each family has
//! had one fault injected and healed.
use std::collections::BTreeSet;

use super::{ClusterView, Fault, FaultFamily, FaultKind, Nemesis, RandomNemesis, Rng};

/// How many times in a row [`EveryFamily`] picks a kind whose fault was not
/// injected before it moves on to the next kind.
pub const TRIES_PER_KIND: usize = 3;

/// Goes round `kinds` in order, one fault of each, drawing targets from the
/// seed, and keeps the campaign going until every family in the order has
/// had a fault injected and healed.
///
/// A fault can be refused for reasons the cluster is right about: the
/// operator API turns a move down with a 409 while the broker it names has
/// only just restarted, or while placement and the elected leader still
/// disagree about who leads. The campaign logs that and goes on. A schedule
/// that visits each family once would then end without the family, so a
/// refused kind is picked again, with a fresh view, up to
/// [`TRIES_PER_KIND`] times.
#[derive(Debug, Clone)]
pub struct EveryFamily {
    kinds: Vec<RandomNemesis>,
    next: usize,
    /// Refusals in a row of the kind at `next`.
    refused: usize,
    families: BTreeSet<FaultFamily>,
    healed: BTreeSet<FaultFamily>,
    /// Every fault picked, in order.
    pub picked: Vec<Fault>,
}

impl EveryFamily {
    pub fn new(order: Vec<FaultKind>) -> Self {
        Self {
            families: order.iter().map(|kind| kind.family()).collect(),
            kinds: order
                .into_iter()
                .map(|kind| RandomNemesis::new(vec![kind]))
                .collect(),
            next: 0,
            refused: 0,
            healed: BTreeSet::new(),
            picked: Vec::new(),
        }
    }

    /// The families with a fault injected and healed so far.
    pub fn healed(&self) -> &BTreeSet<FaultFamily> {
        &self.healed
    }

    fn advance(&mut self) {
        self.next += 1;
        self.refused = 0;
    }
}

impl Nemesis for EveryFamily {
    fn next_fault(&mut self, rng: &mut Rng, view: &ClusterView) -> Option<Fault> {
        if self.kinds.is_empty() {
            return None;
        }
        let slot = self.next % self.kinds.len();
        let fault = self.kinds[slot].next_fault(rng, view)?;
        self.picked.push(fault.clone());
        Some(fault)
    }

    fn outcome(&mut self, fault: &Fault, healed: bool) {
        if healed {
            self.healed.insert(fault.family());
            self.advance();
        } else {
            self.refused += 1;
            if self.refused >= TRIES_PER_KIND {
                self.advance();
            }
        }
    }

    fn satisfied(&self) -> bool {
        self.families.is_subset(&self.healed)
    }

    fn needs_proxy_links(&self) -> bool {
        self.kinds.iter().any(Nemesis::needs_proxy_links)
    }

    fn needs_fsync_on_commit(&self) -> bool {
        self.kinds.iter().any(Nemesis::needs_fsync_on_commit)
    }

    fn needs_power_loss(&self) -> bool {
        self.kinds.iter().any(Nemesis::needs_power_loss)
    }

    fn needs_election(&self) -> bool {
        self.kinds.iter().any(Nemesis::needs_election)
    }
}

#[cfg(test)]
mod tests;
