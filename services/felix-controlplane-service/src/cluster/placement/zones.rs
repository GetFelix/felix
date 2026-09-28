//! Failure domains: which copies of a shard could be lost together.
//!
//! A broker's zone is what it registered. A broker without one is its own
//! domain, sharing it with nobody, so a cluster that reports no zones is
//! already as spread as it can be and every rule here is a no-op for it.
//!
//! Only live brokers eligible for the shard are looked up. Any other node in
//! a replica set (down, or draining) counts as its own domain: its copy is
//! leaving or may never return, so spreading around it gains nothing.
use std::collections::BTreeSet;

use crate::model::Node;

/// A failure domain. The two variants keep a zone named like a node id from
/// being mistaken for that node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) enum Domain<'a> {
    Zone(&'a str),
    Alone(&'a str),
}

/// The domain `id` counts against.
pub(super) fn domain<'a>(eligible: &[&'a Node], id: &'a str) -> Domain<'a> {
    eligible
        .iter()
        .find(|node| node.node_id == id)
        .map_or(Domain::Alone(id), |node| domain_of(node))
}

pub(super) fn domain_of(node: &Node) -> Domain<'_> {
    match node.spec.zone.as_deref() {
        Some(zone) => Domain::Zone(zone),
        None => Domain::Alone(&node.node_id),
    }
}

/// Distinct domains among `ids`.
pub(super) fn spread<'a>(eligible: &[&'a Node], ids: impl IntoIterator<Item = &'a str>) -> usize {
    ids.into_iter()
        .map(|id| domain(eligible, id))
        .collect::<BTreeSet<_>>()
        .len()
}

/// Whether `set` spans fewer domains than it could: fewer than its size, when
/// the eligible brokers offer more domains than it uses.
pub(super) fn unspread<'a>(eligible: &[&'a Node], set: &[&'a str]) -> bool {
    let have = spread(eligible, set.iter().copied());
    let offered = eligible
        .iter()
        .map(|node| domain_of(node))
        .chain(set.iter().map(|id| domain(eligible, id)))
        .collect::<BTreeSet<_>>()
        .len();
    have < set.len().min(offered)
}

/// `staying` cut to `wanted`, keeping first the copies that add a domain not
/// already in `holding`, so dropping the extras never costs spread. Order is
/// otherwise kept, which is all this does when there are no zones.
pub(super) fn keep_spread<'a>(
    eligible: &[&'a Node],
    holding: &[&'a str],
    staying: &[&'a str],
    wanted: usize,
) -> Vec<&'a str> {
    let mut used: BTreeSet<Domain<'a>> = holding.iter().map(|id| domain(eligible, id)).collect();
    let mut kept: Vec<&'a str> = Vec::with_capacity(wanted);
    for id in staying {
        if kept.len() < wanted && used.insert(domain(eligible, id)) {
            kept.push(id);
        }
    }
    for id in staying {
        if kept.len() < wanted && !kept.contains(id) {
            kept.push(id);
        }
    }
    // Back in the order they came, so a set that loses nothing is unchanged.
    kept.sort_by_key(|id| staying.iter().position(|s| s == id));
    kept
}
