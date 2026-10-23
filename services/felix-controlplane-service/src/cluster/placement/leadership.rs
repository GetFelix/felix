//! Who leads what, as a pass will leave it: per node across the cluster, and
//! per node within each stream or cache.
//!
//! Balancing only the cluster-wide count lets one stream land 1/1/10 while
//! the totals look even, and a stream's traffic then runs on one broker. So
//! placement balances each stream's leaders first and the totals second, and
//! a move for the totals never widens a stream's spread. Every move then
//! lowers (each stream's spread, the cluster's spread) in that order, which
//! is what keeps the two from trading shards back and forth.
use std::collections::HashMap;

use super::plan::owner_of;
use crate::model::ShardKey;

/// Leader counts, cluster-wide and per stream or cache.
#[derive(Debug, Default)]
pub(super) struct Leadership<'a> {
    total: HashMap<&'a str, u32>,
    owned: HashMap<String, HashMap<&'a str, u32>>,
}

impl<'a> Leadership<'a> {
    pub(super) fn add(&mut self, key: &ShardKey, node: &'a str) {
        *self.total.entry(node).or_default() += 1;
        *self
            .owned
            .entry(owner_of(key))
            .or_default()
            .entry(node)
            .or_default() += 1;
    }

    pub(super) fn remove(&mut self, key: &ShardKey, node: &str) {
        if let Some(count) = self.total.get_mut(node) {
            *count = count.saturating_sub(1);
        }
        if let Some(count) = self
            .owned
            .get_mut(&owner_of(key))
            .and_then(|owned| owned.get_mut(node))
        {
            *count = count.saturating_sub(1);
        }
    }

    /// Shards `node` leads, cluster-wide.
    pub(super) fn total(&self, node: &str) -> u32 {
        self.total.get(node).copied().unwrap_or(0)
    }

    /// Shards of `key`'s stream or cache that `node` leads.
    pub(super) fn of(&self, key: &ShardKey, node: &str) -> u32 {
        self.owned
            .get(&owner_of(key))
            .and_then(|owned| owned.get(node))
            .copied()
            .unwrap_or(0)
    }
}

/// The even shares one shard is placed against.
#[derive(Debug, Clone, Copy)]
pub(super) struct Shares {
    /// Leaders per node cluster-wide, rounded up.
    pub(super) cluster: u32,
    /// Leaders per node of this shard's stream, rounded down and up.
    pub(super) floor: u32,
    pub(super) ceil: u32,
}

impl Shares {
    /// `shards` of one stream over `nodes` eligible nodes.
    pub(super) fn new(cluster: u32, shards: u32, nodes: usize) -> Self {
        if nodes == 0 {
            return Self {
                cluster,
                floor: 0,
                ceil: u32::MAX,
            };
        }
        let nodes = nodes as u32;
        Self {
            cluster,
            floor: shards / nodes,
            ceil: shards.div_ceil(nodes).max(1),
        }
    }
}
