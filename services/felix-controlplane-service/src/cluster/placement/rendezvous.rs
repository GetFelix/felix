//! Scoring nodes against a shard, and choosing by score.
//!
//! Every choice here is a deterministic function of the shard key and the
//! node ids, which is what lets two instances plan the same cluster alike.
use std::collections::HashMap;

use super::CaughtUp;
use crate::model::{Node, ShardAssignment, ShardKey, ShardKind};

/// Highest-scoring node that still keeps the cluster balanced.
///
/// **Bounded-load rendezvous.** Pure highest-random-weight balances only in the
/// limit of many keys; at the handful-of-shards scale a cluster actually starts
/// at, its variance is large — 24 shards on three nodes landed 11/5/8, and a
/// staggered start can funnel *everything* onto the first broker to register.
/// That is single-node saturation while the rest idle. So a shard goes to its
/// highest-scoring node *unless that node is already carrying its balanced share*
/// (`cap`), in which case it spills to the next-highest node under its share.
/// Locality is nearly preserved (a shard only moves off its top choice when that
/// node is full) and placement stays a deterministic function of the snapshot,
/// because `plan` walks the shards in sorted order and `load` accumulates the
/// same way on every instance. `None` only when no node has capacity at all — the
/// last pass drops the balance cap so a full-but-uncapped cluster still places,
/// and goes to the least loaded node so the overflow is spread too.
///
/// Ties break on `node_id`, which cannot itself tie: node identity is unique.
pub(super) fn choose<'a>(
    key: &ShardKey,
    eligible: &[&'a Node],
    load: &HashMap<&str, u32>,
    cap: u32,
    leaders: &HashMap<&str, u32>,
    leader_share: u32,
) -> Option<&'a str> {
    let has_capacity = |node: &Node| match node.spec.capacity.max_shards {
        Some(max) => load.get(node.node_id.as_str()).copied().unwrap_or(0) < max,
        None => true,
    };
    let under_share = |node: &Node| load.get(node.node_id.as_str()).copied().unwrap_or(0) < cap;
    // Leaders are bounded as well as roles. With a replication factor equal
    // to the node count every node holds a role for every shard, so the role
    // cap says nothing about who leads -- and a cluster placed with every
    // leader on one node would be moved apart again by the next pass.
    let under_leader_share =
        |node: &Node| leaders.get(node.node_id.as_str()).copied().unwrap_or(0) < leader_share;
    let best = |led: bool| {
        eligible
            .iter()
            .filter(|node| has_capacity(node))
            .filter(|node| under_share(node))
            .filter(|node| !led || under_leader_share(node))
            .max_by(|a, b| {
                score(key, &a.node_id)
                    .cmp(&score(key, &b.node_id))
                    .then_with(|| a.node_id.cmp(&b.node_id))
            })
    };
    // Every node is at its share. Take the least loaded rather than the best
    // scoring, or the one node that scores highest for most of the remaining
    // shards collects all of them.
    let least_loaded = || {
        eligible
            .iter()
            .filter(|node| has_capacity(node))
            .min_by(|a, b| {
                let load_of = |node: &Node| load.get(node.node_id.as_str()).copied().unwrap_or(0);
                load_of(a)
                    .cmp(&load_of(b))
                    .then_with(|| score(key, &b.node_id).cmp(&score(key, &a.node_id)))
                    .then_with(|| a.node_id.cmp(&b.node_id))
            })
    };
    best(true)
        .or_else(|| best(false))
        .or_else(least_loaded)
        .map(|n| n.node_id.as_str())
}

/// The next best-scoring nodes for a shard, after the leader.
///
/// Fewer than `wanted` is normal and not an error: a three-node cluster cannot
/// hold four copies. Placement records what it could achieve rather than
/// claiming a replica set it did not create, because an assignment that names a
/// node holding nothing is exactly the lie failover would act on.
pub(super) fn choose_replicas<'a>(
    key: &ShardKey,
    eligible: &[&'a Node],
    load: &mut HashMap<&'a str, u32>,
    leader: &str,
    wanted: u32,
) -> Vec<String> {
    let mut chosen = Vec::new();
    for _ in 0..wanted {
        let Some(node) = eligible
            .iter()
            .filter(|node| node.node_id != leader)
            .filter(|node| !chosen.iter().any(|taken: &String| taken == &node.node_id))
            .filter(|node| match node.spec.capacity.max_shards {
                // A replica holds a copy, so it counts against capacity just as
                // leadership does.
                Some(max) => load.get(node.node_id.as_str()).copied().unwrap_or(0) < max,
                None => true,
            })
            .max_by(|a, b| {
                score(key, &a.node_id)
                    .cmp(&score(key, &b.node_id))
                    .then_with(|| a.node_id.cmp(&b.node_id))
            })
        else {
            break;
        };
        *load.entry(node.node_id.as_str()).or_default() += 1;
        chosen.push(node.node_id.clone());
    }
    chosen
}

/// The best eligible follower that is caught up, if any.
///
/// Ties break on score then node id, the same way leadership does, so the choice
/// is a deterministic function of the shard and the cluster.
pub(super) fn promote<'a>(
    key: &ShardKey,
    previous: &ShardAssignment,
    eligible: &[&'a Node],
    caught_up: &dyn CaughtUp,
) -> Option<&'a str> {
    eligible
        .iter()
        .filter(|node| previous.replicas.iter().any(|r| r == &node.node_id))
        .filter(|node| caught_up.is_caught_up(key, &node.node_id))
        .max_by(|a, b| {
            // Furthest ahead first. Score only breaks ties, so a replica that
            // holds more is never passed over for one that merely scores
            // better — the failover would otherwise discard the difference.
            caught_up
                .reported_offset(key, &a.node_id)
                .cmp(&caught_up.reported_offset(key, &b.node_id))
                .then_with(|| score(key, &a.node_id).cmp(&score(key, &b.node_id)))
                .then_with(|| a.node_id.cmp(&b.node_id))
        })
        .map(|node| node.node_id.as_str())
}

/// Score a (shard, node) pair.
///
/// The shard key and the node id are hashed independently and then mixed,
/// rather than run through one hash over the concatenation. Both converge to an
/// even split eventually, but the single-pass form is badly behaved at the size
/// a cluster actually is: over 300 shards on four nodes it put 43 on one and 94
/// on another, a 43% deviation, against 8% for this. The difference is entirely
/// in the small-sample behaviour, which is the only sample there is.
///
/// FNV-1a is written out rather than taken from `DefaultHasher`: the standard
/// hasher's seeding is not part of its contract, and a placement decision that
/// changed with the Rust version -- or differed between two control-plane
/// instances -- would be silently catastrophic.
pub(super) fn score(key: &ShardKey, node_id: &str) -> u64 {
    /// Fractional part of the golden ratio, the usual choice for spreading a
    /// multiplicative mix across the whole word.
    const GOLDEN: u64 = 0x9e37_79b9_7f4a_7c15;

    // A stream contributes no kind field, so every score computed before caches
    // were placed is unchanged bit-for-bit: adding the field unconditionally
    // would reshuffle the placement of every stream in every fresh cluster for
    // no gain. A cache does contribute one, which is what stops a cache and a
    // stream of the same name from scoring identically and tracking each other
    // onto the same node forever.
    let shard = finalize(match key.kind {
        ShardKind::Stream => fnv1a(&[
            key.tenant_id.as_bytes(),
            key.namespace.as_bytes(),
            key.stream.as_bytes(),
            &key.shard.to_be_bytes(),
        ]),
        ShardKind::Cache => fnv1a(&[
            key.tenant_id.as_bytes(),
            key.namespace.as_bytes(),
            key.stream.as_bytes(),
            &key.shard.to_be_bytes(),
            key.kind.as_str().as_bytes(),
        ]),
    });
    let node = finalize(fnv1a(&[node_id.as_bytes()]));
    finalize(shard.wrapping_mul(GOLDEN) ^ node.rotate_left(32))
}

/// FNV-1a over several fields, with a separator between them so ("ab", "c") and
/// ("a", "bc") cannot collapse into the same byte stream.
fn fnv1a(fields: &[&[u8]]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = OFFSET;
    for field in fields {
        for byte in *field {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// splitmix64's finalizer. FNV-1a alone avalanches poorly on short inputs, and
/// rendezvous hashing needs the high bits well mixed or placement skews.
fn finalize(mut hash: u64) -> u64 {
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}
