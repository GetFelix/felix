//! Which shard a routing key belongs to.
//!
//! Here, in the wire crate, because it is part of the protocol rather than an
//! implementation detail of either side. The broker decides which shard a
//! publish lands on; a client that routes its publishes to the shard's owner
//! has to reach the same answer, and two copies of a hash are two things that
//! can drift. One definition is the only way "the same key goes to the same
//! shard" survives a change to either side.
//!
//! A client's answer only has to be *self-consistent* to be useful -- it uses
//! the shard as a cache key for the owner it learned empirically, so a client
//! whose shard count is stale still routes to the right broker. Sharing the
//! function is about bounding that cache by shard count rather than by the
//! number of distinct keys.
//!
//! There are two mappings. [`ShardRouting::Modulo`] is `hash % shards`, which
//! every stream used before there was a choice and still uses unless it was
//! created asking otherwise. [`ShardRouting::JumpHash`] is jump consistent
//! hashing: growing a stream from `n - 1` to `n` shards moves about `1/n` of
//! the keys, where modulo moves nearly all of them. A stream's mapping is fixed
//! when it is created and recorded with it, so no existing stream is remapped.

use serde::{Deserialize, Serialize};

/// How a stream maps routing keys to shards. Chosen when the stream is
/// created and never changed afterwards.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardRouting {
    /// `hash(key) % shards`. The default, and the only mapping a stream
    /// created before routing modes existed can have.
    #[default]
    Modulo,
    /// Jump consistent hash (Lamping & Veach) of the same key hash.
    JumpHash,
}

impl ShardRouting {
    /// True for the original mapping, which is what an absent field means on
    /// the wire and in stored metadata.
    pub fn is_modulo(&self) -> bool {
        *self == Self::Modulo
    }
}

/// Map a routing key to a shard number with [`ShardRouting::Modulo`].
///
/// Deterministic and pure, so the same key lands on the same shard on every
/// broker, in every client, and across restarts.
///
/// No key means shard 0: an unkeyed publish has nothing to hash, so a
/// multi-shard stream published to without keys puts everything on one shard.
/// That is the behaviour, not a defect -- the key is what spreads records.
pub fn shard_for(shards: u32, routing_key: Option<&[u8]>) -> u32 {
    if shards <= 1 {
        return 0;
    }
    match routing_key {
        // FNV-1a with a finalizer, written out rather than taken from a hasher
        // so the mapping cannot shift with a toolchain change. The same
        // construction placement uses.
        Some(key) => (finalize(fnv1a(key)) % u64::from(shards)) as u32,
        None => 0,
    }
}

/// Map a routing key to a shard number with the stream's own mapping.
///
/// No key is shard 0 under either mapping, and so is every key of a stream
/// with one shard.
pub fn shard_for_routing(routing: ShardRouting, shards: u32, routing_key: Option<&[u8]>) -> u32 {
    match (routing, routing_key) {
        (ShardRouting::Modulo, _) => shard_for(shards, routing_key),
        (ShardRouting::JumpHash, Some(key)) if shards > 1 => {
            jump_hash(finalize(fnv1a(key)), shards)
        }
        (ShardRouting::JumpHash, _) => 0,
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

fn finalize(mut hash: u64) -> u64 {
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

/// Jump consistent hash, as published: the constants and the float arithmetic
/// are the paper's, so any implementation of it gives these answers.
fn jump_hash(mut key: u64, buckets: u32) -> u32 {
    let mut bucket: i64 = -1;
    let mut next: i64 = 0;
    while next < i64::from(buckets) {
        bucket = next;
        key = key.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(1);
        next = ((bucket + 1) as f64 * ((1u64 << 31) as f64 / ((key >> 33) + 1) as f64)) as i64;
    }
    bucket as u32
}

#[cfg(test)]
mod tests;
