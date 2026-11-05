//! Which shards took a durable append since replication last looked.

use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Bits in the set, as words.
const WORDS: usize = 64;

/// Wakes replication after a durable append, and says which shards it was for.
///
/// A shard is a bit chosen by hashing its name ([`AppendedShards::slot`]), set
/// with one atomic `or` per append: no lock, no allocation. Two shards that
/// hash to the same bit each get a pass when either is appended to, which
/// costs a pass and nothing else. A bit is never missed: it is set before the
/// wake, and the driver reads the set after waking.
#[derive(Debug)]
pub struct AppendedShards {
    wake: Arc<tokio::sync::Notify>,
    bits: [Word; WORDS],
}

/// One word of the set on its own cache line, so publishers to shards in
/// different words do not contend. 128 bytes covers Apple silicon's line and
/// x86's paired-line prefetch.
#[derive(Debug, Default)]
#[repr(align(128))]
struct Word(AtomicU64);

/// The bits [`AppendedShards::take`] cleared.
#[derive(Debug, Clone, Copy)]
pub struct Appended([u64; WORDS]);

impl Default for AppendedShards {
    fn default() -> Self {
        Self {
            wake: Arc::new(tokio::sync::Notify::new()),
            bits: std::array::from_fn(|_| Word::default()),
        }
    }
}

impl AppendedShards {
    /// The bit for a shard. Kind-blind, so a stream's group logs and a cache's
    /// counter log share their shard's bit.
    pub fn slot(tenant_id: &str, namespace: &str, name: &str, shard: u32) -> u16 {
        // Fixed keys, so every caller in the process agrees on a shard's bit.
        let mut hasher = std::hash::DefaultHasher::new();
        (tenant_id, namespace, name, shard).hash(&mut hasher);
        (hasher.finish() % (WORDS as u64 * 64)) as u16
    }

    /// `slot`'s shard took an append: mark it, and wake replication if it was
    /// not already marked.
    pub fn mark(&self, slot: u16) {
        let slot = usize::from(slot);
        let bit = 1 << (slot % 64);
        // Wake only when this append set the bit. If it was already set, its
        // setter has woken or is about to wake the driver, and the `take` that
        // clears it comes after this `or`, so it still covers this append.
        // That keeps the shared `Notify` off the hot path of a busy shard.
        if self.bits[slot / 64].0.fetch_or(bit, Ordering::Release) & bit == 0 {
            // `notify_one`, not `notify_waiters`: it leaves a permit when the
            // driver is mid-pass, and stores only one, so a burst becomes a
            // single extra scan rather than a storm.
            self.wake.notify_one();
        }
    }

    /// What replication waits on.
    pub fn wake(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.wake)
    }

    /// Take every bit set since the last call.
    pub fn take(&self) -> Appended {
        let mut taken = [0; WORDS];
        for (word, bits) in taken.iter_mut().zip(&self.bits) {
            // Most words are clear; a load keeps a scan from writing to them.
            if bits.0.load(Ordering::Relaxed) != 0 {
                *word = bits.0.swap(0, Ordering::Acquire);
            }
        }
        Appended(taken)
    }
}

impl Appended {
    /// Whether `slot`'s shard took an append.
    pub fn contains(&self, slot: u16) -> bool {
        let slot = usize::from(slot);
        self.0[slot / 64] & (1 << (slot % 64)) != 0
    }

    /// Every slot that took an append, in slot order.
    pub fn slots(&self) -> impl Iterator<Item = u16> + '_ {
        self.0.iter().enumerate().flat_map(|(word, &bits)| {
            let mut bits = bits;
            std::iter::from_fn(move || {
                if bits == 0 {
                    return None;
                }
                let bit = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                Some((word * 64 + bit) as u16)
            })
        })
    }
}

#[cfg(test)]
mod tests;
