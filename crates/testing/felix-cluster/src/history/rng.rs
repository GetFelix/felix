//! A small seeded generator for the workload and the nemesis.
//!
//! SplitMix64, written out rather than pulled in: the seed a failing run
//! prints has to mean the same schedule on every machine and toolchain, and a
//! dependency's algorithm is free to change under a version bump.

use std::time::Duration;

/// A deterministic stream of numbers from one seed.
#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A number in `0..n`. `n` must not be zero.
    pub fn below(&mut self, n: u64) -> u64 {
        // Modulo bias is irrelevant at the sizes a campaign picks from.
        self.next_u64() % n
    }

    /// True with probability `percent` / 100.
    pub fn percent(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    /// A duration uniformly in `[low, high]`, to the millisecond.
    pub fn between(&mut self, low: Duration, high: Duration) -> Duration {
        let low_ms = low.as_millis() as u64;
        let span = (high.as_millis() as u64).saturating_sub(low_ms);
        Duration::from_millis(low_ms + self.below(span + 1))
    }

    /// One element of `items`, which must not be empty.
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }

    /// An independent generator, so each client's choices do not depend on
    /// how many numbers the others drew.
    pub fn fork(&mut self) -> Rng {
        Rng::new(self.next_u64())
    }
}

#[cfg(test)]
mod tests;
