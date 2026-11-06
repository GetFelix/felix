//! Random jitter for retry waits, which a simulator can make repeatable.
//!
//! A retry that waits a random time keeps two brokers from retrying in step.
//! In a real process the draw comes from the OS's per-process hash keys. A
//! simulation needs the same draws every run of a seed, so once it calls
//! [`sim::seed`] the thread it runs on draws from a generator seeded with that
//! seed instead. Builds without the simulator have no such generator.

use std::hash::{BuildHasher, Hasher};

/// A number from 0 to `max`, both included.
pub fn up_to(max: u64) -> u64 {
    let raw = draw();
    match max.checked_add(1) {
        Some(bound) => raw % bound,
        None => raw,
    }
}

pub(crate) fn draw() -> u64 {
    #[cfg(any(debug_assertions, test, feature = "sim"))]
    if let Some(drawn) = sim::next() {
        return drawn;
    }
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish()
}

/// Jitter drawn from a seed, for a simulator.
#[cfg(any(debug_assertions, test, feature = "sim"))]
pub mod sim {
    use std::cell::Cell;

    thread_local! {
        static STATE: Cell<Option<u64>> = const { Cell::new(None) };
    }

    /// Draw this thread's jitter from `seed` from now on. A simulation runs on
    /// one thread, so parallel runs on other threads keep their own sequences.
    pub fn seed(seed: u64) {
        STATE.with(|state| state.set(Some(seed)));
    }

    /// Whether this thread draws from a seeded generator.
    pub(crate) fn is_seeded() -> bool {
        STATE.with(|state| state.get().is_some())
    }

    /// The next draw of this thread's generator, if it has been seeded.
    pub(super) fn next() -> Option<u64> {
        STATE.with(|state| {
            let current = state.get()?;
            // splitmix64: one add and a mix per draw, every seed a full cycle.
            let next = current.wrapping_add(0x9e37_79b9_7f4a_7c15);
            state.set(Some(next));
            let mut z = next;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            Some(z ^ (z >> 31))
        })
    }
}

#[cfg(test)]
mod tests;
