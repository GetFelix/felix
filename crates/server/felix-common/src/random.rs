//! Random ids and hash keys, which a simulator can make repeatable.
//!
//! A real process draws them from the OS. A simulation needs the same ids and
//! the same map layouts every run of a seed, so once it calls
//! [`crate::jitter::sim::seed`] these draw from that thread's seeded
//! generator instead. Builds without the simulator never do.

use uuid::Uuid;

/// A random version 4 UUID.
pub fn uuid_v4() -> Uuid {
    #[cfg(any(debug_assertions, test, feature = "sim"))]
    if crate::jitter::sim::is_seeded() {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&crate::jitter::draw().to_le_bytes());
        bytes[8..].copy_from_slice(&crate::jitter::draw().to_le_bytes());
        return uuid::Builder::from_random_bytes(bytes).into_uuid();
    }
    Uuid::new_v4()
}

/// A hasher state for the maps on hot paths. Random per map in a real
/// process; fixed by the seed under a simulator, so a map iterates in the
/// same order every run.
#[cfg(feature = "hasher")]
pub fn hash_state() -> ahash::RandomState {
    #[cfg(any(debug_assertions, test, feature = "sim"))]
    if crate::jitter::sim::is_seeded() {
        return ahash::RandomState::with_seeds(
            crate::jitter::draw(),
            crate::jitter::draw(),
            crate::jitter::draw(),
            crate::jitter::draw(),
        );
    }
    ahash::RandomState::new()
}

#[cfg(test)]
mod tests;
