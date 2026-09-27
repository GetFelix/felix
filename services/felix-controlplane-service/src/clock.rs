//! The process wall clock, in the milliseconds the metadata model stores.

/// Wall-clock milliseconds since the Unix epoch.
///
/// Read through `felix_common::clock`, the seam the broker's lease clock
/// shares, so a test can skew this process's view of time the same way.
/// Clamped at zero so a clock behind the epoch cannot panic the caller.
pub fn now_millis() -> u64 {
    felix_common::clock::wall_millis()
}
