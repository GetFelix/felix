//! Incarnations: which creation of a name a stream or cache is.

/// The incarnation a stream or cache created once its incarnations are
/// finalized gets: the `proposed` one (the creating request's clock, in
/// microseconds), raised above the name's tombstone. Above every earlier
/// incarnation of the name either way, and above any given before a restore
/// of the control plane to an earlier point, which rewinds the tombstones but
/// not the clock. Never 0.
pub(crate) fn next_incarnation(proposed: u64, deleted_through: Option<u64>) -> u64 {
    proposed.max(deleted_through.map_or(1, |deleted| deleted.saturating_add(1)))
}

/// A proposal for [`next_incarnation`]: this clock in microseconds, below
/// 2^53 so it reads back exactly as a JSON number anywhere.
pub(crate) fn proposed_incarnation() -> u64 {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_micros() as u64)
        .unwrap_or(1);
    micros.clamp(1, (1 << 53) - 1)
}
