//! The open-file limit.
//!
//! A shard's log keeps [`FDS_PER_SHARD_LOG`] files open for as long as the
//! broker holds the shard, so the limit caps the shards a broker can hold.
//! The sealed-segment cache (`FELIX_DURABLE_MAX_OPEN_SEALED_SEGMENTS`) and a
//! reserve for sockets come off the top. Cache and counter shards open a log
//! each too, so they count as shards here.
//! Most Linux systems start a process with a soft limit of 1,024, which a
//! full cache would take a quarter of, and a hard limit far above it. The broker takes the hard
//! limit at start, and says so when even that is small.

use felix_storage::log::{FDS_PER_SHARD_LOG, SealedFileCache};

/// The open-file limit this broker runs with.
pub(crate) const OPEN_FILE_LIMIT: &str = "felix_broker_open_file_limit";

/// Files kept back for everything that is not a held shard's log or a slot of
/// the sealed-segment cache: sockets, the runtime, files being replaced.
const SOCKET_RESERVE: u64 = 1024;
/// A limit that holds fewer shards than this is worth a warning. It is the
/// design point of the groups-per-broker gate.
/// The smallest cache the limit clamp leaves.
const MIN_SEALED_SLOTS: usize = 16;
const EXPECTED_SHARDS: u64 = 2_000;

/// Raise the soft open-file limit to the hard one, and report what the
/// broker is left with. Never lowers it and never fails the start: a
/// broker with few shards runs fine under a small limit.
pub(super) fn raise_limit(configured_slots: usize, cache: Option<&SealedFileCache>) {
    let limit = match felix_storage::open_files::raise_soft_limit() {
        Ok((before, limit)) => {
            if limit > before {
                tracing::info!(
                    before,
                    limit,
                    "raised the open-file limit to the hard limit"
                );
            }
            limit
        }
        Err((limit, err)) => {
            tracing::warn!(limit, %err, "could not raise the open-file limit");
            limit
        }
    };
    metrics::gauge!(OPEN_FILE_LIMIT).set(limit as f64);
    let sealed_slots = sealed_slots_within(limit, configured_slots);
    if sealed_slots < configured_slots {
        tracing::info!(
            limit,
            configured = configured_slots,
            effective = sealed_slots,
            "FELIX_DURABLE_MAX_OPEN_SEALED_SEGMENTS is more than a quarter of the open-file \
             limit, so the sealed-segment cache is held to {sealed_slots}"
        );
    }
    if let Some(cache) = cache {
        cache.set_capacity(sealed_slots);
    }
    let shards = shards_within(limit, sealed_slots);
    if shards < EXPECTED_SHARDS {
        tracing::warn!(
            limit,
            shards,
            "the open-file limit leaves room for about {shards} shards on this broker, and a \
             shard past that fails to open with \"Too many open files\"; raise the hard limit \
             (LimitNOFILE= in a systemd unit, ulimits.nofile in Compose, ulimit -Hn in a shell)"
        );
    }
}

/// The sealed-segment cache size a broker runs with under `limit` open files:
/// `configured`, held to a quarter of the limit (never under 16) so readers
/// cannot fill the descriptors that opens and accepts need.
pub(super) fn sealed_slots_within(limit: u64, configured: usize) -> usize {
    let quarter = usize::try_from(limit / 4).unwrap_or(usize::MAX);
    configured.min(quarter.max(MIN_SEALED_SLOTS))
}

/// The shards a broker can hold under `limit` open files, with a sealed
/// segment cache of `sealed_slots`.
pub(super) fn shards_within(limit: u64, sealed_slots: usize) -> u64 {
    limit
        .saturating_sub(SOCKET_RESERVE)
        .saturating_sub(sealed_slots as u64)
        / FDS_PER_SHARD_LOG as u64
}

#[cfg(test)]
mod tests;
