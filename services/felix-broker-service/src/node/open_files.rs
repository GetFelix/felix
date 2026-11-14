//! The open-file limit.
//!
//! A shard's log keeps [`FDS_PER_SHARD_LOG`] files open for as long as the
//! broker holds the shard, so the limit caps the shards a broker can hold.
//! Most systems start a process with a soft limit of 1,024, which is under
//! 200 shards, and a hard limit far above it. The broker takes the hard
//! limit at start, and says so when even that is small.

use std::io;

use felix_storage::log::FDS_PER_SHARD_LOG;

/// The open-file limit this broker runs with.
pub const OPEN_FILE_LIMIT: &str = "felix_broker_open_file_limit";

/// Files kept back for everything that is not a held shard's log: sockets,
/// the runtime, segments being read, files being replaced.
const RESERVED: u64 = 512;
/// A limit that holds fewer shards than this is worth a warning. It is the
/// design point of the groups-per-broker gate.
const EXPECTED_SHARDS: u64 = 2_000;

/// Raise the soft open-file limit to the hard one, and report what the
/// broker is left with. Never lowers it and never fails the start: a
/// broker with few shards runs fine under a small limit.
pub(super) fn raise_limit() {
    let limit = match raise() {
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
    let shards = shards_within(limit);
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

/// The shards a broker can hold under `limit` open files.
pub(super) fn shards_within(limit: u64) -> u64 {
    limit.saturating_sub(RESERVED) / FDS_PER_SHARD_LOG as u64
}

/// The soft limit to ask for, given the current pair.
fn target(soft: u64, hard: u64) -> u64 {
    // macOS reports an unlimited hard limit and refuses a soft limit above
    // `OPEN_MAX`.
    #[cfg(target_os = "macos")]
    let hard = hard.min(libc::OPEN_MAX as u64);
    soft.max(hard)
}

/// `(before, after)` soft limits, or the limit still in force and why it
/// could not be raised.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn raise() -> Result<(u64, u64), (u64, io::Error)> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid `rlimit` for the call to fill.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err((0, io::Error::last_os_error()));
    }
    let before = limit.rlim_cur;
    let wanted = target(before, limit.rlim_max);
    if wanted == before {
        return Ok((before, before));
    }
    limit.rlim_cur = wanted;
    // SAFETY: `limit` is a valid `rlimit`, read above and changed only in
    // its soft value, which stays at or under the hard one.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
        return Err((before, io::Error::last_os_error()));
    }
    Ok((before, wanted))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn raise() -> Result<(u64, u64), (u64, io::Error)> {
    Ok((u64::MAX, u64::MAX))
}

#[cfg(test)]
mod tests;
