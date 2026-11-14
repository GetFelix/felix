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

use std::io;

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

/// macOS's `OPEN_MAX` (`sys/syslimits.h`), which the `libc` crate does not
/// export: the soft limit it always accepts.
#[cfg(target_os = "macos")]
const OPEN_MAX: u64 = 10_240;

/// Raise the soft open-file limit to the hard one, and report what the
/// broker is left with. Never lowers it and never fails the start: a
/// broker with few shards runs fine under a small limit.
pub(super) fn raise_limit(configured_slots: usize, cache: Option<&SealedFileCache>) {
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

/// The soft limit to ask for, given the current pair and the most the
/// platform grants one process.
fn target(soft: u64, hard: u64, ceiling: u64) -> u64 {
    soft.max(hard.min(ceiling))
}

/// macOS reports an unlimited hard limit and refuses a soft limit above
/// `kern.maxfilesperproc`, which sits well over `OPEN_MAX`.
#[cfg(target_os = "macos")]
fn ceiling() -> u64 {
    let mut value: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>();
    // SAFETY: the name is NUL-terminated, and `value` and `len` describe a
    // buffer the call fills.
    let rc = unsafe {
        libc::sysctlbyname(
            c"kern.maxfilesperproc".as_ptr(),
            (&raw mut value).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc == 0 && value > 0 {
        value as u64
    } else {
        OPEN_MAX
    }
}

#[cfg(target_os = "linux")]
fn ceiling() -> u64 {
    u64::MAX
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn limits() -> io::Result<libc::rlimit> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid `rlimit` for the call to fill.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(limit)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn set_soft(limit: &libc::rlimit, soft: u64) -> io::Result<()> {
    let wanted = libc::rlimit {
        rlim_cur: soft as libc::rlim_t,
        rlim_max: limit.rlim_max,
    };
    // SAFETY: `wanted` is a valid `rlimit`, the one read with its soft value
    // changed to at most the hard one.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &wanted) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// `(before, after)` soft limits, or the limit still in force and why it
/// could not be raised. `after` is what the kernel reports once the change
/// is made, not what was asked for.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn raise() -> Result<(u64, u64), (u64, io::Error)> {
    let limit = limits().map_err(|err| (0, err))?;
    let before = limit.rlim_cur as u64;
    let wanted = target(before, limit.rlim_max as u64, ceiling());
    if wanted > before {
        let result = set_soft(&limit, wanted);
        // The ceiling is a guess on some systems; fall back to the one
        // number macOS always accepts.
        #[cfg(target_os = "macos")]
        let result = if result.is_err() && wanted > OPEN_MAX {
            set_soft(&limit, OPEN_MAX.max(before))
        } else {
            result
        };
        if let Err(err) = result {
            return Err((before, err));
        }
    }
    let after = limits().map_err(|err| (before, err))?;
    Ok((before, after.rlim_cur as u64))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn raise() -> Result<(u64, u64), (u64, io::Error)> {
    Ok((u64::MAX, u64::MAX))
}

#[cfg(test)]
mod tests;
