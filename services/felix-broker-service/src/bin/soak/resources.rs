//! Process resource sampling for the soak harness.
//!
//! Deliberately dependency-free and best-effort: a sampler that fails to read a
//! counter must not abort a soak run, so every reader falls back to 0 and the
//! harness treats a flat-zero series as "unavailable on this platform" rather
//! than as evidence of anything.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// One point in the resource time series.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ResourceSample {
    pub unix_ms: u128,
    pub rss_kb: u64,
    pub open_fds: u64,
    pub alive_tasks: usize,
}

impl ResourceSample {
    pub(crate) fn capture() -> Self {
        Self {
            unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0),
            rss_kb: read_rss_kb(),
            open_fds: count_open_fds(),
            alive_tasks: tokio::runtime::Handle::try_current()
                .map(|handle| handle.metrics().num_alive_tasks())
                .unwrap_or(0),
        }
    }
}

/// Resident set size in KiB.
///
/// Linux exposes this in `/proc/self/statm` as a page count. macOS has no
/// equivalent procfs entry, so we shell out to `ps` — slow, but the sampler
/// runs twice a second, not in a hot loop.
#[cfg(target_os = "linux")]
pub(crate) fn read_rss_kb() -> u64 {
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
        return 0;
    };
    // Field 2 is resident pages.
    let Some(resident_pages) = statm.split_whitespace().nth(1) else {
        return 0;
    };
    let pages: u64 = resident_pages.parse().unwrap_or(0);
    let page_kb = 4; // Linux page size on every platform Felix targets.
    pages * page_kb
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn read_rss_kb() -> u64 {
    let pid = std::process::id();
    let Ok(output) = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
    else {
        return 0;
    };
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap_or(0)
}

/// Allocator view of the heap, in KiB: bytes the program holds live, and bytes
/// the allocator keeps resident. Live growing across identical cycles is a
/// leak; resident growing with live flat is allocator retention.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct HeapStats {
    pub live_kb: u64,
    pub resident_kb: u64,
}

#[cfg(all(unix, feature = "soak-jemalloc"))]
pub(crate) fn heap_stats() -> HeapStats {
    use tikv_jemalloc_ctl::{epoch, stats};
    if epoch::advance().is_err() {
        return HeapStats::default();
    }
    HeapStats {
        live_kb: stats::allocated::read().unwrap_or(0) as u64 / 1024,
        resident_kb: stats::resident::read().unwrap_or(0) as u64 / 1024,
    }
}

/// glibc: `uordblks` plus mmapped chunks is live; `arena` plus mmapped is held.
#[cfg(all(
    target_os = "linux",
    target_env = "gnu",
    not(feature = "soak-jemalloc")
))]
pub(crate) fn heap_stats() -> HeapStats {
    // SAFETY: `mallinfo2` has no preconditions and returns a plain struct.
    let info = unsafe { libc::mallinfo2() };
    HeapStats {
        live_kb: (info.uordblks + info.hblkhd) as u64 / 1024,
        resident_kb: (info.arena + info.hblkhd) as u64 / 1024,
    }
}

#[cfg(not(any(
    feature = "soak-jemalloc",
    all(target_os = "linux", target_env = "gnu")
)))]
pub(crate) fn heap_stats() -> HeapStats {
    HeapStats::default()
}

/// Write a jemalloc heap profile of live allocations to `path`.
#[cfg(feature = "soak-jemalloc-prof")]
pub(crate) fn dump_heap_profile(path: &str) {
    let Ok(path) = std::ffi::CString::new(path) else {
        return;
    };
    // SAFETY: `prof.dump` takes a NUL-terminated path that outlives the call.
    let result = unsafe { tikv_jemalloc_ctl::raw::write(b"prof.dump\0", path.as_ptr()) };
    println!("  heap profile dump: {result:?}");
}

#[cfg(not(feature = "soak-jemalloc-prof"))]
pub(crate) fn dump_heap_profile(_path: &str) {}

/// Ask glibc to return free arena pages to the OS. Used under
/// `SOAK_TRIM_AFTER_CYCLE` to show how much RSS is only retained, not live.
pub(crate) fn trim_heap() {
    #[cfg(all(
        target_os = "linux",
        target_env = "gnu",
        not(feature = "soak-jemalloc")
    ))]
    // SAFETY: `malloc_trim` has no preconditions.
    unsafe {
        libc::malloc_trim(0);
    }
}

/// Extract gauge values from rendered Prometheus text.
///
/// Only gauges are collected: counters grow monotonically by design and say
/// nothing about whether resources were released, whereas a gauge that fails to
/// return to zero after quiescence is exactly the leak signal we want. Labelled
/// series are summed across labels, since "any connection still registered" is
/// the question, not which one.
pub(crate) fn scrape_gauges(rendered: &str) -> HashMap<String, f64> {
    let mut gauge_names = Vec::new();
    let mut values: HashMap<String, f64> = HashMap::new();

    for line in rendered.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let mut parts = rest.split_whitespace();
            if let (Some(name), Some(kind)) = (parts.next(), parts.next())
                && kind == "gauge"
            {
                gauge_names.push(name.to_string());
            }
            continue;
        }
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let Some((key, raw_value)) = line.rsplit_once(' ') else {
            continue;
        };
        let name = key.split('{').next().unwrap_or(key).trim();
        if !gauge_names.iter().any(|g| g == name) {
            continue;
        }
        if let Ok(value) = raw_value.trim().parse::<f64>() {
            *values.entry(name.to_string()).or_insert(0.0) += value;
        }
    }
    values
}

/// Count of open file descriptors.
///
/// `/dev/fd` is present on both Linux (symlinked to `/proc/self/fd`) and macOS,
/// so one path covers both. The directory handle opened to read it is itself an
/// fd, but it is opened and closed identically on every sample, so it cancels
/// out of any comparison.
fn count_open_fds() -> u64 {
    let Ok(entries) = std::fs::read_dir("/dev/fd") else {
        return 0;
    };
    entries.count() as u64
}

#[cfg(test)]
mod tests;
