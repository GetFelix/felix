//! The cache behaving as a cache, and as a log.
//!
//! The headline is `a_cache_survives_a_restart`: it is what "the cache is a
//! log" buys, and what the in-memory cache could never do.

mod basics;
mod closing;
mod compaction;
mod concurrency;
mod expiry;
mod observer;

use std::time::Duration;

use super::*;
use crate::log::AppendOnlyLog;

const T: &str = "t1";
const NS: &str = "ns";
const C: &str = "sessions";

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 64 * 1024,
        index_spacing_bytes: 256,
        fsync_mode: crate::log::FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

async fn cache(dir: &std::path::Path) -> LogCache {
    LogCache::open(dir, config()).expect("open")
}

/// Collects every change it is shown, in the order shown.
#[derive(Debug, Default)]
struct RecordingObserver {
    changes: parking_lot::Mutex<Vec<CacheChange>>,
}

impl CacheObserver for RecordingObserver {
    fn cache_changed(&self, change: CacheChange) {
        self.changes.lock().push(change);
    }
}
