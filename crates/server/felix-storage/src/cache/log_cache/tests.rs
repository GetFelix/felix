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
#[cfg(target_os = "linux")]
mod power_loss;

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

/// Put `keys` keys, then overwrite each once, so every key's first record is
/// garbage. Returns the value each key should read back.
async fn overwritten(cache: &LogCache, keys: usize) -> Vec<(String, Bytes)> {
    let mut expected = Vec::with_capacity(keys);
    for round in 0..2u8 {
        for i in 0..keys {
            let key = format!("k{i}");
            let value = Bytes::from(vec![round; 512 + i]);
            cache
                .put_checked(T, NS, C, 0, &key, value.clone(), None)
                .await
                .expect("put");
            if round == 1 {
                expected.push((key, value));
            }
        }
    }
    expected
}

async fn assert_reads(cache: &LogCache, expected: &[(String, Bytes)], when: &str) {
    for (key, value) in expected {
        let found = cache.get_checked(T, NS, C, 0, key).await.expect("get");
        // Compared whole, reported by shape: the values are hundreds of bytes.
        assert!(
            found.as_ref() == Some(value),
            "{key} {when}: read {:?}, wanted {} bytes of {}",
            found.map(|v| (v.len(), v.first().copied())),
            value.len(),
            value[0],
        );
    }
}
