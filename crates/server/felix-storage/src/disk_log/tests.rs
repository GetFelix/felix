//! End-to-end behaviour of `DiskLog` through the `AppendOnlyLog` trait: the
//! durability guarantee each fsync mode actually delivers, restart semantics,
//! and the bounds on a range read.

mod append_and_read;
mod durability;
mod placed_at_a_base_offset;
#[cfg(target_os = "linux")]
mod power_loss;
mod producers;
mod provider;
mod replica_state;
mod restore;
mod retention;
mod rollover;
mod truncation;

use std::time::Duration;

use bytes::Bytes;
use tempfile::{TempDir, tempdir};

use super::*;
use crate::log::{AppendOnlyLog, FsyncMode, LogProvider, ShardKey};

fn record(payload: &str) -> AppendRecord {
    AppendRecord {
        payload: Bytes::copy_from_slice(payload.as_bytes()),
        timestamp_micros: 1_700_000_000,
        mark: Default::default(),
    }
}

fn records(payloads: &[&str]) -> Vec<AppendRecord> {
    payloads.iter().map(|p| record(p)).collect()
}

/// A small-segment config so rollover is exercised without writing megabytes.
fn config(fsync_mode: FsyncMode) -> LogConfig {
    LogConfig {
        segment_size_bytes: crate::segment::SEGMENT_HEADER_LEN + 120,
        index_spacing_bytes: 48,
        fsync_mode,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn open(dir: &TempDir, fsync_mode: FsyncMode) -> DiskLog {
    DiskLog::open(dir.path(), "t/ns/s/0", config(fsync_mode)).expect("open")
}

async fn read_all(log: &DiskLog, start: Offset) -> Vec<String> {
    log.read_range(ReadRange {
        start,
        max_bytes: usize::MAX,
    })
    .await
    .expect("read")
    .into_iter()
    .map(|r| String::from_utf8(r.payload.to_vec()).expect("utf8"))
    .collect()
}
