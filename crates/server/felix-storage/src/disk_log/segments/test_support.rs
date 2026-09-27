//! A small segment set in a temp directory, for the segments tests.

use bytes::Bytes;
use tempfile::TempDir;

use super::SegmentSet;
use crate::log::{AppendRecord, FsyncMode, LogConfig, Offset};
use crate::segment::{ReadBudget, SegmentWriter};

pub(super) fn record(payload: &str) -> AppendRecord {
    AppendRecord {
        payload: Bytes::copy_from_slice(payload.as_bytes()),
        timestamp_micros: 7,
        mark: Default::default(),
    }
}

pub(super) fn config(segment_size_bytes: u64) -> LogConfig {
    LogConfig {
        segment_size_bytes,
        index_spacing_bytes: 64,
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

pub(super) fn new_set(dir: &TempDir, segment_size_bytes: u64) -> SegmentSet {
    let config = config(segment_size_bytes);
    let active =
        SegmentWriter::create(dir.path(), 0, 0, 1, 0, config.index_spacing_bytes).expect("create");
    SegmentSet::new(
        dir.path().to_path_buf(),
        "t/ns/s/0".to_string(),
        config,
        Vec::new(),
        active,
        crate::disk_log::sealed::SealedFiles::new(16),
    )
    .expect("set")
}

pub(super) fn read_all(set: &SegmentSet, start: Offset) -> Vec<String> {
    set.read(start, ReadBudget::unbounded())
        .expect("read")
        .into_iter()
        .map(|record| String::from_utf8(record.payload.to_vec()).expect("utf8"))
        .collect()
}
