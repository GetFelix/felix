//! Retiring a stream finds every directory of it, including one an open that
//! failed part-way left behind after the directory index was read.
//!
//! Its own test binary because the injected flush failure is process-wide.

use felix_storage::DiskLogProvider;
use felix_storage::disk_log::layout;
use felix_storage::fault::{FsyncFailure, set_fsync_failure};
use felix_storage::log::{FsyncMode, LogConfig, ShardKey};

#[tokio::test]
async fn a_directory_a_failed_open_left_is_retired() {
    let dir = tempfile::tempdir().expect("dir");
    let provider = DiskLogProvider::new(
        dir.path(),
        LogConfig {
            fsync_mode: FsyncMode::OnCommit,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("provider");
    provider.set_stream_incarnation("t", "ns", "orders", Some(7));
    // The index is read now, before the directory exists.
    assert!(
        provider
            .stream_incarnations_on_disk("t", "ns", "orders")
            .expect("scan")
            .is_empty()
    );

    let key = ShardKey {
        tenant: "t".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: 0,
    };
    set_fsync_failure(FsyncFailure::Always);
    let opened = provider.open_shard(&key);
    set_fsync_failure(FsyncFailure::None);
    assert!(opened.is_err(), "the open was meant to fail");
    let left = layout::incarnation_dir(dir.path(), &key, 7);
    assert!(left.exists(), "the failed open left no directory to find");

    provider
        .retire_stream("t", "ns", "orders", 7)
        .await
        .expect("retire");
    assert!(!left.exists(), "a directory left by a failed open was kept");
}
