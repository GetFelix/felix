use tempfile::tempdir;

use super::*;
use crate::DiskLog;
use crate::log::{FsyncMode, LogConfig};
use crate::raft_log::RaftLog;
use crate::raft_log::tests::{Codec, blank, normal, store};
use crate::segment::parse_segment_file_name;

/// A copy whose log holds an adopted base: a blank and two records.
async fn adopted() -> tempfile::TempDir {
    let dir = tempdir().expect("dir");
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        raft_log: true,
        ..LogConfig::default()
    };
    let log = DiskLog::open(dir.path(), "t/ns/raft/0", config).expect("open");
    let raft = RaftLog::new(log, Codec).expect("a Raft log");
    store(
        &raft,
        &[blank(2, 0), normal(2, 1, "one"), normal(2, 2, "two")],
    )
    .await
    .expect("store");
    raft.disk_log().close().await.expect("close");
    dir
}

fn segment_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .expect("dir")
        .map(|entry| entry.expect("entry"))
        .filter(|entry| {
            parse_segment_file_name(entry.file_name().to_str().expect("name")).is_some()
        })
        .map(|entry| entry.metadata().expect("meta").len())
        .sum()
}

const LONG: Duration = Duration::from_secs(3600);

/// A base asked to be dropped is gone at the next open, so the copy is
/// blank for the next pull.
#[tokio::test]
async fn a_requested_discard_empties_the_copy_at_its_next_open() {
    let dir = adopted().await;
    let held = segment_bytes(dir.path());
    assert!(held > crate::segment::format::SEGMENT_HEADER_LEN);
    assert_eq!(
        request_discard(dir.path(), 10, LONG).expect("request"),
        BaseDiscard::Requested(1)
    );
    assert_eq!(
        segment_bytes(dir.path()),
        held,
        "nothing goes before the open"
    );
    assert!(super::super::start::adopt_pending(dir.path()).is_ok());
    assert_eq!(segment_bytes(dir.path()), 0);
    assert!(!dir.path().join(DISCARD).exists());
    assert!(
        !discard_pending(dir.path()).expect("again"),
        "the marker is gone"
    );
}

/// A copy that took a snapshot since keeps its log: it caught up and may
/// hold acknowledged records.
#[tokio::test]
async fn a_copy_that_caught_up_keeps_its_log() {
    let dir = adopted().await;
    let held = segment_bytes(dir.path());
    request_discard(dir.path(), 10, LONG).expect("request");
    std::fs::write(dir.path().join(crate::raft_log::snapshot::FILE_NAME), b"").expect("snap");
    assert!(!discard_pending(dir.path()).expect("discard"));
    assert_eq!(segment_bytes(dir.path()), held);
    assert!(!dir.path().join(DISCARD).exists());
}

/// Each discard of an epoch waits longer than the last, the count lives
/// on disk, and an epoch gets a bounded number. A new epoch starts over.
#[test]
fn discards_back_off_and_are_bounded_per_epoch() {
    let dir = tempdir().expect("dir");
    for done in 0..MAX_BASE_DISCARDS {
        let wait = discard_wait(done);
        assert!(done == 0 || wait > discard_wait(done - 1) || wait == MAX_WAIT);
        assert_eq!(
            request_discard(dir.path(), 10, wait - Duration::from_millis(1)).expect("early"),
            BaseDiscard::Wait
        );
        assert_eq!(
            request_discard(dir.path(), 10, wait).expect("due"),
            BaseDiscard::Requested(done + 1)
        );
    }
    assert_eq!(
        request_discard(dir.path(), 10, LONG).expect("spent"),
        BaseDiscard::Exhausted
    );
    assert_eq!(discard_wait(40), MAX_WAIT);
    assert_eq!(
        request_discard(dir.path(), 20, LONG).expect("new epoch"),
        BaseDiscard::Requested(1)
    );
}
