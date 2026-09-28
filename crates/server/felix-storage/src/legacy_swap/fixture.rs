//! The pre-0.6 compaction swap, ported so a test can stop it at every step.
//!
//! The renames, syncs and names are the removed `log_swap::swap_in_compacted`
//! and the staging setup that preceded it, unchanged.

use std::path::Path;

use crate::DiskLog;
use crate::io::sync_dir;
use crate::log::AppendRecord;
use crate::log::LogConfig;

/// Where the old swap stopped. Each is a state a crash could leave on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stop {
    /// Half the live set written to `.compacting`.
    StagingHalfWritten,
    /// `.compacting` complete, nothing renamed yet.
    StagingWritten,
    /// The live directory renamed to `.retired`; the shard directory is missing.
    LiveRetired,
    /// The compacted log renamed into place; `.retired` still there.
    CompactedIn,
    /// Part way through deleting `.retired`.
    RetiredHalfDeleted,
    /// The swap finished.
    Done,
}

impl Stop {
    pub(crate) const ALL: [Stop; 6] = [
        Stop::StagingHalfWritten,
        Stop::StagingWritten,
        Stop::LiveRetired,
        Stop::CompactedIn,
        Stop::RetiredHalfDeleted,
        Stop::Done,
    ];
}

/// Run the old compaction of the (shut down) shard log in `dir` up to `stop`,
/// writing `live` as its compacted records.
pub(crate) async fn old_swap(dir: &Path, config: LogConfig, live: Vec<bytes::Bytes>, stop: Stop) {
    let staging = dir.with_extension("compacting");
    let retired = dir.with_extension("retired");

    let resume_at = {
        let log = DiskLog::open(dir, "legacy", config.clone()).expect("open live");
        let tail = crate::log::AppendOnlyLog::tail_offset(&log)
            .await
            .expect("tail");
        log.shutdown().await.expect("shutdown live");
        tail
    };
    let fresh = DiskLog::open_at(&staging, "legacy", config, resume_at).expect("open staging");
    let half = live.len() / 2;
    let upto = if stop == Stop::StagingHalfWritten {
        half
    } else {
        live.len()
    };
    for payload in live.into_iter().take(upto) {
        crate::log::AppendOnlyLog::append(
            &fresh,
            &[AppendRecord {
                payload,
                timestamp_micros: 0,
                mark: Default::default(),
            }],
        )
        .await
        .expect("append staging");
    }
    fresh.shutdown().await.expect("shutdown staging");
    if matches!(stop, Stop::StagingHalfWritten | Stop::StagingWritten) {
        return;
    }

    let parent = dir.parent().expect("parent");
    std::fs::rename(dir, &retired).expect("retire live");
    sync_dir(parent).expect("sync");
    if stop == Stop::LiveRetired {
        return;
    }
    std::fs::rename(&staging, dir).expect("swap in");
    sync_dir(parent).expect("sync");
    if stop == Stop::CompactedIn {
        return;
    }
    if stop == Stop::RetiredHalfDeleted {
        let mut files: Vec<_> = std::fs::read_dir(&retired)
            .expect("read retired")
            .map(|entry| entry.expect("entry").path())
            .collect();
        files.sort();
        for file in files.iter().take(files.len().div_ceil(2)) {
            std::fs::remove_file(file).expect("delete retired file");
        }
        return;
    }
    std::fs::remove_dir_all(&retired).expect("delete retired");
}

/// Neither swap sibling of `dir` is left.
pub(crate) fn assert_no_siblings(dir: &Path, when: &str) {
    for ext in ["retired", "compacting"] {
        let sibling = dir.with_extension(ext);
        assert!(
            !sibling.exists(),
            "{} still exists {when}",
            sibling.display()
        );
    }
}
