use std::error::Error;

use super::*;

#[test]
fn storage_error_display() {
    let err = StorageError::Unsupported("feature");
    assert!(err.to_string().contains("feature"));

    let err = StorageError::InvalidRange;
    assert!(err.to_string().contains("invalid range"));

    let err = StorageError::NotFound;
    assert!(err.to_string().contains("not found"));

    let err = StorageError::Corruption(Corruption::new(CorruptionKind::IndexVersion { found: 9 }));
    assert!(err.to_string().contains("corruption"));
    assert!(err.to_string().contains("unsupported index version 9"));

    let err = StorageError::SyncFailed("disk full".into());
    assert!(err.to_string().contains("disk full"));
}

#[test]
fn storage_error_from_io_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let storage_err = StorageError::from(io_err);
    assert!(matches!(storage_err, StorageError::Io(_)));
}

#[test]
fn storage_error_source() {
    let io_err = std::io::Error::other("test");
    let storage_err = StorageError::from(io_err);
    assert!(storage_err.source().is_some());

    let storage_err = StorageError::NotFound;
    assert!(storage_err.source().is_none());
}

/// **Every storage error names the values an operator needs to act on it**:
/// the offsets, generations, shard and directory involved, and for a poisoned
/// log, that it refuses writes until reopened.
#[test]
fn storage_errors_name_what_they_refused() {
    let cases: Vec<(StorageError, &[&str])> = vec![
        (
            StorageError::InvalidConfig("segment size is 0"),
            &["invalid configuration", "segment size is 0"],
        ),
        (
            StorageError::Trimmed {
                requested: 5,
                oldest: 40,
                trim_headers: None,
            },
            &["offset 5 is no longer available", "starts at 40"],
        ),
        (
            StorageError::Poisoned {
                shard: "t/ns/s/0".into(),
                reason: "a sync failed".into(),
            },
            &[
                "t/ns/s/0",
                "a sync failed",
                "refuses writes until it is reopened",
            ],
        ),
        (
            StorageError::BelowCommit {
                offset: 7,
                commit: 9,
            },
            &["from offset 7", "below 9 is committed"],
        ),
        (
            StorageError::OutsideLog {
                offset: 50,
                base: 10,
                tail: 20,
            },
            &["offset 50", "[10, 20)"],
        ),
        (
            StorageError::Closed("t/ns/s/0".into()),
            &["the log for t/ns/s/0 is closed"],
        ),
        (
            StorageError::ShardMismatch {
                dir: "/data/x".into(),
                expected: "t/ns/a/0".into(),
                found: "t/ns/b/0".into(),
            },
            &["/data/x belongs to t/ns/b/0, not t/ns/a/0", "refusing"],
        ),
        (
            StorageError::GenerationBelowFloor {
                offset: 12,
                generation: 3,
                floor: 4,
            },
            &["record at 12", "generation 3, below 4"],
        ),
        (
            StorageError::WriterUnknown("no generation record"),
            &["cannot name the generation", "no generation record"],
        ),
        (
            StorageError::Full(std::io::Error::other("no space left")),
            &["storage full", "no space left"],
        ),
        (
            StorageError::OpenFileLimit(std::io::Error::other("quota")),
            &["open-file limit: quota"],
        ),
    ];
    for (err, needles) in cases {
        let message = err.to_string();
        for needle in needles {
            assert!(message.contains(needle), "{needle:?} not in {message:?}");
        }
    }
}

/// **An exhausted open-file limit says how to recover**, whether it arrived as
/// an open-file error or a plain I/O one, and the I/O error stays the source.
#[cfg(unix)]
#[test]
fn an_exhausted_file_limit_says_how_to_recover() {
    for err in [
        StorageError::OpenFileLimit(std::io::Error::from_raw_os_error(libc::EMFILE)),
        StorageError::Io(std::io::Error::from_raw_os_error(libc::ENFILE)),
    ] {
        let message = err.to_string();
        assert!(
            message.contains("open-file limit is exhausted"),
            "{message}"
        );
        assert!(message.contains("RLIMIT_NOFILE"), "{message}");
        assert!(err.source().is_some());
    }
    let full = StorageError::Full(std::io::Error::other("disk"));
    assert!(full.source().is_some());
}
