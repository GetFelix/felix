use super::*;

#[test]
fn the_target_is_the_hard_limit_and_never_below_the_soft_one() {
    assert_eq!(target(1024, 4096, u64::MAX), 4096);
    assert_eq!(target(4096, 4096, u64::MAX), 4096);
    assert_eq!(target(8192, 4096, u64::MAX), 8192);
}

#[test]
fn the_target_stays_under_the_platform_ceiling() {
    // macOS reports an unlimited hard limit.
    assert_eq!(target(256, u64::MAX, 10_240), 10_240);
    assert_eq!(target(20_000, u64::MAX, 10_240), 20_000);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn raising_leaves_the_soft_limit_at_the_target() {
    let soft = || {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `limit` is a valid `rlimit` for the call to fill.
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) },
            0
        );
        (limit.rlim_cur, limit.rlim_max)
    };
    let (before, hard) = soft();
    let (reported_before, after) = raise().expect("raise");
    assert_eq!(reported_before, before);
    assert_eq!(after, target(before, hard, ceiling()));
    assert_eq!(soft().0, after);
    // A second call has nothing to do.
    assert_eq!(raise().expect("raise"), (after, after));
}

#[cfg(unix)]
#[test]
fn an_exhausted_limit_is_found_through_wrapping() {
    let emfile = io::Error::from_raw_os_error(libc::EMFILE);
    assert!(is_exhausted(&emfile));
    assert!(is_exhausted(&io::Error::from_raw_os_error(libc::ENFILE)));
    assert!(!is_exhausted(&io::Error::from_raw_os_error(libc::ENOENT)));
    let storage = StorageError::from(io::Error::from_raw_os_error(libc::EMFILE));
    assert!(matches!(storage, StorageError::OpenFileLimit(_)));
    assert!(storage.to_string().contains("open-file limit"));
    assert!(exhausted_in(&storage));
    let wrapped = io::Error::other(storage);
    assert!(exhausted_in(&wrapped));
    assert!(!exhausted_in(&io::Error::other("unrelated")));
}
