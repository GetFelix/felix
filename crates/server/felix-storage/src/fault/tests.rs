// Parsing and the re-arm decision only. The injection itself is process-wide, and turning it on here
// would fail the flushes of every other test running in this binary; the
// cluster harness's fsync tests exercise it end to end in a broker.

use super::*;

#[test]
fn a_missing_or_empty_file_is_no_fault() {
    assert_eq!(FileSetting::parse(""), FileSetting::default());
    assert_eq!(
        FileSetting::parse("fsync=\nnonsense\n"),
        FileSetting::default()
    );
}

#[test]
fn the_file_names_a_delay_and_a_failure() {
    assert_eq!(
        FileSetting::parse("fsync_delay_ms=250\nfsync=fail\n"),
        FileSetting {
            delay: Duration::from_millis(250),
            failure: FsyncFailure::Always,
            generation: 0,
        },
    );
    assert_eq!(
        FileSetting::parse("fsync = fail_once\ngeneration = 3"),
        FileSetting {
            delay: Duration::ZERO,
            failure: FsyncFailure::Once,
            generation: 3,
        },
    );
}

/// A value it does not know is less of a fault, not a different one.
#[test]
fn an_unknown_failure_mode_is_no_failure() {
    assert_eq!(
        FileSetting::parse("fsync=explode").failure,
        FsyncFailure::None
    );
}

fn fail_once(generation: u64) -> FileSetting {
    FileSetting {
        delay: Duration::ZERO,
        failure: FsyncFailure::Once,
        generation,
    }
}

#[test]
fn the_first_reading_arms_whatever_it_says() {
    assert!(rearms(None, &fail_once(1)));
    assert!(rearms(None, &FileSetting::default()));
}

/// A `fail_once` already used up stays used up while the file is unchanged.
#[test]
fn a_consumed_fail_once_with_the_same_generation_does_not_rearm() {
    assert!(!rearms(Some(&fail_once(1)), &fail_once(1)));
}

#[test]
fn a_new_generation_rearms() {
    assert!(rearms(Some(&fail_once(1)), &fail_once(2)));
}

#[test]
fn changing_only_the_delay_does_not_rearm_a_failure() {
    let slower = FileSetting {
        delay: Duration::from_millis(100),
        ..fail_once(1)
    };
    assert!(!rearms(Some(&fail_once(1)), &slower));
}

#[test]
fn a_new_failure_mode_rearms() {
    let always = FileSetting {
        failure: FsyncFailure::Always,
        ..fail_once(1)
    };
    assert!(rearms(Some(&fail_once(1)), &always));
    assert!(rearms(
        Some(&always),
        &FileSetting {
            generation: 1,
            ..FileSetting::default()
        }
    ));
}
