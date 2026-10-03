// Parsing and the re-arm decisions only. The injection itself is process-wide, and turning it on here
// would fail the flushes and writes of every other test running in this binary;
// the cluster harness's fsync and write tests exercise it end to end in a broker.

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
            ..FileSetting::default()
        },
    );
    assert_eq!(
        FileSetting::parse("fsync = fail_once\ngeneration = 3"),
        FileSetting {
            delay: Duration::ZERO,
            failure: FsyncFailure::Once,
            generation: 3,
            ..FileSetting::default()
        },
    );
}

#[test]
fn the_file_names_a_write_delay() {
    assert_eq!(
        FileSetting::parse("write_delay_ms=150\nwrite=eio_once\n"),
        FileSetting {
            write: WriteFailure::IoOnce,
            write_delay: Duration::from_millis(150),
            ..FileSetting::default()
        },
    );
    assert_eq!(
        FileSetting::parse("write_delay_ms=soon").write_delay,
        Duration::ZERO
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
        ..FileSetting::default()
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

#[test]
fn the_file_names_a_write_failure() {
    for (value, failure) in [
        ("enospc", WriteFailure::NoSpace),
        ("eio", WriteFailure::Io),
        ("eio_once", WriteFailure::IoOnce),
        ("explode", WriteFailure::None),
    ] {
        let setting = FileSetting::parse(&format!("write={value}\nwrite_generation=2\n"));
        assert_eq!(setting.write, failure, "write={value}");
        assert_eq!(setting.write_generation, 2);
        assert_eq!(setting.failure, FsyncFailure::None);
    }
}

fn write_once(write_generation: u64) -> FileSetting {
    FileSetting {
        write: WriteFailure::IoOnce,
        write_generation,
        ..FileSetting::default()
    }
}

/// A consumed `eio_once` stays consumed when only the flush fault changes.
#[test]
fn arming_a_flush_fault_does_not_rearm_a_write_failure() {
    let with_fsync = FileSetting {
        failure: FsyncFailure::Always,
        generation: 5,
        ..write_once(1)
    };
    assert!(!rearms_write(Some(&write_once(1)), &with_fsync));
    assert!(rearms(Some(&write_once(1)), &with_fsync));
}

#[test]
fn a_new_write_generation_rearms_the_write_failure() {
    assert!(rearms_write(Some(&write_once(1)), &write_once(2)));
    assert!(!rearms_write(Some(&write_once(1)), &write_once(1)));
    assert!(!rearms(Some(&write_once(1)), &write_once(2)));
}
