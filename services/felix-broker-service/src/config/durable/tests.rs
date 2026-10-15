use serial_test::serial;

use super::*;

/// The environment is process-global, so these tests take a lock and clean
/// up after themselves rather than running in parallel against each other.
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

const VARS: &[&str] = &[
    "FELIX_DURABLE_STORAGE_DIR",
    "FELIX_DURABLE_SEGMENT_BYTES",
    "FELIX_DURABLE_INDEX_SPACING_BYTES",
    "FELIX_DURABLE_FSYNC_MODE",
    "FELIX_DURABLE_FSYNC_INTERVAL_MS",
    "FELIX_DURABLE_MAX_RECORDS_PER_READ",
    "FELIX_DURABLE_PREALLOCATE",
    "FELIX_DURABLE_VERIFY_ALL_ON_OPEN",
    "FELIX_DURABLE_REPAIR_CHECKSUM_TAIL",
    "FELIX_DURABLE_OFFLOAD_DIR",
];

/// Every caller is `#[serial]`, which is what actually keeps these apart
/// from the `config` tests: those clear every `FELIX_*` variable, including
/// the ones set here, and a local lock cannot exclude a test that does not
/// take it. The lock below is kept as a second belt for direct callers.
fn with_env<T>(pairs: &[(&str, &str)], body: impl FnOnce() -> T) -> T {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|err| err.into_inner());
    for name in VARS {
        // SAFETY: the lock above makes this the only thread touching the
        // environment for the duration of the test.
        unsafe { std::env::remove_var(name) };
    }
    for (name, value) in pairs {
        // SAFETY: as above.
        unsafe { std::env::set_var(name, value) };
    }
    let result = body();
    for name in VARS {
        // SAFETY: as above.
        unsafe { std::env::remove_var(name) };
    }
    result
}

#[serial]
#[test]
fn durability_is_off_unless_a_directory_is_set() {
    with_env(&[], || {
        assert!(DurableStorageConfig::from_env().expect("config").is_none());
    });
    with_env(&[("FELIX_DURABLE_STORAGE_DIR", "   ")], || {
        assert!(DurableStorageConfig::from_env().expect("config").is_none());
    });
}

#[serial]
#[test]
fn a_directory_alone_enables_the_defaults() {
    with_env(&[("FELIX_DURABLE_STORAGE_DIR", "/var/lib/felix")], || {
        let config = DurableStorageConfig::from_env()
            .expect("config")
            .expect("enabled");
        assert_eq!(config.root, PathBuf::from("/var/lib/felix"));
        assert_eq!(config.log.fsync_mode, LogConfig::default().fsync_mode);
        assert_eq!(
            config.log.segment_size_bytes,
            LogConfig::default().segment_size_bytes
        );
    });
}

#[serial]
#[test]
fn every_fsync_mode_is_selectable() {
    for (raw, expected) in [
        ("none", FsyncMode::None),
        ("off", FsyncMode::None),
        ("on_commit", FsyncMode::OnCommit),
        ("on-commit", FsyncMode::OnCommit),
        ("ON_COMMIT", FsyncMode::OnCommit),
        (
            "periodic",
            FsyncMode::Periodic {
                interval: Duration::from_millis(250),
            },
        ),
    ] {
        with_env(
            &[
                ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
                ("FELIX_DURABLE_FSYNC_MODE", raw),
            ],
            || {
                let config = DurableStorageConfig::from_env()
                    .expect("config")
                    .expect("enabled");
                assert_eq!(config.log.fsync_mode, expected, "mode {raw}");
            },
        );
    }
}

#[serial]
#[test]
fn a_periodic_interval_is_honoured() {
    with_env(
        &[
            ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
            ("FELIX_DURABLE_FSYNC_MODE", "periodic"),
            ("FELIX_DURABLE_FSYNC_INTERVAL_MS", "40"),
        ],
        || {
            let config = DurableStorageConfig::from_env()
                .expect("config")
                .expect("enabled");
            assert_eq!(
                config.log.fsync_mode,
                FsyncMode::Periodic {
                    interval: Duration::from_millis(40)
                }
            );
        },
    );
}

#[serial]
#[test]
fn an_interval_without_a_mode_still_applies() {
    with_env(
        &[
            ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
            ("FELIX_DURABLE_FSYNC_INTERVAL_MS", "75"),
        ],
        || {
            let config = DurableStorageConfig::from_env()
                .expect("config")
                .expect("enabled");
            assert_eq!(
                config.log.fsync_mode,
                FsyncMode::Periodic {
                    interval: Duration::from_millis(75)
                }
            );
        },
    );
}

#[serial]
#[test]
fn an_unknown_fsync_mode_is_rejected() {
    with_env(
        &[
            ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
            ("FELIX_DURABLE_FSYNC_MODE", "sometimes"),
        ],
        || {
            let err = DurableStorageConfig::from_env().expect_err("bad mode");
            assert!(err.to_string().contains("FELIX_DURABLE_FSYNC_MODE"));
        },
    );
}

#[serial]
#[test]
fn a_zero_periodic_interval_is_rejected_at_startup() {
    with_env(
        &[
            ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
            ("FELIX_DURABLE_FSYNC_MODE", "periodic"),
            ("FELIX_DURABLE_FSYNC_INTERVAL_MS", "0"),
        ],
        || {
            // Zero is parsed as "unset" by `parse_env`'s caller only for
            // empty strings, so this reaches validation as a real zero.
            let config = DurableStorageConfig::from_env();
            match config {
                Err(err) => assert!(err.to_string().contains("interval")),
                Ok(Some(config)) => assert!(
                    !matches!(
                        config.log.fsync_mode,
                        FsyncMode::Periodic {
                            interval: Duration::ZERO
                        }
                    ),
                    "a zero interval must not survive validation"
                ),
                Ok(None) => panic!("durability should be enabled"),
            }
        },
    );
}

#[serial]
#[test]
fn segment_and_index_sizes_are_configurable() {
    with_env(
        &[
            ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
            ("FELIX_DURABLE_SEGMENT_BYTES", "1048576"),
            ("FELIX_DURABLE_INDEX_SPACING_BYTES", "8192"),
            ("FELIX_DURABLE_MAX_RECORDS_PER_READ", "500"),
            ("FELIX_DURABLE_PREALLOCATE", "false"),
            ("FELIX_DURABLE_VERIFY_ALL_ON_OPEN", "yes"),
        ],
        || {
            let config = DurableStorageConfig::from_env()
                .expect("config")
                .expect("enabled");
            assert_eq!(config.log.segment_size_bytes, 1_048_576);
            assert_eq!(config.log.index_spacing_bytes, 8_192);
            assert_eq!(config.log.max_records_per_read, 500);
            assert!(!config.log.preallocate_segments);
            assert!(config.log.verify_all_on_open);
        },
    );
}

#[serial]
#[test]
fn an_unparseable_size_is_reported_with_its_variable_name() {
    with_env(
        &[
            ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
            ("FELIX_DURABLE_SEGMENT_BYTES", "big"),
        ],
        || {
            let err = DurableStorageConfig::from_env().expect_err("bad size");
            assert!(
                err.to_string().contains("FELIX_DURABLE_SEGMENT_BYTES"),
                "{err}"
            );
        },
    );
}

#[serial]
#[test]
fn a_segment_too_small_to_hold_a_record_is_rejected() {
    with_env(
        &[
            ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
            ("FELIX_DURABLE_SEGMENT_BYTES", "8"),
        ],
        || {
            let err = DurableStorageConfig::from_env().expect_err("tiny segment");
            assert!(err.to_string().contains("segment_size_bytes"), "{err}");
        },
    );
}

#[serial]
#[test]
fn an_invalid_boolean_is_rejected() {
    with_env(
        &[
            ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
            ("FELIX_DURABLE_PREALLOCATE", "maybe"),
        ],
        || {
            let err = DurableStorageConfig::from_env().expect_err("bad bool");
            assert!(
                err.to_string().contains("FELIX_DURABLE_PREALLOCATE"),
                "{err}"
            );
        },
    );
}

#[serial]
#[test]
fn the_summary_names_the_durability_policy() {
    with_env(
        &[
            ("FELIX_DURABLE_STORAGE_DIR", "/tmp/felix"),
            ("FELIX_DURABLE_FSYNC_MODE", "on_commit"),
        ],
        || {
            let config = DurableStorageConfig::from_env()
                .expect("config")
                .expect("enabled");
            let summary = config.summary();
            assert!(summary.contains("/tmp/felix"), "{summary}");
            assert!(summary.contains("acknowledgement"), "{summary}");
        },
    );
}

#[serial]
#[test]
fn offload_is_off_unless_a_directory_is_set_and_reaches_only_stream_logs() {
    with_env(&[("FELIX_DURABLE_STORAGE_DIR", "/var/lib/felix")], || {
        let config = DurableStorageConfig::from_env()
            .expect("config")
            .expect("enabled");
        assert_eq!(config.offload_dir, None);
        assert_eq!(config.stream_log(None).expect("config").offload, None);
    });
    with_env(
        &[
            ("FELIX_DURABLE_STORAGE_DIR", "/var/lib/felix"),
            ("FELIX_DURABLE_OFFLOAD_DIR", "/mnt/cold"),
        ],
        || {
            let config = DurableStorageConfig::from_env()
                .expect("config")
                .expect("enabled");
            assert_eq!(
                config.log.offload, None,
                "caches and counters do not offload"
            );
            assert_eq!(
                config.stream_log(Some("broker-a")).expect("config").offload,
                Some(OffloadTarget::LocalDir {
                    dir: PathBuf::from("/mnt/cold"),
                    node: "broker-a".to_string(),
                })
            );
        },
    );
}

#[test]
fn a_broker_without_a_node_id_offloads_under_one_kept_in_its_data_directory() {
    let dir = tempfile::tempdir().expect("dir");
    let config = |root: &std::path::Path| DurableStorageConfig {
        root: root.to_path_buf(),
        log: LogConfig::default(),
        offload_dir: Some(PathBuf::from("/mnt/cold")),
    };
    let node =
        |config: &DurableStorageConfig| match config.stream_log(None).expect("config").offload {
            Some(OffloadTarget::LocalDir { node, .. }) => node,
            None => panic!("offload is on"),
        };
    let first = node(&config(dir.path()));
    assert!(!first.is_empty());
    assert_eq!(node(&config(dir.path())), first, "a restart keeps the id");
    let other = tempfile::tempdir().expect("other");
    assert_ne!(
        node(&config(other.path())),
        first,
        "another broker's differs"
    );
    config(dir.path())
        .stream_log(None)
        .expect("config")
        .validate()
        .expect("a generated id is a valid key segment");
}
