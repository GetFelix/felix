use super::*;

#[test]
fn default_log_config_values() {
    let config = LogConfig::default();
    assert_eq!(config.segment_size_bytes, 256 * 1024 * 1024);
    assert_eq!(config.index_spacing_bytes, 4 * 1024);
    assert_eq!(
        config.fsync_mode,
        FsyncMode::Periodic {
            interval: Duration::from_millis(250)
        }
    );
}

#[test]
fn a_segment_must_fit_one_v9_record() {
    use crate::segment::SEGMENT_HEADER_LEN;
    use crate::segment::format::GENERATION_HEADER_LEN;
    let sized = |segment_size_bytes| LogConfig {
        segment_size_bytes,
        ..LogConfig::default()
    };
    assert!(
        sized(SEGMENT_HEADER_LEN + GENERATION_HEADER_LEN)
            .validate()
            .is_err()
    );
    assert!(
        sized(SEGMENT_HEADER_LEN + GENERATION_HEADER_LEN + 1)
            .validate()
            .is_ok()
    );
}
