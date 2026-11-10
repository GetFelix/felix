use super::*;

#[test]
fn store_config_change_window_clamps() {
    let cfg = StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: None,
    };
    assert_eq!(cfg.change_window(), 100);

    let cfg = StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(10),
    };
    assert_eq!(cfg.change_window(), 100);

    let cfg = StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(250),
    };
    assert_eq!(cfg.change_window(), 250);
}

#[test]
fn store_error_from_conversions_wrap_unexpected() {
    let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
    let err = StoreError::from(json_err);
    assert!(matches!(err, StoreError::Unexpected(_)));
}
