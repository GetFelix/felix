use serde_json::json;

use super::*;

#[test]
fn fleet_features_render_as_lines() {
    let response = json!({
        "supported": ["a", "b"], "enabled": [], "serving_nodes": 3,
    });
    assert_eq!(
        render_features(&response),
        "serving brokers: 3\nsupported: a, b\nenabled:   -"
    );
}

#[test]
fn a_dry_run_says_why_a_feature_cannot_be_finalized() {
    let refused = json!({
        "feature": "a", "dry_run": true, "enabled": false, "would_enable": false,
        "lacking": ["broker-2"], "serving_nodes": 3,
    });
    assert_eq!(
        render_finalize(&refused),
        "dry run: a cannot be finalized: broker-2 do not support it"
    );
    let early = json!({
        "feature": "majority_ack", "dry_run": true, "enabled": false, "would_enable": false,
        "lacking": [], "requires": ["generation_start"], "serving_nodes": 3,
    });
    assert_eq!(
        render_finalize(&early),
        "dry run: majority_ack cannot be finalized: finalize generation_start first"
    );
    let idle = json!({
        "feature": "a", "dry_run": true, "enabled": false, "would_enable": false,
        "lacking": [], "serving_nodes": 0,
    });
    assert_eq!(
        render_finalize(&idle),
        "dry run: a cannot be finalized: no broker is serving"
    );
}

#[test]
fn a_dry_run_that_would_succeed_and_a_real_finalize() {
    let ok = json!({
        "feature": "a", "dry_run": true, "enabled": false, "would_enable": true,
        "serving_nodes": 3,
    });
    assert_eq!(
        render_finalize(&ok),
        "dry run: a can be finalized; all 3 serving brokers support it"
    );
    let done = json!({ "feature": "a", "dry_run": false, "enabled": true });
    assert_eq!(
        render_finalize(&done),
        "a enabled; brokers without it will be refused from now on"
    );
    let already = json!({ "feature": "a", "dry_run": true, "enabled": true });
    assert_eq!(render_finalize(&already), "dry run: a is already enabled");
}

#[test]
fn finalizing_without_yes_off_a_terminal_changes_nothing() {
    let err = crate::manage::confirm_with("Finalize a?", Confirm { yes: false }, false, || {
        unreachable!("no prompt off a terminal")
    })
    .expect_err("refused");
    assert_eq!(crate::error::exit_for(&err), crate::error::Exit::Usage);
}
