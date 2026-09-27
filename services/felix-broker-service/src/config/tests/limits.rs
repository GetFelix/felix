use std::collections::HashMap;

use crate::config::{LimitsConfig, TenantQuota};

fn parse(vars: &[(&str, &str)]) -> anyhow::Result<LimitsConfig> {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    LimitsConfig::from_lookup(|name| vars.get(name).cloned())
}

#[test]
fn defaults_cap_connections_and_set_no_quota() {
    let config = parse(&[]).expect("parse");
    assert_eq!(config, LimitsConfig::default());
    assert_eq!(config.max_connections_per_ip, 512);
    assert!(!config.any_quota());
    assert_eq!(config.tenant_metrics_max, 100);
}

#[test]
fn every_setting_is_read() {
    let config = parse(&[
        ("FELIX_MAX_CONNECTIONS_PER_IP", "8"),
        ("FELIX_TENANT_PUBLISH_BYTES_PER_SEC", "1048576"),
        ("FELIX_TENANT_PUBLISH_MSGS_PER_SEC", "1000"),
        ("FELIX_TENANT_PUBLISH_BURST_MS", "250"),
        ("FELIX_TENANT_METRICS_MAX", "5"),
        (
            "FELIX_TENANT_PUBLISH_QUOTAS",
            " acme:10485760:5000 , free:0:0,urn:org:x:1:2",
        ),
    ])
    .expect("parse");
    assert_eq!(config.max_connections_per_ip, 8);
    assert_eq!(config.tenant_publish_burst_ms, 250);
    assert_eq!(config.tenant_metrics_max, 5);
    assert_eq!(
        config.quota_for("other"),
        TenantQuota {
            bytes_per_sec: 1_048_576,
            msgs_per_sec: 1_000,
        }
    );
    assert_eq!(
        config.quota_for("acme"),
        TenantQuota {
            bytes_per_sec: 10_485_760,
            msgs_per_sec: 5_000,
        }
    );
    assert!(config.quota_for("free").is_unlimited());
    assert_eq!(
        config.quota_for("urn:org:x"),
        TenantQuota {
            bytes_per_sec: 1,
            msgs_per_sec: 2,
        },
        "the numbers are taken from the right, so a tenant id may contain ':'"
    );
}

#[test]
fn an_override_alone_turns_quotas_on() {
    let config = parse(&[("FELIX_TENANT_PUBLISH_QUOTAS", "acme:0:10")]).expect("parse");
    assert!(config.any_quota());
    assert!(config.quota_for("other").is_unlimited());
}

#[test]
fn a_bad_value_fails_rather_than_meaning_unlimited() {
    for vars in [
        &[("FELIX_TENANT_PUBLISH_BYTES_PER_SEC", "10MB")][..],
        &[("FELIX_TENANT_PUBLISH_MSGS_PER_SEC", "-1")][..],
        &[("FELIX_MAX_CONNECTIONS_PER_IP", "many")][..],
        &[("FELIX_TENANT_PUBLISH_BURST_MS", "0")][..],
        &[("FELIX_TENANT_PUBLISH_QUOTAS", "acme:10")][..],
        &[("FELIX_TENANT_PUBLISH_QUOTAS", "acme:ten:5")][..],
        &[("FELIX_TENANT_PUBLISH_QUOTAS", ":1:1")][..],
        &[("FELIX_TENANT_PUBLISH_QUOTAS", "acme:1:1,acme:2:2")][..],
    ] {
        assert!(parse(vars).is_err(), "{vars:?} was accepted");
    }
}
