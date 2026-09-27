use std::collections::HashMap;

use crate::config::KafkaListenerConfig;

fn parse(vars: &[(&str, &str)]) -> anyhow::Result<Option<KafkaListenerConfig>> {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    KafkaListenerConfig::from_lookup(|name| vars.get(name).cloned())
}

#[test]
fn the_listener_is_off_unless_listen_is_set() {
    assert_eq!(parse(&[]).expect("parse"), None);
    assert_eq!(parse(&[("FELIX_KAFKA_LISTEN", "  ")]).expect("parse"), None);
    assert_eq!(
        parse(&[("FELIX_KAFKA_ADVERTISE_ADDR", "kafka:9092")]).expect("parse"),
        None
    );
}

#[test]
fn defaults_are_tls_on_and_advertise_the_bind_address() {
    let config = parse(&[("FELIX_KAFKA_LISTEN", "127.0.0.1:9092")])
        .expect("parse")
        .expect("on");
    assert_eq!(config.listen.to_string(), "127.0.0.1:9092");
    assert_eq!(config.advertise, "127.0.0.1:9092");
    assert!(config.tls);
    assert_eq!(config.anonymous_tenant, None);
    assert_eq!(config.default_namespace, None);
    assert_eq!(config.max_connections, 1024);
    assert_eq!(config.max_connections_per_ip, 128);
    assert_eq!(config.auth_timeout_ms, 10_000);
}

#[test]
fn every_setting_is_read() {
    let config = parse(&[
        ("FELIX_KAFKA_LISTEN", "0.0.0.0:9092"),
        (
            "FELIX_KAFKA_ADVERTISE_ADDR",
            "broker-1.kafka.internal:19092",
        ),
        ("FELIX_KAFKA_TLS", "false"),
        ("FELIX_KAFKA_ANONYMOUS_TENANT", "dev"),
        ("FELIX_KAFKA_DEFAULT_NAMESPACE", "orders"),
        ("FELIX_KAFKA_MAX_CONNECTIONS", "10"),
        ("FELIX_KAFKA_MAX_CONNECTIONS_PER_IP", "0"),
        ("FELIX_KAFKA_AUTH_TIMEOUT_MS", "2500"),
    ])
    .expect("parse")
    .expect("on");
    assert_eq!(config.advertise, "broker-1.kafka.internal:19092");
    assert!(!config.tls);
    assert_eq!(config.anonymous_tenant.as_deref(), Some("dev"));
    assert_eq!(config.default_namespace.as_deref(), Some("orders"));
    assert_eq!(config.max_connections, 10);
    assert_eq!(config.max_connections_per_ip, 0, "zero is unlimited");
    assert_eq!(config.auth_timeout_ms, 2_500);
}

#[test]
fn malformed_values_fail_startup_rather_than_defaulting() {
    let listen = ("FELIX_KAFKA_LISTEN", "127.0.0.1:9092");
    for bad in [
        vec![("FELIX_KAFKA_LISTEN", "localhost")],
        vec![listen, ("FELIX_KAFKA_ADVERTISE_ADDR", "no-port")],
        vec![listen, ("FELIX_KAFKA_ADVERTISE_ADDR", "host:0")],
        vec![listen, ("FELIX_KAFKA_TLS", "yes")],
        vec![listen, ("FELIX_KAFKA_MAX_CONNECTIONS", "0")],
        vec![listen, ("FELIX_KAFKA_MAX_CONNECTIONS", "many")],
        vec![listen, ("FELIX_KAFKA_MAX_CONNECTIONS_PER_IP", "-1")],
        vec![listen, ("FELIX_KAFKA_AUTH_TIMEOUT_MS", "0")],
    ] {
        assert!(parse(&bad).is_err(), "{bad:?}");
    }
}
