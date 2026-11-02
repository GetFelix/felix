use std::collections::HashMap;
use std::time::Duration;

use crate::config::MqttListenerConfig;

fn parse(vars: &[(&str, &str)]) -> anyhow::Result<Option<MqttListenerConfig>> {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    MqttListenerConfig::from_lookup(|name| vars.get(name).cloned())
}

#[test]
fn the_listener_is_off_unless_listen_is_set() {
    assert_eq!(parse(&[]).expect("parse"), None);
    assert_eq!(parse(&[("FELIX_MQTT_LISTEN", "  ")]).expect("parse"), None);
    assert_eq!(parse(&[("FELIX_MQTT_TLS", "false")]).expect("parse"), None);
}

#[test]
fn defaults_are_tls_on_and_authentication_required() {
    let config = parse(&[("FELIX_MQTT_LISTEN", "127.0.0.1:8883")])
        .expect("parse")
        .expect("on");
    assert_eq!(config.listen.to_string(), "127.0.0.1:8883");
    assert!(config.tls);
    assert_eq!(config.settings.anonymous_tenant, None);
    assert_eq!(config.worker_threads, 2);
    assert_eq!(config.max_connections, 100_000);
    assert_eq!(config.max_connections_per_ip, 1024);
    assert_eq!(config.connect_rate, 2000);
    assert_eq!(config.settings.auth_timeout, Duration::from_secs(10));
    assert_eq!(config.settings.max_packet_bytes, 1024 * 1024);
    assert_eq!(config.settings.max_topic_bytes, 1024);
    assert_eq!(config.settings.max_topic_levels, 16);
    assert_eq!(config.settings.max_subscriptions, 64);
    assert_eq!(config.settings.keepalive_max_secs, 600);
}

#[test]
fn every_setting_is_read() {
    let config = parse(&[
        ("FELIX_MQTT_LISTEN", "0.0.0.0:1883"),
        ("FELIX_MQTT_TLS", "false"),
        ("FELIX_MQTT_ANONYMOUS_TENANT", "dev"),
        ("FELIX_MQTT_WORKER_THREADS", "4"),
        ("FELIX_MQTT_MAX_CONNECTIONS", "10"),
        ("FELIX_MQTT_MAX_CONNECTIONS_PER_IP", "0"),
        ("FELIX_MQTT_CONNECT_RATE", "0"),
        ("FELIX_MQTT_AUTH_TIMEOUT_MS", "250"),
        ("FELIX_MQTT_MAX_PACKET_BYTES", "4096"),
        ("FELIX_MQTT_MAX_TOPIC_BYTES", "200"),
        ("FELIX_MQTT_MAX_TOPIC_LEVELS", "8"),
        ("FELIX_MQTT_MAX_SUBSCRIPTIONS", "5"),
        ("FELIX_MQTT_KEEPALIVE_MAX_SECS", "120"),
    ])
    .expect("parse")
    .expect("on");
    assert!(!config.tls);
    assert_eq!(config.settings.anonymous_tenant.as_deref(), Some("dev"));
    assert_eq!(config.worker_threads, 4);
    assert_eq!(config.max_connections, 10);
    assert_eq!(config.max_connections_per_ip, 0);
    assert_eq!(config.connect_rate, 0);
    assert_eq!(config.settings.auth_timeout, Duration::from_millis(250));
    assert_eq!(config.settings.max_packet_bytes, 4096);
    assert_eq!(config.settings.max_topic_bytes, 200);
    assert_eq!(config.settings.max_topic_levels, 8);
    assert_eq!(config.settings.max_subscriptions, 5);
    assert_eq!(config.settings.keepalive_max_secs, 120);
}

#[test]
fn a_bad_value_names_its_variable() {
    for (name, value) in [
        ("FELIX_MQTT_LISTEN", "localhost"),
        ("FELIX_MQTT_TLS", "maybe"),
        ("FELIX_MQTT_WORKER_THREADS", "0"),
        ("FELIX_MQTT_MAX_CONNECTIONS", "0"),
        ("FELIX_MQTT_MAX_CONNECTIONS_PER_IP", "-1"),
        ("FELIX_MQTT_AUTH_TIMEOUT_MS", "0"),
        ("FELIX_MQTT_MAX_PACKET_BYTES", "10"),
        ("FELIX_MQTT_KEEPALIVE_MAX_SECS", "70000"),
    ] {
        let mut vars = vec![("FELIX_MQTT_LISTEN", "127.0.0.1:8883")];
        vars.retain(|(set, _)| *set != name);
        vars.push((name, value));
        let err = parse(&vars).expect_err(name);
        assert!(format!("{err:#}").contains(name), "{name}: {err:#}");
    }
}

#[test]
fn the_websocket_listener_is_off_unless_its_address_is_set() {
    let config = parse(&[("FELIX_MQTT_LISTEN", "127.0.0.1:8883")])
        .expect("parse")
        .expect("on");
    assert_eq!(config.ws, None);
}

#[test]
fn the_websocket_listener_has_defaults_and_takes_overrides() {
    use felix_mqtt::ws::AllowedOrigins;

    let config = parse(&[
        ("FELIX_MQTT_LISTEN", "127.0.0.1:8883"),
        ("FELIX_MQTT_WS_LISTEN", "127.0.0.1:8884"),
    ])
    .expect("parse")
    .expect("on")
    .ws
    .expect("websocket on");
    assert_eq!(config.listen.to_string(), "127.0.0.1:8884");
    assert_eq!(config.settings.allowed_origins, AllowedOrigins::None);
    assert_eq!(config.settings.max_header_bytes, 8 * 1024);
    assert_eq!(config.settings.upgrade_timeout, Duration::from_secs(10));

    let config = |origins: &str| {
        parse(&[
            ("FELIX_MQTT_LISTEN", "127.0.0.1:8883"),
            ("FELIX_MQTT_WS_LISTEN", "127.0.0.1:8884"),
            ("FELIX_MQTT_WS_ALLOWED_ORIGINS", origins),
            ("FELIX_MQTT_WS_MAX_HEADER_BYTES", "2048"),
            ("FELIX_MQTT_WS_UPGRADE_TIMEOUT_MS", "500"),
        ])
        .expect("parse")
        .expect("on")
        .ws
        .expect("websocket on")
    };
    let listed = config("https://a.example, https://b.example ,");
    assert_eq!(
        listed.settings.allowed_origins,
        AllowedOrigins::Only(vec!["https://a.example".into(), "https://b.example".into()])
    );
    assert_eq!(listed.settings.max_header_bytes, 2048);
    assert_eq!(listed.settings.upgrade_timeout, Duration::from_millis(500));
    assert_eq!(config("*").settings.allowed_origins, AllowedOrigins::Any);
}

#[test]
fn allowed_origins_are_normalised_or_refused() {
    use felix_mqtt::ws::AllowedOrigins;

    let parse_origins = |origins: &str| {
        parse(&[
            ("FELIX_MQTT_LISTEN", "127.0.0.1:8883"),
            ("FELIX_MQTT_WS_LISTEN", "127.0.0.1:8884"),
            ("FELIX_MQTT_WS_ALLOWED_ORIGINS", origins),
        ])
    };
    // What a browser sends has no default port, so these match it.
    let config =
        parse_origins("HTTPS://App.Example.com:443, http://dev.example:80, https://x.example:8443")
            .expect("parse")
            .expect("on")
            .ws
            .expect("websocket on");
    assert_eq!(
        config.settings.allowed_origins,
        AllowedOrigins::Only(vec![
            "https://app.example.com".into(),
            "http://dev.example".into(),
            "https://x.example:8443".into(),
        ])
    );
    for bad in [
        "app.example.com",
        "https://app.example.com/",
        "https://app.example.com/x",
        "://x",
        "https://",
    ] {
        let err = parse_origins(bad).unwrap_err();
        assert!(
            err.to_string().contains("FELIX_MQTT_WS_ALLOWED_ORIGINS"),
            "{bad}: {err}"
        );
    }
}

#[test]
fn the_websocket_listener_needs_the_mqtt_listener() {
    let err = parse(&[("FELIX_MQTT_WS_LISTEN", "127.0.0.1:8884")]).unwrap_err();
    assert!(err.to_string().contains("FELIX_MQTT_LISTEN"), "{err}");
}

#[test]
fn bad_websocket_settings_name_the_variable() {
    for (name, value) in [
        ("FELIX_MQTT_WS_LISTEN", "nope"),
        ("FELIX_MQTT_WS_MAX_HEADER_BYTES", "10"),
        ("FELIX_MQTT_WS_UPGRADE_TIMEOUT_MS", "0"),
    ] {
        let vars = [
            ("FELIX_MQTT_LISTEN", "127.0.0.1:8883"),
            ("FELIX_MQTT_WS_LISTEN", "127.0.0.1:8884"),
            (name, value),
        ];
        let err = parse(&vars).unwrap_err();
        assert!(err.to_string().contains(name), "{name}: {err}");
    }
}
