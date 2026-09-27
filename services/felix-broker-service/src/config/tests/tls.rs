//! Client TLS, the control-plane CA, and the rule that a cluster member
//! authenticates its peers.

use std::collections::HashMap;

use super::*;

fn client_tls(vars: &[(&str, &str)]) -> anyhow::Result<ClientTlsConfig> {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    ClientTlsConfig::from_lookup(|name| vars.get(name).cloned())
}

#[test]
fn nothing_set_is_the_generated_certificate() {
    assert_eq!(client_tls(&[]).expect("empty"), ClientTlsConfig::default());
}

#[test]
fn the_certificate_paths_and_client_ca_reach_the_config() {
    let config = client_tls(&[
        ("FELIX_TLS_CERT", "/etc/felix/tls/tls.crt"),
        ("FELIX_TLS_KEY", "/etc/felix/tls/tls.key"),
        ("FELIX_TLS_CLIENT_CA", "/etc/felix/tls/clients.crt"),
        ("FELIX_TLS_REQUIRE_CERT", "true"),
    ])
    .expect("parse");
    assert_eq!(
        config.files,
        Some(ClientTlsFiles {
            cert_path: "/etc/felix/tls/tls.crt".into(),
            key_path: "/etc/felix/tls/tls.key".into(),
            client_ca_path: Some("/etc/felix/tls/clients.crt".into()),
        })
    );
    assert!(config.require_cert);
    config.validate().expect("a complete configuration");
}

/// Half a configuration is someone who believes the listener uses it.
#[test]
fn a_partial_configuration_is_refused() {
    let err = client_tls(&[("FELIX_TLS_CERT", "/c.pem")]).expect_err("cert alone");
    assert!(err.to_string().contains("FELIX_TLS_KEY"), "{err}");
    let err = client_tls(&[("FELIX_TLS_KEY", "/k.pem")]).expect_err("key alone");
    assert!(err.to_string().contains("FELIX_TLS_CERT"), "{err}");
    let err = client_tls(&[("FELIX_TLS_CLIENT_CA", "/ca.pem")]).expect_err("client ca alone");
    assert!(err.to_string().contains("FELIX_TLS_CLIENT_CA"), "{err}");
    let err = client_tls(&[("FELIX_TLS_REQUIRE_CERT", "maybe")]).expect_err("not a bool");
    assert!(err.to_string().contains("FELIX_TLS_REQUIRE_CERT"), "{err}");
}

#[test]
fn requiring_a_certificate_refuses_the_generated_one() {
    let config = BrokerConfig {
        client_tls: client_tls(&[("FELIX_TLS_REQUIRE_CERT", "true")]).expect("parse"),
        ..BrokerConfig::default()
    };
    let err = config.validate().expect_err("generated cert with require");
    assert!(format!("{err:#}").contains("FELIX_TLS_CERT"), "{err:#}");
}

/// The export is a trust root for clients; a configured leaf is not one.
#[test]
fn exporting_a_configured_certificate_is_refused() {
    let config = BrokerConfig {
        client_tls: client_tls(&[
            ("FELIX_TLS_CERT", "/c.pem"),
            ("FELIX_TLS_KEY", "/k.pem"),
            ("FELIX_TLS_CERT_EXPORT", "/export/broker-cert.pem"),
        ])
        .expect("parse"),
        ..BrokerConfig::default()
    };
    let err = config.validate().expect_err("export with configured cert");
    assert!(
        format!("{err:#}").contains("FELIX_TLS_CERT_EXPORT"),
        "{err:#}"
    );
}

#[test]
fn a_control_plane_ca_over_plain_http_is_refused() {
    let config = BrokerConfig {
        controlplane_url: Some("http://controlplane:8443".into()),
        controlplane_ca: Some("/etc/felix/cp-ca.pem".into()),
        ..BrokerConfig::default()
    };
    let err = config.validate().expect_err("CA with http");
    assert!(format!("{err:#}").contains("https://"), "{err:#}");

    let config = BrokerConfig {
        controlplane_url: Some("https://controlplane:8443".into()),
        ..config
    };
    config.validate().expect("CA with https");
}

fn cluster_member(tls: bool, allow_unauthenticated: bool) -> BrokerConfig {
    BrokerConfig {
        peer_transport: Some(crate::peer::PeerTransportConfig {
            bind: "0.0.0.0:5001".parse().expect("addr"),
            tls: tls.then(|| crate::peer::config::PeerTlsConfig {
                cert_path: "/etc/felix/peer/tls.crt".into(),
                key_path: "/etc/felix/peer/tls.key".into(),
                ca_path: "/etc/felix/peer/ca.crt".into(),
            }),
            allow_unauthenticated,
            ..Default::default()
        }),
        ..BrokerConfig::default()
    }
}

/// Anything that reaches an unauthenticated internal port can rewrite a
/// replica, so a cluster member refuses to start that way unless told to.
#[test]
fn a_cluster_member_without_peer_mtls_is_refused_unless_opted_out() {
    let err = cluster_member(false, false)
        .validate()
        .expect_err("unauthenticated peers accepted");
    let message = format!("{err:#}");
    assert!(message.contains("FELIX_INTERNAL_TLS_CERT"), "{message}");
    assert!(
        message.contains("FELIX_INTERNAL_ALLOW_UNAUTHENTICATED"),
        "{message}"
    );

    cluster_member(false, true)
        .validate()
        .expect("the explicit opt-out");
    cluster_member(true, false).validate().expect("peer mTLS");
}

#[serial]
#[test]
fn the_peer_opt_out_is_read_from_the_environment() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:5001");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
    }
    let err = BrokerConfig::from_env_or_yaml().expect_err("refused without opt-out");
    assert!(
        format!("{err:#}").contains("FELIX_INTERNAL_ALLOW_UNAUTHENTICATED"),
        "{err:#}"
    );

    unsafe {
        env::set_var("FELIX_INTERNAL_ALLOW_UNAUTHENTICATED", "true");
    }
    let config = BrokerConfig::from_env_or_yaml().expect("opted out");
    assert!(config.peer_transport.expect("peer").allow_unauthenticated);

    unsafe {
        env::set_var("FELIX_INTERNAL_ALLOW_UNAUTHENTICATED", "sure");
    }
    BrokerConfig::from_env().expect_err("not a bool");
    clear_felix_env();
}
