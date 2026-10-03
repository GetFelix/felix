//! How many client listeners bind, and on which ports.

use super::*;

/// Unset, startup derives the count from the cores, on consecutive ports
/// from the configured address.
#[serial]
#[test]
fn the_default_count_is_derived_from_the_cores() {
    clear_felix_env();
    let config = BrokerConfig::from_env_or_yaml().expect("config");
    let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    let expected = super::super::env::default_quic_listeners(cores);
    assert_eq!(config.quic_listeners, expected);
    assert_eq!(config.quic_binds()[0].to_string(), "0.0.0.0:5000");
    assert_eq!(config.quic_binds().len(), expected);
}

/// A config built from the environment alone keeps one listener: code that
/// binds its own server and hands this to `serve` must not advertise ports it
/// never bound.
#[serial]
#[test]
fn from_env_alone_keeps_one_listener() {
    clear_felix_env();
    let config = BrokerConfig::from_env().expect("config");
    assert_eq!(config.quic_listeners, 1);
}

#[test]
fn the_default_count_is_half_the_cores_between_one_and_four() {
    use super::super::env::default_quic_listeners;
    for (cores, listeners) in [
        (1, 1),
        (2, 1),
        (3, 1),
        (4, 2),
        (8, 4),
        (9, 4),
        (16, 4),
        (64, 4),
    ] {
        assert_eq!(default_quic_listeners(cores), listeners, "{cores} cores");
    }
}

/// A derived count stops short of the internal listener and the last port,
/// so the default never fails a check an explicit count would.
#[test]
fn a_derived_count_fits_the_free_ports() {
    use super::super::env::fit_listener_range;
    assert_eq!(fit_listener_range(4, 5000, None), 4);
    // The default internal port is 5001.
    assert_eq!(fit_listener_range(4, 5000, Some(5001)), 1);
    assert_eq!(fit_listener_range(4, 5000, Some(5003)), 3);
    assert_eq!(fit_listener_range(4, 5000, Some(5004)), 4);
    assert_eq!(fit_listener_range(4, 5000, Some(4000)), 4);
    assert_eq!(fit_listener_range(4, 65534, None), 2);
    assert_eq!(fit_listener_range(4, 5000, Some(5000)), 1);
    assert_eq!(fit_listener_range(4, 0, None), 1);
}

/// A cluster member on the default ports (client 5000, internal 5001) keeps
/// one listener instead of failing startup.
#[serial]
#[test]
fn a_cluster_member_on_default_ports_derives_one_listener() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:5001");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
        env::set_var("FELIX_INTERNAL_ALLOW_UNAUTHENTICATED", "true");
    }
    let config = BrokerConfig::from_env_or_yaml().expect("config");
    assert_eq!(config.quic_listeners, 1);
    clear_felix_env();
}

/// The I/O pool's default is sized from the same endpoint count, so an unset
/// `FELIX_IO_RUNTIME_THREADS` never conflicts with a derived listener count.
#[test]
fn the_default_io_pool_covers_every_derived_listener() {
    use super::super::env::default_quic_listeners;
    for cores in [1, 2, 4, 8, 16, 64] {
        for clustered in [false, true] {
            let mut config = BrokerConfig {
                quic_listeners: default_quic_listeners(cores),
                peer_transport: clustered.then(|| felix_replication::peer::PeerTransportConfig {
                    allow_unauthenticated: true,
                    ..Default::default()
                }),
                ..BrokerConfig::default()
            };
            config.validate().expect("unset pool");
            config.io_runtime_threads = Some(felix_transport::required_io_runtime_threads(
                config.server_endpoints(),
            ));
            config
                .validate()
                .unwrap_or_else(|err| panic!("{cores} cores, clustered {clustered}: {err}"));
        }
    }
}

#[serial]
#[test]
fn listeners_occupy_consecutive_ports_from_the_bind_address() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_QUIC_BIND", "127.0.0.1:7000");
        env::set_var("FELIX_QUIC_LISTENERS", "3");
    }
    let config = BrokerConfig::from_env().expect("config");
    assert_eq!(
        config
            .quic_binds()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"],
    );
    clear_felix_env();
}

/// Refused rather than clamped: a broker that silently bound fewer
/// listeners than asked reads as the feature not working.
#[serial]
#[test]
fn a_listener_range_past_the_last_port_is_refused() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_QUIC_BIND", "0.0.0.0:65534");
        env::set_var("FELIX_QUIC_LISTENERS", "4");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("65535"), "{err}");
    clear_felix_env();
}

#[serial]
#[test]
fn zero_listeners_is_refused() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_QUIC_LISTENERS", "0");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("serves nothing"), "{err}");
    clear_felix_env();
}
