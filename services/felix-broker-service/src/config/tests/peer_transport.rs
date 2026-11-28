//! The internal listener's address and the peer mTLS settings.

use super::*;

/// A broker in a cluster listens for peers; one on its own has no peers to
/// listen for, so it binds nothing.
#[serial]
#[test]
fn the_internal_listener_is_configured_only_for_a_cluster_member() {
    clear_felix_env();
    assert!(
        BrokerConfig::from_env()
            .expect("config")
            .peer_transport
            .is_none(),
        "a standalone broker must not bind an internal listener",
    );

    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:5001");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
        env::set_var("FELIX_INTERNAL_BIND", "0.0.0.0:5001");
    }
    let peer = BrokerConfig::from_env()
        .expect("config")
        .peer_transport
        .expect("peer transport");
    assert_eq!(peer.bind.to_string(), "0.0.0.0:5001");
}

/// The two roles must not be reachable at the same place. Sharing a port
/// would put client traffic and peer traffic on one listener, which is the
/// separation the internal protocol exists to keep.
#[serial]
#[test]
fn an_internal_listener_sharing_the_client_port_fails_startup() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:5000");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
        env::set_var("FELIX_QUIC_BIND", "0.0.0.0:5000");
        env::set_var("FELIX_INTERNAL_BIND", "0.0.0.0:5000");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("share a port"), "{err}");
}

/// A listener range must clear the internal port too. The collision is
/// easier to hit than the single-port one -- the port that clashes is one
/// nobody wrote down, it is merely `quic_bind + n`.
#[serial]
#[test]
fn an_internal_listener_inside_the_client_range_fails_startup() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:5010");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
        env::set_var("FELIX_QUIC_BIND", "0.0.0.0:5000");
        env::set_var("FELIX_QUIC_LISTENERS", "4");
        // Inside 5000-5003, but not equal to 5000, so only the range check
        // catches it.
        env::set_var("FELIX_INTERNAL_BIND", "0.0.0.0:5002");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("5000-5003"), "{err}");
    clear_felix_env();
}

/// Just past the range is fine -- the check must not be off by one.
#[serial]
#[test]
fn an_internal_listener_just_past_the_client_range_is_accepted() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:5004");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
        env::set_var("FELIX_QUIC_BIND", "0.0.0.0:5000");
        env::set_var("FELIX_QUIC_LISTENERS", "4");
        env::set_var("FELIX_INTERNAL_BIND", "0.0.0.0:5004");
    }
    let config = BrokerConfig::from_env().expect("config");
    assert_eq!(config.quic_listeners, 4);
    clear_felix_env();
}

/// Peer mTLS is all three variables or none: one or two would look
/// secured while either presenting nothing or verifying nothing.
#[serial]
#[test]
fn a_partly_configured_peer_mtls_is_refused() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
        env::set_var("FELIX_INTERNAL_TLS_CERT", "/etc/felix/peer/tls.crt");
        env::set_var("FELIX_INTERNAL_TLS_KEY", "/etc/felix/peer/tls.key");
    }
    let err = BrokerConfig::from_env().expect_err("two of three accepted");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("FELIX_INTERNAL_TLS_CA not set"),
        "{rendered}"
    );
}

/// With peer mTLS the node id is the certificate's DNS name, so a node id
/// no certificate can carry -- here a label ending in a hyphen -- is
/// refused at startup, not at the first dial.
#[serial]
#[test]
fn a_node_id_that_is_not_a_dns_name_is_refused_under_peer_mtls() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
        env::set_var("FELIX_INTERNAL_TLS_CERT", "/etc/felix/peer/tls.crt");
        env::set_var("FELIX_INTERNAL_TLS_KEY", "/etc/felix/peer/tls.key");
        env::set_var("FELIX_INTERNAL_TLS_CA", "/etc/felix/peer/ca.crt");
    }
    let err = BrokerConfig::from_env().expect_err("a trailing hyphen is not a DNS name");
    assert!(
        format!("{err:#}").contains("not a valid DNS name"),
        "{err:#}"
    );

    // The same id is fine without mTLS, where nothing names it.
    unsafe {
        env::remove_var("FELIX_INTERNAL_TLS_CERT");
        env::remove_var("FELIX_INTERNAL_TLS_KEY");
        env::remove_var("FELIX_INTERNAL_TLS_CA");
    }
    let config = BrokerConfig::from_env().expect("config");
    assert!(config.peer_transport.expect("peer").tls.is_none());
}

#[serial]
#[test]
fn the_three_peer_mtls_paths_reach_the_config() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
        env::set_var("FELIX_INTERNAL_TLS_CERT", "/etc/felix/peer/tls.crt");
        env::set_var("FELIX_INTERNAL_TLS_KEY", "/etc/felix/peer/tls.key");
        env::set_var("FELIX_INTERNAL_TLS_CA", "/etc/felix/peer/ca.crt");
    }
    let config = BrokerConfig::from_env().expect("config");
    let tls = config.peer_transport.expect("peer").tls.expect("tls");
    assert_eq!(tls.cert_path, "/etc/felix/peer/tls.crt");
    assert_eq!(tls.key_path, "/etc/felix/peer/tls.key");
    assert_eq!(tls.ca_path, "/etc/felix/peer/ca.crt");
}

/// A cluster member's environment, so `from_env` builds a peer transport.
fn cluster_member_env() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:5001");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
    }
}

fn peer_from_env() -> std::io::Result<felix_replication::peer::PeerTransportConfig> {
    BrokerConfig::from_env()
        .map(|config| config.peer_transport.expect("peer transport"))
        .map_err(|err| std::io::Error::other(format!("{err:#}")))
}

/// **An internal bind that is not an address is refused**, quoting it.
#[serial]
#[test]
fn an_unparseable_internal_bind_is_refused() {
    cluster_member_env();
    unsafe {
        env::set_var("FELIX_INTERNAL_BIND", "peers.local");
    }
    let err = peer_from_env().expect_err("bad bind");
    let message = err.to_string();
    assert!(message.contains("FELIX_INTERNAL_BIND"), "{message}");
    assert!(message.contains("peers.local"), "{message}");
}

/// **The unauthenticated-peer switch refuses anything but a boolean**: a
/// typo in a safety switch must not pick a side silently.
#[serial]
#[test]
fn peer_switches_refuse_values_that_are_not_booleans() {
    let key = "FELIX_INTERNAL_ALLOW_UNAUTHENTICATED";
    cluster_member_env();
    unsafe {
        env::set_var(key, "sometimes");
    }
    let err = peer_from_env().expect_err(key);
    let message = err.to_string();
    assert!(message.contains(key), "{key}: {message}");
    assert!(message.contains("sometimes"), "{key}: {message}");
}

/// **Unset, the fence and unified fence are on and unauthenticated peers and
/// elections are off**, and unauthenticated peers can be allowed.
#[serial]
#[test]
fn peer_switches_default_safe_and_can_be_flipped() {
    cluster_member_env();
    let peer = peer_from_env().expect("defaults");
    assert!(peer.fence);
    assert!(peer.unified_fence);
    assert!(!peer.allow_unauthenticated);
    assert!(!peer.election);

    unsafe {
        env::set_var("FELIX_INTERNAL_ALLOW_UNAUTHENTICATED", " YES ");
    }
    let peer = peer_from_env().expect("flipped");
    assert!(peer.allow_unauthenticated);
}

/// **The hybrid's switches are retired**: set, they change nothing, so a
/// broker offers its peers what it offered with them unset.
#[serial]
#[test]
fn retired_hybrid_switches_change_nothing() {
    cluster_member_env();
    unsafe {
        env::set_var("FELIX_INTERNAL_FENCE", "false");
        env::set_var("FELIX_INTERNAL_ELECTION", "1");
        env::set_var("FELIX_UNIFIED_FENCE", "0");
    }
    let peer = peer_from_env().expect("retired switches are not parsed");
    assert!(peer.fence);
    assert!(!peer.election);
    assert!(peer.unified_fence);
}

/// **The pool, admission and timing knobs are read from their variables; a
/// zero count keeps the default**, and a blank partition file is no file.
#[serial]
#[test]
fn peer_limits_and_timeouts_are_read_from_the_environment() {
    cluster_member_env();
    let defaults = felix_replication::peer::PeerTransportConfig::default();
    for (key, value) in [
        ("FELIX_INTERNAL_CONNS_PER_PEER", "3"),
        ("FELIX_INTERNAL_STREAMS_PER_CONN", "0"),
        ("FELIX_INTERNAL_MAX_SERVED_FORWARDS", "11"),
        ("FELIX_INTERNAL_MAX_INFLIGHT", "13"),
        ("FELIX_INTERNAL_MAX_INBOUND_CONNECTIONS", "40"),
        ("FELIX_INTERNAL_MAX_INBOUND_PER_SOURCE", "8"),
        ("FELIX_INTERNAL_REQUEST_TIMEOUT_MS", "1500"),
        ("FELIX_INTERNAL_IDLE_TIMEOUT_MS", "9000"),
        ("FELIX_INTERNAL_RECONNECT_BASE_MS", "20"),
        ("FELIX_INTERNAL_RECONNECT_MAX_MS", "400"),
        ("FELIX_INTERNAL_HANDSHAKE_TIMEOUT_MS", "700"),
        ("FELIX_PEER_PARTITION_FILE", "/tmp/partition"),
    ] {
        unsafe {
            env::set_var(key, value);
        }
    }
    let peer = peer_from_env().expect("config");
    assert_eq!(peer.conns_per_peer, 3);
    assert_eq!(peer.streams_per_conn, defaults.streams_per_conn, "zero");
    assert_eq!(peer.max_served_forwards, 11);
    assert_eq!(peer.max_inflight_per_peer, 13);
    assert_eq!(peer.max_inbound_connections, 40);
    assert_eq!(peer.max_inbound_per_source, 8);
    assert_eq!(peer.request_timeout, std::time::Duration::from_millis(1500));
    assert_eq!(peer.idle_timeout, std::time::Duration::from_millis(9000));
    assert_eq!(peer.reconnect_base, std::time::Duration::from_millis(20));
    assert_eq!(peer.reconnect_max, std::time::Duration::from_millis(400));
    assert_eq!(
        peer.handshake_timeout,
        std::time::Duration::from_millis(700)
    );
    assert_eq!(
        peer.partition_file,
        Some(std::path::PathBuf::from("/tmp/partition"))
    );

    unsafe {
        env::set_var("FELIX_PEER_PARTITION_FILE", "  ");
    }
    assert_eq!(peer_from_env().expect("blank").partition_file, None);
}

/// **A per-source inbound limit above the total is refused**: it could never
/// be the limit that applies.
#[serial]
#[test]
fn a_per_source_limit_above_the_total_is_refused() {
    cluster_member_env();
    unsafe {
        env::set_var("FELIX_INTERNAL_MAX_INBOUND_CONNECTIONS", "4");
        env::set_var("FELIX_INTERNAL_MAX_INBOUND_PER_SOURCE", "5");
    }
    let err = peer_from_env().expect_err("per source above total");
    assert!(
        err.to_string()
            .contains("FELIX_INTERNAL_MAX_INBOUND_PER_SOURCE (5)"),
        "{err}"
    );

    unsafe {
        env::set_var("FELIX_INTERNAL_MAX_INBOUND_PER_SOURCE", "4");
    }
    assert!(peer_from_env().is_ok(), "equal to the total is allowed");
}

/// **A reconnect base above its ceiling is refused**; equal is allowed.
#[serial]
#[test]
fn a_reconnect_base_above_its_ceiling_is_refused() {
    cluster_member_env();
    unsafe {
        env::set_var("FELIX_INTERNAL_RECONNECT_BASE_MS", "500");
        env::set_var("FELIX_INTERNAL_RECONNECT_MAX_MS", "499");
    }
    let err = peer_from_env().expect_err("base above max");
    assert!(
        err.to_string().contains("FELIX_INTERNAL_RECONNECT_BASE_MS"),
        "{err}"
    );

    unsafe {
        env::set_var("FELIX_INTERNAL_RECONNECT_MAX_MS", "500");
    }
    assert!(peer_from_env().is_ok(), "equal is allowed");
}
