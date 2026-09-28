//! Cluster identity: what a node id needs alongside it before startup accepts it.

use super::*;

/// A broker with no identity is a single node, and registering one would
/// put a node in the catalog placement would then try to use.
#[serial]
#[test]
fn membership_is_off_without_a_node_id() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
    }
    assert!(
        BrokerConfig::from_env()
            .expect("config")
            .membership
            .is_none()
    );
}

/// A broker that guessed its advertised address would register something
/// unreachable, and the failure would surface later as peers unable to
/// connect to a node the catalog says is live.
#[serial]
#[test]
fn a_node_id_without_an_advertise_address_fails_startup() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(
        err.to_string().contains("FELIX_NODE_ADVERTISE_ADDR"),
        "{err}"
    );
}

/// Rejected at startup rather than as a failed registration once everything
/// else is already running.
#[serial]
#[test]
fn a_malformed_advertise_address_fails_startup() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "not-an-address");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("valid host:port"), "{err}");
}

#[serial]
#[test]
fn a_node_id_without_a_control_plane_fails_startup() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("FELIX_CONTROLPLANE_URL"), "{err}");
}

#[serial]
#[test]
fn a_complete_identity_is_accepted() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
        env::set_var("FELIX_REGION_ID", "eu-central-1");
        env::set_var("FELIX_NODE_ZONE", " eu-central-1a ");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
    }
    let membership = BrokerConfig::from_env()
        .expect("config")
        .membership
        .expect("membership");
    assert_eq!(membership.node_id, "broker-a");
    assert_eq!(membership.advertise_addr, "10.0.0.4:7000");
    assert_eq!(membership.region, "eu-central-1");
    assert_eq!(membership.zone.as_deref(), Some("eu-central-1a"));
}

/// No zone, or a blank one, registers none rather than an empty zone every
/// other zoneless broker would then share.
#[serial]
#[test]
fn a_blank_zone_is_no_zone() {
    for zone in [None, Some("  ")] {
        clear_felix_env();
        unsafe {
            env::set_var("FELIX_NODE_ID", "broker-a");
            env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
            env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
            env::set_var("FELIX_NODE_TOKEN", "a-node-token");
            if let Some(zone) = zone {
                env::set_var("FELIX_NODE_ZONE", zone);
            }
        }
        let membership = BrokerConfig::from_env()
            .expect("config")
            .membership
            .expect("membership");
        assert_eq!(membership.zone, None, "{zone:?}");
    }
}

/// A broker with an identity and no credential cannot register. Starting it
/// to fail every control-plane call on a loop is worse than refusing.
#[serial]
#[test]
fn an_identity_without_a_credential_fails_startup() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("FELIX_NODE_TOKEN"), "{err}");
}

/// The bridge allowlist is read with the identity, and a malformed one stops
/// startup: a pair skipped quietly is a region cut off that nobody asked for.
#[serial]
#[test]
fn region_bridges_are_read_and_a_malformed_one_fails_startup() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-token");
        env::set_var("FELIX_REGION_BRIDGES", "us-west-2>eu-west-1");
    }
    let membership = BrokerConfig::from_env()
        .expect("config")
        .membership
        .expect("membership");
    assert_eq!(
        membership.region_bridges,
        vec![("us-west-2".to_string(), "eu-west-1".to_string())]
    );

    unsafe {
        env::set_var("FELIX_REGION_BRIDGES", "us-west-2");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("FELIX_REGION_BRIDGES"), "{err}");
}

/// Only a broker running the Kafka listener registers a Kafka address, and it
/// defaults to the bind address when nothing more specific is advertised.
#[test]
fn the_kafka_address_is_registered_only_with_the_listener_on() {
    use super::super::membership::kafka_advertise_addr;

    assert_eq!(kafka_advertise_addr(None, None), None);
    assert_eq!(kafka_advertise_addr(Some("kafka.example:9092"), None), None);
    assert_eq!(
        kafka_advertise_addr(Some("kafka.example:9092"), Some("  ")),
        None
    );
    assert_eq!(
        kafka_advertise_addr(None, Some("0.0.0.0:9092")).as_deref(),
        Some("0.0.0.0:9092")
    );
    assert_eq!(
        kafka_advertise_addr(Some(" "), Some("0.0.0.0:9092")).as_deref(),
        Some("0.0.0.0:9092")
    );
    assert_eq!(
        kafka_advertise_addr(Some(" host.docker.internal:9092 "), Some("0.0.0.0:9092")).as_deref(),
        Some("host.docker.internal:9092")
    );
}
