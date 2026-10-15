//! Which retention hold logs open with, decided from configuration alone.
use felix_storage::log::RetentionHold;

use super::*;
use crate::config::MembershipConfig;

fn membership() -> MembershipConfig {
    MembershipConfig {
        node_id: "broker-a".to_string(),
        advertise_addr: "10.0.0.4:7000".to_string(),
        client_advertise_addr: None,
        kafka_advertise_addr: None,
        region: "us-west-2".to_string(),
        zone: None,
        region_bridges: Vec::new(),
        features: Default::default(),
    }
}

/// Nothing advances a standalone broker's commit offsets, so a saved hold
/// would stop retention for good.
#[test]
fn a_standalone_broker_lifts_saved_holds() {
    assert_eq!(
        retention_hold(&BrokerConfig::default()),
        RetentionHold::Lifted
    );
}

/// A clustered broker opens its logs before its replication starts; it must
/// keep their holds through that.
#[test]
fn a_clustered_broker_keeps_saved_holds() {
    let member = BrokerConfig {
        membership: Some(membership()),
        ..BrokerConfig::default()
    };
    assert_eq!(retention_hold(&member), RetentionHold::AsSaved);
    let peer = BrokerConfig {
        peer_transport: Some(felix_replication::peer::PeerTransportConfig::default()),
        ..BrokerConfig::default()
    };
    assert_eq!(retention_hold(&peer), RetentionHold::AsSaved);
}
