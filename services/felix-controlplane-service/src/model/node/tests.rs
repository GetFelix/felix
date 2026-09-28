//! Unit tests for the node model: validation, patching, and serialization.
use super::*;

fn node() -> Node {
    Node {
        node_id: "broker-1".to_string(),
        spec: NodeSpec {
            advertise_addr: "10.0.0.4:7000".to_string(),
            client_addr: None,
            kafka_addr: None,
            region: "us-west-2".to_string(),
            zone: None,
            labels: BTreeMap::from([("rack".to_string(), "a1".to_string())]),
            capacity: NodeCapacity {
                max_shards: Some(64),
                weight: 2,
            },
        },
        status: NodeStatus {
            lifecycle: NodeLifecycle::Live,
            last_heartbeat_at_millis: 1_700_000_000_000,
            registered_at_millis: 1_699_000_000_000,
            incarnation: 3,
        },
    }
}

#[test]
fn a_well_formed_node_validates() {
    assert_eq!(node().validate(), Ok(()));
}

#[test]
fn an_empty_node_id_is_rejected() {
    let mut node = node();
    node.node_id = String::new();
    assert_eq!(node.validate(), Err(NodeValidationError::EmptyNodeId));
}

#[test]
fn a_node_id_needing_quoting_elsewhere_is_rejected() {
    for (id, bad) in [("Broker-1", 'B'), ("broker 1", ' '), ("broker/1", '/')] {
        let mut node = node();
        node.node_id = id.to_string();
        assert_eq!(
            node.validate(),
            Err(NodeValidationError::NodeIdCharacter(bad)),
            "{id} should be rejected",
        );
    }
}

#[test]
fn an_over_long_node_id_is_rejected() {
    let mut node = node();
    node.node_id = "a".repeat(MAX_IDENTIFIER_LEN + 1);
    assert_eq!(node.validate(), Err(NodeValidationError::NodeIdTooLong));

    node.node_id = "a".repeat(MAX_IDENTIFIER_LEN);
    assert_eq!(node.validate(), Ok(()));
}

#[test]
fn an_unusable_advertise_address_is_rejected() {
    for addr in ["", "10.0.0.4", "not-an-address:7000", "10.0.0.4:port"] {
        let mut node = node();
        node.spec.advertise_addr = addr.to_string();
        assert_eq!(
            node.validate(),
            Err(NodeValidationError::InvalidAdvertiseAddr(addr.to_string())),
            "{addr} should be rejected",
        );
    }
}

/// Port 0 parses, but a listener reads it as "any port", so it is never dialable.
#[test]
fn a_zero_advertise_port_is_rejected() {
    let mut node = node();
    node.spec.advertise_addr = "10.0.0.4:0".to_string();
    assert_eq!(node.validate(), Err(NodeValidationError::ZeroAdvertisePort));
}

#[test]
fn an_ipv6_advertise_address_is_accepted() {
    let mut node = node();
    node.spec.advertise_addr = "[2001:db8::1]:7000".to_string();
    assert_eq!(node.validate(), Ok(()));
}

#[test]
fn an_empty_region_is_rejected() {
    let mut node = node();
    node.spec.region = "   ".to_string();
    assert_eq!(node.validate(), Err(NodeValidationError::EmptyRegion));
}

#[test]
fn a_blank_or_overlong_zone_is_rejected() {
    let mut node = node();
    node.spec.zone = Some(" ".to_string());
    assert_eq!(node.validate(), Err(NodeValidationError::EmptyZone));
    node.spec.zone = Some("z".repeat(254));
    assert_eq!(node.validate(), Err(NodeValidationError::ZoneTooLong));
    node.spec.zone = Some("us-west-2a".to_string());
    assert_eq!(node.validate(), Ok(()));
}

/// A record stored before zones existed has no `zone` key, and one without a
/// zone serializes without it, so old and new instances read each other.
#[test]
fn a_node_without_a_zone_has_no_zone_key() {
    let json = serde_json::to_value(node()).expect("serialize");
    assert!(json["spec"].get("zone").is_none(), "{json}");
    let back: Node = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back.spec.zone, None);
}

#[test]
fn malformed_labels_are_rejected() {
    let mut node = node();
    node.spec.labels = BTreeMap::from([(String::new(), "a".to_string())]);
    assert_eq!(node.validate(), Err(NodeValidationError::EmptyLabelKey));

    let long = "a".repeat(MAX_IDENTIFIER_LEN + 1);
    node.spec.labels = BTreeMap::from([(long.clone(), "a".to_string())]);
    assert_eq!(
        node.validate(),
        Err(NodeValidationError::LabelKeyTooLong(long))
    );

    node.spec.labels = BTreeMap::from([("rack".to_string(), "a".repeat(MAX_IDENTIFIER_LEN + 1))]);
    assert_eq!(
        node.validate(),
        Err(NodeValidationError::LabelValueTooLong("rack".to_string()))
    );
}

#[test]
fn a_zero_max_shards_hint_is_rejected() {
    let mut node = node();
    node.spec.capacity.max_shards = Some(0);
    assert_eq!(node.validate(), Err(NodeValidationError::ZeroMaxShards));

    node.spec.capacity.max_shards = None;
    assert_eq!(node.validate(), Ok(()));
}

#[test]
fn a_node_that_is_not_serving_cannot_start_draining() {
    for from in [NodeLifecycle::Down, NodeLifecycle::Left] {
        assert!(
            !from.can_transition_to(NodeLifecycle::Draining),
            "{from:?} should not be drainable",
        );
    }
}

/// A broker keeps its identity across a restart, so re-registering has to be
/// able to revive a record that reached `Down` or `Left`.
#[test]
fn a_restart_revives_a_down_or_departed_node() {
    for from in [NodeLifecycle::Down, NodeLifecycle::Left] {
        assert!(from.can_transition_to(NodeLifecycle::Live));
    }
}

#[test]
fn every_lifecycle_can_stay_where_it_is() {
    for state in [
        NodeLifecycle::Live,
        NodeLifecycle::Draining,
        NodeLifecycle::Down,
        NodeLifecycle::Left,
    ] {
        assert!(state.can_transition_to(state), "{state:?} should be stable");
    }
}

#[test]
fn a_patch_updates_only_the_fields_it_names() {
    let before = node();
    let patch = NodePatchRequest {
        region: Some("eu-central-1".to_string()),
        ..NodePatchRequest::default()
    };

    let after = patch.apply(&before).expect("patch");
    assert_eq!(after.spec.region, "eu-central-1");
    assert_eq!(after.spec.labels, before.spec.labels);
    assert_eq!(after.spec.capacity, before.spec.capacity);
    assert_eq!(after.status, before.status);
}

#[test]
fn a_patch_can_drive_an_allowed_lifecycle_transition() {
    let before = node();
    let patch = NodePatchRequest {
        lifecycle: Some(NodeLifecycle::Draining),
        ..NodePatchRequest::default()
    };

    let after = patch.apply(&before).expect("patch");
    assert_eq!(after.status.lifecycle, NodeLifecycle::Draining);
    assert_eq!(
        after.status.last_heartbeat_at_millis,
        before.status.last_heartbeat_at_millis
    );
}

#[test]
fn a_patch_cannot_drive_an_unsupported_lifecycle_transition() {
    let mut before = node();
    before.status.lifecycle = NodeLifecycle::Down;
    let patch = NodePatchRequest {
        lifecycle: Some(NodeLifecycle::Draining),
        ..NodePatchRequest::default()
    };

    assert_eq!(
        patch.apply(&before),
        Err(NodeValidationError::UnsupportedTransition {
            from: NodeLifecycle::Down,
            to: NodeLifecycle::Draining,
        }),
    );
}

#[test]
fn a_rejected_patch_leaves_the_node_untouched() {
    let before = node();
    let patch = NodePatchRequest {
        region: Some(String::new()),
        ..NodePatchRequest::default()
    };

    assert_eq!(patch.apply(&before), Err(NodeValidationError::EmptyRegion));
    assert_eq!(before, node());
}

/// The whole point of splitting spec from status: an admin patch has no field
/// that could claim a broker is alive.
#[test]
fn a_patch_cannot_forge_observed_liveness() {
    let before = node();
    let json = serde_json::json!({
        "region": "eu-central-1",
        "lastHeartbeatAtMillis": 9_999_999_999_999u64,
        "last_heartbeat_at_millis": 9_999_999_999_999u64,
        "incarnation": 99,
        "registered_at_millis": 0,
    });

    let patch: NodePatchRequest = serde_json::from_value(json).expect("deserialize");
    let after = patch.apply(&before).expect("patch");
    assert_eq!(after.status, before.status);
}

#[test]
fn a_node_round_trips_through_json() {
    let before = node();
    let encoded = serde_json::to_string(&before).expect("serialize");
    let after: Node = serde_json::from_str(&encoded).expect("deserialize");
    assert_eq!(after, before);
}

/// Labels sit in a `BTreeMap` so a change payload for the same node is byte
/// identical however the map was built.
#[test]
fn label_order_does_not_change_the_encoding() {
    let mut ascending = node();
    ascending.spec.labels = BTreeMap::from([
        ("a".to_string(), "1".to_string()),
        ("z".to_string(), "2".to_string()),
    ]);
    let mut descending = node();
    descending.spec.labels = BTreeMap::from([
        ("z".to_string(), "2".to_string()),
        ("a".to_string(), "1".to_string()),
    ]);

    assert_eq!(
        serde_json::to_string(&ascending).expect("serialize"),
        serde_json::to_string(&descending).expect("serialize"),
    );
}

#[test]
fn optional_spec_fields_default_when_absent() {
    let json = serde_json::json!({
        "advertise_addr": "10.0.0.4:7000",
        "region": "us-west-2",
    });

    let spec: NodeSpec = serde_json::from_value(json).expect("deserialize");
    assert!(spec.labels.is_empty());
    assert!(spec.kafka_addr.is_none());
    assert_eq!(spec.capacity, NodeCapacity::default());
    assert_eq!(spec.validate(), Ok(()));
}

/// `weight` defaults to 1 rather than 0, so a spec that omits it is not silently
/// given zero share of placement.
#[test]
fn an_omitted_capacity_weight_defaults_to_one() {
    let capacity: NodeCapacity =
        serde_json::from_value(serde_json::json!({})).expect("deserialize");
    assert_eq!(capacity.weight, 1);
    assert_eq!(NodeCapacity::default().weight, 1);
}

#[test]
fn change_ops_serialize_as_camel_case() {
    for (op, expected) in [
        (NodeChangeOp::Registered, "\"registered\""),
        (NodeChangeOp::Updated, "\"updated\""),
        (NodeChangeOp::Deregistered, "\"deregistered\""),
    ] {
        assert_eq!(serde_json::to_string(&op).expect("serialize"), expected);
    }
}

/// A deregistration carries no node body, matching the other change feeds.
#[test]
fn a_change_round_trips_with_and_without_a_body() {
    for change in [
        NodeChange {
            seq: 7,
            op: NodeChangeOp::Registered,
            node_id: "broker-1".to_string(),
            node: Some(node()),
        },
        NodeChange {
            seq: 8,
            op: NodeChangeOp::Deregistered,
            node_id: "broker-1".to_string(),
            node: None,
        },
    ] {
        let encoded = serde_json::to_string(&change).expect("serialize");
        let decoded: NodeChange = serde_json::from_str(&encoded).expect("deserialize");
        assert_eq!(decoded.seq, change.seq);
        assert_eq!(decoded.op, change.op);
        assert_eq!(decoded.node_id, change.node_id);
        assert_eq!(decoded.node, change.node);
    }
}

/// A client address is optional, and a node without one is valid: that is every
/// broker registered before clients could be told where to connect.
#[test]
fn a_node_without_a_client_address_is_valid() {
    let node = node();
    assert!(node.spec.client_addr.is_none());
    assert!(node.validate().is_ok());
}

#[test]
fn a_valid_client_address_is_accepted() {
    let mut node = node();
    node.spec.client_addr = Some("10.0.0.4:5000".to_string());
    assert!(node.validate().is_ok());
}

/// Held to the same rules as the internal address: a client has to dial it,
/// and an address that cannot be parsed or a port that cannot be connected to
/// is worse stored than absent -- absent means "ask someone else", stored means
/// "go here" to somewhere nothing is listening.
#[test]
fn an_unusable_client_address_is_rejected() {
    for addr in ["not-an-address", "10.0.0.4", "", "10.0.0.4:"] {
        let mut node = node();
        node.spec.client_addr = Some(addr.to_string());
        assert!(
            node.validate().is_err(),
            "client_addr {addr:?} should be rejected"
        );
    }
}

#[test]
fn a_zero_client_port_is_rejected() {
    let mut node = node();
    node.spec.client_addr = Some("10.0.0.4:0".to_string());
    assert!(matches!(
        node.validate(),
        Err(NodeValidationError::ZeroClientPort)
    ));
}

#[test]
fn a_kafka_address_may_be_a_hostname_or_an_ip() {
    for addr in [
        "host.docker.internal:9092",
        "broker-1:9092",
        "10.0.0.4:9092",
        "[::1]:9092",
        "[2001:db8::7]:19092",
    ] {
        let mut node = node();
        node.spec.kafka_addr = Some(addr.to_string());
        assert_eq!(node.validate(), Ok(()), "kafka_addr {addr:?}");
    }
}

#[test]
fn an_unusable_kafka_address_is_rejected() {
    for addr in [
        "",
        "broker-1",
        "broker-1:",
        ":9092",
        "broker-1:port",
        "broker-1:70000",
        "::1:9092",
        "[::1:9092",
        "[not-v6]:9092",
        "bad host:9092",
        "-lead.example:9092",
        "a..b:9092",
    ] {
        let mut node = node();
        node.spec.kafka_addr = Some(addr.to_string());
        assert_eq!(
            node.validate(),
            Err(NodeValidationError::InvalidKafkaAddr(addr.to_string())),
            "kafka_addr {addr:?} should be rejected"
        );
    }
}

#[test]
fn a_zero_kafka_port_is_rejected() {
    let mut node = node();
    node.spec.kafka_addr = Some("host.docker.internal:0".to_string());
    assert_eq!(node.validate(), Err(NodeValidationError::ZeroKafkaPort));
}

/// An operator can put a draining broker back into placement...
#[test]
fn a_patch_can_undrain_a_draining_node() {
    let mut before = node();
    before.status.lifecycle = NodeLifecycle::Draining;
    let patch = NodePatchRequest {
        lifecycle: Some(NodeLifecycle::Live),
        ..NodePatchRequest::default()
    };

    let after = patch.apply(&before).expect("patch");
    assert_eq!(after.status.lifecycle, NodeLifecycle::Live);
}

/// ...but not declare a silent one alive. Only registration revives a node.
#[test]
fn a_patch_cannot_revive_a_node_nothing_has_heard_from() {
    for from in [NodeLifecycle::Down, NodeLifecycle::Left] {
        let mut before = node();
        before.status.lifecycle = from;
        let patch = NodePatchRequest {
            lifecycle: Some(NodeLifecycle::Live),
            ..NodePatchRequest::default()
        };

        assert_eq!(
            patch.apply(&before),
            Err(NodeValidationError::UnsupportedTransition {
                from,
                to: NodeLifecycle::Live,
            }),
        );
    }
}
