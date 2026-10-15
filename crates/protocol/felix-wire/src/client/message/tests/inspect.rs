use crate::{
    InspectedAssignment, InspectedFence, InspectedLease, InspectedReplica, InspectedSubscription,
    Message, ShardInspection, ShardKind, SubscriptionCursor, SubscriptionFilter,
};

fn fencing_leader() -> ShardInspection {
    ShardInspection {
        node_id: "broker-a".to_string(),
        shards: 4,
        role: "leader".to_string(),
        phase: "fencing".to_string(),
        serving: false,
        reason: Some("fencing".to_string()),
        detail: Some("0 of 2 replicas took the fence".to_string()),
        generation: Some(42),
        assignment: Some(InspectedAssignment {
            generation: 42,
            leader: "broker-a".to_string(),
            replicas: vec!["broker-b".to_string(), "broker-c".to_string()],
            draining: false,
            successor: Some("broker-c".to_string()),
        }),
        fence: Some(InspectedFence {
            took: vec![],
            pending: vec!["broker-b".to_string(), "broker-c".to_string()],
            attempts: 4,
            retry_in_ms: Some(2000),
        }),
        lease: Some(InspectedLease {
            held: true,
            remaining_ms: 7100,
        }),
        tail: Some(1_048_576),
        committed: Some(1_048_510),
        accepted_generation: Some(42),
        replicas: vec![InspectedReplica {
            node_id: "broker-b".to_string(),
            role: "follower".to_string(),
            next_offset: None,
            lag: None,
            fence: Some(false),
            state: "fencing".to_string(),
            halted: None,
        }],
    }
}

#[test]
fn a_shard_inspect_exchange_round_trips() {
    let request = Message::ShardInspect {
        tenant_id: "acme".to_string(),
        namespace: "default".to_string(),
        name: "orders".to_string(),
        kind: ShardKind::Stream,
        shard: 3,
        request_id: 9,
    };
    assert_eq!(
        Message::decode(request.encode().expect("encode")).expect("decode"),
        request
    );
    let answer = Message::ShardInspectInfo {
        view: Box::new(fencing_leader()),
        request_id: 9,
    };
    assert_eq!(
        Message::decode(answer.encode().expect("encode")).expect("decode"),
        answer
    );
}

/// A broker with nothing to say about the shard leaves every optional field
/// off the wire, and the frame still decodes.
#[test]
fn a_bare_inspection_carries_only_its_identity() {
    let view = ShardInspection {
        node_id: String::new(),
        shards: 0,
        role: "none".to_string(),
        phase: "unassigned".to_string(),
        serving: false,
        reason: Some("not_assigned_here".to_string()),
        detail: None,
        generation: None,
        assignment: None,
        fence: None,
        lease: None,
        tail: None,
        committed: None,
        accepted_generation: None,
        replicas: vec![],
    };
    let frame = Message::ShardInspectInfo {
        view: Box::new(view.clone()),
        request_id: 1,
    }
    .encode()
    .expect("encode");
    let json: serde_json::Value = serde_json::from_slice(&frame.payload).expect("json");
    assert_eq!(
        json,
        serde_json::json!({
            "type": "shard_inspect_info",
            "view": {
                "node_id": "",
                "shards": 0,
                "role": "none",
                "phase": "unassigned",
                "serving": false,
                "reason": "not_assigned_here",
            },
            "request_id": 1,
        })
    );
}

/// A newer broker may add fields and words. An older felixctl ignores the
/// fields and keeps the words as text, rather than failing to decode.
#[test]
fn an_inspection_from_a_newer_broker_still_decodes() {
    let payload = serde_json::json!({
        "type": "shard_inspect_info",
        "view": {
            "node_id": "broker-a",
            "shards": 1,
            "role": "observer",
            "phase": "rebalancing",
            "serving": false,
            "reason": "something_new",
            "added_later": {"x": 1},
            "replicas": [{
                "node_id": "broker-b",
                "role": "witness",
                "state": "sleeping",
                "added_later": true,
            }],
        },
        "request_id": 2,
    });
    let decoded: Message = serde_json::from_value(payload).expect("decode");
    let Message::ShardInspectInfo { view, request_id } = decoded else {
        panic!("not an inspection: {decoded:?}");
    };
    assert_eq!(request_id, 2);
    assert_eq!(view.phase, "rebalancing");
    assert_eq!(view.replicas[0].state, "sleeping");
}

/// To a broker that predates inspection the request is a type it does not
/// know, which it can answer with `unsupported` rather than lose the stream.
#[test]
fn an_unknown_request_still_names_its_type_and_id() {
    let frame = crate::client::frame::Frame::new(
        0,
        bytes::Bytes::from_static(br#"{"type":"shard_inspect_v2","tenant_id":"t","request_id":5}"#),
    )
    .expect("frame");
    let unknown = Message::unknown_request(&frame).expect("readable");
    assert_eq!(unknown.request_type, "shard_inspect_v2");
    assert_eq!(unknown.request_id, Some(5));
}

fn dropping_subscriber() -> InspectedSubscription {
    InspectedSubscription {
        tenant_id: "acme".to_string(),
        namespace: "default".to_string(),
        stream: "orders".to_string(),
        shard: 3,
        subscriber_id: 17,
        subscription_id: Some(9001),
        connection: Some(42),
        peer: Some("10.0.0.7:51234".to_string()),
        principal: Some("p:billing".to_string()),
        policy: "drop_new".to_string(),
        depth: 1024,
        capacity: 1024,
        dropped: 3812,
        position: Some(1_040_000),
        tail: 1_048_576,
        age_ms: 61_000,
    }
}

#[test]
fn a_subscriptions_list_exchange_round_trips() {
    let request = Message::SubscriptionsList {
        filter: Box::new(SubscriptionFilter {
            tenant_id: Some("acme".to_string()),
            stream: Some("orders".to_string()),
            dropping: true,
            ..SubscriptionFilter::default()
        }),
        limit: Some(50),
        cursor: Some(SubscriptionCursor {
            tenant_id: "acme".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
            shard: 2,
            subscriber_id: 4,
        }),
        request_id: 7,
    };
    assert_eq!(
        Message::decode(request.encode().expect("encode")).expect("decode"),
        request
    );
    let answer = Message::SubscriptionsListInfo {
        node_id: "broker-a".to_string(),
        subscriptions: vec![dropping_subscriber()],
        next_cursor: None,
        request_id: 7,
    };
    assert_eq!(
        Message::decode(answer.encode().expect("encode")).expect("decode"),
        answer
    );
}

/// A request with no filter, limit or cursor is just its type and id, and a
/// subscriber with no owner leaves the owner's fields off.
#[test]
fn an_unfiltered_list_and_an_unowned_subscriber_stay_small() {
    let request = Message::SubscriptionsList {
        filter: Box::default(),
        limit: None,
        cursor: None,
        request_id: 1,
    };
    let json: serde_json::Value =
        serde_json::from_slice(&request.encode().expect("encode").payload).expect("json");
    assert_eq!(
        json,
        serde_json::json!({"type": "subscriptions_list", "filter": {}, "request_id": 1})
    );
    let decoded: Message =
        serde_json::from_value(serde_json::json!({"type": "subscriptions_list", "request_id": 1}))
            .expect("decode without a filter");
    assert_eq!(decoded, request);

    let bare = InspectedSubscription {
        subscription_id: None,
        connection: None,
        peer: None,
        principal: None,
        position: None,
        ..dropping_subscriber()
    };
    let json = serde_json::to_value(&bare).expect("json");
    for absent in [
        "subscription_id",
        "connection",
        "peer",
        "principal",
        "position",
    ] {
        assert!(json.get(absent).is_none(), "{absent} in {json}");
    }
}
