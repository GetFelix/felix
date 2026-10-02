use crate::Message;

#[test]
fn a_topology_exchange_round_trips() {
    let request = Message::Topology;
    assert_eq!(
        Message::decode(request.encode().expect("encode")).expect("decode"),
        request
    );

    let view = Message::TopologyView {
        brokers: vec![
            crate::BrokerEndpoint {
                node_id: "broker-a".to_string(),
                addr: "127.0.0.1:5000".to_string(),
            },
            crate::BrokerEndpoint {
                node_id: "broker-b".to_string(),
                addr: "127.0.0.1:5010".to_string(),
            },
        ],
    };
    assert_eq!(
        Message::decode(view.encode().expect("encode")).expect("decode"),
        view
    );
}

/// A cluster with nothing to report says so, rather than failing.
#[test]
fn an_empty_topology_round_trips() {
    let view = Message::TopologyView { brokers: vec![] };
    assert_eq!(
        Message::decode(view.encode().expect("encode")).expect("decode"),
        view
    );
}

#[test]
fn a_not_leader_round_trips() {
    let message = Message::NotLeader {
        node_id: "broker-b".to_string(),
        addr: Some("10.0.0.5:5000".to_string()),
        generation: 7,
    };
    assert_eq!(
        Message::decode(message.encode().expect("encode")).expect("decode"),
        message
    );
}

/// **A redirect with no address still names the owner.** "Not here, and here is
/// who has it" is more use than "not here", and a client that knows that broker
/// from discovery can act on the name alone.
#[test]
fn a_not_leader_without_an_address_round_trips() {
    let message = Message::NotLeader {
        node_id: "broker-b".to_string(),
        addr: None,
        generation: 7,
    };
    let decoded = Message::decode(message.encode().expect("encode")).expect("decode");
    assert_eq!(decoded, message);

    let json = String::from_utf8(message.encode().expect("encode").payload.to_vec()).expect("utf8");
    assert!(
        !json.contains("addr"),
        "an absent address must not appear: {json}"
    );
}

#[test]
fn cache_shards_round_trips() {
    for message in [
        Message::CacheShards {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            cache: "sessions".to_string(),
            request_id: 3,
        },
        Message::CacheShardsView {
            shards: 4,
            request_id: 3,
        },
    ] {
        let frame = message.encode().expect("encode");
        assert_eq!(Message::decode(frame).expect("decode"), message);
    }
    let json = serde_json::to_string(&Message::CacheShardsView {
        shards: 4,
        request_id: 3,
    })
    .expect("serialize");
    assert_eq!(
        json,
        r#"{"type":"cache_shards_view","shards":4,"request_id":3}"#
    );
}

#[test]
fn shard_owners_round_trips() {
    for message in [
        Message::ShardOwners {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            name: "orders".to_string(),
            kind: crate::ShardKind::Cache,
            request_id: 5,
        },
        Message::ShardOwnersView {
            owners: vec![
                crate::ShardOwner {
                    shard: 0,
                    node_id: Some("broker-a".to_string()),
                    addr: Some("10.0.0.4:5000".to_string()),
                    generation: 3,
                    unavailable: None,
                },
                crate::ShardOwner {
                    shard: 1,
                    node_id: None,
                    addr: None,
                    generation: 0,
                    unavailable: Some("not_assigned".to_string()),
                },
            ],
            request_id: 5,
        },
    ] {
        let frame = message.encode().expect("encode");
        assert_eq!(Message::decode(frame).expect("decode"), message);
    }
    let json = serde_json::to_string(&Message::ShardOwners {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        name: "orders".to_string(),
        kind: crate::ShardKind::Stream,
        request_id: 5,
    })
    .expect("serialize");
    assert_eq!(
        json,
        r#"{"type":"shard_owners","tenant_id":"t1","namespace":"ns","name":"orders","kind":"stream","request_id":5}"#
    );
    // Absent fields are left out, not written as null.
    let json = serde_json::to_string(&crate::ShardOwner {
        shard: 2,
        node_id: None,
        addr: None,
        generation: 0,
        unavailable: None,
    })
    .expect("serialize");
    assert_eq!(json, r#"{"shard":2,"generation":0}"#);
}
