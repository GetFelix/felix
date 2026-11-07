use napi::bindgen_prelude::{BigInt, Either};

use super::{
    InspectedSubscription, ShardInspection, SubscriptionCursor, SubscriptionFilter, kind_of,
};

fn is_null<T>(value: &Either<T, napi::bindgen_prelude::Null>) -> bool {
    matches!(value, Either::B(_))
}

fn minimal() -> felix_client::ShardInspection {
    felix_client::ShardInspection {
        node_id: "n1".into(),
        shards: 4,
        role: "none".into(),
        phase: "unassigned".into(),
        serving: false,
        reason: None,
        detail: None,
        generation: None,
        assignment: None,
        fence: None,
        lease: None,
        tail: None,
        committed: None,
        accepted_generation: None,
        replicas: vec![],
    }
}

#[test]
fn a_view_with_nothing_optional_keeps_every_absent_field_null() {
    let view = ShardInspection::from(minimal());
    assert_eq!(
        (view.node_id.as_str(), view.shards, view.serving),
        ("n1", 4, false)
    );
    assert!(is_null(&view.reason) && is_null(&view.generation) && is_null(&view.assignment));
    assert!(is_null(&view.fence) && is_null(&view.lease) && is_null(&view.tail));
    assert!(is_null(&view.committed) && is_null(&view.accepted_generation));
    assert!(view.replicas.is_empty());
}

#[test]
fn a_leaders_view_carries_its_assignment_fence_lease_and_replicas() {
    let view = ShardInspection::from(felix_client::ShardInspection {
        role: "leader".into(),
        phase: "active".into(),
        serving: true,
        generation: Some(7),
        tail: Some(100),
        committed: Some(98),
        assignment: Some(felix_client::InspectedAssignment {
            generation: 7,
            leader: "n1".into(),
            replicas: vec!["n2".into()],
            draining: true,
            successor: Some("n3".into()),
        }),
        fence: Some(felix_client::InspectedFence {
            took: vec!["n2".into()],
            pending: vec!["n3".into()],
            attempts: 2,
            retry_in_ms: Some(500),
        }),
        lease: Some(felix_client::InspectedLease {
            held: true,
            remaining_ms: 900,
        }),
        replicas: vec![felix_client::InspectedReplica {
            node_id: "n2".into(),
            role: "follower".into(),
            next_offset: Some(99),
            lag: Some(1),
            fence: None,
            state: "shipping".into(),
            halted: None,
        }],
        ..minimal()
    });
    assert!(view.serving);
    let Either::A(assignment) = view.assignment else {
        panic!("assignment dropped")
    };
    assert_eq!(assignment.leader, "n1");
    assert!(assignment.draining);
    assert!(matches!(assignment.successor, Either::A(ref s) if s == "n3"));
    let Either::A(fence) = view.fence else {
        panic!("fence dropped")
    };
    assert_eq!(
        (fence.attempts, fence.took, fence.pending),
        (2, vec!["n2".to_string()], vec!["n3".to_string()])
    );
    assert!(matches!(fence.retry_in_ms, Either::A(ref ms) if ms.get_u64().1 == 500));
    let Either::A(lease) = view.lease else {
        panic!("lease dropped")
    };
    assert!(lease.held && lease.remaining_ms.get_u64().1 == 900);
    assert!(matches!(view.committed, Either::A(ref c) if c.get_u64().1 == 98));
    assert_eq!(view.replicas.len(), 1);
    assert_eq!(view.replicas[0].state, "shipping");
    assert!(matches!(view.replicas[0].lag, Either::A(ref l) if l.get_u64().1 == 1));
    assert!(is_null(&view.replicas[0].fence) && is_null(&view.replicas[0].halted));
}

#[test]
fn a_filter_with_nothing_set_lists_everything_and_dropping_defaults_off() {
    let filter: felix_client::SubscriptionFilter = SubscriptionFilter {
        tenant_id: None,
        namespace: None,
        stream: Some("orders".into()),
        shard: Some(2),
        principal: None,
        dropping: None,
    }
    .into();
    assert_eq!(filter.stream.as_deref(), Some("orders"));
    assert_eq!(filter.shard, Some(2));
    assert!(!filter.dropping && filter.tenant_id.is_none() && filter.principal.is_none());
}

#[test]
fn a_cursor_survives_the_round_trip_and_a_negative_id_is_refused() {
    let cursor = SubscriptionCursor {
        tenant_id: "t".into(),
        namespace: "ns".into(),
        stream: "s".into(),
        shard: 3,
        subscriber_id: BigInt::from(42u64),
    };
    let wire = cursor.into_client().unwrap();
    assert_eq!((wire.shard, wire.subscriber_id), (3, 42));
    let bad = SubscriptionCursor {
        tenant_id: "t".into(),
        namespace: "ns".into(),
        stream: "s".into(),
        shard: 0,
        subscriber_id: BigInt::from(-1i64),
    };
    assert!(bad.into_client().is_err());
}

#[test]
fn a_subscription_keeps_its_queue_numbers_and_nulls_what_the_broker_left_out() {
    let sub = InspectedSubscription::from(felix_client::InspectedSubscription {
        tenant_id: "t".into(),
        namespace: "ns".into(),
        stream: "s".into(),
        shard: 1,
        subscriber_id: 9,
        subscription_id: None,
        connection: Some(5),
        peer: None,
        principal: Some("p:a".into()),
        policy: "drop_new".into(),
        depth: 3,
        capacity: 64,
        dropped: 2,
        position: None,
        tail: 500,
        age_ms: 1200,
    });
    assert_eq!((sub.shard, sub.policy.as_str()), (1, "drop_new"));
    assert_eq!(sub.depth.get_u64().1, 3);
    assert_eq!(sub.capacity.get_u64().1, 64);
    assert_eq!(sub.dropped.get_u64().1, 2);
    assert_eq!(sub.tail.get_u64().1, 500);
    assert!(is_null(&sub.subscription_id) && is_null(&sub.peer) && is_null(&sub.position));
    assert!(matches!(sub.connection, Either::A(ref c) if c.get_u64().1 == 5));
    assert!(matches!(sub.principal, Either::A(ref p) if p == "p:a"));
}

#[test]
fn a_shard_kind_is_stream_or_cache_and_nothing_else() {
    assert!(matches!(
        kind_of("stream"),
        Ok(felix_client::ShardKind::Stream)
    ));
    assert!(matches!(
        kind_of("cache"),
        Ok(felix_client::ShardKind::Cache)
    ));
    assert!(kind_of("table").is_err());
}
