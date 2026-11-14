//! The gate on the publish path itself, not just the decision behind it.

use felix_common::random::HashMap;

use super::*;

/// A shard this broker owns and has opened resolves normally.
#[tokio::test]
async fn an_owned_shard_resolves() {
    let broker = broker_with_stream().await;
    let ingress = ingress_for("broker-a", true);
    let mut cache = HashMap::default();
    let mut key = String::new();

    let route = resolve_route(
        &broker,
        Authority {
            ingress: Some(&ingress),
            lease: None,
        },
        &mut cache,
        &mut key,
        "t1",
        "ns",
        "stream",
        0,
    )
    .await;
    assert!(
        matches!(route, PublishRoute::Local { .. }),
        "an owned, open shard must be servable",
    );
}

/// The acceptance criterion, on the real path: a broker must not write a
/// shard it does not own, however healthy the stream is locally. It routes
/// the publish to the owner instead.
#[tokio::test]
async fn a_shard_owned_elsewhere_is_forwarded_not_served_locally() {
    let broker = broker_with_stream().await;
    let ingress = ingress_for("broker-b", false);
    let mut cache = HashMap::default();
    let mut key = String::new();

    let route = resolve_route(
        &broker,
        Authority {
            ingress: Some(&ingress),
            lease: None,
        },
        &mut cache,
        &mut key,
        "t1",
        "ns",
        "stream",
        0,
    )
    .await;
    match route {
        PublishRoute::Forward(target) => assert_eq!(target.node_id, "broker-b"),
        other => panic!("must route to the owner, not serve locally: {other:?}"),
    }
}

/// Owned but not yet opened. The stream exists locally, so only the gate
/// can refuse this.
#[tokio::test]
async fn an_owned_but_unopened_shard_is_refused() {
    let broker = broker_with_stream().await;
    let ingress = ingress_for("broker-a", false);
    let mut cache = HashMap::default();
    let mut key = String::new();

    let route = resolve_route(
        &broker,
        Authority {
            ingress: Some(&ingress),
            lease: None,
        },
        &mut cache,
        &mut key,
        "t1",
        "ns",
        "stream",
        0,
    )
    .await;
    let PublishRoute::Refused(refusal) = route else {
        panic!("an unopened shard must not accept writes");
    };
    // And the client is told why, in a form it can act on.
    let felix_wire::Message::Error { code, detail, .. } = refusal.into_message() else {
        unreachable!()
    };
    assert_eq!(code, Some(felix_wire::ErrorCode::ShardUnavailable));
    assert_eq!(detail.and_then(|d| d.reason).as_deref(), Some("not_ready"));
}

/// A subscribe to a shard nobody can serve names the reason, not only prose.
#[tokio::test]
async fn an_unservable_subscribe_is_shard_unavailable() {
    let ingress = ingress_for("broker-a", false);
    let answer = crate::serving::quic::handlers::redirect::redirect_for(
        Some(&ingress),
        None,
        "t1",
        "ns",
        "stream",
        0,
        crate::shards::ShardKind::Stream,
        felix_wire::FEATURE_ERROR_CODES,
    );
    let Some(felix_wire::Message::Error {
        code,
        retry,
        detail,
        ..
    }) = answer
    else {
        panic!("expected an error, got {answer:?}");
    };
    assert_eq!(code, Some(felix_wire::ErrorCode::ShardUnavailable));
    assert_eq!(retry, Some(felix_wire::RetryClass::Retry));
    assert_eq!(detail.and_then(|d| d.reason).as_deref(), Some("not_ready"));
}

/// Ownership is checked outside the handle cache, so a reassignment takes
/// effect immediately rather than after the cache TTL.
#[tokio::test]
async fn losing_a_shard_takes_effect_without_waiting_for_the_cache() {
    let broker = broker_with_stream().await;
    let ingress = ingress_for("broker-a", true);
    let mut cache = HashMap::default();
    let mut key = String::new();

    assert!(
        matches!(
            resolve_route(
                &broker,
                Authority {
                    ingress: Some(&ingress),
                    lease: None
                },
                &mut cache,
                &mut key,
                "t1",
                "ns",
                "stream",
                0
            )
            .await,
            PublishRoute::Local { .. }
        ),
        "warm the handle cache while the shard is ours",
    );

    // The shard moves away. The stream handle is still cached and valid, so
    // only the gate can notice.
    let moved = ingress_for("broker-b", false);
    assert!(
        matches!(
            resolve_route(
                &broker,
                Authority {
                    ingress: Some(&moved),
                    lease: None
                },
                &mut cache,
                &mut key,
                "t1",
                "ns",
                "stream",
                0
            )
            .await,
            PublishRoute::Forward(_)
        ),
        "a cached handle must not outlive ownership",
    );
}

/// The single-node path: no router, no gate, unchanged behaviour.
#[tokio::test]
async fn a_single_node_broker_is_unaffected() {
    let broker = broker_with_stream().await;
    let mut cache = HashMap::default();
    let mut key = String::new();

    let route = resolve_route(
        &broker,
        Authority {
            ingress: None,
            lease: None,
        },
        &mut cache,
        &mut key,
        "t1",
        "ns",
        "stream",
        0,
    )
    .await;
    assert!(matches!(route, PublishRoute::Local { .. }));
}

/// **A publish to a Raft shard this broker leads resolves here at the
/// group's term, with no broker lease held.** The term is what a Raft
/// leader holds the shard by, and the write has no place in the hybrid's
/// fence.
#[cfg(feature = "raft")]
#[tokio::test]
async fn a_raft_shard_led_here_resolves_without_the_broker_lease() {
    use crate::shards::raft::tests as fixture;
    use crate::shards::routing::EngineShards;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = fixture::broker(root.path()).await;
    let shards = fixture::shards(&broker);
    let orders = fixture::key("orders", 0);
    let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
    shards.take(&mut fixture::assignments([assigned])).await;
    fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
    let term = shards.leading(&orders).expect("term");
    let ingress = fixture::ingress(&shards);
    // Never renewed, so it does not look valid.
    let lease = crate::cluster::lease::LeaseState::new(Duration::from_secs(10));
    let mut cache = HashMap::default();
    let mut key = String::new();

    let route = resolve_route(
        &broker,
        Authority {
            ingress: Some(&ingress),
            lease: Some(&lease),
        },
        &mut cache,
        &mut key,
        "t1",
        "ns",
        "orders",
        0,
    )
    .await;

    match route {
        PublishRoute::Local {
            generation, fenced, ..
        } => {
            assert_eq!(generation, term);
            assert!(fenced.is_none(), "a Raft write entered the hybrid's fence");
        }
        other => panic!("a Raft leader must take the publish: {other:?}"),
    }
    EngineShards::shutdown(&*shards).await;
}
