//! Subscribing to the broker that does not own the shard.
//!
//! Such a subscribe used to be accepted and then delivered
//! nothing (#118) -- the worst available answer, because it is indistinguishable from
//! a stream with no traffic, and an application waits forever without an error
//! to act on. `docs/subscribe-routing.md` records the decision: redirect to the
//! owner rather than proxy for it.
//!
//! Run with `cargo test -p felix-cluster --test routing redirect::`.
use std::sync::Arc;
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_wire::AckMode;
use serial_test::serial;

const STREAM: &str = "orders";

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::replicated(STREAM, 1, 1)],
        ..Default::default()
    }
}

/// **A subscribe to a non-owner is refused, not silently served.** The whole
/// point: an application that asks the wrong broker is told so.
#[serial]
#[tokio::test]
async fn a_subscribe_to_a_non_owner_is_redirected() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let (owner, non_owner) = cluster
        .owner_and_non_owner(STREAM)
        .await
        .expect("resolve owners");

    let client = felix_cluster::client::connect(
        cluster.node(&non_owner).expect("the non-owner").client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect to the non-owner");

    let error = match client
        .subscribe(&cluster.tenant_id, &cluster.namespace, STREAM)
        .await
    {
        Ok(_) => panic!("a broker that does not own the shard must not serve it"),
        Err(err) => err,
    };
    let redirect = error
        .downcast_ref::<felix_client::NotLeaderError>()
        .expect("the refusal should be a typed redirect");

    assert_eq!(
        redirect.node_id, owner,
        "the redirect named {} but {owner} owns the shard",
        redirect.node_id,
    );
    assert!(
        redirect.addr.is_some(),
        "the redirect gave no address to follow",
    );

    cluster.shutdown().await;
}

/// **A cluster client follows the redirect and receives records.** The end an
/// application cares about: it asked the wrong broker and still ends up
/// subscribed to the right one.
#[serial]
#[tokio::test]
async fn a_cluster_client_follows_the_redirect_to_the_owner() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let (owner, non_owner) = cluster
        .owner_and_non_owner(STREAM)
        .await
        .expect("resolve owners");

    // Seeded with the non-owner alone, so the subscribe below can only succeed
    // by following the redirect.
    let seed = cluster.node(&non_owner).expect("the non-owner").client_addr;
    let client = Arc::new(
        felix_cluster::client::connect_cluster(
            &[seed],
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("connect"),
    );

    let mut subscription = client
        .subscribe(&cluster.tenant_id, &cluster.namespace, STREAM)
        .await
        .expect("the redirect should have been followed to the owner");

    cluster
        .publish_via(&owner, STREAM, b"followed".to_vec())
        .await
        .expect("publish to the owner");

    let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
        .await
        .expect("a record published to the owner should reach the redirected subscriber")
        .expect("subscription error")
        .expect("subscription closed");
    assert_eq!(event.payload.as_ref(), b"followed");

    cluster.shutdown().await;
}

/// Subscribing on the owner itself is unaffected: no redirect, no extra hop.
#[serial]
#[tokio::test]
async fn a_subscribe_to_the_owner_is_served_directly() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");

    let (_client, mut subscription) = cluster
        .subscribe_on(&owner, STREAM)
        .await
        .expect("the owner should serve its own shard");

    cluster
        .publish_via(&owner, STREAM, b"direct".to_vec())
        .await
        .expect("publish");

    let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
        .await
        .expect("the owner should deliver")
        .expect("subscription error")
        .expect("subscription closed");
    assert_eq!(event.payload.as_ref(), b"direct");

    cluster.shutdown().await;
}

/// A publish is still forwarded rather than redirected: the two paths made
/// opposite choices deliberately, and this pins that they still differ.
#[serial]
#[tokio::test]
async fn a_publish_to_a_non_owner_is_still_forwarded() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let (owner, non_owner) = cluster
        .owner_and_non_owner(STREAM)
        .await
        .expect("resolve owners");

    cluster
        .publish_via(&non_owner, STREAM, b"forwarded".to_vec())
        .await
        .expect("a publish to a non-owner is forwarded, not refused");

    let (_client, mut subscription) = cluster
        .subscribe_on(&owner, STREAM)
        .await
        .expect("subscribe on the owner");
    cluster
        .publish_via(&non_owner, STREAM, b"forwarded-again".to_vec())
        .await
        .expect("publish");

    let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
        .await
        .expect("the forwarded record should reach the owner")
        .expect("subscription error")
        .expect("subscription closed");
    assert_eq!(event.payload.as_ref(), b"forwarded-again");

    let _ = AckMode::PerMessage;
    cluster.shutdown().await;
}
