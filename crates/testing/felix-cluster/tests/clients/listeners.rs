//! A broker with more than one client-facing listener.
//!
//! One UDP socket is one `quinn` endpoint, and its driver is a single task that
//! reads every datagram for that socket and routes it by connection id. That
//! task cannot use more than one core, and it is what holds a broker to
//! ~900 MB/s while the rest of the machine idles (#557). Several ports are
//! several sockets, which are several drivers -- but only if clients actually
//! spread across them, which is what these cover.
//!
//! Run with `cargo test -p felix-cluster --test clients listeners::`.
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";

/// **A client spreads its pools across every listener the broker advertises.**
///
/// The point of binding four ports is that the work lands on four endpoint
/// drivers. A client that learned the ports and dialled one of them anyway
/// would leave the ceiling exactly where it was.
#[serial]
#[tokio::test]
async fn a_client_spreads_its_pool_across_the_advertised_listeners() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, 1)],
        quic_listeners: 4,
        ..Default::default()
    })
    .await
    .expect("start cluster");

    let owner = cluster.owner(STREAM).await.expect("owner");
    let node = cluster.node(&owner).expect("node");
    let base = node.client_addr;

    let client = cluster.client_on(&owner).await.expect("connect");
    let listeners = client.listeners_in_use();

    assert!(
        listeners.len() > 1,
        "a four-listener broker should spread the pool, got {listeners:?}",
    );
    // Every one of them is this broker, on a port inside the advertised run.
    for addr in &listeners {
        assert_eq!(addr.ip(), base.ip(), "{addr} is not the broker dialled");
        assert!(
            (base.port()..base.port() + 4).contains(&addr.port()),
            "{addr} is outside the advertised range",
        );
    }
    // And the address actually dialled is still one of them: it is the only one
    // demonstrated to work.
    assert!(listeners.contains(&base), "{listeners:?} dropped {base}");

    cluster.shutdown().await;
}

/// **The default is one listener, and a client stays on the one address.**
///
/// The common case must be untouched: a broker that has not asked for more
/// listeners advertises nothing, and its clients behave exactly as before.
#[serial]
#[tokio::test]
async fn a_single_listener_broker_leaves_the_client_on_one_address() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");

    let owner = cluster.owner(STREAM).await.expect("owner");
    let node = cluster.node(&owner).expect("node");
    let client = cluster.client_on(&owner).await.expect("connect");

    assert_eq!(client.listeners_in_use(), [node.client_addr]);

    cluster.shutdown().await;
}

/// **Publishing works across the spread.** The pools are on different sockets;
/// the stream they serve is the same one.
#[serial]
#[tokio::test]
async fn publishes_and_replays_work_across_several_listeners() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, 1)],
        quic_listeners: 4,
        ..Default::default()
    })
    .await
    .expect("start cluster");

    for i in 0..10 {
        cluster
            .publish_via_any(STREAM, format!("m{i}").into_bytes())
            .await
            .expect("publish");
    }

    let owner = cluster.owner(STREAM).await.expect("owner");
    let (_client, mut sub) = cluster.replay_on(&owner, STREAM).await.expect("replay");
    let mut seen = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(std::time::Duration::from_secs(3), sub.next_event()).await
    {
        seen.push(String::from_utf8_lossy(&event.payload).to_string());
    }
    for i in 0..10 {
        assert!(
            seen.contains(&format!("m{i}")),
            "m{i} missing from {seen:?}"
        );
    }

    cluster.shutdown().await;
}
