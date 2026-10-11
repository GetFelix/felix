//! A [`ClusterClient`] acting as another principal reaches every broker over
//! its parent's connections.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use felix_wire::Message;

use super::connections::{default_config, policy};
use super::stub_broker::StubBroker;
use crate::cluster::ClusterClient;
use crate::test_support::build_server_config;

fn subscribed() -> Message {
    Message::Subscribed {
        subscription_id: 0,
        start_offset: None,
        live_offset: None,
        queue_capacity: None,
    }
}

/// **A moved shard is followed over the parent's connection, as the
/// identity.** A ends alice's subscription with a move to B, which the parent
/// has never reached. Her subscription resumes on B under her token, over the
/// one connection the parent now holds to B, which the parent itself uses.
#[tokio::test]
async fn an_identity_follows_a_moved_shard_over_the_parents_connection() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let a = StubBroker::start_with(server_config.clone(), |_| subscribed())?;
    let b = StubBroker::start_with(server_config, |_| subscribed())?;
    let b_addr = b.addr.to_string();
    a.set_events(move |_, _| {
        vec![Message::ShardMoved {
            subscription_id: 0,
            resume_from: None,
            node_id: Some("b".to_string()),
            addr: Some(b_addr.clone()),
            generation: 2,
        }]
    });
    let cluster =
        ClusterClient::connect_with_policy(&[a.addr], "localhost", default_config(cert)?, policy())
            .await?;
    let alice = Arc::new(cluster.with_identity_token("t1", "alice").await?);

    let mut feed = alice.subscribe("t1", "default", "orders").await?;
    // B's subscription stays open with nothing to deliver, so this waits
    // after the move has been followed.
    assert!(
        tokio::time::timeout(Duration::from_millis(500), feed.next_event())
            .await
            .is_err()
    );
    assert_eq!(feed.moves(), 1);
    assert_eq!(b.subscribes(), 1);

    assert_eq!(a.accepted_connections(), 1, "alice dialled A");
    assert_eq!(b.accepted_connections(), 1, "alice dialled B");
    // Alice's publish, cache and subscribe streams on B, and the parent's
    // that opened its connection there.
    assert_eq!(b.streams_for("alice"), 3);
    assert!(b.streams_for("test-token") > 0);
    assert!(cluster.connect_to(b.addr).await?.is_usable());
    assert_eq!(b.accepted_connections(), 1, "the parent dialled B again");
    Ok(())
}

/// **An identity's broker whose connection dies is rebuilt from a fresh
/// parent connection.** One connection replaces the dead one, and the parent
/// and the identity both use it.
#[tokio::test]
async fn an_identity_rebuilds_over_the_parents_new_connection() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let a = StubBroker::start_with(server_config.clone(), |_| subscribed())?;
    let b = StubBroker::start_with(server_config, |_| subscribed())?;
    let cluster =
        ClusterClient::connect_with_policy(&[a.addr], "localhost", default_config(cert)?, policy())
            .await?;
    let alice = cluster.with_identity_token("t1", "alice").await?;
    let before = alice.connect_to(b.addr).await?;

    b.drop_connections();
    tokio::time::timeout(Duration::from_secs(5), async {
        while before.is_usable() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    let after = alice.connect_to(b.addr).await?;
    assert!(!Arc::ptr_eq(&before, &after), "alice's client was rebuilt");
    assert!(after.is_usable());
    assert!(cluster.connect_to(b.addr).await?.is_usable());
    assert_eq!(b.accepted_connections(), 2);
    assert_eq!(b.live_connections(), 1);
    assert_eq!(b.streams_for("alice"), 4, "two streams per alice client");
    Ok(())
}

/// **Identities of one cluster client share its connections.** Two users,
/// each of whom has reached both brokers, cost no connection the parent does
/// not already hold, and an identity derived from an identity is derived from
/// the parent.
#[tokio::test]
async fn identities_add_no_connections() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let a = StubBroker::start_with(server_config.clone(), |_| subscribed())?;
    let b = StubBroker::start_with(server_config, |_| subscribed())?;
    let cluster =
        ClusterClient::connect_with_policy(&[a.addr], "localhost", default_config(cert)?, policy())
            .await?;
    let alice = cluster.with_identity_token("t1", "alice").await?;
    let bob = alice.with_identity_token("t1", "bob").await?;
    alice.connect_to(b.addr).await?;
    bob.connect_to(b.addr).await?;

    assert_eq!(a.accepted_connections(), 1);
    assert_eq!(b.accepted_connections(), 1);
    assert_eq!(a.streams_for("bob"), 2);
    assert_eq!(b.streams_for("bob"), 2);
    assert_eq!(
        cluster.connections_per_node().await,
        vec![(a.addr.min(b.addr), 1), (a.addr.max(b.addr), 1)]
    );
    Ok(())
}
