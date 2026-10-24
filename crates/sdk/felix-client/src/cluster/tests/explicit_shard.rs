//! Where a [`ClusterClient`] sends a write that names its shard, against stub
//! brokers.

use anyhow::Result;
use felix_wire::{AckMode, Message};

use super::connections::{default_config, policy};
use super::stub_broker::{STREAM_SHARDS, StubBroker};
use crate::CommitOp;
use crate::cluster::ClusterClient;
use crate::test_support::build_server_config;

fn ok(request_id: u64) -> Message {
    Message::PublishOk {
        request_id,
        offset: None,
    }
}

/// Shards 0 and 2 on `a`, 1 and 3 on `b`.
fn owners_of(a: &StubBroker, b: &StubBroker) -> Vec<felix_wire::ShardOwner> {
    (0..STREAM_SHARDS)
        .map(|shard| {
            let (node, broker) = if shard % 2 == 0 { ("a", a) } else { ("b", b) };
            felix_wire::ShardOwner {
                shard,
                node_id: Some(node.to_string()),
                addr: Some(broker.addr.to_string()),
                generation: 1,
                unavailable: None,
            }
        })
        .collect()
}

fn shards_named(broker: &StubBroker) -> Vec<(&'static str, Option<u32>)> {
    broker
        .named()
        .into_iter()
        .map(|(request, shard, _)| (request, shard))
        .collect()
}

/// **A named shard picks its owner**, as a key's shard does: each batch goes
/// straight to the broker that owns the shard it names, with the shard on the
/// wire, and the entry broker forwards nothing.
#[tokio::test]
#[serial_test::serial]
async fn a_named_shard_goes_to_its_owner() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let a = StubBroker::start_with(server_config.clone(), ok)?;
    let b = StubBroker::start_with(server_config, ok)?;
    a.honour_named_shards();
    b.honour_named_shards();
    a.set_cache_owners(owners_of(&a, &b));
    let cluster =
        ClusterClient::connect_with_policy(&[a.addr], "localhost", default_config(cert)?, policy())
            .await?;

    for shard in [3, 1, 2] {
        cluster
            .publish_batch_on_shard(
                "t1",
                "default",
                "orders",
                shard,
                vec![format!("to-{shard}").into_bytes()],
                AckMode::PerBatch,
            )
            .await?;
    }

    assert_eq!(
        shards_named(&b),
        [("publish_batch", Some(3)), ("publish_batch", Some(1))]
    );
    assert_eq!(shards_named(&a), [("publish_batch", Some(2))]);
    assert!(a.batches().is_empty() && b.batches().is_empty());
    Ok(())
}

/// **A commit to a named shard follows the leader's redirect** and carries
/// the shard to it, rather than re-hashing the entity key.
#[tokio::test]
#[serial_test::serial]
async fn a_commit_to_a_named_shard_follows_the_leader() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let leader = StubBroker::start_with(server_config.clone(), ok)?;
    let leader_addr = leader.addr.to_string();
    let entry = StubBroker::start_with(server_config, move |_| Message::NotLeader {
        node_id: "leader".to_string(),
        addr: Some(leader_addr.clone()),
        generation: 1,
    })?;
    entry.honour_named_shards();
    leader.honour_named_shards();
    let cluster = ClusterClient::connect_with_policy(
        &[entry.addr],
        "localhost",
        default_config(cert)?,
        policy(),
    )
    .await?;

    let receipt = cluster
        .commit_on_shard(
            "t1",
            "default",
            3,
            b"d00",
            vec![CommitOp::enqueue("orders", "tick")],
        )
        .await?;

    assert_eq!(receipt.offset, 7);
    assert_eq!(shards_named(&entry), [("commit", Some(3))]);
    assert_eq!(shards_named(&leader), [("commit", Some(3))]);
    Ok(())
}

/// **A broker that did not advertise the bit is sent nothing.** It would
/// ignore the shard and write wherever the key hashes, so the client refuses
/// before sending.
#[tokio::test]
#[serial_test::serial]
async fn a_broker_without_the_feature_is_not_sent_a_named_shard() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let a = StubBroker::start_with(server_config, ok)?;
    let cluster =
        ClusterClient::connect_with_policy(&[a.addr], "localhost", default_config(cert)?, policy())
            .await?;

    let published = cluster
        .publish_on_shard(
            "t1",
            "default",
            "orders",
            1,
            b"x".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect_err("no FEATURE_EXPLICIT_SHARD");
    assert!(
        format!("{published:#}").contains("FEATURE_EXPLICIT_SHARD"),
        "{published:#}"
    );
    let committed = cluster
        .commit_on_shard(
            "t1",
            "default",
            1,
            b"d01",
            vec![CommitOp::enqueue("orders", "tick")],
        )
        .await
        .expect_err("no FEATURE_EXPLICIT_SHARD");
    assert!(
        format!("{committed:#}").contains("FEATURE_EXPLICIT_SHARD"),
        "{committed:#}"
    );
    assert!(a.named().is_empty());
    assert_eq!(a.publishes(), 0);
    Ok(())
}
