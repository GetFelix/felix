//! A stream created again at another width, against a stub that checks the
//! width each keyed batch declares: the caches that number or route by a
//! width drop it and take the new one.

use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use felix_wire::{AckMode, Message};

use super::stub_broker::{STREAM_SHARDS, StubBroker};
use crate::ClusterClient;
use crate::client::Client;
use crate::test_support::build_client_config_with_overrides;

fn ok(request_id: u64) -> Message {
    Message::PublishOk {
        request_id,
        offset: None,
    }
}

/// A key whose shard differs between the old width and the new one.
fn moving_key(new_width: u32) -> Bytes {
    (0..1000)
        .map(|n| Bytes::from(format!("key-{n}")))
        .find(|key| {
            felix_wire::routing::shard_for(STREAM_SHARDS, Some(key))
                != felix_wire::routing::shard_for(new_width, Some(key))
        })
        .expect("a key that moves")
}

/// **A `ClusterClient` drops its width on `stale_width`.** It picks the
/// broker by the shard, so a width it kept would send keys that now share a
/// shard through two brokers, and so two writers.
#[tokio::test]
#[serial_test::serial]
async fn a_cluster_client_drops_a_stale_width() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok)?;
    broker.check_widths();
    let cluster = ClusterClient::connect(
        &[broker.addr],
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
    )
    .await?;
    let key = moving_key(2);
    let publish = || {
        cluster.publish_keyed(
            "t1",
            "default",
            "orders",
            b"x".to_vec(),
            key.clone(),
            AckMode::PerBatch,
        )
    };
    publish().await?;
    assert!(broker.widths_declared() > 0, "no width was declared");

    broker.set_width(2);
    let refused = publish().await.expect_err("routed by the old width");
    assert!(
        format!("{refused:#}").contains("stale width"),
        "{refused:#}"
    );
    publish().await?;
    let (_, width) = cluster
        .stream_widths()
        .shard_of("t1", "default", "orders", &key)
        .await;
    assert_eq!(width.shards, 2, "the cluster client kept the old width");
    Ok(())
}

/// **An idempotent producer numbers by the width the batch is checked by,
/// and starts again under a new one.** Numbered by a kept width, the batches
/// of two old shards would share one new shard's sequence, and the leader
/// would refuse them or take one for a duplicate of another. The key here
/// keeps its shard number across the change, so only a reset starts it at 0.
#[tokio::test]
#[serial_test::serial]
async fn a_producer_drops_a_stale_width_and_numbers_afresh() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok)?;
    broker.check_widths();
    let cluster = Arc::new(
        ClusterClient::connect(
            &[broker.addr],
            "localhost",
            build_client_config_with_overrides(cert, 1)?,
        )
        .await?,
    );
    let producer = cluster.idempotent_producer().await?;
    let key = (0..1000)
        .map(|n| Bytes::from(format!("key-{n}")))
        .find(|key| {
            felix_wire::routing::shard_for(STREAM_SHARDS, Some(key))
                == felix_wire::routing::shard_for(2, Some(key))
        })
        .expect("a key that stays");
    let publish = || producer.publish_keyed("t1", "default", "orders", key.clone(), b"x".to_vec());
    publish().await?;
    publish().await?;

    broker.set_width(2);
    publish().await.expect_err("numbered by the old width");
    publish().await?;
    assert_eq!(
        broker.sequences(),
        vec![
            (Some(key.clone()), 0),
            (Some(key.clone()), 1),
            // The first batch under the new width: a stream created again
            // has no sequences from before.
            (Some(key), 0),
        ]
    );
    Ok(())
}

/// **Background re-asks share one stream.** Each would otherwise open and
/// authenticate a stream of its own, every few seconds for every stream a
/// client publishes keyed batches to.
#[tokio::test]
#[serial_test::serial]
async fn re_asks_share_one_stream() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok)?;
    broker.check_widths();
    let client = Client::connect(
        broker.addr,
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
    )
    .await?;
    let before = broker.streams_for("demo-token");
    let recheck = client.publish_widths().recheck();
    for stream in ["a", "b", "c"] {
        let (shards, _) = recheck("t1".into(), "default".into(), stream.into()).await?;
        assert_eq!(shards, STREAM_SHARDS);
    }
    assert_eq!(broker.streams_for("demo-token") - before, 1);
    assert_eq!(broker.width_asks(), 3);
    Ok(())
}

/// **The broker checks the width that chose it.** The entry broker's own
/// client already holds the new width, so if it routed by its own the batch
/// would be taken, and keys the cluster client sent to two brokers by its
/// stale width would reach one shard from two writers.
#[tokio::test]
#[serial_test::serial]
async fn a_stale_cluster_width_is_refused_not_taken() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok)?;
    broker.check_widths();
    let cluster = ClusterClient::connect(
        &[broker.addr],
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
    )
    .await?;
    let key = moving_key(2);
    cluster
        .publish_keyed(
            "t1",
            "default",
            "orders",
            b"x".to_vec(),
            key.clone(),
            AckMode::PerBatch,
        )
        .await?;

    // Created again two shards wide, and the entry broker's client has
    // already caught up; the cluster client has not.
    broker.set_width(2);
    let entry = cluster.client().await;
    entry.publish_widths().forget("t1", "default", "orders");
    entry
        .publisher()
        .await?
        .publish_keyed(
            "t1",
            "default",
            "orders",
            key.clone(),
            b"x".to_vec(),
            AckMode::PerBatch,
        )
        .await?;

    let refused = cluster
        .publish_keyed(
            "t1",
            "default",
            "orders",
            b"x".to_vec(),
            key,
            AckMode::PerBatch,
        )
        .await
        .expect_err("routed by the cluster client's stale width");
    assert!(
        format!("{refused:#}").contains("stale width"),
        "{refused:#}"
    );
    Ok(())
}

/// **A width that flaps does not reuse a producer's sequences.** Each change
/// starts the stream's cursors again under a fresh producer id, so going
/// back to an earlier width never numbers from 0 under an id a leader still
/// holds sequences for.
#[tokio::test]
#[serial_test::serial]
async fn a_flapping_width_never_reuses_a_producer_sequence() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok)?;
    broker.check_widths();
    let cluster = Arc::new(
        ClusterClient::connect(
            &[broker.addr],
            "localhost",
            build_client_config_with_overrides(cert, 1)?,
        )
        .await?,
    );
    let producer = cluster.idempotent_producer().await?;
    let key = (0..1000)
        .map(|n| Bytes::from(format!("key-{n}")))
        .find(|key| {
            felix_wire::routing::shard_for(STREAM_SHARDS, Some(key))
                == felix_wire::routing::shard_for(2, Some(key))
        })
        .expect("a key that stays");
    let publish = || producer.publish_keyed("t1", "default", "orders", key.clone(), b"x".to_vec());
    publish().await?;
    for width in [2, STREAM_SHARDS, 2] {
        broker.set_width(width);
        publish().await.expect_err("numbered by the old width");
        publish().await?;
    }
    let taken = broker.producer_sequences();
    assert_eq!(taken.len(), 4, "{taken:?}");
    let mut unique = taken.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
        unique.len(),
        taken.len(),
        "a sequence was reused: {taken:?}"
    );
    Ok(())
}

/// **A thousand streams are re-asked at once without waiting on each
/// other.** Taking turns on one stream, each question would wait out every
/// round trip ahead of it and most would give up.
#[tokio::test]
#[serial_test::serial]
async fn a_thousand_re_asks_overlap() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok)?;
    broker.check_widths();
    broker.delay_widths(20);
    let client = Client::connect(
        broker.addr,
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
    )
    .await?;
    let before = broker.streams_for("demo-token");
    let recheck = client.publish_widths().recheck();
    let mut asks = tokio::task::JoinSet::new();
    for n in 0..1000 {
        let recheck = Arc::clone(&recheck);
        asks.spawn(async move {
            tokio::time::timeout(
                crate::publish::REVALIDATE_TIMEOUT,
                recheck("t1".into(), "default".into(), format!("stream-{n}")),
            )
            .await
        });
    }
    let mut answered = 0;
    while let Some(answer) = asks.join_next().await {
        if matches!(answer, Ok(Ok(Ok((STREAM_SHARDS, _))))) {
            answered += 1;
        }
    }
    assert_eq!(answered, 1000);
    assert_eq!(broker.streams_for("demo-token") - before, 1);
    Ok(())
}

/// **A cluster client re-asks widths whatever its first broker negotiated.**
/// Re-asking needs only `stream_shards`; the declaration it builds goes only
/// to a stream that negotiated it, which here is none.
#[tokio::test]
#[serial_test::serial]
async fn a_cluster_client_re_asks_even_against_an_older_broker() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok)?;
    let cluster = ClusterClient::connect(
        &[broker.addr],
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
    )
    .await?;
    let key = moving_key(2);
    cluster
        .publish_keyed(
            "t1",
            "default",
            "orders",
            b"x".to_vec(),
            key.clone(),
            AckMode::PerBatch,
        )
        .await?;
    let (_, width) = cluster
        .stream_widths()
        .shard_of("t1", "default", "orders", &key)
        .await;
    assert!(
        width.declaration.is_some(),
        "the cluster client's width is never asked again"
    );
    assert_eq!(
        broker.widths_declared(),
        0,
        "declared to a broker without the bit"
    );
    Ok(())
}
