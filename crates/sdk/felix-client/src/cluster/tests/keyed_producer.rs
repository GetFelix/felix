//! A keyed idempotent producer numbers each shard on its own, against stub
//! brokers whose streams have [`STREAM_SHARDS`] shards.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use bytes::Bytes;
use felix_wire::{ErrorCode, Message, RetryClass};

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

/// Two keys that the broker would route to different shards.
fn keys_on_two_shards() -> (Bytes, Bytes) {
    let shard = |key: &Bytes| felix_wire::routing::shard_for(STREAM_SHARDS, Some(key.as_ref()));
    let first = Bytes::from_static(b"key-0");
    let other = (1..100)
        .map(|n| Bytes::from(format!("key-{n}")))
        .find(|key| shard(key) != shard(&first))
        .expect("some key lands elsewhere");
    (first, other)
}

/// **Each shard's sequence starts at zero and advances alone.** The leader of
/// a shard numbers only the batches it receives, so one counter shared by two
/// shards would leave each with gaps it refuses.
#[tokio::test]
#[serial_test::serial]
async fn each_shard_has_its_own_sequence() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok)?;
    let client = Client::connect(
        broker.addr,
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
    )
    .await?;
    let client = Arc::new(client);
    let producer = client.idempotent_producer().await?;
    let (a, b) = keys_on_two_shards();

    for key in [&a, &b, &a, &b, &a] {
        producer
            .publish_keyed("t1", "default", "orders", key.clone(), b"x".to_vec())
            .await?;
    }
    producer
        .publish("t1", "default", "orders", b"unkeyed".to_vec())
        .await?;

    let sent: Vec<(Option<Bytes>, u64)> = broker.sequences();
    let a_shard = felix_wire::routing::shard_for(STREAM_SHARDS, Some(a.as_ref()));
    // Unkeyed is shard 0's, so it continues that shard's count if `a` is
    // there too.
    let unkeyed = if a_shard == 0 { 3 } else { 0 };
    assert_eq!(
        sent,
        vec![
            (Some(a.clone()), 0),
            (Some(b.clone()), 0),
            (Some(a.clone()), 1),
            (Some(b), 1),
            (Some(a), 2),
            (None, unkeyed),
        ]
    );
    Ok(())
}

/// **A batch in doubt is re-sent only with its own key.** Another key on the
/// same shard is a different record under the same sequence, which the
/// leader would acknowledge from memory without appending.
#[tokio::test]
#[serial_test::serial]
async fn a_batch_in_doubt_is_not_re_sent_under_another_key() -> Result<()> {
    let answered = Arc::new(AtomicUsize::new(0));
    let script = {
        let answered = Arc::clone(&answered);
        move |request_id| match answered.fetch_add(1, Ordering::SeqCst) {
            0 => Message::PublishError {
                request_id,
                message: "no majority".to_string(),
                code: Some(ErrorCode::QuorumTimeout),
                retry: Some(RetryClass::OutcomeUnknown),
                detail: None,
            },
            _ => ok(request_id),
        }
    };
    let (broker, cert) = StubBroker::start(script)?;
    let client = Client::connect(
        broker.addr,
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
    )
    .await?;
    let client = Arc::new(client);
    let producer = client.idempotent_producer().await?;
    let first = Bytes::from_static(b"key-0");
    let shard = felix_wire::routing::shard_for(STREAM_SHARDS, Some(first.as_ref()));
    let same_shard = (1..100)
        .map(|n| Bytes::from(format!("key-{n}")))
        .find(|key| felix_wire::routing::shard_for(STREAM_SHARDS, Some(key.as_ref())) == shard)
        .expect("some key shares the shard");

    producer
        .publish_keyed("t1", "default", "orders", first.clone(), b"x".to_vec())
        .await
        .expect_err("the outcome is unknown");
    let err = producer
        .publish_keyed("t1", "default", "orders", same_shard, b"x".to_vec())
        .await
        .expect_err("another key under a sequence in doubt must not be sent");
    assert!(format!("{err:#}").contains("same key"), "{err:#}");
    assert_eq!(broker.publishes(), 1, "nothing was sent for the other key");

    producer
        .publish_keyed("t1", "default", "orders", first.clone(), b"x".to_vec())
        .await?;
    assert_eq!(
        broker.sequences(),
        vec![(Some(first.clone()), 0), (Some(first), 0)],
        "the re-send reuses the sequence in doubt"
    );
    Ok(())
}

/// **Under a `ClusterClient`, a keyed batch goes out on its own shard's
/// stream.** Each shard's sequences have one writer; sent on shard 0's
/// stream, a keyed batch would wait behind a stalled shard 0.
#[tokio::test]
#[serial_test::serial]
async fn a_cluster_keyed_batch_goes_out_on_its_shards_stream() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok)?;
    let cluster = ClusterClient::connect(
        &[broker.addr],
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
    )
    .await?;
    let cluster = Arc::new(cluster);
    let producer = cluster.idempotent_producer().await?;
    let shard = |key: &Bytes| felix_wire::routing::shard_for(STREAM_SHARDS, Some(key.as_ref()));
    let (a, b) = keys_on_two_shards();
    let (a, b) = if shard(&a) == 0 { (b, a) } else { (a, b) };

    for key in [&a, &b, &a] {
        producer
            .publish_keyed("t1", "default", "orders", key.clone(), b"x".to_vec())
            .await?;
    }
    producer
        .publish("t1", "default", "orders", b"unkeyed".to_vec())
        .await?;

    let sent = broker.batch_streams();
    let stream_of = |key: Option<&Bytes>| {
        let streams: Vec<u64> = sent
            .iter()
            .filter(|(sent_key, _)| sent_key.as_ref() == key)
            .map(|(_, stream)| *stream)
            .collect();
        assert!(
            streams.windows(2).all(|pair| pair[0] == pair[1]),
            "one shard went out on two streams: {sent:?}"
        );
        streams[0]
    };
    let (on_a, on_b, unkeyed) = (stream_of(Some(&a)), stream_of(Some(&b)), stream_of(None));
    assert_ne!(
        on_a,
        unkeyed,
        "shard {}'s batch went on shard 0's stream",
        shard(&a)
    );
    assert_ne!(on_a, on_b, "two shards shared a stream");
    if shard(&b) == 0 {
        assert_eq!(on_b, unkeyed, "shard 0 went out on two streams");
    }
    Ok(())
}
