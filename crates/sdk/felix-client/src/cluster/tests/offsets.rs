//! The offset a publish ack carries, as the publish calls return it.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use bytes::Bytes;
use felix_wire::{AckMode, ErrorCode, Message, RetryClass};

use super::connections::{default_config, policy};
use super::stub_broker::StubBroker;
use crate::Client;
use crate::cluster::ClusterClient;

fn ok_at(offset: Option<u64>) -> impl Fn(u64) -> Message + Send + Sync + 'static {
    move |request_id| Message::PublishOk { request_id, offset }
}

#[tokio::test]
async fn a_publisher_returns_the_offset_its_ack_carried() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok_at(Some(41)))?;
    let client = Client::connect(broker.addr, "localhost", default_config(cert)?).await?;
    let publisher = client.publisher().await?;
    let one = publisher
        .publish(
            "t1",
            "default",
            "orders",
            b"a".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    let batch = publisher
        .publish_batch(
            "t1",
            "default",
            "orders",
            vec![b"b".to_vec(), b"c".to_vec()],
            AckMode::PerBatch,
        )
        .await?;
    let keyed = publisher
        .publish_keyed(
            "t1",
            "default",
            "orders",
            Bytes::from_static(b"k"),
            b"d".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    assert_eq!((one, batch, keyed), (Some(41), Some(41), Some(41)));
    // Unacked, nothing comes back to say where it landed.
    let unacked = publisher
        .publish("t1", "default", "orders", b"e".to_vec(), AckMode::None)
        .await?;
    assert_eq!(unacked, None);
    Ok(())
}

/// An ack without the offset bit, as a broker that predates it or one that
/// answered before the write sends, is `None` and not zero.
#[tokio::test]
async fn an_ack_without_an_offset_returns_none() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok_at(None))?;
    let cluster = ClusterClient::connect_with_policy(
        &[broker.addr],
        "localhost",
        default_config(cert)?,
        policy(),
    )
    .await?;
    let offset = cluster
        .publish(
            "t1",
            "default",
            "orders",
            b"a".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    assert_eq!(offset, None);
    Ok(())
}

#[tokio::test]
async fn a_cluster_client_returns_the_offset_its_ack_carried() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok_at(Some(7)))?;
    let cluster = ClusterClient::connect_with_policy(
        &[broker.addr],
        "localhost",
        default_config(cert)?,
        policy(),
    )
    .await?;
    let once = cluster
        .publish(
            "t1",
            "default",
            "orders",
            b"a".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    let keyed = cluster
        .publish_keyed(
            "t1",
            "default",
            "orders",
            b"b".to_vec(),
            Bytes::from_static(b"k"),
            AckMode::PerMessage,
        )
        .await?;
    let at_least_once = cluster
        .publish_at_least_once(
            "t1",
            "default",
            "orders",
            b"c".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    assert_eq!((once, keyed, at_least_once), (Some(7), Some(7), Some(7)));
    Ok(())
}

/// The offset comes from the ack that finally settled the batch, not the
/// failure before it.
#[tokio::test]
async fn an_idempotent_producer_returns_the_offset_of_the_ack_that_settled_it() -> Result<()> {
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
            n => Message::PublishOk {
                request_id,
                offset: Some(100 + n as u64),
            },
        }
    };
    let (broker, cert) = StubBroker::start(script)?;
    let cluster = ClusterClient::connect_with_policy(
        &[broker.addr],
        "localhost",
        default_config(cert)?,
        policy(),
    )
    .await?;
    let producer = cluster.idempotent_producer().await?;
    let first = producer
        .publish("t1", "default", "orders", b"a".to_vec())
        .await?;
    assert_eq!(first, Some(101), "the re-send's offset, not the failure's");
    let second = producer
        .publish_batch("t1", "default", "orders", vec![b"b".to_vec()])
        .await?;
    assert_eq!(second, Some(102));
    assert_eq!(broker.publishes(), 3);
    Ok(())
}
