//! A publish handed to an idempotent producer runs to its answer even when
//! the caller stops waiting, so a dropped future leaves the producer usable.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Result;
use felix_wire::{ErrorCode, Message, RetryClass};

use super::stub_broker::StubBroker;
use crate::ClusterClient;
use crate::client::Client;
use crate::test_support::build_client_config_with_overrides;

fn ok(request_id: u64) -> Message {
    Message::PublishOk {
        request_id,
        offset: None,
    }
}

/// Polls `publish` until the broker has received `count` batches, then drops it.
async fn drop_once_received(
    broker: &StubBroker,
    count: usize,
    publish: impl Future<Output = Result<Option<u64>>>,
) {
    let received = async {
        while broker.publishes() < count {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::select! {
        answered = publish => panic!("answered while the broker held its ack: {answered:?}"),
        () = received => {}
    }
}

/// **Dropping a publish mid-flight neither stops the producer nor reuses its
/// sequence.** The dropped batch still gets its answer, so the next batch
/// goes out under the next number and nothing is sent twice.
#[tokio::test]
#[serial_test::serial]
async fn a_dropped_publish_leaves_the_producer_usable() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok)?;
    let client = Arc::new(
        Client::connect(
            broker.addr,
            "localhost",
            build_client_config_with_overrides(cert, 1)?,
        )
        .await?,
    );
    let producer = client.idempotent_producer().await?;

    broker.hold_acks();
    drop_once_received(
        &broker,
        1,
        producer.publish("t1", "default", "orders", b"dropped".to_vec()),
    )
    .await;
    broker.release_acks();

    producer
        .publish("t1", "default", "orders", b"next".to_vec())
        .await?;
    assert_eq!(
        broker.sequences(),
        vec![(None, 0), (None, 1)],
        "each batch went out once, under its own sequence"
    );
    Ok(())
}

/// The same through a `ClusterClient`, whose producer owns the client too.
#[tokio::test]
#[serial_test::serial]
async fn a_dropped_cluster_publish_leaves_the_producer_usable() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok)?;
    let cluster = Arc::new(
        ClusterClient::connect(
            &[broker.addr],
            "localhost",
            build_client_config_with_overrides(cert, 1)?,
        )
        .await?,
    );
    let producer = cluster.idempotent_producer().await?;
    // Owning its client, the producer can be moved into a task.
    let producer = tokio::spawn(async move {
        producer
            .publish("t1", "default", "orders", b"first".to_vec())
            .await
            .map(|_| producer)
    })
    .await??;

    broker.hold_acks();
    drop_once_received(
        &broker,
        2,
        producer.publish("t1", "default", "orders", b"in flight".to_vec()),
    )
    .await;
    // Behind the first, so still queued in the producer when dropped.
    let queued = tokio::time::timeout(
        Duration::from_millis(50),
        producer.publish("t1", "default", "orders", b"queued".to_vec()),
    )
    .await;
    assert!(queued.is_err(), "answered while the broker held its ack");
    broker.release_acks();

    producer
        .publish("t1", "default", "orders", b"next".to_vec())
        .await?;
    assert_eq!(
        broker.sequences(),
        vec![(None, 0), (None, 1), (None, 2), (None, 3)]
    );
    Ok(())
}

/// **A batch left in doubt with nobody waiting is settled before the next.**
/// Its caller cannot re-send it, so the producer does, under the sequence it
/// was sent with, and only then sends the next caller's batch.
#[tokio::test]
#[serial_test::serial]
async fn an_abandoned_batch_in_doubt_is_re_sent_before_the_next() -> Result<()> {
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
    let client = Arc::new(
        Client::connect(
            broker.addr,
            "localhost",
            build_client_config_with_overrides(cert, 1)?,
        )
        .await?,
    );
    let producer = client.idempotent_producer().await?;

    broker.hold_acks();
    drop_once_received(
        &broker,
        1,
        producer.publish("t1", "default", "orders", b"abandoned".to_vec()),
    )
    .await;
    broker.release_acks();

    producer
        .publish("t1", "default", "orders", b"next".to_vec())
        .await?;
    assert_eq!(
        broker.sequences(),
        vec![(None, 0), (None, 0), (None, 1)],
        "the abandoned batch was re-sent under its own sequence, then the next went out"
    );
    Ok(())
}

/// **Dropping the producer lets what it was handed finish.** `close` waits
/// for it.
#[tokio::test]
#[serial_test::serial]
async fn a_closed_producer_finishes_what_it_was_handed() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok)?;
    let client = Arc::new(
        Client::connect(
            broker.addr,
            "localhost",
            build_client_config_with_overrides(cert, 1)?,
        )
        .await?,
    );
    let producer = client.idempotent_producer().await?;

    broker.hold_acks();
    drop_once_received(
        &broker,
        1,
        producer.publish("t1", "default", "orders", b"in flight".to_vec()),
    )
    .await;
    let queued = tokio::time::timeout(
        Duration::from_millis(50),
        producer.publish("t1", "default", "orders", b"queued".to_vec()),
    )
    .await;
    assert!(queued.is_err(), "answered while the broker held its ack");
    let closed = tokio::spawn(producer.close());
    broker.release_acks();
    closed.await??;

    assert_eq!(broker.sequences(), vec![(None, 0), (None, 1)]);
    Ok(())
}

/// **A producer that cannot finish in time is stopped, not left running.**
/// `close_within` gives up at its timeout and nothing handed to the producer
/// goes out afterwards.
#[tokio::test]
#[serial_test::serial]
async fn a_producer_closed_within_a_timeout_sends_nothing_after_it() -> Result<()> {
    let (broker, cert) = StubBroker::start(ok)?;
    let client = Arc::new(
        Client::connect(
            broker.addr,
            "localhost",
            build_client_config_with_overrides(cert, 1)?,
        )
        .await?,
    );
    let producer = client.idempotent_producer().await?;

    broker.hold_acks();
    drop_once_received(
        &broker,
        1,
        producer.publish("t1", "default", "orders", b"in flight".to_vec()),
    )
    .await;
    let queued = tokio::time::timeout(
        Duration::from_millis(50),
        producer.publish("t1", "default", "orders", b"queued".to_vec()),
    )
    .await;
    assert!(queued.is_err(), "answered while the broker held its ack");
    let closed = producer.close_within(Duration::from_millis(100)).await;
    assert!(closed.is_err(), "closed while the broker held its ack");

    broker.release_acks();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        broker.sequences(),
        vec![(None, 0)],
        "the queued batch went out after the producer was stopped"
    );
    Ok(())
}
