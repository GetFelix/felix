//! What an acknowledged publish waits for by default: a durable stream's
//! answer comes after the commit, an in-memory stream's on enqueue, and
//! `FELIX_ACK_ON_COMMIT=0` puts durable streams back on enqueue.
//!
//! The queue has no executors, so a job is never settled. An answer that
//! arrives anyway was sent on enqueue.

use felix_broker::{DurableStorage, StreamMetadata};
use felix_storage::log::LogConfig;

use crate::config::BrokerConfig;

use super::*;

async fn broker(durable: bool) -> (Broker, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("dir");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(
        DurableStorage::open(dir.path(), LogConfig::default()).expect("storage"),
    );
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            StreamMetadata {
                durable,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    (broker, dir)
}

/// Publish one acknowledged message with the connection not asking for commit
/// acks, and say whether an answer came while the job sat in the queue.
async fn answered_while_queued(durable: bool, ack_durable_on_commit: bool) -> bool {
    let (broker, _dir) = broker(durable).await;
    let (mut ctx, _rx, _tx) = make_publish_context(4);
    ctx.ack_durable_on_commit = ack_durable_on_commit;
    let (out_tx, mut out_rx) = mpsc::channel(4);
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let commit_acks = CommitAcks::for_test(&out_tx, Arc::new(Semaphore::new(4)));
    handle_publish_message(
        &broker,
        &ctx,
        &mut HashMap::new(),
        &mut String::new(),
        false,
        false,
        &out_tx,
        &Arc::new(AtomicUsize::new(0)),
        &throttle_tx,
        &Arc::new(parking_lot::Mutex::new(
            AckTimeoutState::new(Instant::now()),
        )),
        &cancel_tx,
        &commit_acks,
        "t1".to_string(),
        "ns".to_string(),
        "orders".to_string(),
        b"payload".to_vec(),
        None,
        Some(1),
        Some(felix_wire::AckMode::PerMessage),
        false,
        String::new(),
    )
    .await
    .expect("publish handled");
    tokio::time::timeout(Duration::from_millis(500), out_rx.recv())
        .await
        .is_ok()
}

#[tokio::test]
async fn a_durable_ack_waits_for_the_commit_under_the_default_config() {
    let default = BrokerConfig::default().ack_durable_on_commit;
    assert!(
        !answered_while_queued(true, default).await,
        "the durable publish was acknowledged before it committed"
    );
}

#[tokio::test]
async fn opting_out_acknowledges_a_durable_publish_on_enqueue() {
    assert!(answered_while_queued(true, false).await);
}

#[tokio::test]
async fn an_in_memory_ack_stays_on_enqueue_under_the_default_config() {
    let default = BrokerConfig::default().ack_durable_on_commit;
    assert!(answered_while_queued(false, default).await);
}
