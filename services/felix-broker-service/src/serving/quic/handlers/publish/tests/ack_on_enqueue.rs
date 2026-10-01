//! A publish acknowledged when it is queued, not when it is written, and a
//! move that closes the shard's write fence while it waits in the queue. The
//! client already holds an ack, so the write must land: refusing it at the
//! claim would lose a record the client was told is stored.

use crate::config::BrokerConfig;
use crate::serving::quic::ClusterContext;
use crate::test_support::leader::{DURABLE, Leader, NAMESPACE, TENANT, stream_key};

use super::*;

/// A context whose queue the test holds, so it decides when the job is
/// claimed; and a real scheduler to hand the job to afterwards.
fn queue_and_worker(leader: &Leader) -> (PublishContext, TestReceiver, PublishContext) {
    let (mut queued, rx, _tx) = make_publish_context(8);
    queued.ingress = Some(Arc::clone(&leader.ingress));
    let worker = build_publish_context(
        Arc::clone(&leader.broker),
        &BrokerConfig::default(),
        ClusterContext {
            ingress: Some(Arc::clone(&leader.ingress)),
            ..ClusterContext::default()
        },
    );
    (queued, rx, worker)
}

/// Move the shard while the job is queued, let the worker have it, and
/// return how many records the log holds once the fence has quiesced.
async fn fence_then_claim(mut leader: Leader, mut rx: TestReceiver, worker: PublishContext) -> u64 {
    let job = rx.recv().await.expect("queued job");
    let key = stream_key(DURABLE);
    leader.fence_move(&key);
    worker.scheduler.send(job).await;
    tokio::time::timeout(Duration::from_secs(5), leader.ingress.fence().quiesce(&key))
        .await
        .expect("the fence quiesces");
    // A refused job leaves nothing in flight, so quiesce says nothing about
    // whether the worker has seen it yet; give it the chance to write.
    tokio::time::sleep(Duration::from_millis(100)).await;
    leader.tail(DURABLE).await
}

#[tokio::test]
async fn a_publish_acked_on_enqueue_is_written_across_a_move() {
    let leader = Leader::start().await;
    let (queued, rx, worker) = queue_and_worker(&leader);
    let (out_tx, mut out_rx) = mpsc::channel(4);
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);
    handle_publish_message(
        &leader.broker,
        &queued,
        &mut HashMap::new(),
        &mut String::new(),
        false,
        // Acked on enqueue: the default.
        false,
        &out_tx,
        &Arc::new(AtomicUsize::new(0)),
        &throttle_tx,
        &Arc::new(Mutex::new(AckTimeoutState::new(Instant::now()))),
        &cancel_tx,
        &Arc::new(Semaphore::new(1)),
        &ack_waiter_tx,
        Duration::from_secs(1),
        TENANT.to_string(),
        NAMESPACE.to_string(),
        DURABLE.to_string(),
        b"acked".to_vec(),
        None,
        Some(7),
        Some(felix_wire::AckMode::PerMessage),
        false,
        String::new(),
    )
    .await
    .expect("publish handled");
    match out_rx.recv().await.expect("ack") {
        Outgoing::Message(Message::PublishOk {
            request_id: 7,
            offset: None,
        }) => {}
        other => panic!("expected an ack on enqueue, got {other:?}"),
    }

    let tail = fence_then_claim(leader, rx, worker).await;
    assert_eq!(tail, 1, "an acknowledged publish was not written");
}

/// The batch path, which the binary publish frame goes through.
#[tokio::test]
async fn a_batch_acked_on_enqueue_is_written_across_a_move() {
    let leader = Leader::start().await;
    let (queued, rx, worker) = queue_and_worker(&leader);
    let (out_tx, mut out_rx) = mpsc::channel(4);
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);
    handle_publish_batch_message(
        0,
        &leader.broker,
        &queued,
        &mut HashMap::new(),
        &mut String::new(),
        false,
        false,
        AckEncoding::Binary,
        &out_tx,
        &Arc::new(AtomicUsize::new(0)),
        &throttle_tx,
        &Arc::new(Mutex::new(AckTimeoutState::new(Instant::now()))),
        &cancel_tx,
        &Arc::new(Semaphore::new(1)),
        &ack_waiter_tx,
        TENANT.to_string(),
        NAMESPACE.to_string(),
        DURABLE.to_string(),
        vec![
            bytes::Bytes::from_static(b"one"),
            bytes::Bytes::from_static(b"two"),
        ],
        None,
        Some(9),
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
    )
    .await
    .expect("batch handled");
    match out_rx.recv().await.expect("ack") {
        Outgoing::PublishAck {
            request_id: 9,
            error: None,
            ..
        } => {}
        other => panic!("expected an ack on enqueue, got {other:?}"),
    }

    let tail = fence_then_claim(leader, rx, worker).await;
    assert_eq!(tail, 2, "an acknowledged batch was not written");
}
