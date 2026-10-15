use std::sync::Arc;
use std::time::Duration;

use felix_wire::Message;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::time::timeout;

use super::*;
use crate::serving::quic::telemetry::t_instant_now;

fn acks(out_tx: &mpsc::Sender<Outgoing>, waiters: usize, ack_timeout: Duration) -> CommitAcks {
    CommitAcks::new(
        out_tx.clone(),
        Arc::new(AtomicUsize::new(0)),
        watch::channel(false).0,
        Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now()))),
        watch::channel(false).0,
        Arc::new(Semaphore::new(waiters)),
        ack_timeout,
    )
}

fn request(request_id: u64, encoding: AckEncoding) -> AckRequest {
    AckRequest {
        request_id,
        encoding,
        payload_bytes: 1,
        forwarded_to: None,
        start: t_instant_now(),
        single: true,
    }
}

async fn next(out_rx: &mut mpsc::Receiver<Outgoing>) -> Outgoing {
    timeout(Duration::from_secs(2), out_rx.recv())
        .await
        .expect("an answer")
        .expect("open queue")
}

fn publish_error(outgoing: Outgoing) -> (u64, Option<felix_wire::ErrorCode>, String) {
    match outgoing {
        Outgoing::Message(Message::PublishError {
            request_id,
            code,
            message,
            ..
        }) => (request_id, code, message),
        other => panic!("expected a publish_error, got {other:?}"),
    }
}

#[tokio::test]
async fn a_settled_publish_is_answered_once_with_its_result() {
    let (out_tx, mut out_rx) = mpsc::channel(8);
    let acks = acks(&out_tx, 4, Duration::from_secs(5));

    let (reply, pending) = acks.expect(request(1, AckEncoding::Json));
    assert_eq!(pending.arm(), Armed::Waiting);
    assert!(out_rx.try_recv().is_err(), "answered before it settled");
    reply.send(Ok(Some(7)));
    match next(&mut out_rx).await {
        Outgoing::Message(Message::PublishOk { request_id, offset }) => {
            assert_eq!((request_id, offset), (1, Some(7)));
        }
        other => panic!("expected publish_ok, got {other:?}"),
    }

    let (reply, pending) = acks.expect(request(2, AckEncoding::Json));
    assert_eq!(pending.arm(), Armed::Waiting);
    reply.send(Err(anyhow::anyhow!("stream full")));
    let (request_id, _, message) = publish_error(next(&mut out_rx).await);
    assert_eq!(request_id, 2);
    assert!(message.contains("stream full"), "{message}");
    assert!(out_rx.try_recv().is_err());
}

#[tokio::test]
async fn a_publish_settled_before_it_is_armed_is_still_answered() {
    let (out_tx, mut out_rx) = mpsc::channel(8);
    let acks = acks(&out_tx, 1, Duration::from_secs(5));
    let (reply, pending) = acks.expect(request(3, AckEncoding::Binary));
    reply.send(Ok(None));
    assert_eq!(pending.arm(), Armed::Answered);
    assert!(matches!(
        next(&mut out_rx).await,
        Outgoing::PublishAck {
            request_id: 3,
            error: None,
            ..
        }
    ));
    // The permit taken for the arm went back.
    assert_eq!(acks.inner.waiters.available_permits(), 1);
}

#[tokio::test]
async fn a_dropped_publish_is_answered_with_an_error() {
    let (out_tx, mut out_rx) = mpsc::channel(8);
    let acks = acks(&out_tx, 4, Duration::from_secs(5));

    // Dropped after it was queued.
    let (reply, pending) = acks.expect(request(4, AckEncoding::Json));
    assert_eq!(pending.arm(), Armed::Waiting);
    drop(reply);
    let (request_id, _, message) = publish_error(next(&mut out_rx).await);
    assert_eq!(request_id, 4);
    assert!(message.contains("dropped"), "{message}");

    // Queued and dropped before the control loop armed it.
    let (reply, pending) = acks.expect(request(5, AckEncoding::Json));
    drop(reply);
    assert_eq!(pending.arm(), Armed::Answered);
    assert_eq!(publish_error(next(&mut out_rx).await).0, 5);
}

#[tokio::test]
async fn a_publish_that_was_never_queued_is_answered_by_the_control_loop() {
    let (out_tx, mut out_rx) = mpsc::channel(8);
    let acks = acks(&out_tx, 4, Duration::from_secs(5));
    let (reply, pending) = acks.expect(request(6, AckEncoding::Json));
    // The enqueue dropped the job.
    drop(reply);
    assert!(pending.refuse(), "the control loop answers a refusal");
    assert!(out_rx.try_recv().is_err(), "the dropped reply answered too");

    // A publish that settled anyway keeps its own answer.
    let (reply, pending) = acks.expect(request(7, AckEncoding::Json));
    reply.send(Ok(None));
    assert!(!pending.refuse());
    assert!(matches!(
        next(&mut out_rx).await,
        Outgoing::Message(Message::PublishOk { request_id: 7, .. })
    ));
}

#[tokio::test]
async fn too_many_owed_answers_refuse_the_next() {
    let (out_tx, mut out_rx) = mpsc::channel(8);
    let acks = acks(&out_tx, 1, Duration::from_secs(5));
    let (first, held) = acks.expect(request(8, AckEncoding::Json));
    assert_eq!(held.arm(), Armed::Waiting);
    let (second, pending) = acks.expect(request(9, AckEncoding::Json));
    assert_eq!(pending.arm(), Armed::Exhausted);
    // The caller sends the refusal; the publish's own answer is discarded.
    second.send(Ok(None));
    assert!(out_rx.try_recv().is_err());
    first.send(Ok(None));
    assert!(matches!(
        next(&mut out_rx).await,
        Outgoing::Message(Message::PublishOk { request_id: 8, .. })
    ));
    assert_eq!(acks.inner.waiters.available_permits(), 1);
}

#[tokio::test]
async fn a_publish_that_outlives_its_timeout_is_answered_once() {
    let (out_tx, mut out_rx) = mpsc::channel(8);
    let acks = acks(&out_tx, 4, Duration::from_millis(20));
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let sweep = tokio::spawn(acks.clone().run_deadlines(cancel_rx));

    let (late, pending) = acks.expect(request(10, AckEncoding::Json));
    assert_eq!(pending.arm(), Armed::Waiting);
    let (request_id, code, message) = publish_error(next(&mut out_rx).await);
    assert_eq!(request_id, 10);
    assert_eq!(code, Some(felix_wire::ErrorCode::Unacknowledged));
    assert!(message.contains("timeout"), "{message}");
    assert_eq!(acks.inner.waiters.available_permits(), 4);
    // Settling late changes nothing.
    late.send(Ok(None));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(out_rx.try_recv().is_err());

    // One that settles in time is not timed out afterwards.
    let (prompt, pending) = acks.expect(request(11, AckEncoding::Binary));
    assert_eq!(pending.arm(), Armed::Waiting);
    prompt.send(Ok(None));
    assert!(matches!(
        next(&mut out_rx).await,
        Outgoing::PublishAck {
            request_id: 11,
            error: None,
            ..
        }
    ));
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(out_rx.try_recv().is_err());

    let _ = cancel_tx.send(true);
    timeout(Duration::from_secs(2), sweep)
        .await
        .expect("the sweep ends on cancel")
        .expect("sweep");
}

#[tokio::test]
async fn the_sweep_ends_once_closed_and_everything_is_answered() {
    let (out_tx, mut out_rx) = mpsc::channel(8);
    let acks = acks(&out_tx, 4, Duration::from_secs(5));
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let sweep = tokio::spawn(acks.clone().run_deadlines(cancel_rx));

    let (reply, pending) = acks.expect(request(12, AckEncoding::Json));
    assert_eq!(pending.arm(), Armed::Waiting);
    acks.close();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!sweep.is_finished(), "ended with an answer still owed");
    reply.send(Ok(None));
    next(&mut out_rx).await;
    timeout(Duration::from_secs(2), sweep)
        .await
        .expect("the sweep ends")
        .expect("sweep");
}

#[tokio::test]
async fn an_answer_for_a_closed_stream_cancels_it() {
    let (out_tx, out_rx) = mpsc::channel(8);
    drop(out_rx);
    let (cancel_tx, mut cancelled) = watch::channel(false);
    let acks = CommitAcks::new(
        out_tx,
        Arc::new(AtomicUsize::new(0)),
        watch::channel(false).0,
        Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now()))),
        cancel_tx,
        Arc::new(Semaphore::new(1)),
        Duration::from_secs(5),
    );
    let (reply, pending) = acks.expect(request(13, AckEncoding::Json));
    assert_eq!(pending.arm(), Armed::Waiting);
    reply.send(Ok(None));
    assert!(*cancelled.borrow_and_update());
}

#[tokio::test]
async fn a_full_queue_still_gets_the_answer() {
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let acks = acks(&out_tx, 4, Duration::from_secs(5));
    for request_id in [14, 15] {
        let (reply, pending) = acks.expect(request(request_id, AckEncoding::Json));
        assert_eq!(pending.arm(), Armed::Waiting);
        reply.send(Ok(None));
    }
    let mut answered = Vec::new();
    for _ in 0..2 {
        match next(&mut out_rx).await {
            Outgoing::Message(Message::PublishOk { request_id, .. }) => answered.push(request_id),
            other => panic!("expected publish_ok, got {other:?}"),
        }
    }
    answered.sort_unstable();
    assert_eq!(answered, vec![14, 15]);
}

/// A quorum timeout reaches the client as `quorum_timeout`, outcome unknown,
/// in both encodings: the leader wrote the batch, so it is not a refusal.
#[tokio::test]
async fn a_quorum_timeout_is_answered_as_outcome_unknown() {
    let (out_tx, mut out_rx) = mpsc::channel(8);
    let acks = acks(&out_tx, 4, Duration::from_secs(5));
    let timed_out = || {
        anyhow::Error::from(felix_replication::quorum::QuorumError::TimedOut {
            what: "batch",
            timeout: Duration::from_millis(50),
        })
    };
    for (request_id, encoding) in [(31, AckEncoding::Json), (32, AckEncoding::Binary)] {
        let (reply, pending) = acks.expect(AckRequest {
            single: false,
            ..request(request_id, encoding)
        });
        assert_eq!(pending.arm(), Armed::Waiting);
        reply.send(Err(timed_out()));
    }
    match next(&mut out_rx).await {
        Outgoing::Message(Message::PublishError {
            request_id: 31,
            code,
            retry,
            ..
        }) => {
            assert_eq!(code, Some(felix_wire::ErrorCode::QuorumTimeout));
            assert_eq!(retry, Some(felix_wire::RetryClass::OutcomeUnknown));
        }
        other => panic!("expected a json publish_error, got {other:?}"),
    }
    match next(&mut out_rx).await {
        Outgoing::PublishAck {
            request_id: 32,
            code,
            error: Some(_),
            ..
        } => assert_eq!(
            code,
            Some((
                felix_wire::ErrorCode::QuorumTimeout,
                felix_wire::RetryClass::OutcomeUnknown
            ))
        ),
        other => panic!("expected a binary ack, got {other:?}"),
    }
}
