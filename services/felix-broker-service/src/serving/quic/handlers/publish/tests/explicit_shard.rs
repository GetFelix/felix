//! A publish that names its shard goes there, and only to a shard the stream
//! has.

use felix_wire::routing::{ShardRouting, shard_for_routing};

use super::routing::ingress_with_routing;
use super::*;
use crate::serving::quic::handlers::publish::route::{
    DeclaredWidth, named_shard, resolve_declared_shard, resolve_publish_shard,
};

/// A named shard is used as given, not hashed, whatever key rides with it.
#[tokio::test]
async fn a_named_shard_is_used_instead_of_the_key() {
    let (mut context, _rx, _tx) = make_publish_context(1);
    context.ingress = Some(Arc::new(ingress_with_routing(4, ShardRouting::Modulo)));
    let key = Some(&b"d00"[..]);
    let hashed = shard_for_routing(ShardRouting::Modulo, 4, key);
    for shard in 0..4 {
        assert_eq!(
            resolve_publish_shard(&context, "t1", "ns", "stream", key, Some(shard))
                .expect("in range"),
            shard
        );
    }
    assert_eq!(
        resolve_publish_shard(&context, "t1", "ns", "stream", key, None).expect("keyed"),
        hashed
    );
}

/// A shard at or past the stream's width is refused, never wrapped onto
/// another shard.
#[tokio::test]
async fn a_shard_out_of_range_is_refused() {
    let (mut context, _rx, _tx) = make_publish_context(1);
    context.ingress = Some(Arc::new(ingress_with_routing(4, ShardRouting::Modulo)));
    for shard in [4, 5, u32::MAX] {
        let refusal = named_shard(&context, "t1", "ns", "stream", shard).expect_err("out of range");
        assert_eq!(*refusal.code(), felix_wire::ErrorCode::InvalidRequest);
        assert_eq!(refusal.retry(), felix_wire::RetryClass::Fatal);
    }
}

/// A broker with no cluster serves one shard per stream, so only shard 0 is
/// in range.
#[tokio::test]
async fn a_single_node_has_only_shard_zero() {
    let (context, _rx, _tx) = make_publish_context(1);
    assert_eq!(
        named_shard(&context, "t1", "ns", "stream", 0).expect("shard 0"),
        0
    );
    named_shard(&context, "t1", "ns", "stream", 1).expect_err("shard 1");
}

/// No width chose a named shard, so a declared width does not check it: a
/// stale declaration does not refuse it, and a range check still does.
#[tokio::test]
async fn a_named_shard_skips_the_declared_width() {
    let (mut context, _rx, _tx) = make_publish_context(1);
    context.ingress = Some(Arc::new(ingress_with_routing(8, ShardRouting::JumpHash)));
    let stale = DeclaredWidth {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream".to_string(),
        shards: 4,
        routing: ShardRouting::Modulo,
    };
    let key = Some(&b"k"[..]);
    assert_eq!(
        resolve_declared_shard(&context, Some(&stale), "t1", "ns", "stream", key, Some(6))
            .expect("named"),
        6
    );
    let refusal =
        resolve_declared_shard(&context, Some(&stale), "t1", "ns", "stream", key, Some(8))
            .expect_err("out of range");
    assert_eq!(*refusal.code(), felix_wire::ErrorCode::InvalidRequest);
}

/// An acknowledged publish to a shard the stream does not have is answered
/// with `invalid_request`, and nothing is queued.
#[tokio::test]
async fn an_acked_publish_out_of_range_is_answered_and_not_written() {
    let broker = broker_with_stream().await;
    let (publish_ctx, mut rx, _tx) = make_publish_context(1);
    let mut cache = HashMap::new();
    let mut key = String::new();
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
    let out_ack_depth = Arc::new(AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(parking_lot::Mutex::new(
        AckTimeoutState::new(Instant::now()),
    ));
    handle_publish_batch_message(
        0,
        &broker,
        &publish_ctx,
        None,
        &mut cache,
        &mut key,
        false,
        false,
        AckEncoding::Json,
        &out_ack_tx,
        &out_ack_depth,
        &ack_throttle_tx,
        &ack_timeout_state,
        &cancel_tx,
        &CommitAcks::detached(Arc::new(Semaphore::new(1))),
        "t1".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![Bytes::from_static(b"payload")],
        None,
        Some(1),
        Some(7),
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
    )
    .await
    .expect("handled");
    match out_ack_rx.recv().await {
        Some(Outgoing::Message(Message::PublishError {
            request_id: 7,
            code: Some(felix_wire::ErrorCode::InvalidRequest),
            message,
            ..
        })) => assert!(message.contains("out of range"), "{message}"),
        other => panic!("expected invalid_request, got {other:?}"),
    }
    assert!(rx.try_recv().is_err(), "a refused publish was queued");
}

/// A stream the routing snapshot does not know yet, as on an entry broker
/// just after the stream is created, is not refused as out of range: the
/// shard may well exist, so the answer is retryable.
#[tokio::test]
async fn a_stream_not_routed_yet_is_retryable_not_out_of_range() {
    let (mut context, _rx, _tx) = make_publish_context(1);
    context.ingress = Some(Arc::new(ingress_with_routing(4, ShardRouting::Modulo)));
    let refusal = named_shard(&context, "t1", "ns", "just-created", 3).expect_err("unrouted");
    assert_eq!(*refusal.code(), felix_wire::ErrorCode::ShardUnavailable);
    assert_ne!(refusal.retry(), felix_wire::RetryClass::Fatal);
    assert_eq!(
        named_shard(&context, "t1", "ns", "just-created", 0).expect("shard 0"),
        0
    );
}
