//! A keyed publish routed by a width the stream no longer has is refused
//! rather than written.

use felix_wire::routing::{ShardRouting, shard_for_routing};

use super::routing::ingress_with_routing;
use super::*;
use crate::serving::quic::handlers::publish::route::{DeclaredWidth, resolve_declared_shard};

fn declared(stream: &str, shards: u32, routing: ShardRouting) -> DeclaredWidth {
    DeclaredWidth {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shards,
        routing,
    }
}

#[tokio::test]
async fn a_declared_width_must_match_the_streams_own() {
    let (mut context, _rx, _tx) = make_publish_context(1);
    context.ingress = Some(Arc::new(ingress_with_routing(8, ShardRouting::JumpHash)));
    let key = Some(&b"customer-7"[..]);
    let resolve = |declared: Option<&DeclaredWidth>, stream: &str, key: Option<&[u8]>| {
        resolve_declared_shard(&context, declared, "t1", "ns", stream, key, None)
    };

    let current = declared("stream", 8, ShardRouting::JumpHash);
    assert_eq!(
        resolve(Some(&current), "stream", key).expect("current width"),
        shard_for_routing(ShardRouting::JumpHash, 8, key)
    );
    // The width of the stream before it was created again, or the same width
    // under another mapping: either sends keys to other writers.
    for stale in [
        declared("stream", 4, ShardRouting::JumpHash),
        declared("stream", 8, ShardRouting::Modulo),
    ] {
        let refusal = resolve(Some(&stale), "stream", key).expect_err("stale width");
        assert_eq!(*refusal.code(), felix_wire::ErrorCode::StaleWidth);
        assert_eq!(refusal.retry(), felix_wire::RetryClass::Retry);
    }
    // Unkeyed publishes, and publishes to a stream nothing was declared for,
    // are routed as before.
    let stale = declared("stream", 4, ShardRouting::JumpHash);
    assert_eq!(resolve(Some(&stale), "stream", None).expect("unkeyed"), 0);
    let other = declared("other", 4, ShardRouting::JumpHash);
    assert_eq!(
        resolve(Some(&other), "stream", key).expect("undeclared stream"),
        shard_for_routing(ShardRouting::JumpHash, 8, key)
    );
    assert_eq!(
        resolve(None, "stream", key).expect("nothing declared"),
        shard_for_routing(ShardRouting::JumpHash, 8, key)
    );
}

/// A stream the routing snapshot does not know is left to the route, which
/// says `not_found` for a deleted one.
#[tokio::test]
async fn an_unknown_stream_is_not_checked() {
    let (mut context, _rx, _tx) = make_publish_context(1);
    context.ingress = Some(Arc::new(ingress_with_routing(8, ShardRouting::JumpHash)));
    let stale = declared("gone", 4, ShardRouting::Modulo);
    assert_eq!(
        resolve_declared_shard(&context, Some(&stale), "t1", "ns", "gone", Some(b"k"), None)
            .expect("not checked"),
        0
    );
}

/// The issue's case end to end on the unacknowledged path: a client still
/// routing by the width of a stream since created again one shard wide.
#[tokio::test]
async fn an_unacked_batch_routed_by_a_stale_width_is_not_written() {
    let broker = broker_with_stream().await;
    let auth_ctx = make_auth_ctx("t1", &["stream.publish:stream:t1/ns/*"]);
    let payloads = vec![b"payload".to_vec()];
    let frame = felix_wire::binary::encode_publish_batch_keyed(
        Some(b"customer-7"),
        "t1",
        "ns",
        "stream",
        &payloads,
    )
    .expect("encode");
    let publish = |declared: Option<DeclaredWidth>| {
        let broker = &broker;
        let auth_ctx = &auth_ctx;
        let frame = &frame;
        async move {
            let (mut publish_ctx, mut rx, _tx) = make_publish_context(4);
            // One modulo shard, led and served here.
            publish_ctx.ingress = Some(Arc::new(ingress_for("broker-a", true)));
            handle_binary_publish_batch_control(
                broker,
                &mut HashMap::new(),
                &mut String::new(),
                &publish_ctx,
                frame,
                Some(auth_ctx),
                declared.as_ref(),
                false,
                &watch::channel(false).0,
            )
            .await
            .expect("handled");
            tokio::time::timeout(Duration::from_millis(50), rx.recv())
                .await
                .ok()
                .flatten()
                .is_some()
        }
    };
    assert!(
        !publish(Some(declared("stream", 4, ShardRouting::Modulo))).await,
        "a batch routed by a stale width was written"
    );
    assert!(publish(Some(declared("stream", 1, ShardRouting::Modulo))).await);
    assert!(publish(None).await, "an undeclared batch was refused");
}

/// The acknowledged path answers `stale_width`, the code the client drops
/// its width on.
#[tokio::test]
async fn an_acked_batch_routed_by_a_stale_width_is_told_stale_width() {
    let broker = broker_with_stream().await;
    let (mut publish_ctx, _rx, _tx) = make_publish_context(4);
    publish_ctx.ingress = Some(Arc::new(ingress_with_routing(1, ShardRouting::Modulo)));
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let ack_timeout_state = Arc::new(parking_lot::Mutex::new(
        AckTimeoutState::new(Instant::now()),
    ));
    let stale = declared("stream", 4, ShardRouting::Modulo);
    handle_publish_batch_message(
        0,
        &broker,
        &publish_ctx,
        Some(&stale),
        &mut HashMap::new(),
        &mut String::new(),
        false,
        false,
        AckEncoding::Json,
        &out_tx,
        &Arc::new(AtomicUsize::new(0)),
        &watch::channel(false).0,
        &ack_timeout_state,
        &watch::channel(false).0,
        &CommitAcks::detached(Arc::new(Semaphore::new(1))),
        "t1".to_string(),
        "ns".to_string(),
        "stream".to_string(),
        vec![Bytes::from_static(b"payload")],
        Some(Bytes::from_static(b"customer-7")),
        None,
        Some(9),
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
    )
    .await
    .expect("handled");
    match out_rx.recv().await.expect("outgoing") {
        Outgoing::Message(Message::PublishError {
            request_id, code, ..
        }) => {
            assert_eq!(request_id, 9);
            assert_eq!(code, Some(felix_wire::ErrorCode::StaleWidth));
        }
        other => panic!("unexpected outgoing {other:?}"),
    }
}
