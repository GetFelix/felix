//! One shard waiting on something slow must not hold up another.

use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::config::BrokerConfig;
use crate::serving::forward::ForwardKey;
use crate::serving::quic::ClusterContext;
use crate::serving::quic::handlers::publish::ack::EnqueuePolicy;
use crate::serving::quic::handlers::publish::ingress::enqueue_tenant_publish;

/// A forward to a peer that never answers is still in flight when a publish
/// to a different shard, on the same broker and with a single executor, has
/// already been written and acknowledged.
///
/// With a fixed worker pool the forward held its worker for the whole round
/// trip, and every stream hashed to that worker waited behind it.
#[tokio::test]
async fn a_stalled_forward_does_not_hold_up_another_shard() {
    let broker = Arc::new(broker_with_stream().await);
    let handle = broker
        .resolve_stream_handle("t1", "ns", "stream", 0)
        .await
        .expect("handle");
    // Bound and never read, so a handshake to it hangs rather than failing.
    let blackhole = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind a silent peer");
    let peers = felix_replication::peer::PeerPool::new(
        "broker-a".to_string(),
        felix_replication::peer::PeerTransportConfig::default(),
        CancellationToken::new(),
    )
    .expect("bind a peer pool");
    let config = BrokerConfig {
        pub_workers_per_conn: 1,
        // Long enough that the forward is still trying when the test looks.
        ack_wait_timeout_ms: 30_000,
        ..BrokerConfig::default()
    };
    let ctx = build_publish_context(
        Arc::clone(&broker),
        &config,
        ClusterContext {
            peers: Some(peers),
            ..ClusterContext::default()
        },
    );

    let (forward_tx, mut forward_rx) = oneshot::channel();
    let forward = PublishJob {
        target: PublishTarget::Forward {
            target: ForwardTarget {
                node_id: "broker-b".to_string(),
                advertise_addr: blackhole.local_addr().expect("addr"),
                generation: 1,
            },
            key: ForwardKey {
                tenant_id: "t2".to_string(),
                namespace: "ns".to_string(),
                stream: "remote".to_string(),
                shard: 0,
            },
            ack: felix_wire::internal::AckMode::OnCommit,
            credential: "token".to_string(),
        },
        payloads: vec![Bytes::from_static(b"forwarded")],
        response: Some(forward_tx),
        acked_on_enqueue: false,
        admission_permit: None,
        fenced: None,
        publisher: None,
    };
    assert!(
        enqueue_tenant_publish(&ctx, "t2", forward, EnqueuePolicy::Wait, None)
            .await
            .expect("the forward is queued")
    );
    // Let the forward start.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (local_tx, local_rx) = oneshot::channel();
    let local = PublishJob {
        target: PublishTarget::Resolved {
            handle,
            shard: None,
            generation: 0,
            fenced: None,
        },
        payloads: vec![Bytes::from_static(b"local")],
        response: Some(local_tx),
        acked_on_enqueue: false,
        admission_permit: None,
        fenced: None,
        publisher: None,
    };
    assert!(
        enqueue_tenant_publish(&ctx, "t1", local, EnqueuePolicy::Wait, None)
            .await
            .expect("the local publish is queued")
    );
    let answered = tokio::time::timeout(Duration::from_secs(1), local_rx).await;
    assert!(
        matches!(answered, Ok(Ok(Ok(_)))),
        "a publish to another shard waited behind a stalled forward"
    );
    assert!(
        forward_rx.try_recv().is_err(),
        "the forward was meant to still be waiting on its peer"
    );
}
