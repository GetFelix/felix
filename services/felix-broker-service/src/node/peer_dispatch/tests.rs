//! A broker that started from its control-plane view serves no forwarded
//! client request before it registers, and refuses it as retryable.
use std::time::Duration;

use felix_authz::TenantKeyCache;
use felix_broker::Broker;
use felix_router::{RegionRouter, ShardRouter};
use felix_storage::EphemeralCache;
use felix_wire::internal::{AckMode, CacheOpKind, ForwardCacheOp, ForwardPublish, ShardRef};

use super::*;
use crate::serving::auth::{BrokerAuth, ControlPlaneKeyStore};
use crate::shards::routing::IngressRouter;

fn handler() -> BrokerPeerHandler {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "local",
        RegionRouter::new("local".to_string()),
    ));
    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://localhost".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    let forwarding = ForwardingHandler::new(
        Arc::clone(&broker),
        Arc::new(IngressRouter::new(Arc::clone(&router), Arc::default())),
        Arc::clone(&router),
        "127.0.0.1:7001".to_string(),
        None,
        Duration::from_millis(100),
        Arc::new(BrokerAuth::with_key_store(key_store)),
    );
    BrokerPeerHandler::new(forwarding, ReplicaHandler::new(broker, router))
}

fn shard() -> ShardRef {
    ShardRef {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        generation: 0,
        incarnation: None,
    }
}

fn publish() -> InternalMessage {
    InternalMessage::ForwardPublish(ForwardPublish {
        correlation_id: 9,
        shard: shard(),
        ack: AckMode::OnCommit,
        payloads: Vec::new(),
        headers: None,
        producer: None,
        credential: String::new(),
    })
}

fn cache_op() -> ForwardCacheOp {
    ForwardCacheOp {
        correlation_id: 10,
        shard: shard(),
        op: CacheOpKind::Get,
        key: "k".to_string(),
        value: bytes::Bytes::new(),
        ttl_ms: 0,
        credential: String::new(),
        condition: None,
    }
}

fn refused_before_registration(answer: &InternalMessage) -> bool {
    match answer {
        InternalMessage::ForwardPublishError(error) => {
            error.code == ErrorCode::Unavailable && error.detail == NOT_REGISTERED
        }
        InternalMessage::ForwardCacheError(error) => {
            error.code == ErrorCode::Unavailable && error.detail == NOT_REGISTERED
        }
        _ => false,
    }
}

#[tokio::test]
async fn forwarded_client_requests_wait_for_registration() {
    let clients = CancellationToken::new();
    let handler = handler().serving_clients_after(clients.clone());
    for request in [
        publish(),
        InternalMessage::ForwardCacheOp(cache_op()),
        InternalMessage::LeaseForwardCacheOp(cache_op()),
    ] {
        let answer = handler.handle(request).await;
        assert!(refused_before_registration(&answer), "{answer:?}");
        assert!(
            ErrorCode::Unavailable.is_retryable(),
            "the forwarder must try again"
        );
    }

    // Registered: the request reaches the forwarding half, which answers it
    // on its own terms.
    clients.cancel();
    let answer = handler.handle(publish()).await;
    assert!(!refused_before_registration(&answer), "{answer:?}");
}

/// Without the gate, as on every broker that did not start from its view,
/// nothing waits.
#[tokio::test]
async fn without_the_gate_nothing_waits() {
    let answer = handler().handle(publish()).await;
    assert!(!refused_before_registration(&answer), "{answer:?}");
}
