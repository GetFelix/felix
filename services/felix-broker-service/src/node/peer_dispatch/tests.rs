//! A broker that started from its control-plane view serves no forwarded
//! client request before it registers, and refuses it as retryable.
use std::time::Duration;

use felix_authz::TenantKeyCache;
use felix_broker::Broker;
use felix_replication::ReplicaHandler;
use felix_replication::engine::Hybrid;
use felix_router::{RegionRouter, ShardRouter};
use felix_storage::EphemeralCache;
use felix_wire::internal::{
    AckMode, CacheOpKind, Fence, ForwardCacheOp, ForwardPublish, PreVote, ReplicaLog,
    ReplicateBootstrap, ReplicateFetch, ReplicateRebuild, ReplicateRecords, ShardRef,
};

use super::*;
use crate::serving::auth::{BrokerAuth, ControlPlaneKeyStore};
use crate::shards::routing::IngressRouter;

fn handler() -> BrokerPeerHandler<Hybrid<IngressRouter>> {
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
    let engine = Arc::new(Hybrid::new(
        None,
        Some(Arc::new(IngressRouter::new(
            Arc::clone(&router),
            Arc::default(),
        ))),
    ));
    let forwarding = ForwardingHandler::new(
        Arc::clone(&broker),
        Arc::clone(&engine),
        Arc::clone(&router),
        "127.0.0.1:7001".to_string(),
        Duration::from_millis(100),
        Arc::new(BrokerAuth::with_key_store(key_store)),
    );
    engine
        .set_replica(ReplicaHandler::new(broker, router))
        .expect("set once");
    BrokerPeerHandler::new(forwarding, engine)
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

fn records() -> ReplicateRecords {
    ReplicateRecords {
        correlation_id: 11,
        shard: shard(),
        first_offset: 0,
        checksum: 0,
        payloads: Vec::new(),
        marks: Vec::new(),
        publishers: Vec::new(),
        commit_offset: None,
        generations: None,
        times: None,
        headers: None,
        writers: None,
        set_generation: None,
    }
}

fn bootstrap() -> ReplicateBootstrap {
    ReplicateBootstrap {
        correlation_id: 12,
        shard: shard(),
        base_offset: 0,
    }
}

/// Every request the replication engine answers.
fn replication_requests() -> Vec<InternalMessage> {
    vec![
        InternalMessage::ReplicateRecords(records()),
        InternalMessage::ReplicateMarkedRecords(records()),
        InternalMessage::ReplicateCacheRecords(records()),
        InternalMessage::ReplicateGroupRecords(records()),
        InternalMessage::ReplicateDeadLetterRecords(records()),
        InternalMessage::ReplicateCounterRecords(records()),
        InternalMessage::ReplicateBootstrap(bootstrap()),
        InternalMessage::ReplicateCacheBootstrap(bootstrap()),
        InternalMessage::ReplicateGroupBootstrap(bootstrap()),
        InternalMessage::ReplicateDeadLetterBootstrap(bootstrap()),
        InternalMessage::ReplicateCounterBootstrap(bootstrap()),
        InternalMessage::ReplicateRebuild(ReplicateRebuild {
            correlation_id: 13,
            shard: shard(),
            log: ReplicaLog::Stream,
            base_offset: 0,
        }),
        InternalMessage::Fence(Fence {
            correlation_id: 14,
            shard: shard(),
            log: ReplicaLog::Stream,
            election: None,
        }),
        InternalMessage::ReplicateFetch(ReplicateFetch {
            correlation_id: 15,
            shard: shard(),
            log: ReplicaLog::Stream,
            from_offset: 0,
            max_bytes: 1024,
            labelled: false,
            timed: false,
            headers: false,
            writers: false,
        }),
        InternalMessage::PreVote(PreVote {
            correlation_id: 16,
            shard: shard(),
            log: ReplicaLog::Stream,
            last_generation: 0,
            log_end: 0,
            set_generation: 0,
        }),
    ]
}

/// A replication request reaches the engine, which answers it as a replica
/// would; one dropped from the routing falls through to the refusal of a
/// message that is not a request at all.
#[tokio::test]
async fn every_replication_request_reaches_the_engine() {
    let handler = handler();
    for request in replication_requests() {
        let kind = request.kind();
        let answer = handler.handle(request).await;
        let text = format!("{answer:?}");
        assert!(
            !text.contains("is not a"),
            "{kind:?} was not routed: {text}"
        );
    }
}

/// A forwarded write never reaches the engine, and a response is no request.
#[tokio::test]
async fn the_engine_refuses_what_is_not_replication() {
    let answer = handler()
        .engine
        .handle(None, PeerCapabilities::NONE, publish())
        .await;
    assert!(
        matches!(&answer, InternalMessage::ForwardPublishError(error)
        if error.code == ErrorCode::Malformed
            && error.detail.ends_with("is not a replication request")),
        "{answer:?}"
    );
}
