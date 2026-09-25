//! Unit tests for the publish path: admission, sharding, enqueue policy, depth
//! accounting, ack handling, and the control/uni handler entry points.

mod ack;
mod ack_on_enqueue;
mod admission;
mod control_batch;
mod control_binary;
mod control_message;
mod group_commit;
mod idempotent_acks;
mod ingress;
mod lease_headroom;
mod ownership_gate;
mod routing;
mod stream_cache;
mod uni;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use felix_authz::PermissionMatcher;
use felix_broker::Broker;
use felix_storage::EphemeralCache;
use felix_wire::{Frame, Message};
use tokio::sync::{Mutex, Semaphore};
use tokio::sync::{mpsc, watch};

use super::ingress::{enqueue_publish, publish_worker_index};
use super::route::{PublishRoute, publish_target, resolve_route};
use super::stream_cache::{push_decimal, push_stream_cache_key};
use super::*;
use crate::serving::auth::AuthContext;
use crate::serving::forward::ForwardTarget;
use crate::serving::quic::errors::AckEnqueueError;
use crate::serving::quic::{
    ACK_HI_WATER, ACK_TIMEOUT_THRESHOLD, ACK_TIMEOUT_WINDOW, GLOBAL_ACK_DEPTH,
};
use crate::shards::routing::IngressRouter;

// These publish-path tests don't exercise subscription delivery; this just gives
// `PublishContext::lane_manager` a real (if unused) instance to satisfy the type.
fn test_lane_manager() -> Arc<WriterLaneManager> {
    WriterLaneManager::new(&crate::config::BrokerConfig::default())
}

fn reset_global_ack_depth() {
    GLOBAL_ACK_DEPTH.store(0, Ordering::Relaxed);
}

fn make_publish_context(
    buffer: usize,
) -> (
    PublishContext,
    mpsc::Receiver<PublishJob>,
    mpsc::Sender<PublishJob>,
) {
    let (tx, rx) = mpsc::channel(buffer);
    let context = PublishContext {
        ingress: None,
        client_endpoints: None,
        peers: None,
        lease: None,
        lease_headroom: Duration::ZERO,
        marks: None,
        quorum_timeout: Duration::from_secs(1),
        workers: Arc::new(vec![tx.clone()]),
        worker_count: 1,
        depth: Arc::new(AtomicUsize::new(0)),
        wait_timeout: Duration::from_millis(100),
        admission: Arc::new(PublishAdmission::unlimited()),
        conn_admission: Arc::new(PublishAdmission::unlimited()),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: test_lane_manager(),
        ingress_wait: false,
    };
    (context, rx, tx)
}

fn make_job() -> PublishJob {
    PublishJob {
        target: PublishTarget::Named {
            tenant_id: "tenant".to_string(),
            namespace: "ns".to_string(),
            stream: "stream".to_string(),
        },
        payloads: vec![Bytes::from_static(b"payload")],
        response: None,
        acked_on_enqueue: false,
        admission_permit: None,
        fenced: None,
    }
}

fn make_auth_ctx(tenant_id: &str, perms: &[&str]) -> AuthContext {
    let patterns = perms.iter().map(|p| (*p).to_string()).collect::<Vec<_>>();
    let matcher = PermissionMatcher::from_strings(&patterns).expect("parse perms");
    AuthContext {
        tenant_id: tenant_id.to_string(),
        matcher,
        token: "test-token".to_string(),
    }
}

fn make_binary_publish_frame(tenant_id: &str, namespace: &str, stream: &str) -> Frame {
    let payloads = vec![b"payload".to_vec()];
    felix_wire::binary::encode_publish_batch(tenant_id, namespace, stream, &payloads)
        .expect("encode publish batch")
}

/// A broker with the stream registered, so only the ownership gate can
/// refuse anything below.
async fn broker_with_stream() -> Broker {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "t1",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    broker
}

fn watch_key() -> crate::shards::ShardKey {
    crate::shards::ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream".to_string(),
        shard: 0,
        kind: crate::shards::ShardKind::Stream,
    }
}

fn ingress_for(leader: &str, servable: bool) -> IngressRouter {
    let router = Arc::new(felix_router::ShardRouter::new(
        "broker-a",
        "us-west-2",
        felix_router::RegionRouter::new("us-west-2".to_string()),
    ));
    let assignment = crate::shards::watch::ShardAssignment {
        key: watch_key(),
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation: 1,
        state: "active".to_string(),
        successor: None,
    };
    let assignments: HashMap<crate::shards::ShardKey, crate::shards::watch::ShardAssignment> =
        [(watch_key(), assignment)].into_iter().collect();
    // A catalog with both brokers in it, because a route is only usable when
    // the owner's id has an address behind it. An empty catalog would make
    // every remote shard look unavailable rather than forwardable, which is
    // the wrong thing for these tests to be asserting against.
    let nodes: HashMap<String, felix_router::NodeRef> = ["broker-a", "broker-b"]
        .into_iter()
        .enumerate()
        .map(|(index, node_id)| {
            (
                node_id.to_string(),
                felix_router::NodeRef {
                    node_id: node_id.to_string(),
                    advertise_addr: std::net::SocketAddr::from((
                        [127, 0, 0, 1],
                        7000 + index as u16,
                    )),
                    region: "us-west-2".to_string(),
                    live: true,
                },
            )
        })
        .collect();
    router.publish(
        crate::shards::routing::routing_table_from(&assignments, &nodes),
        &nodes,
    );

    let ingress = IngressRouter::new(router, Arc::default());
    if servable {
        ingress.fence().open(&watch_key(), 1);
        ingress.publish_servable([(watch_key(), 1)].into_iter().collect());
    }
    ingress
}
