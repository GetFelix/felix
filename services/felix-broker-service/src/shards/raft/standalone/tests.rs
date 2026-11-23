use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use felix_replication::engine::Engine;
use felix_replication::raft::GroupRuntime;
use tokio_util::sync::CancellationToken;

use super::super::tests::{broker, key, open_root, until};
use super::*;
use crate::serving::quic::handlers::publish::{
    Authority, PublishRoute, StreamHandleCache, resolve_route, shard_key_at,
};
use crate::shards::routing::{Dispatch, StreamEngine, dispatch};

/// A standalone broker's shards and the engine its serving paths ask, fed
/// by [`run`] until the returned token is cancelled.
pub(crate) fn standalone(
    broker: &Arc<Broker>,
) -> (Arc<RaftShards>, Arc<StreamEngine>, CancellationToken) {
    let runtime =
        GroupRuntime::start(LOCAL_MEMBER, Default::default(), Arc::new(NoPeers)).expect("runtime");
    let shards = RaftShards::new(
        Arc::clone(broker),
        runtime,
        LOCAL_MEMBER.to_string(),
        Arc::new(fleet_of_one()),
    );
    let engine = Arc::new(StreamEngine::default());
    engine.set_raft(Arc::clone(&shards));
    broker.set_read_bounds(Arc::new(felix_replication::engine::CommittedReads::new(
        Arc::clone(&engine),
    )));
    open_root(broker, &shards);
    let stop = CancellationToken::new();
    tokio::spawn(run(Arc::clone(&shards), Arc::clone(broker), stop.clone()));
    (shards, engine, stop)
}

/// Publish `payloads` to `orders` shard 0 the way the publish path does:
/// routed by `resolve_route` with no ingress router, then proposed to the
/// group at the term routing answered.
pub(super) async fn publish(
    broker: &Broker,
    engine: &StreamEngine,
    payloads: &[&'static [u8]],
) -> u64 {
    let route = resolve_route(
        broker,
        Authority {
            ingress: None,
            lease: None,
            engine: Some(engine),
        },
        &mut StreamHandleCache::default(),
        &mut String::new(),
        "t1",
        "ns",
        "orders",
        0,
    )
    .await;
    let PublishRoute::Local {
        handle, generation, ..
    } = route
    else {
        panic!("not routed here: {route:?}");
    };
    let shard = shard_key_at(None, Some(engine), "t1", "ns", "orders", 0).expect("a Raft key");
    let proposed = engine
        .publish_on_raft(
            broker,
            &handle,
            &shard,
            generation,
            payloads
                .iter()
                .map(|p| bytes::Bytes::from_static(p))
                .collect(),
            Default::default(),
            Duration::from_secs(10),
        )
        .expect("a Raft shard")
        .expect("proposed");
    let mut offset = None;
    proposed
        .complete_then(|answered| offset = answered.expect("committed"))
        .await;
    offset.expect("an offset")
}

pub(super) async fn led(shards: &Arc<RaftShards>) -> u64 {
    let orders = key("orders", 0);
    until("the standalone group to be led", || {
        shards.leading(&orders).is_some()
    })
    .await;
    shards.leading(&orders).expect("term")
}

/// **A standalone broker runs a durable stream shard as a Raft group of
/// one**, with no peer and no control plane: the feed opens, forms and
/// leads it off the broker's own registration. The hybrid never opens its
/// log, and an in-memory stream stays local with no Raft key.
#[tokio::test]
async fn a_standalone_durable_shard_is_a_group_of_one() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let (shards, engine, stop) = standalone(&broker);
    let orders = key("orders", 0);
    let term = led(&shards).await;

    assert!(engine.on_raft(&orders));
    assert!(!engine.on_raft(&key("scratch", 0)));
    assert_eq!(
        dispatch(None, &engine, &orders),
        Dispatch::Local { generation: term }
    );
    assert_eq!(
        dispatch(None, &engine, &key("scratch", 0)),
        Dispatch::Local { generation: 0 }
    );
    assert!(shard_key_at(None, Some(&engine), "t1", "ns", "scratch", 0).is_none());
    let storage = broker.durable_storage().expect("storage");
    assert!(
        storage
            .open_stream_for(
                "t1",
                "ns",
                "orders",
                0,
                None,
                super::super::tests::INCARNATION
            )
            .is_err(),
        "the hybrid opened the Raft member's log"
    );

    let first = publish(&broker, &engine, &[b"one", b"two"]).await;
    until("the batch commits", || {
        engine.committed_bound(&orders) == felix_broker::ReadBound::Committed(first + 2)
    })
    .await;
    stop.cancel();
    EngineShards::shutdown(&*shards).await;
}

/// **What a standalone broker wrote is there after a restart**, and the
/// group it reopens is led again with no control plane to vouch for it.
#[tokio::test]
async fn a_standalone_group_keeps_its_records_across_a_restart() {
    let root = tempfile::tempdir().expect("tempdir");
    let orders = key("orders", 0);
    let end = {
        let broker = broker(root.path()).await;
        let (shards, engine, stop) = standalone(&broker);
        led(&shards).await;
        let first = publish(&broker, &engine, &[b"one", b"two", b"three"]).await;
        until("the batch commits", || {
            engine.committed_bound(&orders) == felix_broker::ReadBound::Committed(first + 3)
        })
        .await;
        stop.cancel();
        assert!(EngineShards::shutdown(&*shards).await);
        first + 3
    };

    let broker = broker(root.path()).await;
    let (shards, engine, stop) = standalone(&broker);
    led(&shards).await;
    until("the records are readable again", || {
        matches!(
            engine.committed_bound(&orders),
            felix_broker::ReadBound::Committed(bound) if bound >= end
        )
    })
    .await;
    let next = publish(&broker, &engine, &[b"four"]).await;
    assert!(next >= end, "offset {next} reused below {end}");
    stop.cancel();
    EngineShards::shutdown(&*shards).await;
}

/// A standalone broker is its whole fleet, so no Raft feature waits for a
/// fleet to finalize it.
#[test]
fn a_fleet_of_one_has_every_raft_feature() {
    let gate = fleet_of_one();
    for feature in fleet::IMPLEMENTED {
        let raft = feature.name().starts_with("raft_");
        assert_eq!(gate.supports(*feature), raft, "{feature}");
    }
    assert!(gate.supports(fleet::RAFT_CONDITIONAL_BATCHES));
}

mod serving;
