use anyhow::Result;
use bytes::Bytes;
use felix_storage::EphemeralCache;
use tokio::sync::oneshot;

use super::*;

#[tokio::test]
async fn build_publish_context_clamps_worker_and_queue_minimums() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "demo", Default::default())
        .await?;

    let config = BrokerConfig {
        pub_workers_per_conn: 0,
        pub_queue_depth: 0,
        publish_queue_wait_timeout_ms: 17,
        ..BrokerConfig::default()
    };
    let publish_ctx =
        build_publish_context(Arc::clone(&broker), &config, ClusterContext::default());
    assert_eq!(publish_ctx.worker_count, 1);
    assert_eq!(publish_ctx.workers.len(), 1);
    assert_eq!(publish_ctx.wait_timeout, Duration::from_millis(17));

    let (response_tx, response_rx) = oneshot::channel();
    publish_ctx.workers[0]
        .send(PublishJob {
            target: PublishTarget::Named {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "demo".to_string(),
            },
            payloads: vec![Bytes::from_static(b"ok")],
            response: Some(response_tx),
            acked_on_enqueue: false,
            admission_permit: None,
            fenced: None,
        })
        .await
        .expect("enqueue publish");
    response_rx.await.expect("worker response")?;
    Ok(())
}

#[tokio::test]
async fn build_publish_context_worker_returns_publish_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    let config = BrokerConfig::default();
    let publish_ctx = build_publish_context(broker, &config, ClusterContext::default());

    let (response_tx, response_rx) = oneshot::channel();
    publish_ctx.workers[0]
        .send(PublishJob {
            target: PublishTarget::Named {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "missing".to_string(),
            },
            payloads: vec![Bytes::from_static(b"payload")],
            response: Some(response_tx),
            acked_on_enqueue: false,
            admission_permit: None,
            fenced: None,
        })
        .await
        .expect("enqueue publish");
    let err = response_rx
        .await
        .expect("worker response")
        .expect_err("publish should fail");
    assert!(err.to_string().contains("stream"));
    Ok(())
}

/// A `Named` job to this stream panics the worker that takes it.
pub(super) const PANICKING_STREAM: &str = "panics-the-worker";

fn named_job(stream: &str, response: Option<oneshot::Sender<Result<()>>>) -> PublishJob {
    PublishJob {
        target: PublishTarget::Named {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: stream.to_string(),
        },
        payloads: vec![Bytes::from_static(b"payload")],
        acked_on_enqueue: response.is_none(),
        response,
        admission_permit: None,
        fenced: None,
    }
}

/// A worker that panics is replaced on the same queue. Unsupervised, the
/// panic closed the queue and every stream hashed to that worker was refused
/// until the broker restarted.
#[tokio::test]
async fn a_worker_that_panics_is_replaced() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "demo", Default::default())
        .await?;
    let config = BrokerConfig {
        pub_workers_per_conn: 1,
        ..BrokerConfig::default()
    };
    let publish_ctx = build_publish_context(broker, &config, ClusterContext::default());

    let (response_tx, response_rx) = oneshot::channel();
    publish_ctx.workers[0]
        .send(named_job(PANICKING_STREAM, Some(response_tx)))
        .await
        .expect("enqueue the panicking job");
    assert!(
        response_rx.await.is_err(),
        "the panicking job has no answer"
    );

    let (response_tx, response_rx) = oneshot::channel();
    publish_ctx.workers[0]
        .send(named_job("demo", Some(response_tx)))
        .await
        .expect("the queue outlives the worker that panicked");
    tokio::time::timeout(Duration::from_secs(5), response_rx)
        .await
        .expect("a replacement worker answers")
        .expect("worker response")?;
    Ok(())
}

/// Closing the tracker and waiting on it waits for every queued publish to be
/// durable and delivered, including the completions the worker spawns. This
/// is what the shutdown drain relies on for publishes acknowledged on enqueue.
#[tokio::test]
async fn the_tracker_waits_for_queued_publishes_and_their_completions() -> Result<()> {
    use felix_broker::{DurableStorage, StreamMetadata};
    use felix_storage::log::{FsyncMode, LogConfig};

    const JOBS: usize = 32;
    let dir = tempfile::tempdir()?;
    let log_config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let broker = Broker::new(EphemeralCache::new().into())
        .with_durable_storage(DurableStorage::open(dir.path(), log_config)?);
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await?;
    let broker = Arc::new(broker);
    let handle = broker
        .resolve_stream_handle("t1", "default", "orders", 0)
        .await?;
    let mut subscription = broker.subscribe("t1", "default", "orders", 0).await?;

    let work = TaskTracker::new();
    let publish_ctx = build_tracked_publish_context(
        Arc::clone(&broker),
        &BrokerConfig::default(),
        ClusterContext::default(),
        &work,
    );
    let worker = publish_ctx.workers[handle.id() as usize % publish_ctx.worker_count].clone();
    for _ in 0..JOBS {
        worker
            .send(PublishJob {
                target: PublishTarget::Resolved {
                    handle: handle.clone(),
                    shard: None,
                    generation: 0,
                    fenced: None,
                },
                payloads: vec![Bytes::from_static(b"acked on enqueue")],
                response: None,
                acked_on_enqueue: true,
                admission_permit: None,
                fenced: None,
            })
            .await
            .expect("enqueue");
    }
    // What the end of the connections and the accept loop amounts to.
    drop(worker);
    drop(publish_ctx);

    work.close();
    tokio::time::timeout(Duration::from_secs(10), work.wait())
        .await
        .expect("the workers drain once their senders are gone");

    let mut delivered = 0;
    while subscription.try_recv().is_ok() {
        delivered += 1;
    }
    assert_eq!(
        delivered, JOBS,
        "the drain finished before every queued publish was written"
    );
    Ok(())
}

/// The write fence, on the worker. Each of these admits a publish while the
/// shard is served here, lets a move close the fence while the job waits in
/// the queue, and then lets the worker claim it -- which must refuse it and
/// write nothing, since the drained report may already have gone out.
mod fence {
    use std::collections::HashMap;

    use super::*;
    use crate::serving::quic::handlers::publish::route::{
        Authority, PublishRoute, local_shard_key, publish_target, resolve_route,
    };
    use crate::test_support::leader::{DURABLE, EPHEMERAL, Leader, NAMESPACE, TENANT, stream_key};

    #[derive(Clone, Copy)]
    enum Kind {
        Plain,
        Idempotent,
    }

    async fn admitted_then_fenced(stream: &str, kind: Kind) {
        let mut leader = Leader::start().await;
        let publish_ctx = build_publish_context(
            Arc::clone(&leader.broker),
            &BrokerConfig::default(),
            ClusterContext {
                ingress: Some(Arc::clone(&leader.ingress)),
                ..ClusterContext::default()
            },
        );

        let route = resolve_route(
            &leader.broker,
            Authority {
                ingress: Some(&leader.ingress),
                lease: None,
            },
            &mut HashMap::new(),
            &mut String::new(),
            TENANT,
            NAMESPACE,
            stream,
            0,
        )
        .await;
        let target = match (kind, route) {
            (Kind::Plain, route) => publish_target(
                route,
                &publish_ctx,
                TENANT,
                NAMESPACE,
                stream,
                0,
                felix_wire::internal::AckMode::OnCommit,
                "",
            )
            .expect("admitted"),
            (
                Kind::Idempotent,
                PublishRoute::Local {
                    handle,
                    generation,
                    fenced,
                },
            ) => PublishTarget::Idempotent {
                handle,
                shard: local_shard_key(&publish_ctx, TENANT, NAMESPACE, stream, 0),
                generation,
                fenced,
                producer_id: leader.broker.new_producer_id(),
                sequence: 0,
            },
            (_, other) => panic!("admission should serve it here: {other:?}"),
        };

        // The move lands while the job sits in the queue.
        leader.fence_move(&stream_key(stream));

        let (response_tx, response_rx) = oneshot::channel();
        publish_ctx.workers[0]
            .send(PublishJob {
                target,
                payloads: vec![Bytes::from_static(b"late")],
                response: Some(response_tx),
                acked_on_enqueue: false,
                admission_permit: None,
                fenced: None,
            })
            .await
            .expect("enqueue publish");
        let answer = response_rx.await.expect("worker response");
        let refused =
            answer.expect_err("a publish claimed after the fence closed was acknowledged");
        // Nothing was written, so the client is told it may retry elsewhere.
        let refused = crate::serving::quic::client_error::ClientError::from_anyhow(&refused);
        assert_eq!(refused.code(), &felix_wire::ErrorCode::ShardUnavailable);
        assert_eq!(refused.retry(), felix_wire::RetryClass::Retry);
        let tail = leader.tail(stream).await;
        assert_eq!(tail, 0, "the refused publish was written anyway");
    }

    #[tokio::test]
    async fn a_durable_publish_claimed_after_the_fence_is_refused() {
        admitted_then_fenced(DURABLE, Kind::Plain).await;
    }

    #[tokio::test]
    async fn an_ephemeral_publish_claimed_after_the_fence_is_refused() {
        admitted_then_fenced(EPHEMERAL, Kind::Plain).await;
    }

    #[tokio::test]
    async fn an_idempotent_publish_claimed_after_the_fence_is_refused() {
        admitted_then_fenced(DURABLE, Kind::Idempotent).await;
    }

    /// The control: the same path with no move in between is acknowledged,
    /// so the refusals above are the fence and not a broken fixture.
    #[tokio::test]
    async fn a_publish_with_no_move_in_between_is_written() {
        let leader = Leader::start().await;
        let publish_ctx = build_publish_context(
            Arc::clone(&leader.broker),
            &BrokerConfig::default(),
            ClusterContext {
                ingress: Some(Arc::clone(&leader.ingress)),
                ..ClusterContext::default()
            },
        );
        let route = resolve_route(
            &leader.broker,
            Authority {
                ingress: Some(&leader.ingress),
                lease: None,
            },
            &mut HashMap::new(),
            &mut String::new(),
            TENANT,
            NAMESPACE,
            DURABLE,
            0,
        )
        .await;
        let target = publish_target(
            route,
            &publish_ctx,
            TENANT,
            NAMESPACE,
            DURABLE,
            0,
            felix_wire::internal::AckMode::OnCommit,
            "",
        )
        .expect("admitted");
        let (response_tx, response_rx) = oneshot::channel();
        publish_ctx.workers[0]
            .send(PublishJob {
                target,
                payloads: vec![Bytes::from_static(b"on time")],
                response: Some(response_tx),
                acked_on_enqueue: false,
                admission_permit: None,
                fenced: None,
            })
            .await
            .expect("enqueue publish");
        response_rx
            .await
            .expect("worker response")
            .expect("acknowledged");
        let tail = leader.tail(DURABLE).await;
        assert_eq!(tail, 1);
    }
}
