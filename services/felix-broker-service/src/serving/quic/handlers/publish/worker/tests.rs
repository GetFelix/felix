use anyhow::Result;
use bytes::Bytes;
use felix_storage::EphemeralCache;
use tokio::sync::oneshot;

use super::*;

#[tokio::test]
async fn build_publish_context_clamps_executor_and_queue_minimums() -> Result<()> {
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
    assert_eq!(publish_ctx.scheduler.partitions().len(), 1);
    assert_eq!(publish_ctx.wait_timeout, Duration::from_millis(17));

    let (response_tx, response_rx) = oneshot::channel();
    publish_ctx
        .scheduler
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
        .await;
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
    publish_ctx
        .scheduler
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
        .await;
    let err = response_rx
        .await
        .expect("worker response")
        .expect_err("publish should fail");
    assert!(err.to_string().contains("stream"));
    Ok(())
}

/// A `Named` job to this stream panics the worker that takes it.
pub(super) const PANICKING_STREAM: &str = "panics-the-worker";

fn named_job(stream: &str, response: Option<oneshot::Sender<Result<Option<u64>>>>) -> PublishJob {
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

/// An executor that panics is replaced on the same queue, and the lane it
/// held is freed. With one executor, the publish after the panic is only
/// answered if both happened.
#[tokio::test]
async fn an_executor_that_panics_is_replaced() -> Result<()> {
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
    publish_ctx
        .scheduler
        .send(named_job(PANICKING_STREAM, Some(response_tx)))
        .await;
    assert!(
        response_rx.await.is_err(),
        "the panicking job has no answer"
    );

    let (response_tx, response_rx) = oneshot::channel();
    publish_ctx
        .scheduler
        .send(named_job("demo", Some(response_tx)))
        .await;
    tokio::time::timeout(Duration::from_secs(5), response_rx)
        .await
        .expect("a replacement executor answers")
        .expect("worker response")?;
    Ok(())
}

/// An executor with a backlog lets the subscribers its fanout wakes run
/// between jobs. The whole backlog is queued before the executor first runs,
/// and on this single-threaded runtime nothing else runs until it suspends, so
/// an executor that ran job after job would push every record into a queue
/// smaller than the backlog before the subscriber drained any of it.
#[tokio::test]
async fn subscribers_drain_between_an_executors_queued_publishes() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    const JOBS: usize = 32;
    const SUBSCRIBER_QUEUE: usize = 4;
    let broker =
        Arc::new(Broker::new(EphemeralCache::new().into()).with_topic_capacity(SUBSCRIBER_QUEUE)?);
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "demo", Default::default())
        .await?;
    let mut subscription = broker.subscribe("t1", "default", "demo", 0).await?;
    let received = Arc::new(AtomicUsize::new(0));
    let consumer = {
        let received = Arc::clone(&received);
        tokio::spawn(async move {
            while subscription.recv().await.is_some() {
                received.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    let config = BrokerConfig {
        pub_workers_per_conn: 1,
        pub_queue_depth: JOBS,
        ..BrokerConfig::default()
    };
    let publish_ctx = build_publish_context(broker, &config, ClusterContext::default());
    let mut answers = Vec::with_capacity(JOBS);
    for _ in 0..JOBS {
        let (response_tx, response_rx) = oneshot::channel();
        publish_ctx
            .scheduler
            .send(named_job("demo", Some(response_tx)))
            .await;
        answers.push(response_rx);
    }
    for answer in answers {
        answer.await.expect("worker response")?;
    }
    // The last record is fanned out before its answer, and the subscriber
    // needs one more turn to take it.
    tokio::time::timeout(Duration::from_secs(5), async {
        while received.load(Ordering::Relaxed) < JOBS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .ok();
    assert_eq!(
        received.load(Ordering::Relaxed),
        JOBS,
        "a subscriber that keeps up lost records to the executor's backlog"
    );
    consumer.abort();
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
    let scheduler = Arc::clone(&publish_ctx.scheduler);
    for _ in 0..JOBS {
        scheduler
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
            .await;
    }
    // What the end of the connections and the accept loop amounts to.
    drop(scheduler);
    drop(publish_ctx);

    work.close();
    tokio::time::timeout(Duration::from_secs(10), work.wait())
        .await
        .expect("the executors drain once the contexts are gone");

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

/// The write fence, at the claim. Each of these admits a publish while the
/// shard is served here, lets a move close the fence while the job waits in
/// the queue, and then lets it be claimed -- which must refuse it and write
/// nothing, since the drained report may already have gone out.
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
                reuse: felix_broker::SequenceReuse::Refuse,
            },
            (_, other) => panic!("admission should serve it here: {other:?}"),
        };

        // The move lands while the job sits in the queue.
        leader.fence_move(&stream_key(stream));

        let (response_tx, response_rx) = oneshot::channel();
        publish_ctx
            .scheduler
            .send(PublishJob {
                target,
                payloads: vec![Bytes::from_static(b"late")],
                response: Some(response_tx),
                acked_on_enqueue: false,
                admission_permit: None,
                fenced: None,
            })
            .await;
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
        publish_ctx
            .scheduler
            .send(PublishJob {
                target,
                payloads: vec![Bytes::from_static(b"on time")],
                response: Some(response_tx),
                acked_on_enqueue: false,
                admission_permit: None,
                fenced: None,
            })
            .await;
        response_rx
            .await
            .expect("worker response")
            .expect("acknowledged");
        let tail = leader.tail(DURABLE).await;
        assert_eq!(tail, 1);
    }
}
