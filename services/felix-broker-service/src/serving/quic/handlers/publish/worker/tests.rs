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
            headers: None,
            target: PublishTarget::Named {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "demo".to_string(),
            },
            payloads: vec![Bytes::from_static(b"ok")],
            response: Some(response_tx.into()),
            acked_on_enqueue: false,
            admission_permit: None,
            fenced: None,
            publisher: None,
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
            headers: None,
            target: PublishTarget::Named {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "missing".to_string(),
            },
            payloads: vec![Bytes::from_static(b"payload")],
            response: Some(response_tx.into()),
            acked_on_enqueue: false,
            admission_permit: None,
            fenced: None,
            publisher: None,
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
        headers: None,
        target: PublishTarget::Named {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: stream.to_string(),
        },
        payloads: vec![Bytes::from_static(b"payload")],
        acked_on_enqueue: response.is_none(),
        response: response.map(Into::into),
        admission_permit: None,
        fenced: None,
        publisher: None,
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
                headers: None,
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
                publisher: None,
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
    use felix_common::random::HashMap;

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
                engine: std::sync::Arc::new(crate::shards::routing::StreamEngine::new(
                    None,
                    Some(Arc::clone(&leader.ingress)),
                )),
                ..ClusterContext::default()
            },
        );

        let route = resolve_route(
            &leader.broker,
            Authority {
                ingress: Some(&leader.ingress),
                lease: None,
                engine: None,
            },
            &mut HashMap::default(),
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
                &Default::default(),
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
                headers: None,
                target,
                payloads: vec![Bytes::from_static(b"late")],
                response: Some(response_tx.into()),
                acked_on_enqueue: false,
                admission_permit: None,
                fenced: None,
                publisher: None,
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
                engine: std::sync::Arc::new(crate::shards::routing::StreamEngine::new(
                    None,
                    Some(Arc::clone(&leader.ingress)),
                )),
                ..ClusterContext::default()
            },
        );
        let route = resolve_route(
            &leader.broker,
            Authority {
                ingress: Some(&leader.ingress),
                lease: None,
                engine: None,
            },
            &mut HashMap::default(),
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
            &Default::default(),
        )
        .expect("admitted");
        let (response_tx, response_rx) = oneshot::channel();
        publish_ctx
            .scheduler
            .send(PublishJob {
                headers: None,
                target,
                payloads: vec![Bytes::from_static(b"on time")],
                response: Some(response_tx.into()),
                acked_on_enqueue: false,
                admission_permit: None,
                fenced: None,
                publisher: None,
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

/// Durable publishes queued on one lane are claimed together: one append,
/// with each publish answered with its own offsets, in lane order.
mod lane_claim {
    use felix_broker::{DurableStorage, StreamHandle, StreamMetadata};
    use felix_storage::log::{FsyncMode, LogConfig};

    use super::*;
    use crate::test_support::leader::{DURABLE, GENERATION, Leader, NAMESPACE, TENANT, stream_key};

    type Answer = oneshot::Receiver<Result<Option<u64>>>;

    async fn durable_broker() -> Result<(tempfile::TempDir, Arc<Broker>, StreamHandle)> {
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
        Ok((dir, broker, handle))
    }

    fn job(
        handle: &StreamHandle,
        shard: Option<crate::shards::ShardKey>,
        generation: u64,
        records: usize,
    ) -> (PublishJob, Answer) {
        let (response_tx, response_rx) = oneshot::channel();
        let job = PublishJob {
            headers: None,
            target: PublishTarget::Resolved {
                handle: handle.clone(),
                shard,
                generation,
                fenced: None,
            },
            payloads: (0..records)
                .map(|n| Bytes::from(format!("record {n}")))
                .collect(),
            response: Some(response_tx.into()),
            acked_on_enqueue: false,
            admission_permit: None,
            fenced: None,
            publisher: None,
        };
        (job, response_rx)
    }

    /// One executor, so nothing runs until the test first waits: every job
    /// is queued on the lane before the executor takes the first.
    fn context(broker: &Arc<Broker>, cluster: ClusterContext) -> PublishContext {
        let config = BrokerConfig {
            pub_workers_per_conn: 1,
            pub_queue_depth: 64,
            ..BrokerConfig::default()
        };
        build_publish_context(Arc::clone(broker), &config, cluster)
    }

    fn offset(answer: Result<Result<Option<u64>>, oneshot::error::RecvError>) -> Option<u64> {
        answer
            .expect("worker response")
            .expect("publish acknowledged")
    }

    #[tokio::test]
    async fn queued_publishes_on_one_lane_are_claimed_as_one_append() -> Result<()> {
        let (_dir, broker, handle) = durable_broker().await?;
        let (mut deliveries, _guard) = broker
            .subscribe("t1", "default", "orders", 0)
            .await?
            .into_parts();
        let publish_ctx = context(&broker, ClusterContext::default());
        // A publish of three records in the middle, so each answer has to be
        // sliced from the claim by record count, not by position.
        let sizes = [1, 1, 3, 1, 1, 1, 1, 1];
        let mut answers = Vec::new();
        for records in sizes {
            let (job, answer) = job(&handle, None, 0, records);
            publish_ctx.scheduler.send(job).await;
            answers.push(answer);
        }

        let mut expected = 0;
        for (answer, records) in answers.into_iter().zip(sizes) {
            assert_eq!(offset(answer.await), Some(expected));
            expected += records as u64;
        }
        let envelope = deliveries.try_recv().expect("the claim was fanned out");
        assert_eq!(envelope.base_offset(), Some(0));
        assert_eq!(
            envelope.len() as u64,
            expected,
            "the queued publishes were appended one by one, not as one claim"
        );
        assert!(deliveries.try_recv().is_err(), "more than one append");
        Ok(())
    }

    /// The members of a claim are answered in lane order: by the time the
    /// last is answered, every earlier one already has been.
    #[tokio::test]
    async fn no_publish_in_a_claim_is_answered_before_an_earlier_one() -> Result<()> {
        let (_dir, broker, handle) = durable_broker().await?;
        let publish_ctx = context(&broker, ClusterContext::default());
        let mut answers = Vec::new();
        for _ in 0..6 {
            let (job, answer) = job(&handle, None, 0, 1);
            publish_ctx.scheduler.send(job).await;
            answers.push(answer);
        }
        let last = answers.pop().expect("six answers");
        assert_eq!(offset(last.await), Some(5));
        for (n, mut answer) in answers.into_iter().enumerate() {
            let earlier = answer
                .try_recv()
                .expect("an earlier publish was answered after a later one");
            assert_eq!(earlier.expect("acknowledged"), Some(n as u64));
        }
        Ok(())
    }

    /// A claim that cannot be appended fails every publish in it, and
    /// nothing is written: the members share the one append.
    #[tokio::test]
    async fn a_claim_that_fails_fails_every_publish_in_it() -> Result<()> {
        let (_dir, broker, handle) = durable_broker().await?;
        broker.remove_stream("t1", "default", "orders").await?;
        let publish_ctx = context(&broker, ClusterContext::default());
        let mut answers = Vec::new();
        for _ in 0..4 {
            let (job, answer) = job(&handle, None, 0, 1);
            publish_ctx.scheduler.send(job).await;
            answers.push(answer);
        }
        for answer in answers {
            let err = answer
                .await
                .expect("worker response")
                .expect_err("a publish in a failed claim was acknowledged");
            let err = crate::serving::quic::client_error::ClientError::from_anyhow(&err);
            assert_ne!(
                err.code(),
                &felix_wire::ErrorCode::Internal,
                "the shared failure lost its classification: {err:?}"
            );
        }
        Ok(())
    }

    /// A quorum timeout on a claim leaves every member's outcome unknown, so
    /// none of them may be told it is safe to send again.
    #[tokio::test]
    async fn a_claim_whose_quorum_wait_times_out_leaves_every_member_unknown() {
        let mut answers = Vec::new();
        let mut members = Vec::new();
        for _ in 0..2 {
            let (tx, rx) = oneshot::channel();
            members.push(GroupMember {
                response: Some(tx.into()),
                acked_on_enqueue: false,
                records: 1,
            });
            answers.push(rx);
        }
        let timeout = felix_replication::quorum::QuorumError::TimedOut {
            what: "publish",
            timeout: Duration::from_millis(5),
        };
        settle_group(members, None, Err(timeout.into()));
        for answer in answers {
            let err = answer
                .await
                .expect("worker response")
                .expect_err("a publish whose quorum wait timed out was acknowledged");
            let err = crate::serving::quic::client_error::ClientError::from_anyhow(&err);
            assert_eq!(err.code(), &felix_wire::ErrorCode::QuorumTimeout, "{err:?}");
            assert_eq!(
                err.retry(),
                felix_wire::RetryClass::OutcomeUnknown,
                "{err:?}"
            );
        }
    }

    /// A publish whose caller stopped waiting is still written in its place,
    /// and the publishes around it keep their own offsets and answers.
    #[tokio::test]
    async fn a_publish_nobody_waits_for_keeps_its_place_in_the_claim() -> Result<()> {
        let (_dir, broker, handle) = durable_broker().await?;
        let publish_ctx = context(&broker, ClusterContext::default());
        let mut answers = Vec::new();
        for _ in 0..4 {
            let (job, answer) = job(&handle, None, 0, 1);
            publish_ctx.scheduler.send(job).await;
            answers.push(answer);
        }
        drop(answers.remove(1));
        let offsets: Vec<_> = futures::future::join_all(answers)
            .await
            .into_iter()
            .map(offset)
            .collect();
        assert_eq!(offsets, [Some(0), Some(2), Some(3)]);
        let tail = broker
            .cursor_tail("t1", "default", "orders", 0)
            .await?
            .next_seq();
        assert_eq!(tail, 4);
        Ok(())
    }

    /// The fence is checked per publish. One it refuses is answered with the
    /// refusal and left out of the claim; the rest are written around it
    /// with contiguous offsets, and none of them is failed for it.
    #[tokio::test]
    async fn a_publish_the_fence_refuses_is_left_out_of_the_claim() {
        let leader = Leader::start().await;
        let handle = leader
            .broker
            .resolve_stream_handle(TENANT, NAMESPACE, DURABLE, 0)
            .await
            .expect("handle");
        let publish_ctx = context(
            &leader.broker,
            ClusterContext {
                engine: std::sync::Arc::new(crate::shards::routing::StreamEngine::new(
                    None,
                    Some(Arc::clone(&leader.ingress)),
                )),
                ..ClusterContext::default()
            },
        );
        let key = Some(stream_key(DURABLE));
        let mut answers = Vec::new();
        for n in 0..4 {
            // Admitted at a generation the shard is not served at.
            let generation = if n == 2 { GENERATION + 1 } else { GENERATION };
            let (job, answer) = job(&handle, key.clone(), generation, 1);
            publish_ctx.scheduler.send(job).await;
            answers.push(answer);
        }
        let mut answers = futures::future::join_all(answers).await.into_iter();
        assert_eq!(offset(answers.next().expect("first")), Some(0));
        assert_eq!(offset(answers.next().expect("second")), Some(1));
        let refused = answers
            .next()
            .expect("third")
            .expect("worker response")
            .expect_err("a fenced publish was acknowledged");
        let refused = crate::serving::quic::client_error::ClientError::from_anyhow(&refused);
        assert_eq!(refused.code(), &felix_wire::ErrorCode::ShardUnavailable);
        assert_eq!(offset(answers.next().expect("fourth")), Some(2));
        assert_eq!(leader.tail(DURABLE).await, 3);
    }
}

/// Queue a single tenant's one-byte jobs without letting the executors run, until the queue
/// refuses. Returns how many were queued and the refusal.
async fn fill_until_refused(config: &BrokerConfig) -> (usize, String) {
    use crate::serving::quic::handlers::publish::ack::EnqueuePolicy;
    use crate::serving::quic::handlers::publish::ingress::enqueue_publish;

    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let publish_ctx = build_publish_context(broker, config, ClusterContext::default());
    // Nothing here yields, so the executors never drain the queue.
    tokio::task::unconstrained(async {
        let mut queued = 0;
        loop {
            let mut job = named_job("demo", None);
            job.payloads = vec![Bytes::from_static(b"x")];
            match enqueue_publish(&publish_ctx, "t1", job, EnqueuePolicy::Fail, None).await {
                Ok(true) => queued += 1,
                Ok(false) => unreachable!("Fail never drops"),
                Err(err) => return (queued, err.to_string()),
            }
        }
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn the_default_queue_holds_2048_jobs() {
    let config = BrokerConfig::default();
    assert_eq!(config.pub_queue_depth, 512);
    assert_eq!(config.pub_workers_per_conn, 4);
    let (queued, refused) = fill_until_refused(&config).await;
    // A lone tenant is kept off the last `pub_queue_depth` slots of the 2048.
    assert_eq!(queued, 2048 - 512);
    assert!(refused.contains("queue full"), "refused by: {refused}");
}

#[tokio::test(flavor = "current_thread")]
async fn an_explicit_queue_depth_still_caps_the_queue() {
    let config = BrokerConfig {
        pub_workers_per_conn: 2,
        pub_queue_depth: 3,
        ..BrokerConfig::default()
    };
    let (queued, refused) = fill_until_refused(&config).await;
    // Capacity 6, minus the 3 slots a lone tenant cannot take.
    assert_eq!(queued, 3);
    assert!(refused.contains("queue full"), "refused by: {refused}");
}

/// A peer that offers nothing, so a headered forward is declined before it is
/// sent.
struct NoHeaders;

impl felix_replication::peer::PeerRequester for NoHeaders {
    async fn request(
        &self,
        node_id: &str,
        _addr: std::net::SocketAddr,
        message: felix_wire::internal::InternalMessage,
    ) -> std::result::Result<
        felix_wire::internal::InternalMessage,
        felix_replication::peer::pool::PeerError,
    > {
        Err(felix_replication::peer::pool::PeerError::Unsupported {
            node_id: node_id.to_string(),
            kind: message.kind(),
        })
    }

    fn recorded_capabilities(
        &self,
        _node_id: &str,
    ) -> Option<felix_wire::internal::PeerCapabilities> {
        Some(felix_wire::internal::PeerCapabilities::NONE)
    }

    async fn capabilities(
        &self,
        _node_id: &str,
        _addr: std::net::SocketAddr,
    ) -> std::result::Result<
        felix_wire::internal::PeerCapabilities,
        felix_replication::peer::pool::PeerError,
    > {
        Ok(felix_wire::internal::PeerCapabilities::NONE)
    }
}

fn forward_job(headers: Option<Vec<Option<Bytes>>>) -> PublishJob {
    PublishJob {
        headers,
        target: PublishTarget::Forward {
            target: crate::serving::forward::ForwardTarget {
                node_id: "broker-b".to_string(),
                advertise_addr: "127.0.0.1:9".parse().expect("addr"),
                generation: 1,
                incarnation: 0,
            },
            key: crate::serving::forward::ForwardKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "demo".to_string(),
                shard: 0,
            },
            ack: felix_wire::internal::AckMode::OnCommit,
            credential: "token".into(),
        },
        payloads: vec![Bytes::from_static(b"a")],
        response: None,
        acked_on_enqueue: false,
        admission_permit: None,
        fenced: None,
        publisher: None,
    }
}

/// **A headered forward to an owner that lacks `RECORD_HEADERS` is refused as
/// `feature_not_enabled`**, whether that owner was the first target or a
/// redirect's. The same batch without headers is not mistaken for it.
#[tokio::test]
async fn a_headered_forward_to_an_owner_without_the_capability_is_not_enabled() {
    let job = forward_job(Some(vec![Some(Bytes::from_static(b"block"))]));
    let err = forward(&NoHeaders, &job, Duration::from_secs(5))
        .await
        .expect_err("refused");
    let client = err.downcast_ref::<ClientError>().expect("a client error");
    assert_eq!(
        client.code(),
        &felix_wire::ErrorCode::FeatureNotEnabled,
        "{err}"
    );
    assert_eq!(
        client.detail().and_then(|detail| detail.reason.as_deref()),
        Some(felix_wire::feature_reason::RECORD_HEADERS)
    );

    let plain = forward(&NoHeaders, &forward_job(None), Duration::from_secs(5))
        .await
        .expect_err("refused");
    assert!(plain.downcast_ref::<ClientError>().is_none(), "{plain}");
}

/// **An owner whose Raft headers gate is closed is named in the refusal.** Its
/// `UnsupportedKind` reaches this broker as text, and the client message must
/// say `raft_record_headers`, not just `record_headers`.
#[test]
fn an_owner_refusal_naming_the_raft_gate_is_passed_on() {
    use crate::serving::forward::ForwardError;
    let refused = |detail: &str| ForwardError::Refused {
        stream: "demo".to_string(),
        detail: detail.to_string(),
        code: Some(felix_wire::internal::ErrorCode::UnsupportedKind),
    };
    let owner = felix_broker::BrokerError::RecordHeadersNotEnabled {
        feature: felix_common::fleet::RAFT_RECORD_HEADERS.name(),
    };
    let err = headers_unsupported(refused(&format!("UnsupportedKind: {owner}")), true);
    let client = err.downcast_ref::<ClientError>().expect("a client error");
    assert_eq!(client.code(), &felix_wire::ErrorCode::FeatureNotEnabled);
    assert!(client.message().contains("raft_record_headers"), "{err}");

    let err = headers_unsupported(refused("UnsupportedKind: too old"), true);
    let client = err.downcast_ref::<ClientError>().expect("a client error");
    assert!(!client.message().contains("raft_record_headers"), "{err}");
}

/// The wait on a `Quorum` stream, from each place the worker makes it: held
/// to the mark at the generation the publish was admitted at, for the
/// configured time.
mod quorum_wait {
    use felix_replication::quorum::QuorumMarks;

    use super::*;
    use crate::test_support::leader::{GENERATION, Leader, QUORUM, stream_key};

    #[derive(Clone, Copy)]
    enum Path {
        Claimed,
        Idempotent,
    }

    async fn send(
        leader: &Leader,
        path: Path,
        marks: &Arc<QuorumMarks>,
    ) -> (Result<Option<u64>>, Duration) {
        let config = BrokerConfig {
            publish_quorum_timeout_ms: 300,
            ..BrokerConfig::default()
        };
        let publish_ctx = build_publish_context(
            Arc::clone(&leader.broker),
            &config,
            ClusterContext {
                engine: std::sync::Arc::new(crate::shards::routing::StreamEngine::new(
                    Some(Arc::clone(marks)),
                    Some(Arc::clone(&leader.ingress)),
                )),
                ..ClusterContext::default()
            },
        );
        let handle = leader
            .broker
            .resolve_stream_handle("t1", "ns", QUORUM, 0)
            .await
            .expect("handle");
        let key = stream_key(QUORUM);
        let (response_tx, response_rx) = oneshot::channel();
        let target = match path {
            Path::Claimed => PublishTarget::Resolved {
                handle,
                shard: Some(key),
                generation: GENERATION,
                fenced: None,
            },
            Path::Idempotent => PublishTarget::Idempotent {
                handle,
                shard: Some(key),
                generation: GENERATION,
                fenced: None,
                producer_id: 7,
                sequence: 0,
                reuse: felix_broker::SequenceReuse::Refuse,
            },
        };
        let started = std::time::Instant::now();
        publish_ctx
            .scheduler
            .send(PublishJob {
                headers: None,
                target,
                payloads: vec![Bytes::from_static(b"a")],
                response: Some(response_tx.into()),
                acked_on_enqueue: false,
                admission_permit: None,
                fenced: None,
                publisher: None,
            })
            .await;
        let answer = response_rx.await.expect("worker response");
        (answer, started.elapsed())
    }

    async fn acknowledged_at_its_own_generation(path: Path) {
        let leader = Leader::start().await;
        let marks = Arc::new(QuorumMarks::new());
        marks.publish(&stream_key(QUORUM), GENERATION, 1_000);
        let (answer, _) = send(&leader, path, &marks).await;
        assert_eq!(answer.expect("acknowledged"), Some(0));
    }

    async fn held_by_another_generation_for_the_timeout(path: Path) {
        let leader = Leader::start().await;
        let marks = Arc::new(QuorumMarks::new());
        marks.publish(&stream_key(QUORUM), GENERATION + 1, 1_000);
        let (answer, waited) = send(&leader, path, &marks).await;
        answer.expect_err("a mark at another generation released it");
        assert!(
            waited >= Duration::from_millis(300),
            "ended early after {waited:?}"
        );
        assert!(
            waited < Duration::from_secs(3),
            "ended late after {waited:?}"
        );
    }

    #[tokio::test]
    async fn a_claimed_publish_is_acknowledged_at_its_own_generation() {
        acknowledged_at_its_own_generation(Path::Claimed).await;
    }

    #[tokio::test]
    async fn an_idempotent_publish_is_acknowledged_at_its_own_generation() {
        acknowledged_at_its_own_generation(Path::Idempotent).await;
    }

    #[tokio::test]
    async fn a_claimed_publish_is_not_released_by_another_generation() {
        held_by_another_generation_for_the_timeout(Path::Claimed).await;
    }

    #[tokio::test]
    async fn an_idempotent_publish_is_not_released_by_another_generation() {
        held_by_another_generation_for_the_timeout(Path::Idempotent).await;
    }
}

#[cfg(feature = "raft")]
mod on_raft {
    use super::*;
    use crate::serving::quic::handlers::publish::AdmissionPermit;
    use crate::shards::raft::tests as fixture;
    use crate::shards::routing::EngineShards;

    type Answer = oneshot::Receiver<Result<Option<u64>>>;

    fn job(
        handle: &felix_broker::StreamHandle,
        shard: &crate::shards::ShardKey,
        term: u64,
        payload: &'static [u8],
        headers: Option<Vec<Option<Bytes>>>,
    ) -> (PublishJob, Answer) {
        let (response_tx, response_rx) = oneshot::channel();
        let job = PublishJob {
            headers,
            target: PublishTarget::Resolved {
                handle: handle.clone(),
                shard: Some(shard.clone()),
                generation: term,
                fenced: None,
            },
            payloads: vec![Bytes::from_static(payload)],
            response: Some(response_tx.into()),
            acked_on_enqueue: false,
            admission_permit: None,
            fenced: None,
            publisher: None,
        };
        (job, response_rx)
    }

    /// **A queued publish to a Raft shard is proposed to its group and
    /// answered with the offset it committed at**, in lane order. One
    /// admitted at a term this broker no longer serves, or carrying headers
    /// this broker does not take, is refused with nothing proposed.
    #[tokio::test]
    async fn a_queued_publish_to_a_raft_shard_commits_through_its_group() -> Result<()> {
        let root = tempfile::tempdir()?;
        let broker = fixture::broker(root.path()).await;
        let shards = fixture::shards(&broker);
        let orders = fixture::key("orders", 0);
        let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
        fixture::take_formed(&shards, assigned).await;
        fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
        let term = shards.leading(&orders).expect("term");
        let engine = Arc::new(fixture::engine(&shards));
        // As startup binds them: readers see what the group committed.
        broker.set_read_bounds(Arc::new(felix_replication::engine::CommittedReads::new(
            Arc::clone(&engine),
        )));
        let cluster = ClusterContext {
            engine,
            ..ClusterContext::default()
        };
        let config = BrokerConfig {
            pub_workers_per_conn: 1,
            pub_queue_depth: 64,
            ..BrokerConfig::default()
        };
        let publish_ctx = build_publish_context(Arc::clone(&broker), &config, cluster);
        let handle = broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await?;
        let (mut receiver, _guard) = broker
            .subscribe("t1", "ns", "orders", 0)
            .await?
            .into_parts();

        let (one, first) = job(&handle, &orders, term, b"one", None);
        let (stale, refused_term) = job(&handle, &orders, term + 1, b"stale", None);
        let headers = Some(vec![Some(Bytes::from_static(b"h"))]);
        let (headered, refused_headers) = job(&handle, &orders, term, b"headered", headers);
        let (two, second) = job(&handle, &orders, term, b"two", None);
        for job in [one, stale, headered, two] {
            publish_ctx.scheduler.send(job).await;
        }

        let first = first.await?.expect("acknowledged").expect("an offset");
        let err = refused_term.await?.expect_err("a stale term was written");
        assert!(err.to_string().contains("does not serve"), "{err}");
        let err = refused_headers.await?.expect_err("headers were dropped");
        assert!(
            err.to_string().contains("record headers are not enabled"),
            "{err}"
        );
        let second = second.await?.expect("acknowledged").expect("an offset");
        assert_eq!(second, first + 1, "a refused publish took an offset");
        assert_eq!(
            fixture::delivered(&mut receiver).await,
            (first, Bytes::from_static(b"one"))
        );
        assert_eq!(
            fixture::delivered(&mut receiver).await,
            (second, Bytes::from_static(b"two"))
        );
        EngineShards::shutdown(&*shards).await;
        Ok(())
    }

    /// **A queued publish to a Raft shard looks its group up once**, as the
    /// lane picks the Raft path; its proposal, its answer and its commit
    /// wait use that group. The other lookups counted are the broker's read
    /// bound, asked before it releases the committed batch.
    #[tokio::test]
    async fn a_queued_publish_to_a_raft_shard_looks_its_group_up_once() -> Result<()> {
        let root = tempfile::tempdir()?;
        let broker = fixture::broker(root.path()).await;
        let shards = fixture::shards(&broker);
        let orders = fixture::key("orders", 0);
        let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
        fixture::take_formed(&shards, assigned).await;
        fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
        let term = shards.leading(&orders).expect("term");
        let engine = Arc::new(fixture::engine(&shards));
        let cluster = ClusterContext {
            engine,
            ..ClusterContext::default()
        };
        let config = BrokerConfig {
            pub_workers_per_conn: 1,
            pub_queue_depth: 64,
            ..BrokerConfig::default()
        };
        let publish_ctx = build_publish_context(Arc::clone(&broker), &config, cluster);
        let handle = broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await?;
        let (mut receiver, _guard) = broker
            .subscribe("t1", "ns", "orders", 0)
            .await?
            .into_parts();
        let raft = shards.engine_for(&orders).expect("a member here").clone();
        let before = raft.lookups();
        let _ = handle.read_bound();
        let per_read = raft.lookups() - before;
        assert_eq!(per_read, 1, "a read bound looks the shard up once");

        let before = raft.lookups();
        let (one, answer) = job(&handle, &orders, term, b"one", None);
        publish_ctx.scheduler.send(one).await;
        let offset = answer.await?.expect("acknowledged").expect("an offset");
        assert_eq!(
            fixture::delivered(&mut receiver).await,
            (offset, Bytes::from_static(b"one"))
        );
        assert_eq!(raft.lookups() - before, 1 + per_read);
        EngineShards::shutdown(&*shards).await;
        Ok(())
    }

    /// **A queued publish to a Raft shard keeps its publisher and headers**
    /// once the fleet stores them on Raft: the subscriber gets both, as on
    /// the hybrid.
    #[tokio::test]
    async fn a_queued_publish_to_a_raft_shard_keeps_its_publisher_and_headers() -> Result<()> {
        let root = tempfile::tempdir()?;
        let broker = fixture::broker(root.path()).await;
        broker.record_headers_when(|| true);
        broker.record_publishers_when(|| true);
        broker.allow_raft_record_headers_when(|| true);
        let shards = fixture::shards(&broker);
        let orders = fixture::key("orders", 0);
        let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
        fixture::take_formed(&shards, assigned).await;
        fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
        let term = shards.leading(&orders).expect("term");
        let engine = Arc::new(fixture::engine(&shards));
        broker.set_read_bounds(Arc::new(felix_replication::engine::CommittedReads::new(
            Arc::clone(&engine),
        )));
        let cluster = ClusterContext {
            engine,
            ..ClusterContext::default()
        };
        let config = BrokerConfig {
            pub_workers_per_conn: 1,
            pub_queue_depth: 64,
            ..BrokerConfig::default()
        };
        let publish_ctx = build_publish_context(Arc::clone(&broker), &config, cluster);
        let handle = broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await?;
        let (mut receiver, _guard) = broker
            .subscribe("t1", "ns", "orders", 0)
            .await?
            .into_parts();

        let headers = Bytes::from_static(b"\x00\x01k\x00\x01v");
        let publisher = Bytes::from_static(b"svc-orders");
        let (mut headered, answer) = job(
            &handle,
            &orders,
            term,
            b"headered",
            Some(vec![Some(headers.clone())]),
        );
        headered.publisher = Some(publisher.clone());
        publish_ctx.scheduler.send(headered).await;
        let offset = answer.await?.expect("acknowledged").expect("an offset");

        let delivered = tokio::time::timeout(Duration::from_secs(10), receiver.recv())
            .await
            .expect("delivered in time")
            .expect("subscription open");
        assert_eq!(delivered.base_offset(), Some(offset));
        assert_eq!(delivered.publisher(), Some(&publisher));
        assert_eq!(delivered.record_headers(), Some(&[Some(headers)][..]));
        EngineShards::shutdown(&*shards).await;
        Ok(())
    }

    fn idempotent(
        handle: &felix_broker::StreamHandle,
        shard: &crate::shards::ShardKey,
        term: u64,
        sequence: u64,
        payload: &'static [u8],
    ) -> (PublishJob, Answer) {
        let (mut job, answer) = job(handle, shard, term, payload, None);
        job.target = PublishTarget::Idempotent {
            handle: handle.clone(),
            shard: Some(shard.clone()),
            generation: term,
            fenced: None,
            producer_id: 9,
            sequence,
            reuse: felix_broker::SequenceReuse::Refuse,
        };
        (job, answer)
    }

    /// **Idempotent publishes queued to a Raft shard are written once each,
    /// in lane order, and a batch queued again is answered with the offset
    /// its first arrival got.** The producer's batches are queued back to
    /// back, so each is checked against a log that holds the one before.
    #[tokio::test]
    async fn queued_idempotent_publishes_to_a_raft_shard_are_written_once() -> Result<()> {
        let root = tempfile::tempdir()?;
        let broker = fixture::broker(root.path()).await;
        let shards = fixture::shards(&broker);
        let orders = fixture::key("orders", 0);
        let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
        fixture::take_formed(&shards, assigned).await;
        fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
        let term = shards.leading(&orders).expect("term");
        let engine = Arc::new(fixture::engine(&shards));
        broker.set_read_bounds(Arc::new(felix_replication::engine::CommittedReads::new(
            Arc::clone(&engine),
        )));
        let cluster = ClusterContext {
            engine,
            ..ClusterContext::default()
        };
        let config = BrokerConfig {
            pub_workers_per_conn: 1,
            pub_queue_depth: 64,
            ..BrokerConfig::default()
        };
        let publish_ctx = build_publish_context(Arc::clone(&broker), &config, cluster);
        let handle = broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await?;
        let (mut receiver, _guard) = broker
            .subscribe("t1", "ns", "orders", 0)
            .await?
            .into_parts();

        let (zero, first) = idempotent(&handle, &orders, term, 0, b"zero");
        let (one, second) = idempotent(&handle, &orders, term, 1, b"one");
        let (again, resent) = idempotent(&handle, &orders, term, 0, b"zero");
        let (skips, gap) = idempotent(&handle, &orders, term, 3, b"three");
        for job in [zero, one, again, skips] {
            publish_ctx.scheduler.send(job).await;
        }

        let first = first.await?.expect("acknowledged").expect("an offset");
        let second = second.await?.expect("acknowledged").expect("an offset");
        assert_eq!(second, first + 1);
        let resent = resent.await?.expect("answered").expect("an offset");
        assert_eq!(resent, first, "a re-send got an offset of its own");
        let err = gap.await?.expect_err("a sequence was skipped");
        // The broker's own refusal, which the control stream names to the client.
        assert!(
            matches!(
                err.downcast_ref::<felix_broker::BrokerError>(),
                Some(felix_broker::BrokerError::SequenceGap { expected: 2 })
            ),
            "{err}"
        );
        let (plain, after) = job(&handle, &orders, term, b"plain", None);
        publish_ctx.scheduler.send(plain).await;
        let after = after.await?.expect("acknowledged").expect("an offset");
        assert_eq!(after, second + 1, "a re-send or a refusal took an offset");
        for (offset, payload) in [(first, &b"zero"[..]), (second, b"one"), (after, b"plain")] {
            assert_eq!(
                fixture::delivered(&mut receiver).await,
                (offset, Bytes::copy_from_slice(payload))
            );
        }
        EngineShards::shutdown(&*shards).await;
        Ok(())
    }

    /// The same job, as read from `conn`.
    fn read_from(
        conn: &Arc<ConnLimits>,
        (mut job, answer): (PublishJob, Answer),
    ) -> (PublishJob, Answer) {
        job.admission_permit = Some(AdmissionPermit::of_connection(conn));
        (job, answer)
    }

    /// **An idempotent batch waiting for its producer's turn holds up no
    /// other connection's publish to the shard, and no later publish of its
    /// own connection passes it.** The wait lasts until the producer's
    /// batch before is on the log. Held on the lane, it would stop every
    /// publish to the shard for as long. Let past, a plain publish its
    /// connection sent after the batch would be committed before it.
    #[tokio::test]
    async fn a_batch_waiting_for_its_producers_turn_holds_up_only_its_own_connection() -> Result<()>
    {
        let root = tempfile::tempdir()?;
        let broker = fixture::broker(root.path()).await;
        let shards = fixture::shards(&broker);
        let orders = fixture::key("orders", 0);
        let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
        fixture::take_formed(&shards, assigned).await;
        fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
        let term = shards.leading(&orders).expect("term");
        let engine = Arc::new(fixture::engine(&shards));
        broker.set_read_bounds(Arc::new(felix_replication::engine::CommittedReads::new(
            Arc::clone(&engine),
        )));
        let cluster = ClusterContext {
            engine,
            ..ClusterContext::default()
        };
        let config = BrokerConfig {
            pub_workers_per_conn: 1,
            pub_queue_depth: 64,
            ..BrokerConfig::default()
        };
        let publish_ctx = build_publish_context(Arc::clone(&broker), &config, cluster);
        let handle = broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await?;
        let (mut receiver, _guard) = broker
            .subscribe("t1", "ns", "orders", 0)
            .await?
            .into_parts();
        let (producer, other) = (ConnLimits::new(&config), ConnLimits::new(&config));

        // The producer's batch before, not yet written.
        let turn = broker.producer_turn(&handle, 9).await;
        let (zero, first) = read_from(&producer, idempotent(&handle, &orders, term, 0, b"zero"));
        let (one, second) = read_from(&producer, idempotent(&handle, &orders, term, 1, b"one"));
        let (behind, mut kept) = read_from(&producer, job(&handle, &orders, term, b"behind", None));
        let (plain, passed) = read_from(&other, job(&handle, &orders, term, b"plain", None));
        for job in [zero, one, behind, plain] {
            publish_ctx.scheduler.send(job).await;
        }
        let passed = tokio::time::timeout(Duration::from_secs(10), passed).await;
        let passed = passed
            .expect("held behind another connection's producer")?
            .expect("acknowledged")
            .expect("an offset");
        assert_eq!(
            fixture::delivered(&mut receiver).await,
            (passed, Bytes::from_static(b"plain"))
        );
        assert!(kept.try_recv().is_err(), "passed its connection's batch");

        // The connection's publishes kept the order they were queued in.
        drop(turn);
        let first = first.await?.expect("acknowledged").expect("an offset");
        let second = second.await?.expect("acknowledged").expect("an offset");
        let kept = kept.await?.expect("acknowledged").expect("an offset");
        assert_eq!((first, second, kept), (passed + 1, passed + 2, passed + 3));

        // The line is gone with its last job: the next is proposed on the lane.
        let (after, last) = read_from(&producer, job(&handle, &orders, term, b"after", None));
        publish_ctx.scheduler.send(after).await;
        let last = last.await?.expect("acknowledged").expect("an offset");
        assert_eq!(last, kept + 1);
        EngineShards::shutdown(&*shards).await;
        Ok(())
    }

    /// **An idempotent batch of several records is written once on a Raft
    /// shard, and its re-send is answered with the same offset.**
    #[tokio::test]
    async fn an_idempotent_batch_of_several_records_is_written_once_on_raft() -> Result<()> {
        let root = tempfile::tempdir()?;
        let broker = fixture::broker(root.path()).await;
        let shards = fixture::shards(&broker);
        let orders = fixture::key("orders", 0);
        let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
        fixture::take_formed(&shards, assigned).await;
        fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
        let term = shards.leading(&orders).expect("term");
        let engine = Arc::new(fixture::engine(&shards));
        broker.set_read_bounds(Arc::new(felix_replication::engine::CommittedReads::new(
            Arc::clone(&engine),
        )));
        let cluster = ClusterContext {
            engine,
            ..ClusterContext::default()
        };
        let config = BrokerConfig {
            pub_workers_per_conn: 1,
            pub_queue_depth: 64,
            ..BrokerConfig::default()
        };
        let publish_ctx = build_publish_context(Arc::clone(&broker), &config, cluster);
        let handle = broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await?;

        let (before, first) = job(&handle, &orders, term, b"before", None);
        let (mut several, taken) = idempotent(&handle, &orders, term, 0, b"a");
        several.payloads.push(Bytes::from_static(b"b"));
        let (mut resent, again) = idempotent(&handle, &orders, term, 0, b"a");
        resent.payloads.push(Bytes::from_static(b"b"));
        for job in [before, several, resent] {
            publish_ctx.scheduler.send(job).await;
        }
        let first = first.await?.expect("acknowledged").expect("an offset");
        let taken = taken.await?.expect("acknowledged").expect("an offset");
        assert_eq!(taken, first + 1);
        let again = again.await?.expect("answered").expect("an offset");
        assert_eq!(again, taken, "the re-send was written again");
        let read = broker.read_durable("t1", "ns", "orders", 0, first, 1 << 20);
        let payloads: Vec<Bytes> = read.await?.into_iter().map(|r| r.payload).collect();
        assert_eq!(payloads, ["before", "a", "b"]);
        EngineShards::shutdown(&*shards).await;
        Ok(())
    }

    /// **A batch of several records is checked at its producer's turn, not
    /// before.** It may be a re-send of a batch the log holds, which only
    /// the log can tell, so it waits behind the producer's batch before and
    /// its connection's later publishes wait with it.
    #[tokio::test]
    async fn a_batch_of_several_records_is_checked_at_its_producers_turn() -> Result<()> {
        let at = Leading::new().await?;
        let (handle, orders, term) = (&at.handle, &at.orders, at.term);
        let conn = ConnLimits::new(&at.config);

        // The producer's batch before, not yet written.
        let turn = at.broker.producer_turn(handle, 9).await;
        let (mut several, mut taken) = read_from(&conn, idempotent(handle, orders, term, 0, b"a"));
        several.payloads.push(Bytes::from_static(b"b"));
        let (behind, mut passed) = read_from(&conn, job(handle, orders, term, b"behind", None));
        for job in [several, behind] {
            at.publish_ctx.scheduler.send(job).await;
        }
        let early = tokio::time::timeout(Duration::from_millis(300), &mut taken).await;
        assert!(early.is_err(), "answered before its producer's turn");
        assert!(passed.try_recv().is_err(), "passed its connection's batch");

        drop(turn);
        let taken = taken.await?.expect("acknowledged").expect("an offset");
        let passed = passed.await?.expect("acknowledged").expect("an offset");
        assert_eq!(passed, taken + 2, "the batch lost its place");
        at.stop().await;
        Ok(())
    }

    /// **A Raft publish whose client ack its control stream times out arms
    /// no timer of its own.** The stream's sweep answers it at the quorum
    /// wait with the engine's error, and the commit wait behind its delivery
    /// has no deadline. Each was one timer per publish.
    #[tokio::test]
    async fn a_raft_publish_acked_to_a_client_arms_no_timer() -> Result<()> {
        use crate::serving::quic::handlers::publish::ack::{AckEncoding, Outgoing};
        use crate::serving::quic::handlers::publish::commit_ack::{AckRequest, Armed};

        let at = Leading::new().await?;
        let (out_tx, mut out_rx) = tokio::sync::mpsc::channel(8);
        let acks = super::super::super::CommitAcks::for_test(
            &out_tx,
            Arc::new(tokio::sync::Semaphore::new(8)),
        );
        let request = AckRequest {
            request_id: 1,
            encoding: AckEncoding::Binary,
            payload_bytes: 3,
            forwarded_to: None,
            start: crate::serving::quic::telemetry::t_instant_now(),
            single: true,
        };
        let (reply, pending) = acks.reserve(request).expect("room");
        let (mut one, _) = job(&at.handle, &at.orders, at.term, b"one", None);
        one.response = Some(super::super::super::PublishReply::Ack(reply));

        let armed = felix_broker::timer_probe::armed();
        at.publish_ctx.scheduler.send(one).await;
        assert_eq!(pending.arm(), Armed::Waiting);
        let answer = tokio::time::timeout(Duration::from_secs(30), out_rx.recv()).await?;
        assert!(
            matches!(
                answer,
                Some(Outgoing::PublishAck {
                    request_id: 1,
                    error: None,
                    ..
                })
            ),
            "{answer:?}"
        );
        assert_eq!(
            felix_broker::timer_probe::armed() - armed,
            0,
            "timers armed for one publish"
        );
        at.stop().await;
        Ok(())
    }

    /// A broker leading `orders` on its own, with a publish context on it.
    struct Leading {
        _root: tempfile::TempDir,
        broker: Arc<Broker>,
        shards: Arc<crate::shards::raft::RaftShards>,
        orders: crate::shards::ShardKey,
        term: u64,
        config: BrokerConfig,
        publish_ctx: PublishContext,
        handle: felix_broker::StreamHandle,
    }

    impl Leading {
        async fn new() -> Result<Self> {
            let root = tempfile::tempdir()?;
            let broker = fixture::broker(root.path()).await;
            let shards = fixture::shards(&broker);
            let orders = fixture::key("orders", 0);
            let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
            fixture::take_formed(&shards, assigned).await;
            fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
            let term = shards.leading(&orders).expect("term");
            let engine = Arc::new(fixture::engine(&shards));
            broker.set_read_bounds(Arc::new(felix_replication::engine::CommittedReads::new(
                Arc::clone(&engine),
            )));
            let cluster = ClusterContext {
                engine,
                ..ClusterContext::default()
            };
            let config = BrokerConfig {
                pub_workers_per_conn: 1,
                pub_queue_depth: 64,
                ..BrokerConfig::default()
            };
            let publish_ctx = build_publish_context(Arc::clone(&broker), &config, cluster);
            let handle = broker
                .resolve_stream_handle("t1", "ns", "orders", 0)
                .await?;
            Ok(Self {
                _root: root,
                broker,
                shards,
                orders,
                term,
                config,
                publish_ctx,
                handle,
            })
        }

        /// Producer 9's batch `sequence` of `payloads`, sent with `reuse`.
        async fn send(
            &self,
            sequence: u64,
            payloads: &[&'static [u8]],
            reuse: felix_broker::SequenceReuse,
        ) -> Result<Option<u64>> {
            let (mut job, answer) =
                idempotent(&self.handle, &self.orders, self.term, sequence, b"");
            job.payloads = payloads.iter().map(|p| Bytes::from_static(p)).collect();
            if let PublishTarget::Idempotent { reuse: sent, .. } = &mut job.target {
                *sent = reuse;
            }
            self.publish_ctx.scheduler.send(job).await;
            answer.await?
        }

        async fn plain(&self, payload: &'static [u8]) -> u64 {
            let (job, answer) = job(&self.handle, &self.orders, self.term, payload, None);
            self.publish_ctx.scheduler.send(job).await;
            let answer = answer.await.expect("answered");
            answer.expect("acknowledged").expect("an offset")
        }

        async fn log_from(&self, from: u64) -> Vec<Bytes> {
            let read = self
                .broker
                .read_durable("t1", "ns", "orders", 0, from, 1 << 20);
            let read = read.await.expect("read");
            read.into_iter().map(|record| record.payload).collect()
        }

        async fn stop(self) {
            EngineShards::shutdown(&*self.shards).await;
        }
    }

    /// Whether `answer` is the broker's own refusal, as the control stream
    /// names it to the client, and `is` that refusal.
    fn refused(
        answer: Result<Option<u64>>,
        is: impl Fn(&felix_broker::BrokerError) -> bool,
    ) -> bool {
        let err = answer.expect_err("taken");
        err.downcast_ref::<felix_broker::BrokerError>()
            .is_some_and(is)
    }

    /// **A re-send of a batch a leader change cut is finished through the
    /// publish handler and answered from the start's first offset.** The
    /// log holds the batch's first two records and another record after
    /// them. Only the two the log does not hold are written. A re-send
    /// whose start differs is refused with `sequence_reused` under either
    /// reuse mode, and one of another length with `sequence_expired`.
    #[tokio::test]
    async fn a_resent_cut_batch_is_finished_and_answered_from_its_start() -> Result<()> {
        use felix_broker::{BrokerError, SequenceReuse};

        let at = Leading::new().await?;
        let zero = at.send(0, &[b"a"], SequenceReuse::Refuse).await?;
        let zero = zero.expect("an offset");
        let start = fixture::write_batch(&at.shards, &at.orders, 9, 1, 0, 4, &[b"b", b"c"]).await;
        assert_eq!(start, zero + 1);
        let theirs = at.plain(b"theirs").await;

        for reuse in [SequenceReuse::Refuse, SequenceReuse::AnswerDuplicate] {
            let other = at.send(1, &[b"w", b"x", b"y", b"z"], reuse).await;
            assert!(
                refused(other, |err| matches!(
                    err,
                    BrokerError::SequenceReused { sequence: 1 }
                )),
                "{reuse:?}"
            );
        }
        let shorter = at.send(1, &[b"b", b"c", b"d"], SequenceReuse::Refuse).await;
        assert!(refused(shorter, |err| matches!(
            err,
            BrokerError::SequenceExpired { sequence: 1 }
        )));
        assert_eq!(at.log_from(zero).await, ["a", "b", "c", "theirs"]);

        let finished = at
            .send(1, &[b"b", b"c", b"d", b"e"], SequenceReuse::Refuse)
            .await?;
        assert_eq!(finished, Some(start));
        let again = at
            .send(1, &[b"b", b"c", b"d", b"e"], SequenceReuse::Refuse)
            .await?;
        assert_eq!(again, Some(start), "a finished batch is answered as held");
        let next = at.send(2, &[b"f"], SequenceReuse::Refuse).await?;
        assert_eq!(next, Some(theirs + 3));
        assert_eq!(
            at.log_from(zero).await,
            ["a", "b", "c", "theirs", "d", "e", "f"]
        );
        at.stop().await;
        Ok(())
    }

    /// **A re-send of a batch of several records the log holds whole is
    /// answered with its first offset through the publish handler**, and
    /// nothing is written again. A different batch under its sequence is
    /// refused unless the client asked to be answered a duplicate.
    #[tokio::test]
    async fn a_resent_batch_the_log_holds_whole_is_answered_as_a_duplicate() -> Result<()> {
        use felix_broker::{BrokerError, SequenceReuse};

        let at = Leading::new().await?;
        let first =
            fixture::write_batch(&at.shards, &at.orders, 9, 0, 0, 3, &[b"a", b"b", b"c"]).await;
        let again = at
            .send(0, &[b"a", b"b", b"c"], SequenceReuse::Refuse)
            .await?;
        assert_eq!(again, Some(first));
        let other = at.send(0, &[b"x", b"y", b"z"], SequenceReuse::Refuse).await;
        assert!(refused(other, |err| matches!(
            err,
            BrokerError::SequenceReused { sequence: 0 }
        )));
        let quiet = at
            .send(0, &[b"x", b"y", b"z"], SequenceReuse::AnswerDuplicate)
            .await?;
        assert_eq!(quiet, Some(first));
        let next = at.send(1, &[b"d"], SequenceReuse::Refuse).await?;
        assert_eq!(next, Some(first + 3), "a re-send took an offset");
        assert_eq!(at.log_from(first).await, ["a", "b", "c", "d"]);
        at.stop().await;
        Ok(())
    }

    /// **A re-send of a cut batch the log keeps only by name is refused
    /// with `sequence_expired` through the publish handler**, and nothing
    /// is written. Here the cut is forgotten because the log holds more
    /// cut batches than it keeps.
    #[tokio::test]
    async fn a_resent_forgotten_batch_is_refused_as_expired() -> Result<()> {
        use felix_broker::{BrokerError, SequenceReuse};

        let at = Leading::new().await?;
        fixture::forget_cut(&at.shards, &at.orders, 9, 0).await;
        let tail = at.plain(b"tail").await;
        for reuse in [SequenceReuse::Refuse, SequenceReuse::AnswerDuplicate] {
            let resent = at.send(0, &[b"cut", b"rest"], reuse).await;
            assert!(
                refused(resent, |err| matches!(
                    err,
                    BrokerError::SequenceExpired { sequence: 0 }
                )),
                "{reuse:?}"
            );
        }
        assert_eq!(
            at.plain(b"after").await,
            tail + 1,
            "a refusal took an offset"
        );
        at.stop().await;
        Ok(())
    }

    /// Plain publishes queued on one Raft shard's lane: merged into one
    /// proposal, each answered with its own offsets.
    mod merged {
        use super::*;
        use crate::serving::quic::client_error::ClientError;

        impl Leading {
            /// Batches handed to the group, and openraft writes made, so far.
            fn counts(&self) -> (u64, u64) {
                let engine = self.shards.engine_for(&self.orders).expect("a member");
                let status = engine.status(&self.orders).expect("a group");
                (engine.proposals(&self.orders), status.client_writes)
            }

            fn job_of(&self, records: usize) -> (PublishJob, Answer) {
                let (mut job, answer) = job(&self.handle, &self.orders, self.term, b"", None);
                job.payloads = (0..records).map(|n| Bytes::from(format!("r{n}"))).collect();
                (job, answer)
            }

            async fn subscribe(&self) -> felix_broker::SubscriptionReceiver {
                let (receiver, guard) = self
                    .broker
                    .subscribe("t1", "ns", "orders", 0)
                    .await
                    .expect("subscribed")
                    .into_parts();
                std::mem::forget(guard);
                receiver
            }
        }

        fn offset(answer: Result<Result<Option<u64>>, oneshot::error::RecvError>) -> u64 {
            answer
                .expect("answered")
                .expect("acknowledged")
                .expect("an offset")
        }

        /// **Publishes queued on one Raft shard's lane are one proposal,
        /// one openraft write and one delivery**, each answered with its
        /// own offsets in lane order, whether its publisher waits for the
        /// commit, was answered on enqueue, or asked for no answer. A lone
        /// publish is still exactly one of each.
        #[tokio::test]
        async fn queued_publishes_to_a_raft_shard_are_one_proposal() -> Result<()> {
            let at = Leading::new().await?;
            let mut receiver = at.subscribe().await;

            let before = at.counts();
            let lone = at.plain(b"lone").await;
            let after = at.counts();
            assert_eq!(
                (after.0 - before.0, after.1 - before.1),
                (1, 1),
                "a lone publish is one proposal and one write"
            );
            assert_eq!(fixture::delivered(&mut receiver).await.0, lone);

            // Three records in the middle, so answers are sliced by count.
            let sizes = [1, 1, 3, 1, 1, 1, 1, 1];
            let mut answers = Vec::new();
            for (n, records) in sizes.into_iter().enumerate() {
                let (mut job, answer) = at.job_of(records);
                match n {
                    // Asked for no answer.
                    4 => job.response = None,
                    // Told it succeeded when it was queued.
                    5 => {
                        job.response = None;
                        job.acked_on_enqueue = true;
                    }
                    _ => {}
                }
                at.publish_ctx.scheduler.send(job).await;
                answers.push(answer);
            }
            let mut expected = lone + 1;
            for (n, (answer, records)) in answers.into_iter().zip(sizes).enumerate() {
                if n != 4 && n != 5 {
                    assert_eq!(offset(answer.await), expected, "publish {n}");
                }
                expected += records as u64;
            }
            let after_merge = at.counts();
            assert_eq!(
                (after_merge.0 - after.0, after_merge.1 - after.1),
                (1, 1),
                "the queued publishes were proposed one by one"
            );
            let envelope = tokio::time::timeout(Duration::from_secs(10), receiver.recv())
                .await
                .expect("delivered in time")
                .expect("subscription open");
            assert_eq!(envelope.base_offset(), Some(lone + 1));
            assert_eq!(envelope.len() as u64, expected - lone - 1);
            assert_eq!(
                at.log_from(lone + 1).await.len() as u64,
                expected - lone - 1
            );
            at.stop().await;
            Ok(())
        }

        /// **A merged proposal looks its group up once**, as a lone publish
        /// does, not once per member: the members taken from the lane use
        /// the group the first one found.
        #[tokio::test]
        async fn a_merged_proposal_looks_its_group_up_once() -> Result<()> {
            let at = Leading::new().await?;
            let mut receiver = at.subscribe().await;
            let raft = at.shards.engine_for(&at.orders).expect("a member");
            let before = raft.lookups();
            let _ = at.handle.read_bound();
            let per_read = raft.lookups() - before;

            // `counts` looks the shard up itself, so it is read outside.
            let proposals = at.counts().0;
            let before = raft.lookups();
            let mut answers = Vec::new();
            for _ in 0..8 {
                let (job, answer) = at.job_of(1);
                at.publish_ctx.scheduler.send(job).await;
                answers.push(answer);
            }
            for answer in answers {
                offset(answer.await);
            }
            let envelope = tokio::time::timeout(Duration::from_secs(10), receiver.recv())
                .await
                .expect("delivered in time")
                .expect("subscription open");
            assert_eq!(envelope.len(), 8);
            assert_eq!(raft.lookups() - before, 1 + per_read);
            assert_eq!(at.counts().0 - proposals, 1, "one proposal");
            at.stop().await;
            Ok(())
        }

        /// **A merge stops at its cap**: 70 queued publishes are two
        /// proposals, 64 and 6, with offsets in lane order.
        #[tokio::test]
        async fn a_merge_stops_at_its_cap() -> Result<()> {
            let at = Leading::new().await?;
            let before = at.counts();
            let mut answers = Vec::new();
            for _ in 0..70 {
                let (job, answer) = at.job_of(1);
                at.publish_ctx.scheduler.send(job).await;
                answers.push(answer);
            }
            let mut offsets = Vec::new();
            for answer in answers {
                offsets.push(offset(answer.await));
            }
            let first = offsets[0];
            assert_eq!(offsets, (first..first + 70).collect::<Vec<_>>());
            assert_eq!(at.counts().0 - before.0, 2, "64 then 6");
            at.stop().await;
            Ok(())
        }

        /// **A publish refused alone does not refuse its neighbours**: one
        /// admitted at a term the group moved past and one with headers
        /// this broker does not take sit between good ones. They are
        /// refused, and the good ones get the offsets they would have had.
        #[tokio::test]
        async fn a_refused_publish_does_not_refuse_its_neighbours() -> Result<()> {
            let at = Leading::new().await?;
            let before = at.counts();
            let (good, first) = at.job_of(1);
            let (stale, refused_term) = job(&at.handle, &at.orders, at.term + 1, b"stale", None);
            let headers = Some(vec![Some(Bytes::from_static(b"h"))]);
            let (headered, refused_headers) =
                job(&at.handle, &at.orders, at.term, b"headered", headers);
            let (two, second) = at.job_of(1);
            let (three, third) = at.job_of(1);
            for job in [good, stale, headered, two, three] {
                at.publish_ctx.scheduler.send(job).await;
            }
            let first = offset(first.await);
            let err = refused_term.await?.expect_err("a stale term was written");
            assert!(err.to_string().contains("does not serve"), "{err}");
            let err = refused_headers.await?.expect_err("headers were dropped");
            assert!(err.to_string().contains("record headers"), "{err}");
            assert_eq!(offset(second.await), first + 1);
            assert_eq!(offset(third.await), first + 2);
            // The first alone, then the two after the refusals together.
            assert_eq!(at.counts().0 - before.0, 2);
            at.stop().await;
            Ok(())
        }

        /// **A merged proposal the group refuses fails every publish in it,
        /// each as it would have failed alone**, and nothing is written.
        #[tokio::test]
        async fn a_refused_merged_proposal_fails_every_publish_in_it() -> Result<()> {
            let at = Leading::new().await?;
            let before = at.counts();
            let (lone, lone_answer) = job(&at.handle, &at.orders, at.term + 1, b"x", None);
            at.publish_ctx.scheduler.send(lone).await;
            let alone = ClientError::from_anyhow(&lone_answer.await?.expect_err("written"));
            let mut answers = Vec::new();
            for _ in 0..4 {
                let (job, answer) = job(&at.handle, &at.orders, at.term + 1, b"x", None);
                at.publish_ctx.scheduler.send(job).await;
                answers.push(answer);
            }
            for answer in answers {
                let err = answer
                    .await?
                    .expect_err("a refused publish was acknowledged");
                let err = ClientError::from_anyhow(&err);
                assert_eq!(err.code(), alone.code(), "{err:?}");
                assert_eq!(err.retry(), alone.retry(), "{err:?}");
            }
            assert_eq!(at.counts().0, before.0, "a refused batch was proposed");
            at.stop().await;
            Ok(())
        }

        /// **A publisher that stops waiting keeps its place in the merge**:
        /// its record is still written, and the others keep their offsets.
        #[tokio::test]
        async fn a_cancelled_publisher_keeps_its_place_in_the_merge() -> Result<()> {
            let at = Leading::new().await?;
            let mut answers = Vec::new();
            for _ in 0..4 {
                let (job, answer) = at.job_of(1);
                at.publish_ctx.scheduler.send(job).await;
                answers.push(answer);
            }
            drop(answers.remove(1));
            let offsets: Vec<_> = futures::future::join_all(answers)
                .await
                .into_iter()
                .map(offset)
                .collect();
            let first = offsets[0];
            assert_eq!(offsets, [first, first + 2, first + 3]);
            assert_eq!(at.log_from(first).await.len(), 4);
            at.stop().await;
            Ok(())
        }

        /// **An idempotent batch between plain publishes is proposed on its
        /// own**, so its producer marks stay its own, and a re-send of it is
        /// answered with where it first landed.
        #[tokio::test]
        async fn an_idempotent_batch_is_never_merged() -> Result<()> {
            let at = Leading::new().await?;
            let before = at.counts();
            let (a, first) = at.job_of(1);
            let (b, second) = at.job_of(1);
            let (idem, sequenced) = idempotent(&at.handle, &at.orders, at.term, 0, b"once");
            let (c, third) = at.job_of(1);
            let (d, fourth) = at.job_of(1);
            for job in [a, b, idem, c, d] {
                at.publish_ctx.scheduler.send(job).await;
            }
            let first = offset(first.await);
            assert_eq!(offset(second.await), first + 1);
            assert_eq!(offset(sequenced.await), first + 2);
            assert_eq!(offset(third.await), first + 3);
            assert_eq!(offset(fourth.await), first + 4);
            assert_eq!(at.counts().0 - before.0, 3, "plain, idempotent, plain");
            let again = at
                .send(0, &[b"once"], felix_broker::SequenceReuse::AnswerDuplicate)
                .await?;
            assert_eq!(again, Some(first + 2));
            at.stop().await;
            Ok(())
        }

        /// **A merged batch on the leader's log that cannot commit leaves
        /// every member unknown**: never acknowledged, never told nothing
        /// was written. The followers are sent no entries but still hear
        /// heartbeats, so the leader keeps its lease and writes the batch,
        /// and no majority can hold it.
        /// Once healed, the batch is in the committed log whole, at
        /// consecutive offsets, or not at all. The merged form of
        /// `shards::raft::tests::a_quorum_stream_is_not_answered_without_a_majority`.
        #[tokio::test]
        async fn a_merged_batch_that_cannot_commit_leaves_every_member_unknown() -> Result<()> {
            let (net, members, leader) = fixture::trio().await;
            let leading = &members[leader];
            let orders = fixture::key("orders", 0);
            let term = leading.shards.leading(&orders).expect("term");
            let config = BrokerConfig {
                pub_workers_per_conn: 1,
                pub_queue_depth: 64,
                publish_quorum_timeout_ms: 500,
                ..BrokerConfig::default()
            };
            let cluster = ClusterContext {
                engine: Arc::clone(&leading.engine),
                ..ClusterContext::default()
            };
            let publish_ctx = build_publish_context(Arc::clone(&leading.broker), &config, cluster);
            let handle = leading
                .broker
                .resolve_stream_handle("t1", "ns", "orders", 0)
                .await?;
            let send = |payload: &'static [u8], publisher: &'static [u8]| {
                let (mut job, answer) = job(&handle, &orders, term, payload, None);
                job.publisher = Some(Bytes::from_static(publisher));
                (job, answer)
            };
            let (before, answer) = send(b"before", b"p0");
            publish_ctx.scheduler.send(before).await;
            let before = offset(answer.await);

            let raft = leading.shards.engine_for(&orders).expect("a member");
            let proposed = raft.proposals(&orders);
            for (at, member) in members.iter().enumerate() {
                if at != leader {
                    net.starved.lock().insert(member.name.to_string());
                }
            }
            let payloads: [&'static [u8]; 4] = [b"m0", b"m1", b"m2", b"m3"];
            let publishers: [&'static [u8]; 4] = [b"p1", b"p2", b"p3", b"p4"];
            let mut answers = Vec::new();
            for (payload, publisher) in payloads.into_iter().zip(publishers) {
                let (job, answer) = send(payload, publisher);
                publish_ctx.scheduler.send(job).await;
                answers.push(answer);
            }
            for answer in answers {
                let err = answer.await?.expect_err("acknowledged without a majority");
                let err = ClientError::from_anyhow(&err);
                assert_eq!(
                    err.retry(),
                    felix_wire::RetryClass::OutcomeUnknown,
                    "a batch on the leader's log was told {err:?}"
                );
            }
            assert_eq!(raft.proposals(&orders) - proposed, 1, "not one proposal");

            net.starved.lock().clear();
            // Its commit commits everything below it. Refused as stalled
            // until the batch settles.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            let after = loop {
                let (after, answer) = send(b"after", b"p5");
                publish_ctx.scheduler.send(after).await;
                match answer.await? {
                    Ok(offset) => break offset.expect("an offset"),
                    Err(err) => {
                        assert!(
                            tokio::time::Instant::now() < deadline,
                            "never published again: {err}"
                        );
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            };
            let read = leading
                .broker
                .read_durable("t1", "ns", "orders", 0, before + 1, 1 << 20)
                .await?;
            let read: Vec<_> = read.into_iter().map(|record| record.payload).collect();
            match read.len() {
                5 => {
                    assert_eq!(read, ["m0", "m1", "m2", "m3", "after"]);
                    assert_eq!(after, before + 5);
                }
                1 => assert_eq!(read, ["after"]),
                _ => panic!("the merged batch was cut: {read:?}"),
            }
            fixture::stop(&members).await;
            Ok(())
        }

        /// Give `job` a client's ack reserved on `acks`, to be armed once
        /// the job is queued.
        fn acked_by(
            acks: &crate::serving::quic::handlers::publish::CommitAcks,
            job: &mut PublishJob,
            request_id: u64,
        ) -> crate::serving::quic::handlers::publish::commit_ack::PendingAck {
            use crate::serving::quic::handlers::publish::ack::AckEncoding;
            use crate::serving::quic::handlers::publish::commit_ack::AckRequest;
            let request = AckRequest {
                request_id,
                encoding: AckEncoding::Binary,
                payload_bytes: 2,
                forwarded_to: None,
                start: crate::serving::quic::telemetry::t_instant_now(),
                single: false,
            };
            let (reply, pending) = acks.reserve(request).expect("room");
            job.response = Some(crate::serving::quic::handlers::publish::PublishReply::Ack(
                reply,
            ));
            pending
        }

        /// The next ack on `out`: its request and offset, or its error's
        /// code and retry class.
        async fn next_ack(
            out: &mut tokio::sync::mpsc::Receiver<
                crate::serving::quic::handlers::publish::ack::Outgoing,
            >,
        ) -> (
            u64,
            Result<Option<u64>, (felix_wire::ErrorCode, felix_wire::RetryClass)>,
        ) {
            use crate::serving::quic::handlers::publish::ack::Outgoing;
            match tokio::time::timeout(Duration::from_secs(10), out.recv())
                .await
                .expect("answered in time")
            {
                Some(Outgoing::PublishAck {
                    request_id,
                    error: None,
                    offset,
                    ..
                }) => (request_id, Ok(offset)),
                Some(Outgoing::PublishAck {
                    request_id,
                    code: Some(code),
                    ..
                }) => (request_id, Err(code)),
                other => panic!("expected a binary ack, got {other:?}"),
            }
        }

        /// **A merged proposal with a member its control stream cannot time
        /// out keeps its engine deadline, and no member gets a second
        /// bound**: one timer for the proposal and nothing in the ack
        /// sweep, each member still answered with its own offset.
        #[tokio::test]
        async fn a_mixed_merged_proposal_keeps_one_bound_per_member() -> Result<()> {
            let at = Leading::new().await?;
            let (out_tx, mut out_rx) = tokio::sync::mpsc::channel(8);
            let acks = crate::serving::quic::handlers::publish::CommitAcks::for_test(
                &out_tx,
                Arc::new(tokio::sync::Semaphore::new(8)),
            );
            let before = at.counts();
            let armed = felix_broker::timer_probe::armed();
            let (mut first, _) = at.job_of(1);
            let first_pending = acked_by(&acks, &mut first, 1);
            let (middle, middle_answer) = at.job_of(1);
            let (mut last, _) = at.job_of(1);
            let last_pending = acked_by(&acks, &mut last, 3);
            for job in [first, middle, last] {
                at.publish_ctx.scheduler.send(job).await;
            }
            first_pending.arm();
            last_pending.arm();

            let middle = offset(middle_answer.await);
            let mut acked = vec![next_ack(&mut out_rx).await, next_ack(&mut out_rx).await];
            acked.sort_by_key(|(request, _)| *request);
            assert_eq!(
                acked,
                [(1, Ok(Some(middle - 1))), (3, Ok(Some(middle + 1)))]
            );
            assert_eq!(at.counts().0 - before.0, 1, "not one proposal");
            assert_eq!(
                felix_broker::timer_probe::armed() - armed,
                1,
                "the proposal's engine deadline"
            );
            assert_eq!(acks.commit_deadlines(), 0, "members bounded twice");
            at.stop().await;
            Ok(())
        }

        /// **A merged proposal whose every member its control stream can
        /// time out arms no engine timer**: each member's deadline is in
        /// the ack sweep, once, and each is answered with its own offset.
        #[tokio::test]
        async fn an_all_bounded_merged_proposal_drops_the_engine_deadline() -> Result<()> {
            let at = Leading::new().await?;
            let (out_tx, mut out_rx) = tokio::sync::mpsc::channel(8);
            let acks = crate::serving::quic::handlers::publish::CommitAcks::for_test(
                &out_tx,
                Arc::new(tokio::sync::Semaphore::new(8)),
            );
            let before = at.counts();
            let armed = felix_broker::timer_probe::armed();
            let mut pending = Vec::new();
            for request in 1..=3 {
                let (mut job, _) = at.job_of(1);
                pending.push(acked_by(&acks, &mut job, request));
                at.publish_ctx.scheduler.send(job).await;
            }
            for pending in pending {
                pending.arm();
            }

            let mut acked = Vec::new();
            for _ in 0..3 {
                acked.push(next_ack(&mut out_rx).await);
            }
            acked.sort_by_key(|(request, _)| *request);
            let first = acked[0]
                .1
                .clone()
                .expect("acknowledged")
                .expect("an offset");
            assert_eq!(
                acked,
                [
                    (1, Ok(Some(first))),
                    (2, Ok(Some(first + 1))),
                    (3, Ok(Some(first + 2)))
                ]
            );
            assert_eq!(at.counts().0 - before.0, 1, "not one proposal");
            assert_eq!(
                felix_broker::timer_probe::armed() - armed,
                0,
                "timers armed"
            );
            assert_eq!(acks.commit_deadlines(), 3, "deadlines in the ack sweep");
            at.stop().await;
            Ok(())
        }

        /// **When a merged proposal's members time out in their control
        /// streams' sweeps, each is answered once, outcome unknown, and
        /// none is left waiting**, across two connections. The late commit
        /// answers none of them again. Starved as in
        /// `a_merged_batch_that_cannot_commit_leaves_every_member_unknown`.
        #[tokio::test]
        async fn merged_members_timed_out_by_their_sweeps_are_each_answered_once() -> Result<()> {
            use crate::serving::quic::handlers::publish::CommitAcks;

            let (net, members, leader) = fixture::trio().await;
            let leading = &members[leader];
            let orders = fixture::key("orders", 0);
            let term = leading.shards.leading(&orders).expect("term");
            let config = BrokerConfig {
                pub_workers_per_conn: 1,
                pub_queue_depth: 64,
                publish_quorum_timeout_ms: 500,
                ..BrokerConfig::default()
            };
            let cluster = ClusterContext {
                engine: Arc::clone(&leading.engine),
                ..ClusterContext::default()
            };
            let publish_ctx = build_publish_context(Arc::clone(&leading.broker), &config, cluster);
            let handle = leading
                .broker
                .resolve_stream_handle("t1", "ns", "orders", 0)
                .await?;
            let (before, answer) = job(&handle, &orders, term, b"before", None);
            publish_ctx.scheduler.send(before).await;
            let before = offset(answer.await);

            let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
            let mut outs = Vec::new();
            let mut conns = Vec::new();
            for _ in 0..2 {
                let (out_tx, out_rx) = tokio::sync::mpsc::channel(8);
                let acks = CommitAcks::for_test(&out_tx, Arc::new(tokio::sync::Semaphore::new(8)));
                tokio::spawn(acks.clone().run_deadlines(cancel_rx.clone()));
                outs.push((out_tx, out_rx));
                conns.push(acks);
            }
            let raft = leading.shards.engine_for(&orders).expect("a member");
            let proposed = raft.proposals(&orders);
            for (at, member) in members.iter().enumerate() {
                if at != leader {
                    net.starved.lock().insert(member.name.to_string());
                }
            }
            let armed = felix_broker::timer_probe::armed();
            let payloads: [&'static [u8]; 3] = [b"m0", b"m1", b"m2"];
            let mut pending = Vec::new();
            for (request, payload) in (1..).zip(payloads) {
                let (mut job, _) = job(&handle, &orders, term, payload, None);
                // Members 1 and 3 share a connection; 2 has its own.
                let conn = usize::from(request == 2);
                pending.push(acked_by(&conns[conn], &mut job, request));
                publish_ctx.scheduler.send(job).await;
            }
            for pending in pending {
                pending.arm();
            }
            let mut answered = Vec::new();
            for _ in 0..2 {
                answered.push(next_ack(&mut outs[0].1).await);
            }
            answered.push(next_ack(&mut outs[1].1).await);
            answered.sort_by_key(|(request, _)| *request);
            // The quorum wait's error, not the longer ack timeout's.
            let unknown = || {
                Err((
                    felix_wire::ErrorCode::QuorumTimeout,
                    felix_wire::RetryClass::OutcomeUnknown,
                ))
            };
            assert_eq!(answered, [(1, unknown()), (2, unknown()), (3, unknown())]);
            assert_eq!(raft.proposals(&orders) - proposed, 1, "not one proposal");
            // The one is the proposal's wait for a majority to have written
            // it; an engine deadline would be a second.
            assert_eq!(
                felix_broker::timer_probe::armed() - armed,
                1,
                "timers armed"
            );

            net.starved.lock().clear();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                let (after, answer) = job(&handle, &orders, term, b"after", None);
                publish_ctx.scheduler.send(after).await;
                match answer.await? {
                    Ok(_) => break,
                    Err(err) => {
                        assert!(
                            tokio::time::Instant::now() < deadline,
                            "never published again: {err}"
                        );
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
            let read = leading
                .broker
                .read_durable("t1", "ns", "orders", 0, before + 1, 1 << 20)
                .await?;
            let read: Vec<_> = read.into_iter().map(|record| record.payload).collect();
            assert!(
                read == ["m0", "m1", "m2", "after"] || read == ["after"],
                "the merged batch was cut: {read:?}"
            );
            for (_, out_rx) in &mut outs {
                assert!(out_rx.try_recv().is_err(), "a member answered twice");
            }
            cancel_tx.send(true).ok();
            fixture::stop(&members).await;
            Ok(())
        }

        /// **A merged batch whose outcome is unknown leaves every member
        /// unknown, and one refused before it was appended leaves every
        /// member failed**, each classified as it would be alone.
        #[test]
        fn a_shared_raft_error_keeps_its_class_for_every_member() {
            use felix_replication::engine::CommitError;
            let cases = [
                CommitError::LeadershipLost {
                    what: "write",
                    detail: "the entry was not reported written",
                },
                CommitError::NotAppended {
                    what: "write",
                    detail: "no majority has answered this leader lately",
                },
            ];
            for error in cases {
                let alone = ClientError::from_anyhow(&error.clone().into());
                let mut answers = Vec::new();
                let mut members = Vec::new();
                for _ in 0..3 {
                    let (tx, rx) = oneshot::channel();
                    members.push(GroupMember {
                        response: Some(tx.into()),
                        acked_on_enqueue: false,
                        records: 1,
                    });
                    answers.push(rx);
                }
                settle_group(members, None, Err(error.clone().into()));
                for mut answer in answers {
                    let err = answer
                        .try_recv()
                        .expect("answered")
                        .expect_err("acknowledged");
                    let err = ClientError::from_anyhow(&err);
                    assert_eq!(err.code(), alone.code(), "{error:?}");
                    assert_eq!(err.retry(), alone.retry(), "{error:?}");
                }
            }
        }
    }
}
