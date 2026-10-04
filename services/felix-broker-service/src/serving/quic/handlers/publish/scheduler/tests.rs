use std::collections::HashMap;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use felix_broker::Broker;
use felix_storage::EphemeralCache;
use metrics::{
    Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    with_local_recorder,
};
use tokio::sync::oneshot;

use super::*;
use crate::config::BrokerConfig;
use crate::observability::tenants;
use crate::serving::quic::ClusterContext;
use crate::serving::quic::client_error::{ClientError, QUEUE_FULL_REASON};
use crate::serving::quic::handlers::publish::ack::EnqueuePolicy;
use crate::serving::quic::handlers::publish::build_publish_context;
use crate::serving::quic::handlers::publish::ingress::enqueue_publish;

async fn broker_with(streams: &[&str]) -> Arc<Broker> {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    for stream in streams {
        broker
            .register_stream("t1", "ns", *stream, Default::default())
            .await
            .expect("stream");
    }
    Arc::new(broker)
}

fn local_job(
    handle: &felix_broker::StreamHandle,
    payload: Bytes,
    response: Option<oneshot::Sender<anyhow::Result<Option<u64>>>>,
) -> PublishJob {
    PublishJob {
        target: PublishTarget::Resolved {
            handle: handle.clone(),
            shard: None,
            generation: 0,
            fenced: None,
        },
        payloads: vec![payload],
        response,
        acked_on_enqueue: false,
        admission_permit: None,
        fenced: None,
        publisher: None,
    }
}

fn named_job(tenant: &str) -> PublishJob {
    PublishJob {
        target: PublishTarget::Named {
            tenant_id: tenant.to_string(),
            namespace: "ns".to_string(),
            stream: "stream".to_string(),
        },
        payloads: vec![Bytes::from_static(b"payload")],
        response: None,
        acked_on_enqueue: false,
        admission_permit: None,
        fenced: None,
        publisher: None,
    }
}

fn assert_busy(err: &anyhow::Error) {
    let busy = ClientError::from_anyhow(err);
    assert_eq!(busy.code(), &felix_wire::ErrorCode::Overloaded);
    assert_eq!(busy.retry(), felix_wire::RetryClass::RetryAfter);
    let detail = busy.detail().expect("a busy answer says why");
    assert_eq!(detail.reason.as_deref(), Some(QUEUE_FULL_REASON));
    assert!(detail.retry_after_ms.is_some(), "a busy answer says when");
}

/// Publishes to one shard are answered in the order they were sent, and what
/// lands is exactly what was accepted, in that order, while the queue keeps
/// filling up and refusing publishes in between.
#[tokio::test]
async fn one_shards_publishes_are_answered_in_order_across_a_full_queue() {
    const PUBLISHES: u32 = 60;
    let broker = broker_with(&["orders"]).await;
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let mut subscription = broker
        .subscribe("t1", "ns", "orders", 0)
        .await
        .expect("subscribe");
    let config = BrokerConfig {
        pub_workers_per_conn: 1,
        pub_queue_depth: 3,
        ..BrokerConfig::default()
    };
    let ctx = build_publish_context(Arc::clone(&broker), &config, ClusterContext::default());

    let answered = Arc::new(StdMutex::new(Vec::new()));
    let mut waiters = Vec::new();
    let mut accepted = Vec::new();
    let mut refused = 0;
    for n in 0..PUBLISHES {
        let (response, answer) = oneshot::channel();
        let job = local_job(
            &handle,
            Bytes::from(n.to_be_bytes().to_vec()),
            Some(response),
        );
        match enqueue_publish(&ctx, "t1", job, EnqueuePolicy::Fail, None).await {
            Ok(true) => {
                accepted.push(n);
                let answered = Arc::clone(&answered);
                // One task per answer, woken in the order the answers are
                // sent: this runtime runs woken tasks first come first served.
                waiters.push(tokio::spawn(async move {
                    answer.await.expect("answered").expect("written");
                    answered.lock().expect("answers").push(n);
                }));
            }
            Ok(false) => panic!("an acked publish was dropped"),
            Err(err) => {
                assert_busy(&err);
                refused += 1;
            }
        }
        // Let the executor drain now and then, so the queue fills and
        // empties over and over.
        if n % 7 == 6 {
            tokio::task::yield_now().await;
        }
    }
    for waiter in waiters {
        waiter.await.expect("waiter");
    }
    assert!(refused > 0, "the queue never filled");
    assert!(accepted.len() > 3, "the queue never drained");
    assert_eq!(
        *answered.lock().expect("answers"),
        accepted,
        "acks for one shard came back out of order"
    );
    let mut landed = Vec::new();
    while let Ok(payload) = subscription.try_recv() {
        landed.push(u32::from_be_bytes(
            payload[..4].try_into().expect("4 bytes"),
        ));
    }
    assert_eq!(
        landed, accepted,
        "the log does not hold what was acknowledged"
    );
}

/// A tenant that fills its share of the queue is told it is busy, and counted,
/// while another tenant's publishes still go in.
#[test]
fn a_flooding_tenant_is_refused_and_counted_while_a_quiet_one_gets_in() {
    let recorder = Counting::default();
    with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(async {
                // No executors, so nothing leaves the queue: room is 8, with
                // 2 kept back for tenants holding less than their share.
                let mut ctx =
                    super::super::tests::context_with(Arc::new(PublishScheduler::new(1, 8, 2)));
                ctx.wait_timeout = Duration::from_millis(1);
                let mut flooded = 0;
                for _ in 0..20 {
                    match enqueue_publish(
                        &ctx,
                        "flood",
                        named_job("flood"),
                        EnqueuePolicy::Fail,
                        None,
                    )
                    .await
                    {
                        Ok(true) => {}
                        Ok(false) => panic!("an acked publish was dropped"),
                        Err(err) => {
                            assert_busy(&err);
                            flooded += 1;
                        }
                    }
                }
                // Fire-and-forget publishes are shed, and counted too.
                for _ in 0..3 {
                    let queued = enqueue_publish(
                        &ctx,
                        "flood",
                        named_job("flood"),
                        EnqueuePolicy::Drop,
                        None,
                    )
                    .await
                    .expect("a shed publish is not an error");
                    assert!(!queued);
                }
                // A commit-ack publish waits for room, then is told it is busy.
                let err =
                    enqueue_publish(&ctx, "flood", named_job("flood"), EnqueuePolicy::Wait, None)
                        .await
                        .expect_err("there was never room");
                assert_busy(&err);
                assert_eq!(flooded, 14);

                for _ in 0..2 {
                    assert!(
                        enqueue_publish(
                            &ctx,
                            "quiet",
                            named_job("quiet"),
                            EnqueuePolicy::Fail,
                            None
                        )
                        .await
                        .expect("the quiet tenant gets in")
                    );
                }
            });
    });
    assert_eq!(
        recorder.get(tenants::QUEUE_FULL_TOTAL, "flood", "refused"),
        15
    );
    assert_eq!(
        recorder.get(tenants::QUEUE_FULL_TOTAL, "flood", "dropped"),
        3
    );
    assert_eq!(
        recorder.get(tenants::QUEUE_FULL_TOTAL, "quiet", "refused"),
        0
    );
}

/// The queue closes when the last context holding it goes, and executors
/// drain what is queued before they exit.
#[tokio::test]
async fn a_closed_queue_refuses_new_work_and_drains_old() {
    let (scheduler, tx, mut rx) = test_channel(4);
    tx.try_send(named_job("t1")).expect("room");
    scheduler.close();
    assert!(matches!(
        scheduler
            .submit("t1", named_job("t1"), std::future::ready(()))
            .await,
        Err(Rejected::Closed)
    ));
    assert!(rx.recv().await.is_some(), "a queued job was lost on close");
    assert!(rx.recv().await.is_none());
}

/// Counter values by name, `tenant` and `action`.
type Counts = Arc<StdMutex<HashMap<(String, String, String), u64>>>;

/// Sums counters by name, `tenant` and `action`.
#[derive(Default)]
struct Counting {
    counts: Counts,
}

impl Counting {
    fn get(&self, name: &str, tenant: &str, action: &str) -> u64 {
        self.counts
            .lock()
            .expect("counts")
            .get(&(name.to_string(), tenant.to_string(), action.to_string()))
            .copied()
            .unwrap_or(0)
    }
}

impl Recorder for Counting {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        let label = |name: &str| {
            key.labels()
                .find(|label| label.key() == name)
                .map(|label| label.value().to_string())
                .unwrap_or_default()
        };
        Counter::from_arc(Arc::new(Slot {
            key: (key.name().to_string(), label("tenant"), label("action")),
            counts: Arc::clone(&self.counts),
        }))
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

struct Slot {
    key: (String, String, String),
    counts: Counts,
}

impl CounterFn for Slot {
    fn increment(&self, value: u64) {
        *self
            .counts
            .lock()
            .expect("counts")
            .entry(self.key.clone())
            .or_default() += value;
    }

    fn absolute(&self, value: u64) {
        self.counts
            .lock()
            .expect("counts")
            .insert(self.key.clone(), value);
    }
}
