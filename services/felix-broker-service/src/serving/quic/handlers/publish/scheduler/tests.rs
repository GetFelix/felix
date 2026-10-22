use std::collections::HashMap;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use felix_broker::Broker;
use felix_storage::EphemeralCache;
use metrics::{
    Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    with_local_recorder,
};
use std::sync::atomic::{AtomicUsize, Ordering};
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
        response: response.map(Into::into),
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

fn tenant_of(job: &PublishJob) -> String {
    match &job.target {
        PublishTarget::Named { tenant_id, .. } => tenant_id.clone(),
        _ => unreachable!("tests queue named jobs"),
    }
}

/// A submitter parked on `scheduler` as `tenant`, and how often it was polled.
/// A woken task is polled again, so the count is the number of wake-ups.
fn parked_submitter(
    scheduler: &Arc<PublishScheduler>,
    tenant: &str,
) -> (
    tokio::task::JoinHandle<Result<(), Rejected>>,
    Arc<AtomicUsize>,
) {
    let polls = Arc::new(AtomicUsize::new(0));
    let handle = tokio::spawn({
        let scheduler = Arc::clone(scheduler);
        let polls = Arc::clone(&polls);
        let tenant = tenant.to_owned();
        async move {
            let submit = scheduler.submit(&tenant, named_job(&tenant), std::future::pending());
            tokio::pin!(submit);
            futures::future::poll_fn(|cx| {
                polls.fetch_add(1, Ordering::SeqCst);
                std::future::Future::poll(submit.as_mut(), cx)
            })
            .await
        }
    });
    (handle, polls)
}

async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

fn admit_wakes(scheduler: &PublishScheduler) -> usize {
    scheduler.partitions()[0]
        .admit_wakes
        .load(Ordering::Relaxed)
}

/// Freeing one slot wakes one submitter, however many are waiting.
#[tokio::test]
async fn a_freed_slot_wakes_exactly_one_waiting_submitter() {
    const WAITING: usize = 16;
    let (scheduler, tx, mut rx) = test_channel(2);
    tx.try_send(named_job("filler")).expect("room");
    tx.try_send(named_job("filler")).expect("room");
    let waiters: Vec<_> = (0..WAITING)
        .map(|n| parked_submitter(&scheduler, &format!("w{n}")))
        .collect();
    settle().await;
    let before: Vec<usize> = waiters
        .iter()
        .map(|(_, polls)| polls.load(Ordering::SeqCst))
        .collect();

    rx.try_recv().expect("a queued job");
    settle().await;

    let woken = waiters
        .iter()
        .zip(&before)
        .filter(|((_, polls), before)| polls.load(Ordering::SeqCst) != **before)
        .count();
    assert_eq!(woken, 1, "one freed slot woke {woken} submitters");
    assert_eq!(admit_wakes(&scheduler), 1);
    assert_eq!(waiters.iter().filter(|(h, _)| h.is_finished()).count(), 1);
}

/// Waiting submitters get in in the order they arrived.
#[tokio::test]
async fn waiting_submitters_are_admitted_in_arrival_order() {
    const WAITING: usize = 8;
    let (scheduler, tx, mut rx) = test_channel(2);
    tx.try_send(named_job("filler")).expect("room");
    tx.try_send(named_job("filler")).expect("room");
    let mut waiters = Vec::new();
    for n in 0..WAITING {
        waiters.push(parked_submitter(&scheduler, &format!("w{n}")));
        settle().await;
    }
    // The fair queue decides how fillers and waiters interleave on the way
    // out; the waiters among them must come out in the order they arrived.
    let mut order = Vec::new();
    for _ in 0..WAITING + 2 {
        order.push(tenant_of(&rx.try_recv().expect("queued")));
        settle().await;
    }
    order.retain(|tenant| tenant != "filler");
    let expected: Vec<_> = (0..WAITING).map(|n| format!("w{n}")).collect();
    assert_eq!(order, expected);
}

/// A waiter that goes away, by being dropped or by giving up, does not hold
/// a slot or swallow the wake meant for the next one.
#[tokio::test]
async fn a_departed_waiter_does_not_strand_the_next() {
    let (scheduler, tx, mut rx) = test_channel(1);
    tx.try_send(named_job("filler")).expect("room");
    let (first, _) = parked_submitter(&scheduler, "w0");
    settle().await;
    let (second, _) = parked_submitter(&scheduler, "w1");
    settle().await;
    let (give_up, stop) = oneshot::channel::<()>();
    let leaving = tokio::spawn({
        let scheduler = Arc::clone(&scheduler);
        async move {
            scheduler
                .submit("w2", named_job("w2"), async {
                    let _ = stop.await;
                })
                .await
        }
    });
    settle().await;
    let (third, _) = parked_submitter(&scheduler, "w3");
    settle().await;

    first.abort();
    let _ = first.await;
    give_up.send(()).expect("still waiting");
    match leaving.await.expect("task") {
        Err(Rejected::Full(job)) => assert_eq!(tenant_of(&job), "w2"),
        _ => panic!("a submitter that gave up gets its job back"),
    }

    rx.try_recv().expect("the filler");
    settle().await;
    assert!(second.is_finished(), "the next waiter was not admitted");
    assert!(!third.is_finished());
    assert_eq!(tenant_of(&rx.try_recv().expect("w1")), "w1");
    settle().await;
    assert!(third.is_finished());
    assert_eq!(tenant_of(&rx.try_recv().expect("w3")), "w3");
    assert!(rx.try_recv().is_err(), "the aborted waiter's job leaked in");
}

/// Closing the queue wakes every waiter with `Closed`.
#[tokio::test]
async fn closing_wakes_every_waiting_submitter() {
    let (scheduler, tx, _rx) = test_channel(1);
    tx.try_send(named_job("filler")).expect("room");
    let waiters: Vec<_> = (0..5)
        .map(|n| parked_submitter(&scheduler, &format!("w{n}")))
        .collect();
    settle().await;
    scheduler.close();
    for (waiter, _) in waiters {
        assert!(matches!(waiter.await.expect("task"), Err(Rejected::Closed)));
    }
}

/// A slot goes to a tenant that may take it, not to an older waiter of a
/// tenant already at its share.
#[tokio::test]
async fn a_slot_skips_a_waiter_whose_tenant_is_at_its_share() {
    let scheduler = Arc::new(PublishScheduler::new(1, 4, 2));
    let ready = || std::future::ready(());
    for tenant in ["a", "a", "b", "b"] {
        assert!(
            scheduler
                .submit(tenant, named_job(tenant), ready())
                .await
                .is_ok()
        );
    }
    let (a_waiter, _) = parked_submitter(&scheduler, "a");
    settle().await;
    let (b_waiter, _) = parked_submitter(&scheduler, "b");
    settle().await;

    // Whichever tenant frees a slot, only a waiter of that tenant may take
    // it: the other is at its share with nothing left over to borrow.
    let freed = tenant_of(&scheduler.partitions()[0].try_take().expect("queued"));
    settle().await;
    let (took, left) = if freed == "a" {
        (&a_waiter, &b_waiter)
    } else {
        (&b_waiter, &a_waiter)
    };
    assert!(
        took.is_finished(),
        "the freed tenant's waiter should get in"
    );
    assert!(!left.is_finished());
}

fn tagged_job(tenant: &str, tag: u8) -> PublishJob {
    let mut job = named_job(tenant);
    job.payloads = vec![Bytes::from(vec![tag])];
    job
}

/// A batch taken off a busy lane frees several slots at once: each goes to
/// a waiter, and each admitted job gets an idle executor.
#[tokio::test]
async fn a_batch_taken_from_a_lane_admits_waiters_and_wakes_executors() {
    let scheduler = Arc::new(PublishScheduler::new(1, 4, 4));
    let partition = Arc::clone(&scheduler.partitions()[0]);
    let executors: Vec<_> = (0..5)
        .map(|_| {
            let partition = Arc::clone(&partition);
            tokio::spawn(async move { partition.next().await })
        })
        .collect();
    settle().await;
    let send = |tag: u8| {
        let scheduler = Arc::clone(&scheduler);
        async move {
            let sent = scheduler
                .submit("f", tagged_job("f", tag), std::future::ready(()))
                .await;
            assert!(sent.is_ok());
        }
    };
    for tag in 0..4 {
        send(tag).await;
    }
    // One executor takes the lane's first job and holds the lane; the rest
    // find nothing runnable and sleep.
    settle().await;
    let (done, idle): (Vec<_>, Vec<_>) = executors.into_iter().partition(|e| e.is_finished());
    assert_eq!(done.len(), 1);
    let (_, guard) = done
        .into_iter()
        .next()
        .unwrap()
        .await
        .unwrap()
        .expect("a job");

    send(4).await;
    let waiters: Vec<_> = (0..3)
        .map(|n| parked_submitter(&scheduler, &format!("w{n}")))
        .collect();
    settle().await;
    assert_eq!(admit_wakes(&scheduler), 0, "the queue was full");
    assert!(waiters.iter().all(|(h, _)| !h.is_finished()));

    // Three jobs leave the lane at once: three slots, three waiters.
    let taken = guard.take_more(3, |_, _| true);
    assert_eq!(taken.len(), 3);
    settle().await;
    assert_eq!(admit_wakes(&scheduler), 3);
    assert_eq!(
        idle.iter().filter(|e| e.is_finished()).count(),
        3,
        "every admitted job should reach an idle executor"
    );
}

/// A parked job is queued before a newer submit on the same lane.
#[tokio::test]
async fn a_parked_job_keeps_its_lane_order_against_a_newer_submit() {
    let scheduler = Arc::new(PublishScheduler::new(1, 4, 2));
    let ready = || std::future::ready(());
    for (tenant, tag) in [("a", 0), ("a", 1), ("b", 9), ("b", 9)] {
        let sent = scheduler
            .submit(tenant, tagged_job(tenant, tag), ready())
            .await;
        assert!(sent.is_ok());
    }
    let submit = |tag: u8| {
        let scheduler = Arc::clone(&scheduler);
        tokio::spawn(async move {
            scheduler
                .submit("a", tagged_job("a", tag), std::future::pending())
                .await
        })
    };
    let first = submit(2);
    settle().await;
    // Free a slot from b. Tenant a is still at its share, so this one has
    // room in the queue but may not use it, and must wait behind `first`.
    let partition = &scheduler.partitions()[0];
    let mut seen = Vec::new();
    loop {
        let job = partition.try_take().expect("queued");
        let b = tenant_of(&job) == "b";
        seen.push(job);
        if b {
            break;
        }
    }
    let second = submit(3);
    settle().await;
    while let Some(job) = partition.try_take() {
        seen.push(job);
        settle().await;
    }
    assert!(first.is_finished() && second.is_finished());
    let tags: Vec<u8> = seen
        .iter()
        .filter(|job| tenant_of(job) == "a")
        .map(|job| job.payloads[0][0])
        .collect();
    assert_eq!(tags, vec![0, 1, 2, 3]);
}

/// A submitter dropped after its slot was handed over, before it ran again:
/// the job stays queued and the next waiter still gets in.
#[tokio::test]
async fn a_submitter_dropped_after_admission_leaves_its_job_and_frees_the_next() {
    let (scheduler, tx, mut rx) = test_channel(1);
    tx.try_send(named_job("filler")).expect("room");
    let (first, _) = parked_submitter(&scheduler, "w0");
    let (second, _) = parked_submitter(&scheduler, "w1");
    settle().await;

    rx.try_recv().expect("the filler");
    // No yield in between: w0 has its slot but has not run.
    first.abort();
    settle().await;
    assert_eq!(tenant_of(&rx.try_recv().expect("w0's job")), "w0");
    settle().await;
    assert!(second.is_finished(), "the next waiter was stranded");
    assert_eq!(tenant_of(&rx.try_recv().expect("w1's job")), "w1");
    assert!(rx.try_recv().is_err());
}

/// Admitting one tenant does not walk another tenant's parked jobs.
#[tokio::test]
async fn a_refused_tenants_parked_jobs_are_not_scanned() {
    const PARKED: usize = 200;
    let scheduler = Arc::new(PublishScheduler::new(1, 4, 2));
    let ready = || std::future::ready(());
    for tenant in ["a", "a", "b", "b"] {
        assert!(
            scheduler
                .submit(tenant, named_job(tenant), ready())
                .await
                .is_ok()
        );
    }
    let mut a_waiters = Vec::new();
    for _ in 0..PARKED {
        a_waiters.push(parked_submitter(&scheduler, "a"));
    }
    settle().await;
    let (b_waiter, _) = parked_submitter(&scheduler, "b");
    settle().await;

    let partition = &scheduler.partitions()[0];
    let scanned_before = partition.state.lock().scanned;
    // Pop until a b job leaves; a's waiters are refused each time a b job does.
    loop {
        let job = partition.try_take().expect("queued");
        if tenant_of(&job) == "b" {
            break;
        }
    }
    settle().await;
    let scanned = partition.state.lock().scanned - scanned_before;
    assert!(b_waiter.is_finished());
    assert!(
        scanned < 10,
        "admitting looked at {scanned} tenants or jobs"
    );
}

/// Many submitters and a slow drain: every job gets through, none is lost
/// to a wake-up that went to the wrong place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_submitter_is_lost_under_contention() {
    const SUBMITTERS: usize = 64;
    const EACH: usize = 100;
    let (scheduler, _tx, mut rx) = test_channel(4);
    let mut tasks = Vec::new();
    for n in 0..SUBMITTERS {
        let scheduler = Arc::clone(&scheduler);
        tasks.push(tokio::spawn(async move {
            let tenant = format!("t{}", n % 5);
            for _ in 0..EACH {
                scheduler.send_as(&tenant, named_job(&tenant)).await;
            }
        }));
    }
    let drain = tokio::spawn(async move {
        let mut got = 0;
        while got < SUBMITTERS * EACH {
            rx.recv().await.expect("open");
            got += 1;
            if got % 7 == 0 {
                tokio::task::yield_now().await;
            }
        }
        got
    });
    let total = tokio::time::timeout(Duration::from_secs(60), drain)
        .await
        .expect("a submitter was never woken")
        .expect("drain");
    assert_eq!(total, SUBMITTERS * EACH);
    for task in tasks {
        task.await.expect("submitter");
    }
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
