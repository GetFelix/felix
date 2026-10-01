//! A publish acknowledged on enqueue and a lease that lapses before the worker
//! writes it. The worker must refuse the write -- another broker may lead the
//! shard by then -- so the only safe place to protect the client is admission:
//! when the lease is too close to running out, wait for the write instead of
//! acknowledging the enqueue. And when a pause outlasts even that, the loss has
//! to show up somewhere.
//!
//! Time is paused, and the lease's refresh task never runs, so `looks_valid`
//! stays true while the clock says the lease is gone: a broker suspended
//! across its expiry.

use metrics_exporter_prometheus::PrometheusBuilder;

use crate::cluster::lease::LeaseState;
use crate::config::BrokerConfig;
use crate::serving::quic::ClusterContext;
use crate::test_support::leader::{DURABLE, Leader, NAMESPACE, TENANT};

use super::*;

/// Usable for three seconds once renewed.
const LEASE: Duration = Duration::from_secs(4);
const HEADROOM: Duration = Duration::from_secs(1);

struct Harness {
    leader: Leader,
    lease: Arc<LeaseState>,
    queued: PublishContext,
    rx: TestReceiver,
    worker: PublishContext,
}

impl Harness {
    async fn start() -> Self {
        let leader = Leader::start().await;
        let lease = Arc::new(LeaseState::on_tokio_clock(LEASE));
        lease.renew();
        leader.ingress.fence().bind_lease(Arc::clone(&lease));
        let (mut queued, rx, _tx) = make_publish_context(8);
        queued.ingress = Some(Arc::clone(&leader.ingress));
        queued.lease = Some(Arc::clone(&lease));
        queued.lease_headroom = HEADROOM;
        let worker = build_publish_context(
            Arc::clone(&leader.broker),
            &BrokerConfig::default(),
            ClusterContext {
                ingress: Some(Arc::clone(&leader.ingress)),
                lease: Some(Arc::clone(&lease)),
                ..ClusterContext::default()
            },
        );
        Self {
            leader,
            lease,
            queued,
            rx,
            worker,
        }
    }

    /// Let the lease lapse on the clock, then give the queued job to a worker.
    async fn lapse_then_release(&mut self) {
        tokio::time::advance(LEASE).await;
        assert!(self.lease.looks_valid(), "the cached flag is meant to lag");
        assert!(!self.lease.is_valid_now());
        let job = self.rx.recv().await.expect("queued job");
        self.worker.scheduler.send(job).await;
    }
}

struct Io {
    out_tx: mpsc::Sender<Outgoing>,
    out_rx: mpsc::Receiver<Outgoing>,
    throttle_tx: watch::Sender<bool>,
    cancel_tx: watch::Sender<bool>,
    ack_waiter_tx: mpsc::Sender<AckWaiterMessage>,
    ack_waiter_rx: mpsc::Receiver<AckWaiterMessage>,
}

impl Io {
    fn new() -> Self {
        let (out_tx, out_rx) = mpsc::channel(4);
        let (throttle_tx, _) = watch::channel(false);
        let (cancel_tx, _) = watch::channel(false);
        let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(4);
        Self {
            out_tx,
            out_rx,
            throttle_tx,
            cancel_tx,
            ack_waiter_tx,
            ack_waiter_rx,
        }
    }
}

async fn publish_one(h: &Harness, io: &Io) {
    handle_publish_message(
        &h.leader.broker,
        &h.queued,
        &mut HashMap::new(),
        &mut String::new(),
        false,
        // Acked on enqueue: the default.
        false,
        &io.out_tx,
        &Arc::new(AtomicUsize::new(0)),
        &io.throttle_tx,
        &Arc::new(Mutex::new(AckTimeoutState::new(Instant::now()))),
        &io.cancel_tx,
        &Arc::new(Semaphore::new(1)),
        &io.ack_waiter_tx,
        Duration::from_secs(1),
        TENANT.to_string(),
        NAMESPACE.to_string(),
        DURABLE.to_string(),
        b"late".to_vec(),
        None,
        Some(7),
        Some(felix_wire::AckMode::PerMessage),
        false,
        String::new(),
    )
    .await
    .expect("publish handled");
}

async fn publish_batch(h: &Harness, io: &Io) {
    handle_publish_batch_message(
        0,
        &h.leader.broker,
        &h.queued,
        &mut HashMap::new(),
        &mut String::new(),
        false,
        false,
        AckEncoding::Binary,
        &io.out_tx,
        &Arc::new(AtomicUsize::new(0)),
        &io.throttle_tx,
        &Arc::new(Mutex::new(AckTimeoutState::new(Instant::now()))),
        &io.cancel_tx,
        &Arc::new(Semaphore::new(1)),
        &io.ack_waiter_tx,
        TENANT.to_string(),
        NAMESPACE.to_string(),
        DURABLE.to_string(),
        vec![
            bytes::Bytes::from_static(b"one"),
            bytes::Bytes::from_static(b"two"),
        ],
        None,
        Some(9),
        Some(felix_wire::AckMode::PerBatch),
        false,
        String::new(),
        None,
    )
    .await
    .expect("batch handled");
}

fn assert_refused_as_fenced(result: anyhow::Result<Option<u64>>) {
    let err = result.expect_err("a write after the lease lapsed must be refused");
    assert_eq!(
        *crate::serving::quic::client_error::ClientError::from_anyhow(&err).code(),
        felix_wire::ErrorCode::ShardUnavailable,
        "{err:#}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_publish_admitted_near_lease_expiry_waits_for_the_write() {
    let mut h = Harness::start().await;
    let mut io = Io::new();
    // Half a second of lease left: less than the headroom, and still valid.
    tokio::time::advance(LEASE - LEASE / 4 - Duration::from_millis(500)).await;
    assert!(h.lease.is_valid_now());

    publish_one(&h, &io).await;
    assert!(
        io.out_rx.try_recv().is_err(),
        "acknowledged on enqueue with less lease left than the headroom"
    );
    let Some(AckWaiterMessage::Publish { response_rx, .. }) = io.ack_waiter_rx.recv().await else {
        panic!("expected the publish to wait for its commit");
    };

    h.lapse_then_release().await;
    assert_refused_as_fenced(response_rx.await.expect("the worker answers"));
    assert_eq!(h.leader.tail(DURABLE).await, 0);
}

#[tokio::test(start_paused = true)]
async fn a_batch_admitted_near_lease_expiry_waits_for_the_write() {
    let mut h = Harness::start().await;
    let mut io = Io::new();
    tokio::time::advance(LEASE - LEASE / 4 - Duration::from_millis(500)).await;
    assert!(h.lease.is_valid_now());

    publish_batch(&h, &io).await;
    assert!(
        io.out_rx.try_recv().is_err(),
        "acknowledged on enqueue with less lease left than the headroom"
    );
    let Some(AckWaiterMessage::PublishBatch { response_rx, .. }) = io.ack_waiter_rx.recv().await
    else {
        panic!("expected the batch to wait for its commit");
    };

    h.lapse_then_release().await;
    assert_refused_as_fenced(response_rx.await.expect("the worker answers"));
    assert_eq!(h.leader.tail(DURABLE).await, 0);
}

/// With the full lease left the publish is acknowledged on enqueue, and a
/// pause longer than the lease strands it. Nothing can be written, so the
/// loss is counted.
#[tokio::test(start_paused = true)]
async fn an_acked_publish_the_lease_strands_is_counted() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    // Current-thread runtime: the worker runs on this thread and records here.
    let _local = metrics::set_default_local_recorder(&recorder);

    let mut h = Harness::start().await;
    let mut io = Io::new();
    publish_one(&h, &io).await;
    match io.out_rx.recv().await.expect("ack") {
        Outgoing::Message(Message::PublishOk {
            request_id: 7,
            offset: None,
        }) => {}
        other => panic!("expected an ack on enqueue, got {other:?}"),
    }

    h.lapse_then_release().await;
    let dropped = r#"felix_broker_acked_publishes_dropped_total{reason="fenced"} 1"#;
    for _ in 0..100 {
        if handle.render().contains(dropped) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        handle.render().contains(dropped),
        "the dropped acked publish was not counted:\n{}",
        handle.render()
    );
    assert_eq!(h.leader.tail(DURABLE).await, 0);
}
