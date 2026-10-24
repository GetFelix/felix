//! A publish waiting for room in a full queue parks off the read loop: the
//! control stream keeps serving its other publishes, and the parked one is
//! still admitted (or answered) later.

use std::sync::Mutex as StdMutex;

use metrics::{
    Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    with_local_recorder,
};

use super::*;
use crate::serving::quic::handlers::publish::{
    ParkedPublishes, PublishAdmission, PublishJob, PublishReply, PublishScheduler,
};

/// A broker with two streams whose lanes fall in different partitions of a
/// two-partition scheduler. Returns the broker and the two stream names,
/// with the partition index of each.
async fn two_partition_broker() -> Result<(Arc<Broker>, (String, usize), (String, usize))> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    let mut by_partition: [Option<String>; 2] = [None, None];
    for i in 0..16 {
        let name = format!("s{i}");
        broker
            .register_stream("t1", "default", &name, Default::default())
            .await?;
        let handle = broker
            .resolve_stream_handle("t1", "default", &name, 0)
            .await?;
        by_partition[(handle.id() % 2) as usize].get_or_insert(name);
        if by_partition.iter().all(Option::is_some) {
            break;
        }
    }
    let [Some(even), Some(odd)] = by_partition else {
        anyhow::bail!("no streams in both partitions");
    };
    Ok((broker, (even, 0), (odd, 1)))
}

/// A job for `stream` on the scheduler, as some other tenant would queue it.
async fn filler(broker: &Broker, stream: &str) -> Result<PublishJob> {
    Ok(PublishJob {
        target: PublishTarget::Resolved {
            handle: broker
                .resolve_stream_handle("t1", "default", stream, 0)
                .await?,
            shard: None,
            generation: 0,
            fenced: None,
        },
        payloads: vec![Bytes::from_static(b"filler")],
        response: None,
        acked_on_enqueue: false,
        admission_permit: None,
        fenced: None,
        publisher: None,
    })
}

fn publish(stream: &str, payload: &[u8], request_id: Option<u64>) -> Frame {
    frame_from_message(Message::Publish {
        shard: None,
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: stream.to_string(),
        payload: payload.to_vec(),
        key: None,
        request_id,
        ack: Some(if request_id.is_some() {
            felix_wire::AckMode::PerMessage
        } else {
            felix_wire::AckMode::None
        }),
    })
}

/// Run one control stream's loop over `frames`, returning its answers and,
/// as `handle_stream` would hold it while parked publishes drain, its cancel
/// signal.
async fn spawn_stream(
    broker: &Arc<Broker>,
    auth: &AuthFixture,
    publish_ctx: PublishContext,
    config: BrokerConfig,
    frames: Vec<Frame>,
) -> Result<(
    tokio::task::JoinHandle<Result<bool>>,
    mpsc::Receiver<Outgoing>,
    watch::Sender<bool>,
)> {
    let connection = super::pipeline::idle_connection().await?;
    let mut source = TestFrameSource::new(frames.into_iter().map(|f| Ok(Some(f))).collect());
    let (out_ack_tx, out_ack_rx) = mpsc::channel(16);
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let held = cancel_tx.clone();
    let commit_acks = CommitAcks::for_test(&out_ack_tx, Arc::new(Semaphore::new(16)));
    let ack_timeout_state = Arc::new(parking_lot::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let broker = Arc::clone(broker);
    let auth = Arc::clone(&auth.auth);
    let control = tokio::spawn(async move {
        let mut scratch = crate::serving::quic::FrameScratch::new();
        run_control_loop(
            &mut source,
            broker,
            connection,
            config,
            auth,
            publish_ctx,
            HashMap::new(),
            String::new(),
            out_ack_tx,
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            ack_throttle_rx,
            ack_throttle_tx,
            ack_timeout_state,
            cancel_tx,
            cancel_rx,
            commit_acks,
            &mut scratch,
            Default::default(),
            Default::default(),
        )
        .await
    });
    Ok((control, out_ack_rx, held))
}

/// The next publish answer, skipping the `Auth` reply.
async fn next_answer(out_ack_rx: &mut mpsc::Receiver<Outgoing>) -> Option<Message> {
    loop {
        match timeout(Duration::from_secs(2), out_ack_rx.recv()).await {
            Ok(Some(Outgoing::Message(Message::Ok))) => continue,
            Ok(Some(Outgoing::Message(message))) => return Some(message),
            Ok(Some(_)) => continue,
            Ok(None) | Err(_) => return None,
        }
    }
}

async fn context(broker: &Arc<Broker>) -> (PublishContext, Arc<PublishScheduler>) {
    let mut publish_ctx = build_publish_context(Arc::clone(broker)).await;
    // One slot per partition, and no executors: a queued job stays queued
    // until the test takes it.
    let scheduler = Arc::new(PublishScheduler::new(2, 1, 1));
    publish_ctx.scheduler = Arc::clone(&scheduler);
    publish_ctx.parked = Some(ParkedPublishes::new(8, 8));
    (publish_ctx, scheduler)
}

/// **A full queue holds up its own publish, not the stream.** An unacked
/// publish under `pub_ingress_wait` finds its shard's queue full. The publish
/// after it on the same control stream, to a stream whose queue has room, is
/// still read and answered; and once the full queue frees a slot, the
/// waiting publish is admitted, not dropped.
#[tokio::test]
async fn a_full_queue_does_not_stop_the_stream_reading() -> Result<()> {
    let (broker, (stuck, stuck_partition), (open, open_partition)) = two_partition_broker().await?;
    let auth = auth_fixture("t1", default_perms());
    let (mut publish_ctx, scheduler) = context(&broker).await;
    publish_ctx.ingress_wait = true;
    let parked = Arc::clone(publish_ctx.parked.as_ref().expect("parked"));
    assert!(
        scheduler
            .submit(
                "other",
                filler(&broker, &stuck).await?,
                std::future::ready(())
            )
            .await
            .is_ok(),
        "the filler was not queued"
    );

    let frames = vec![
        frame_from_message(auth_message(&auth)),
        publish(&stuck, b"waits", None),
        publish(&open, b"goes", Some(2)),
    ];
    let (control, mut out_ack_rx, _cancel) =
        spawn_stream(&broker, &auth, publish_ctx, BrokerConfig::default(), frames).await?;

    match next_answer(&mut out_ack_rx).await {
        Some(Message::PublishOk { request_id: 2, .. }) => {}
        other => panic!("the publish behind a full queue was not served: {other:?}"),
    }
    let partitions = scheduler.partitions();
    let goes = partitions[open_partition].try_take().expect("queued");
    assert_eq!(goes.payloads, vec![Bytes::from_static(b"goes")]);
    assert_eq!(parked.len(), 1, "the waiting publish is not parked");

    // Freeing the filler's slot admits the parked publish in its place.
    let first = partitions[stuck_partition].try_take().expect("filler");
    assert_eq!(first.payloads, vec![Bytes::from_static(b"filler")]);
    let waited = partitions[stuck_partition]
        .try_take()
        .expect("the parked publish was dropped");
    assert_eq!(waited.payloads, vec![Bytes::from_static(b"waits")]);

    assert!(timeout(Duration::from_secs(5), control).await???);
    timeout(Duration::from_secs(5), parked.drain()).await?;
    Ok(())
}

/// **An acked publish that waits too long is still answered.** With acks on
/// commit, a publish to a full queue parks while the stream answers the next
/// publish; when its wait runs out it is refused as `queue_full`, which the
/// client can retry, rather than left unanswered.
#[tokio::test]
async fn a_parked_acked_publish_is_refused_when_its_wait_runs_out() -> Result<()> {
    let (broker, (stuck, _), (open, open_partition)) = two_partition_broker().await?;
    let auth = auth_fixture("t1", default_perms());
    let (mut publish_ctx, scheduler) = context(&broker).await;
    publish_ctx.wait_timeout = Duration::from_millis(300);
    assert!(
        scheduler
            .submit(
                "other",
                filler(&broker, &stuck).await?,
                std::future::ready(())
            )
            .await
            .is_ok(),
        "the filler was not queued"
    );

    let frames = vec![
        frame_from_message(auth_message(&auth)),
        publish(&stuck, b"waits", Some(1)),
        publish(&open, b"goes", Some(2)),
    ];
    let config = BrokerConfig {
        ack_on_commit: true,
        ..BrokerConfig::default()
    };
    let (control, mut out_ack_rx, _cancel) =
        spawn_stream(&broker, &auth, publish_ctx, config, frames).await?;

    // Stand in for the executor: settle the publish that found room.
    let partitions = scheduler.partitions();
    let mut goes = timeout(Duration::from_secs(2), async {
        loop {
            if let Some(job) = partitions[open_partition].try_take() {
                return job;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("the publish behind a full queue was never queued")?;
    match goes.response.take() {
        Some(reply @ PublishReply::Ack(_)) => reply.send(Ok(Some(0))),
        _ => panic!("an acked publish carries its reply"),
    }
    match next_answer(&mut out_ack_rx).await {
        Some(Message::PublishOk { request_id: 2, .. }) => {}
        other => panic!("expected the open publish's ack first: {other:?}"),
    }
    match next_answer(&mut out_ack_rx).await {
        Some(Message::PublishError {
            request_id: 1,
            code,
            ..
        }) => assert_eq!(code, Some(felix_wire::ErrorCode::Overloaded)),
        other => panic!("the parked publish was not refused: {other:?}"),
    }
    assert!(timeout(Duration::from_secs(5), control).await???);
    Ok(())
}

/// `felix_publish_requests_total` by its `result` label, for tests that pin
/// down what was counted accepted.
#[derive(Clone, Default)]
struct Results(Arc<StdMutex<HashMap<String, u64>>>);

impl Results {
    fn get(&self, result: &str) -> u64 {
        self.0
            .lock()
            .expect("results")
            .get(result)
            .copied()
            .unwrap_or(0)
    }
}

struct ResultSlot {
    result: Option<String>,
    counts: Results,
}

impl CounterFn for ResultSlot {
    fn increment(&self, value: u64) {
        if let Some(result) = &self.result {
            *self
                .counts
                .0
                .lock()
                .expect("results")
                .entry(result.clone())
                .or_default() += value;
        }
    }

    fn absolute(&self, _: u64) {}
}

impl Recorder for Results {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        let result = (key.name() == "felix_publish_requests_total")
            .then(|| {
                key.labels()
                    .find(|label| label.key() == "result")
                    .map(|label| label.value().to_string())
            })
            .flatten();
        Counter::from_arc(Arc::new(ResultSlot {
            result,
            counts: self.clone(),
        }))
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

/// Run `test` on a current-thread runtime with `Results` as the recorder, so
/// every counter the control loop and its parked waits bump lands in it.
fn counted<F, Fut>(test: F) -> Result<()>
where
    F: FnOnce(Results) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let results = Results::default();
    with_local_recorder(&results.clone(), || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(test(results))
    })
}

async fn until(what: &str, mut done: impl FnMut() -> bool) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .with_context(|| format!("timed out waiting for {what}"))
}

/// A full queue and a stream to publish into it: `stuck`'s partition holds
/// one filler job and has no room.
async fn stuck_stream() -> Result<(
    Arc<Broker>,
    String,
    usize,
    PublishContext,
    Arc<PublishScheduler>,
)> {
    let (broker, (stuck, stuck_partition), _) = two_partition_broker().await?;
    let (mut publish_ctx, scheduler) = context(&broker).await;
    publish_ctx.ingress_wait = true;
    publish_ctx.wait_timeout = Duration::from_secs(30);
    publish_ctx.conn_admission = Arc::new(PublishAdmission::new(1 << 20));
    assert!(
        scheduler
            .submit(
                "other",
                filler(&broker, &stuck).await?,
                std::future::ready(())
            )
            .await
            .is_ok(),
        "the filler was not queued"
    );
    Ok((broker, stuck, stuck_partition, publish_ctx, scheduler))
}

/// **A parked publish is counted accepted when it is admitted, not before.**
/// The accepted counters are the proof that nothing was lost, so a publish
/// that parks and is then given up must never have been counted.
#[test]
fn a_parked_unacked_publish_is_counted_accepted_only_once_admitted() -> Result<()> {
    counted(|results| async move {
        let (broker, stuck, stuck_partition, publish_ctx, scheduler) = stuck_stream().await?;
        let parked = Arc::clone(publish_ctx.parked.as_ref().expect("parked"));
        let auth = auth_fixture("t1", default_perms());
        let frames = vec![
            frame_from_message(auth_message(&auth)),
            publish(&stuck, b"waits", None),
        ];
        let (control, _out, _cancel) =
            spawn_stream(&broker, &auth, publish_ctx, BrokerConfig::default(), frames).await?;
        until("the publish to park", || parked.len() == 1).await?;
        assert_eq!(results.get("accepted"), 0, "counted accepted while parked");

        let partition = &scheduler.partitions()[stuck_partition];
        assert!(partition.try_take().is_some(), "filler");
        until("the parked publish to settle", || parked.len() == 0).await?;
        assert_eq!(results.get("accepted"), 1, "not counted once admitted");
        let waited = partition.try_take().expect("admitted");
        assert_eq!(waited.payloads, vec![Bytes::from_static(b"waits")]);
        assert!(timeout(Duration::from_secs(5), control).await???);
        Ok(())
    })
}

/// **Cancelling a parked publish releases it.** The stream is torn down while
/// its publish waits: the wait ends, the byte budget it held comes back, the
/// job never reaches the queue, and it was never counted accepted.
#[test]
fn cancelling_a_parked_publish_releases_its_budget() -> Result<()> {
    counted(|results| async move {
        let (broker, stuck, stuck_partition, publish_ctx, scheduler) = stuck_stream().await?;
        let parked = Arc::clone(publish_ctx.parked.as_ref().expect("parked"));
        let budget = Arc::clone(&publish_ctx.conn_admission);
        let full = budget.available();
        let auth = auth_fixture("t1", default_perms());
        let frames = vec![
            frame_from_message(auth_message(&auth)),
            publish(&stuck, b"waits", None),
        ];
        let (control, _out, cancel) =
            spawn_stream(&broker, &auth, publish_ctx, BrokerConfig::default(), frames).await?;
        until("the publish to park", || parked.len() == 1).await?;
        assert!(
            budget.available() < full,
            "a parked publish holds its bytes"
        );

        cancel.send(true)?;
        timeout(Duration::from_secs(5), parked.drain())
            .await
            .context("a cancelled parked publish kept waiting")?;
        assert_eq!(
            budget.available(),
            full,
            "the parked publish leaked its bytes"
        );
        assert_eq!(results.get("accepted"), 0);
        let partition = &scheduler.partitions()[stuck_partition];
        assert!(partition.try_take().is_some(), "filler");
        assert!(
            partition.try_take().is_none(),
            "a cancelled publish was queued anyway"
        );
        let _ = timeout(Duration::from_secs(5), control).await?;
        Ok(())
    })
}

/// **A clean stream end does not abandon a parked publish.** The client
/// finishes the stream while an acked publish waits; the publish is still
/// admitted and answered, and its byte budget comes back once it is done.
#[tokio::test]
async fn a_stream_that_ends_cleanly_still_answers_its_parked_publish() -> Result<()> {
    let (broker, stuck, stuck_partition, publish_ctx, scheduler) = stuck_stream().await?;
    let parked = Arc::clone(publish_ctx.parked.as_ref().expect("parked"));
    let budget = Arc::clone(&publish_ctx.conn_admission);
    let full = budget.available();
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![
        frame_from_message(auth_message(&auth)),
        publish(&stuck, b"waits", Some(1)),
    ];
    let config = BrokerConfig {
        ack_on_commit: true,
        ..BrokerConfig::default()
    };
    let (control, mut out_ack_rx, _cancel) =
        spawn_stream(&broker, &auth, publish_ctx, config, frames).await?;
    assert!(
        timeout(Duration::from_secs(5), control).await???,
        "the stream did not end cleanly"
    );
    assert_eq!(parked.len(), 1, "the parked publish went with the stream");

    let partition = &scheduler.partitions()[stuck_partition];
    assert!(partition.try_take().is_some(), "filler");
    let mut waited = partition.try_take().expect("admitted");
    match waited.response.take() {
        Some(reply) => reply.send(Ok(Some(0))),
        None => panic!("an acked publish carries its reply"),
    }
    drop(waited);
    match next_answer(&mut out_ack_rx).await {
        Some(Message::PublishOk { request_id: 1, .. }) => {}
        other => panic!("the parked publish was not answered: {other:?}"),
    }
    timeout(Duration::from_secs(5), parked.drain()).await?;
    assert_eq!(budget.available(), full, "the publish leaked its bytes");
    Ok(())
}

/// **A queue closing under a parked publish answers it.** At shutdown the
/// parked job is handed back and refused as not queued, instead of being
/// dropped without a word.
#[tokio::test]
async fn a_closing_queue_answers_a_parked_publish() -> Result<()> {
    let (broker, stuck, _, publish_ctx, scheduler) = stuck_stream().await?;
    let parked = Arc::clone(publish_ctx.parked.as_ref().expect("parked"));
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![
        frame_from_message(auth_message(&auth)),
        publish(&stuck, b"waits", Some(1)),
    ];
    let config = BrokerConfig {
        ack_on_commit: true,
        ..BrokerConfig::default()
    };
    let (control, mut out_ack_rx, _cancel) =
        spawn_stream(&broker, &auth, publish_ctx, config, frames).await?;
    until("the publish to park", || parked.len() == 1).await?;
    scheduler.close();
    match next_answer(&mut out_ack_rx).await {
        Some(Message::PublishError {
            request_id: 1,
            message,
            ..
        }) => assert!(message.contains("publish queue closed"), "{message}"),
        other => panic!("the parked publish was not answered: {other:?}"),
    }
    timeout(Duration::from_secs(5), parked.drain()).await?;
    assert!(timeout(Duration::from_secs(5), control).await???);
    Ok(())
}

/// **Parking never reorders a shard.** Keyed publishes to one shard are sent
/// down one control stream while its queue has one slot and a stand-in
/// executor drains it, so some publishes are queued directly, some parked
/// and some wait inline past the parked cap. They reach the shard in the
/// order they were sent.
#[tokio::test]
async fn same_key_publishes_keep_send_order_through_parking() -> Result<()> {
    const COUNT: u32 = 60;
    let (broker, stuck, stuck_partition, mut publish_ctx, scheduler) = stuck_stream().await?;
    publish_ctx.parked = Some(ParkedPublishes::new(4, 4));
    let auth = auth_fixture("t1", default_perms());
    let mut frames = vec![frame_from_message(auth_message(&auth))];
    frames.extend((0..COUNT).map(|i| {
        frame_from_message(Message::Publish {
            shard: None,
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: stuck.clone(),
            payload: i.to_be_bytes().to_vec(),
            key: Some(Bytes::from_static(b"same-key")),
            request_id: None,
            ack: Some(felix_wire::AckMode::None),
        })
    }));
    let (control, _out, _cancel) =
        spawn_stream(&broker, &auth, publish_ctx, BrokerConfig::default(), frames).await?;

    let partition = Arc::clone(&scheduler.partitions()[stuck_partition]);
    let mut landed = Vec::new();
    timeout(Duration::from_secs(10), async {
        while landed.len() < COUNT as usize + 1 {
            match partition.try_take() {
                Some(job) => landed.push(job.payloads[0].clone()),
                // Let the stream read and park between slots.
                None => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .context("publishes went missing")?;
    assert_eq!(landed[0], Bytes::from_static(b"filler"));
    let order: Vec<u32> = landed[1..]
        .iter()
        .map(|payload| u32::from_be_bytes(payload[..4].try_into().expect("4 bytes")))
        .collect();
    assert_eq!(
        order,
        (0..COUNT).collect::<Vec<_>>(),
        "a shard was reordered"
    );
    assert!(timeout(Duration::from_secs(5), control).await???);
    Ok(())
}

/// **A stream parks only a few unacked publishes.** Nothing on the client
/// bounds fire-and-forget publishes, so with the acked cap a busy broker's
/// streams parked until the shared byte budget ran out and every publish then
/// waited on it. Past the small unacked cap the stream waits inline, and
/// everything it sent is still admitted in order.
#[tokio::test]
async fn unacked_publishes_park_up_to_their_own_cap() -> Result<()> {
    use crate::serving::quic::{PARKED_PUBLISHES_MAX, PARKED_UNACKED_MAX};
    let sent = PARKED_UNACKED_MAX + 8;
    let (broker, stuck, stuck_partition, mut publish_ctx, scheduler) = stuck_stream().await?;
    let parked = ParkedPublishes::new(PARKED_PUBLISHES_MAX, PARKED_UNACKED_MAX);
    publish_ctx.parked = Some(Arc::clone(&parked));
    let auth = auth_fixture("t1", default_perms());
    let mut frames = vec![frame_from_message(auth_message(&auth))];
    frames.extend((0..sent as u32).map(|i| publish(&stuck, &i.to_be_bytes(), None)));
    let (control, _out, _cancel) =
        spawn_stream(&broker, &auth, publish_ctx, BrokerConfig::default(), frames).await?;

    until("the stream to fill its unacked cap", || {
        parked.len() == PARKED_UNACKED_MAX
    })
    .await?;
    // The next publish waits in the read loop, so no more park.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        parked.len(),
        PARKED_UNACKED_MAX,
        "parked past the unacked cap"
    );

    let partition = Arc::clone(&scheduler.partitions()[stuck_partition]);
    let mut landed = Vec::new();
    timeout(Duration::from_secs(10), async {
        while landed.len() < sent + 1 {
            match partition.try_take() {
                Some(job) => landed.push(job.payloads[0].clone()),
                None => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .context("publishes went missing")?;
    let order: Vec<u32> = landed[1..]
        .iter()
        .map(|payload| u32::from_be_bytes(payload[..4].try_into().expect("4 bytes")))
        .collect();
    assert_eq!(order, (0..sent as u32).collect::<Vec<_>>());
    assert!(timeout(Duration::from_secs(5), control).await???);
    timeout(Duration::from_secs(5), parked.drain()).await?;
    Ok(())
}
