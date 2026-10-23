//! A publish waiting for room in a full queue parks off the read loop: the
//! control stream keeps serving its other publishes, and the parked one is
//! still admitted (or answered) later.

use super::*;
use crate::serving::quic::handlers::publish::{
    ParkedPublishes, PublishJob, PublishReply, PublishScheduler,
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
    publish_ctx.parked = Some(ParkedPublishes::new(8));
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
