//! A follower storing head trims (#1662): the floor applies only once the
//! commit offset passes the trim, and a dead leader's unacknowledged trim is
//! dropped with the rest of its suffix.

use felix_wire::internal::ProducerMark;

use super::*;

async fn quorum_follower() -> (Arc<Broker>, TempDir) {
    let (broker, dir) = broker_with_storage().await;
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            felix_broker::StreamMetadata {
                durable: true,
                consistency: felix_broker::ConsistencyLevel::Quorum,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    (broker, dir)
}

/// A batch at `generation` holding only a trim to `floor`.
fn trim(generation: u64, first_offset: u64, floor: u64, commit: u64) -> ReplicateRecords {
    let mut batch = batch(generation, first_offset, &["tag"]);
    batch.marks = vec![ProducerMark::Trim { floor }];
    batch.checksum = batch_checksum(&batch.payloads, &batch.marks, &[]);
    batch.commit_offset = Some(commit);
    batch
}

fn with_commit(mut batch: ReplicateRecords, commit: u64) -> ReplicateRecords {
    batch.commit_offset = Some(commit);
    batch
}

fn stored_ok(answer: &InternalMessage) -> u64 {
    match answer {
        InternalMessage::ReplicateOk(ok) => ok.durable_offset,
        other => panic!("expected the batch stored, got {other:?}"),
    }
}

/// **A follower of a `Quorum` stream applies a trim's floor only once the
/// leader's commit offset passes the trim.** It holds the record from the
/// moment it is stored, but a later leader may still cut it.
#[tokio::test]
async fn the_floor_applies_when_the_commit_offset_passes_the_trim() {
    let (broker, _dir) = quorum_follower().await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    stored_ok(
        &handler
            .apply(
                SENDER,
                with_commit(batch(4, 0, &["a", "b", "c"]), 0),
                felix_broker::LogKind::Stream,
            )
            .await,
    );
    assert_eq!(
        stored_ok(
            &handler
                .apply(SENDER, trim(4, 3, 2, 3), felix_broker::LogKind::Stream)
                .await
        ),
        4
    );
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    assert_eq!(log.readable_base(), 0, "applied before it committed");

    stored_ok(
        &handler
            .apply(
                SENDER,
                with_commit(batch(4, 4, &["d"]), 4),
                felix_broker::LogKind::Stream,
            )
            .await,
    );
    assert_eq!(log.readable_base(), 2);
}

/// **A dead leader's unacknowledged trim goes with its suffix.** The new
/// leader writes its own record where the trim was, the follower drops the
/// trim and stores it, and no floor is left behind: nothing was hidden that
/// a client was not told about.
#[tokio::test]
async fn an_unacknowledged_trim_is_dropped_with_the_dead_leaders_suffix() {
    let (broker, _dir) = quorum_follower().await;
    let router = router_with(&[LOCAL], 4);
    let handler = ReplicaHandler::new(Arc::clone(&broker), Arc::clone(&router));
    stored_ok(
        &handler
            .apply(
                SENDER,
                with_commit(batch(4, 0, &["a", "b", "c"]), 3),
                felix_broker::LogKind::Stream,
            )
            .await,
    );
    stored_ok(
        &handler
            .apply(SENDER, trim(4, 3, 2, 3), felix_broker::LogKind::Stream)
            .await,
    );

    // Generation 5 never had the trim and writes its own record at 3.
    let nodes: HashMap<String, NodeRef> = [
        ("broker-a".to_string(), node("broker-a", 7001)),
        (LOCAL.to_string(), node(LOCAL, 7002)),
    ]
    .into_iter()
    .collect();
    router.publish(
        RoutingTable::build(
            [(key(), "broker-a".to_string(), vec![LOCAL.to_string()], 5)],
            &nodes,
        ),
        &nodes,
    );
    let answer = handler
        .apply(
            SENDER,
            with_commit(batch(5, 3, &["x"]), 3),
            felix_broker::LogKind::Stream,
        )
        .await;
    let resume = match &answer {
        InternalMessage::ReplicateOk(ok) => ok.durable_offset,
        InternalMessage::ReplicateError(err) => err.expected_offset,
        other => panic!("unexpected {other:?}"),
    };
    if resume < 4 {
        let values = ["a", "b", "c", "x"];
        let from = resume as usize;
        stored_ok(
            &handler
                .apply(
                    SENDER,
                    with_commit(batch(5, resume, &values[from..]), 4),
                    felix_broker::LogKind::Stream,
                )
                .await,
        );
    }

    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.advance_commit_offset(4).await.expect("commit");
    assert_eq!(log.readable_base(), 0, "the dropped trim's floor applied");
    assert!(log.head_trim().is_none());
    let held: Vec<String> = log
        .read_from(0, 1 << 20)
        .await
        .expect("read")
        .into_iter()
        .map(|record| String::from_utf8(record.payload.to_vec()).expect("utf8"))
        .collect();
    assert_eq!(held, ["a", "b", "c", "x"]);
}
