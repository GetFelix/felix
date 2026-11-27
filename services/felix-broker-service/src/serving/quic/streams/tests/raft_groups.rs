//! Consumer groups on a Raft shard through the control stream.

use super::*;
use crate::serving::quic::ClusterContext;
use crate::shards::raft::tests as fixture;
use crate::shards::routing::{EngineShards, StreamEngine};
use felix_common::fleet::{FleetGate, RAFT_CONSUMER_GROUPS};
use felix_wire::StartPosition;

fn frame(message: Message) -> Result<Option<Frame>> {
    Ok(Some(frame_from_message(message)))
}

fn publish_if(request_id: u64, payload: &[u8], expected_offset: u64) -> Result<Option<Frame>> {
    frame(Message::PublishIf {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        payloads: vec![payload.to_vec()],
        key: None,
        shard: Some(0),
        expected_offset,
        request_id,
    })
}

/// A group message for group `workers` of `t1/ns/orders` shard 0.
macro_rules! group {
    ($variant:ident { $($field:ident: $value:expr),* $(,)? }) => {
        frame(Message::$variant {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            group: "workers".to_string(),
            request_id: 1,
            $($field: $value),*
        })
    };
}

fn poll(max_records: u32) -> Result<Option<Frame>> {
    group!(GroupPoll {
        max_records: max_records,
        wait_ms: 0,
        consumer: None,
        reclaim: false,
        visibility_ms: 0,
    })
}

fn offsets(message: &Message) -> Vec<u64> {
    match message {
        Message::GroupRecords { records, .. } => records.iter().map(|r| r.offset).collect(),
        other => panic!("not records: {other:?}"),
    }
}

fn ok(message: &Message) {
    assert!(
        !matches!(message, Message::Error { .. }),
        "refused: {message:?}"
    );
}

/// **Every group operation on a Raft shard is answered as on the hybrid,
/// once the fleet serves groups on Raft.** Before `raft_consumer_groups`
/// is finalized each is refused by name: an older member would refuse the
/// entry. Seek, poll, ack, dead letter, list, redrive, nack, discard,
/// describe and delete each go through the shard's log.
#[tokio::test]
async fn group_operations_on_a_raft_shard_are_answered_as_on_the_hybrid() -> Result<()> {
    let root = tempfile::tempdir()?;
    let broker = fixture::broker_at(
        root.path(),
        1,
        fixture::INCARNATION,
        felix_broker::ConsistencyLevel::Quorum,
        felix_storage::log::FsyncMode::None,
    )
    .await;
    let shards = fixture::shards(&broker);
    let orders = fixture::key("orders", 0);
    let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
    fixture::take_formed(&shards, assigned).await;
    fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
    let gate = Arc::new(FleetGate::new([RAFT_CONSUMER_GROUPS.name()]));
    let marks = Arc::new(felix_replication::quorum::QuorumMarks::with_fleet(
        Arc::clone(&gate),
    ));
    let engine = Arc::new(StreamEngine::new(
        Some(marks),
        Some(fixture::ingress(&shards)),
    ));
    engine.set_raft(Arc::clone(&shards));
    let config = BrokerConfig::default();
    let cluster = ClusterContext {
        engine,
        ..ClusterContext::default()
    };
    let mut perms = default_perms();
    perms.push("group.consume:stream:t1/*/*".to_string());
    perms.push("group.manage:stream:t1/*/*".to_string());
    let auth = auth_fixture("t1", perms);
    // One request per run, so its answer is the last message.
    let run = |request: Result<Option<Frame>>| {
        let frames = vec![Ok(Some(frame_from_message(auth_message(&auth)))), request];
        let run = run_control_loop_on(
            Arc::clone(&broker),
            Arc::clone(&auth.auth),
            frames,
            config.clone(),
            crate::serving::quic::handlers::publish::build_publish_context(
                Arc::clone(&broker),
                &config,
                cluster.clone(),
            ),
        );
        async move {
            let (_, messages) = run.await.expect("ran");
            messages
                .into_iter()
                .rev()
                .find_map(|message| match message {
                    Outgoing::Message(message) => Some(message),
                    _ => None,
                })
                .expect("answered")
        }
    };

    let refused = run(poll(10)).await;
    assert!(
        format!("{refused:?}").contains("raft_consumer_groups"),
        "not refused by name: {refused:?}"
    );
    gate.observe([RAFT_CONSUMER_GROUPS.name()]);

    // Learn the tail, then write three records from it.
    let tail = match run(publish_if(1, b"far", 1_000_000)).await {
        Message::PublishRefused {
            reason: felix_wire::PublishRefusalReason::OffsetMismatch { tail },
            ..
        } => tail,
        other => panic!("not refused with the tail: {other:?}"),
    };
    for (at, payload) in [b"r0", b"r1", b"r2"].iter().enumerate() {
        ok(&run(publish_if(1, *payload, tail + at as u64)).await);
    }
    let [r0, r1, r2] = [tail, tail + 1, tail + 2];

    match run(group!(GroupSeek {
        start: StartPosition::Offset(r0),
        if_new: false
    }))
    .await
    {
        Message::GroupPosition { offset, .. } => assert_eq!(offset, r0),
        other => panic!("not sought: {other:?}"),
    }
    assert_eq!(offsets(&run(poll(2)).await), [r0, r1]);
    ok(&run(group!(GroupAck { offset: r0 })).await);
    ok(&run(group!(GroupDeadLetter { offset: r1 })).await);
    match run(group!(GroupDeadLetters {})).await {
        Message::GroupDeadLetterList { offsets, .. } => assert_eq!(offsets, [r1]),
        other => panic!("not listed: {other:?}"),
    }
    ok(&run(group!(GroupRedrive { offset: r1 })).await);
    assert_eq!(offsets(&run(poll(10)).await), [r1, r2]);
    ok(&run(group!(GroupNack {
        offset: r2,
        delay_ms: 0,
        attempts: 0
    }))
    .await);
    ok(&run(group!(GroupDeadLetter { offset: r2 })).await);
    ok(&run(group!(GroupDiscard { offset: r2 })).await);
    match run(group!(GroupDescribe {})).await {
        Message::GroupInfo {
            dead_letters, tail, ..
        } => {
            assert_eq!(dead_letters, 0);
            // The group entries after r2 hold no record, so not counted.
            assert_eq!(tail, r2 + 1);
        }
        other => panic!("not described: {other:?}"),
    }
    for existed in [true, false] {
        match run(group!(GroupDelete {})).await {
            Message::GroupDeleted { existed: was, .. } => assert_eq!(was, existed),
            other => panic!("not deleted: {other:?}"),
        }
    }
    EngineShards::shutdown(&*shards).await;
    Ok(())
}
