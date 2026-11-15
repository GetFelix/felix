//! A `publish_if` to a Raft shard through the control stream.

use super::*;
use crate::serving::quic::ClusterContext;
use crate::shards::raft::tests as fixture;
use crate::shards::routing::{EngineShards, StreamEngine};

fn publish_if(request_id: u64, payload: &[u8], expected_offset: u64) -> Result<Option<Frame>> {
    Ok(Some(frame_from_message(Message::PublishIf {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        payloads: vec![payload.to_vec()],
        key: None,
        shard: Some(0),
        expected_offset,
        request_id,
    })))
}

fn trim_before(request_id: u64, before: u64, expected_offset: u64) -> Result<Option<Frame>> {
    Ok(Some(frame_from_message(Message::TrimBefore {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        before,
        expected_offset,
        headers: Some(Bytes::from_static(b"holder")),
        request_id,
    })))
}

fn answer(messages: &[Outgoing], id: u64) -> &Message {
    messages
        .iter()
        .find_map(|message| match message {
            Outgoing::Message(
                message @ (Message::PublishOk { request_id, .. }
                | Message::PublishRefused { request_id, .. }
                | Message::TrimOk { request_id, .. }
                | Message::CommitOk { request_id, .. }
                | Message::StateValue { request_id, .. }),
            ) if *request_id == id => Some(message),
            _ => None,
        })
        .unwrap_or_else(|| panic!("request {id} was not answered: {messages:?}"))
}

fn refused_tail(message: &Message) -> u64 {
    match message {
        Message::PublishRefused {
            reason: felix_wire::PublishRefusalReason::OffsetMismatch { tail },
            ..
        } => *tail,
        other => panic!("not refused with the tail: {other:?}"),
    }
}

/// **A `publish_if` to a Raft shard is written at its expected offset and
/// refused off it, with the tail.** The leader's log decides at the entry's
/// final index, so a refusal still takes an offset: the tail it answers is one
/// past the blank written in its place, and only the accepted record is
/// delivered.
#[tokio::test]
async fn a_publish_if_to_a_raft_shard_is_written_only_at_its_offset() -> Result<()> {
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
    shards.take(&mut fixture::assignments([assigned])).await;
    fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
    let engine = Arc::new(StreamEngine::new(None, Some(fixture::ingress(&shards))));
    engine.set_raft(Arc::clone(&shards));
    let config = BrokerConfig::default();
    let cluster = ClusterContext {
        engine,
        ..ClusterContext::default()
    };
    let publish_ctx = || {
        crate::serving::quic::handlers::publish::build_publish_context(
            Arc::clone(&broker),
            &config,
            cluster.clone(),
        )
    };
    let (mut receiver, _guard) = broker
        .subscribe("t1", "ns", "orders", 0)
        .await?
        .into_parts();
    let auth = auth_fixture("t1", default_perms());
    let run = |frames: Vec<Result<Option<Frame>>>| {
        let mut frames_with_auth = vec![Ok(Some(frame_from_message(auth_message(&auth))))];
        frames_with_auth.extend(frames);
        run_control_loop_on(
            Arc::clone(&broker),
            Arc::clone(&auth.auth),
            frames_with_auth,
            config.clone(),
            publish_ctx(),
            Default::default(),
        )
    };

    // Nothing is at a far offset yet, so this one is refused, and the
    // tail it answers is past the blank it left.
    let (_, first) = run(vec![publish_if(1, b"far", 1_000_000)]).await?;
    let tail = refused_tail(answer(&first, 1));

    let (_, next) = run(vec![
        publish_if(2, b"won", tail),
        publish_if(3, b"stale", tail),
    ])
    .await?;
    match answer(&next, 2) {
        Message::PublishOk { offset, .. } => assert_eq!(*offset, Some(tail)),
        other => panic!("the publish at the tail was not written: {other:?}"),
    }
    assert_eq!(
        refused_tail(answer(&next, 3)),
        tail + 2,
        "a refusal on Raft takes one offset"
    );

    let delivered = tokio::time::timeout(Duration::from_secs(10), receiver.recv())
        .await
        .expect("delivered in time")
        .expect("subscription open");
    assert_eq!(delivered.base_offset(), Some(tail));
    assert_eq!(delivered.payloads(), &[Bytes::from_static(b"won")]);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), receiver.recv())
            .await
            .is_err(),
        "a refused publish was delivered"
    );
    EngineShards::shutdown(&*shards).await;
    Ok(())
}

/// **A `trim_before` to a Raft shard is answered as on the hybrid.** It is
/// written at its expected offset, hides the shard below its floor, and a
/// stale one is refused with the tail and nothing written.
#[tokio::test]
async fn a_trim_before_to_a_raft_shard_is_answered_as_on_the_hybrid() -> Result<()> {
    let root = tempfile::tempdir()?;
    let broker = fixture::broker_at(
        root.path(),
        1,
        fixture::INCARNATION,
        felix_broker::ConsistencyLevel::Quorum,
        felix_storage::log::FsyncMode::None,
    )
    .await;
    broker.allow_raft_head_trims_when(|| true);
    let shards = fixture::shards(&broker);
    let orders = fixture::key("orders", 0);
    let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
    shards.take(&mut fixture::assignments([assigned])).await;
    fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
    let engine = Arc::new(StreamEngine::new(None, Some(fixture::ingress(&shards))));
    engine.set_raft(Arc::clone(&shards));
    let config = BrokerConfig::default();
    let cluster = ClusterContext {
        engine,
        ..ClusterContext::default()
    };
    let mut perms = default_perms();
    perms.push("stream.manage:stream:t1/*/*".to_string());
    let auth = auth_fixture("t1", perms);
    let run = |frames: Vec<Result<Option<Frame>>>| {
        let mut frames_with_auth = vec![Ok(Some(frame_from_message(auth_message(&auth))))];
        frames_with_auth.extend(frames);
        run_control_loop_on(
            Arc::clone(&broker),
            Arc::clone(&auth.auth),
            frames_with_auth,
            config.clone(),
            crate::serving::quic::handlers::publish::build_publish_context(
                Arc::clone(&broker),
                &config,
                cluster.clone(),
            ),
            Default::default(),
        )
    };

    let (_, first) = run(vec![publish_if(1, b"far", 1_000_000)]).await?;
    let floor = refused_tail(answer(&first, 1));
    let (_, written) = run(vec![
        publish_if(2, b"r0", floor),
        publish_if(3, b"r1", floor + 1),
    ])
    .await?;
    for id in [2, 3] {
        assert!(matches!(answer(&written, id), Message::PublishOk { .. }));
    }
    let tail = floor + 2;
    let (_, trimmed) = run(vec![
        trim_before(4, floor + 1, tail),
        trim_before(5, tail, tail),
    ])
    .await?;
    match answer(&trimmed, 4) {
        Message::TrimOk { offset, oldest, .. } => {
            assert_eq!((*offset, *oldest), (Some(tail), floor + 1));
        }
        other => panic!("the trim was not written: {other:?}"),
    }
    assert_eq!(
        refused_tail(answer(&trimmed, 5)),
        tail + 1,
        "nothing written"
    );
    let below = broker
        .subscribe_from(
            "t1",
            "ns",
            "orders",
            0,
            felix_broker::StartPosition::Offset(floor),
        )
        .await;
    assert!(
        matches!(below, Err(felix_broker::BrokerError::CursorTooOld { .. })),
        "read below the floor"
    );
    EngineShards::shutdown(&*shards).await;
    Ok(())
}

fn commit(
    request_id: u64,
    value: &'static [u8],
    expected_offset: Option<u64>,
) -> Result<Option<Frame>> {
    Ok(Some(frame_from_message(Message::Commit {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        entity_key: Bytes::from_static(b"order-42"),
        event: Bytes::from_static(b"placed"),
        changes: vec![felix_wire::StateChange::Put {
            key: "status".to_string(),
            value: Bytes::from_static(value),
        }],
        request_id,
        expected_offset,
        shard: Some(0),
    })))
}

fn state_get(request_id: u64) -> Result<Option<Frame>> {
    Ok(Some(frame_from_message(Message::StateGet {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        entity_key: Bytes::from_static(b"order-42"),
        key: "status".to_string(),
        request_id,
        shard: Some(0),
    })))
}

/// **A commit to a Raft shard is written whole, read back by `state_get`,
/// and refused off its expected offset, once the fleet takes commits on
/// Raft.** Before the fleet finalizes `raft_atomic_commit` it is refused by
/// name, since an older member would refuse the entry.
#[tokio::test]
async fn a_commit_to_a_raft_shard_is_answered_as_on_the_hybrid() -> Result<()> {
    use felix_common::fleet::{ATOMIC_COMMIT, FleetGate, RAFT_ATOMIC_COMMIT};

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
    shards.take(&mut fixture::assignments([assigned])).await;
    fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
    let gate = Arc::new(FleetGate::new([
        ATOMIC_COMMIT.name(),
        RAFT_ATOMIC_COMMIT.name(),
    ]));
    gate.observe([ATOMIC_COMMIT.name()]);
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
    let auth = auth_fixture("t1", default_perms());
    let run = |frames: Vec<Result<Option<Frame>>>| {
        let mut frames_with_auth = vec![Ok(Some(frame_from_message(auth_message(&auth))))];
        frames_with_auth.extend(frames);
        run_control_loop_on(
            Arc::clone(&broker),
            Arc::clone(&auth.auth),
            frames_with_auth,
            config.clone(),
            crate::serving::quic::handlers::publish::build_publish_context(
                Arc::clone(&broker),
                &config,
                cluster.clone(),
            ),
            Default::default(),
        )
    };

    let (_, gated) = run(vec![commit(1, b"early", None)]).await?;
    assert!(
        format!("{gated:?}").contains("raft_atomic_commit"),
        "not refused by name: {gated:?}"
    );

    gate.observe([ATOMIC_COMMIT.name(), RAFT_ATOMIC_COMMIT.name()]);
    let (_, written) = run(vec![commit(2, b"placed", None)]).await?;
    let Message::CommitOk { offset, .. } = answer(&written, 2) else {
        panic!("the commit was not written: {written:?}");
    };
    let at = *offset;
    let (_, read) = run(vec![commit(3, b"stale", Some(at)), state_get(4)]).await?;
    assert_eq!(
        refused_tail(answer(&read, 3)),
        at + 2,
        "a refusal on Raft takes one offset"
    );
    match answer(&read, 4) {
        Message::StateValue { value, version, .. } => {
            assert_eq!(value.as_deref(), Some(&b"placed"[..]));
            assert_eq!(*version, Some(at));
        }
        other => panic!("no state: {other:?}"),
    }
    EngineShards::shutdown(&*shards).await;
    Ok(())
}
