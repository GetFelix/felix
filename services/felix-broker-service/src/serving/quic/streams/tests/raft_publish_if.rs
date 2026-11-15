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

fn answer(messages: &[Outgoing], id: u64) -> &Message {
    messages
        .iter()
        .find_map(|message| match message {
            Outgoing::Message(
                message @ (Message::PublishOk { request_id, .. }
                | Message::PublishRefused { request_id, .. }),
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
