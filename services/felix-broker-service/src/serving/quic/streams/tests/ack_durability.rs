//! `stream_shards_view` says what an ack means.

use super::*;

/// The `stream_shards_view` a baseline client gets.
async fn view_for() -> Result<Message> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::StreamShards {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            request_id: 5,
        }))),
    ];
    let (_, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    messages
        .into_iter()
        .find_map(|message| match message {
            Outgoing::Message(message @ Message::StreamShardsView { .. }) => Some(message),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("no stream_shards_view"))
}

/// A client hears what an ack means for an in-memory stream, which is
/// nothing durable.
#[tokio::test]
async fn a_client_hears_ack_durability() -> Result<()> {
    let Message::StreamShardsView { ack_durability, .. } = view_for().await? else {
        unreachable!()
    };
    let said = ack_durability.expect("every client hears it");
    assert_eq!(said.copies, felix_wire::AckCopies::Leader);
    assert_eq!(said.fsync, None, "an in-memory stream has no log to sync");
    Ok(())
}
