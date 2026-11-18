//! `stream_shards_view` says what an ack means, only to a client that asked.

use super::*;

/// The `stream_shards_view` a client gets after offering `client_features_hi`.
async fn view_for(client_features_hi: Option<u32>) -> Result<Message> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let auth = auth_fixture("t1", default_perms());
    let (client_features, client_features_hi) =
        felix_wire::offer_features(felix_wire::KNOWN_FEATURES, client_features_hi.unwrap_or(0));
    let frames = vec![
        Ok(Some(frame_from_message(Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            client_flags: Some(felix_wire::KNOWN_FLAGS),
            client_features: Some(client_features),
            client_features_hi,
            client_flags_ext: None,
        }))),
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

/// A client that did not offer the bit gets the frame every broker sent before
/// the field existed; one that did hears what an ack means for an in-memory
/// stream, which is nothing durable.
#[tokio::test]
async fn only_a_client_that_offered_the_bit_hears_ack_durability() -> Result<()> {
    let old = view_for(None).await?;
    assert_eq!(
        old.encode()?.payload,
        Bytes::from_static(br#"{"type":"stream_shards_view","shards":1,"request_id":5}"#)
    );

    let new = view_for(Some(felix_wire::FEATURE_ACK_DURABILITY)).await?;
    let Message::StreamShardsView { ack_durability, .. } = new else {
        unreachable!()
    };
    let said = ack_durability.expect("offered the bit");
    assert_eq!(said.copies, felix_wire::AckCopies::Leader);
    assert_eq!(said.fsync, None, "an in-memory stream has no log to sync");
    Ok(())
}
