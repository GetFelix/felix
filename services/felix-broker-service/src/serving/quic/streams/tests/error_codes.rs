//! Every client is told the code and retry class of a failure.

use felix_wire::{ErrorCode, RetryClass};

use super::*;

fn auth_offering(fixture: &AuthFixture) -> Message {
    felix_wire::baseline_auth(&fixture.tenant_id, &fixture.token)
}

fn publish(request_id: u64) -> Message {
    Message::Publish {
        shard: None,
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "updates".to_string(),
        payload: b"payload".to_vec(),
        request_id: Some(request_id),
        ack: Some(felix_wire::AckMode::PerMessage),
        key: None,
    }
}

/// Run the frames and return the publish errors as the writer would send them.
async fn publish_errors(broker: Arc<Broker>, auth: &AuthFixture) -> Result<Vec<Message>> {
    let frames = vec![
        Ok(Some(frame_from_message(auth_offering(auth)))),
        Ok(Some(frame_from_message(publish(7)))),
    ];
    let (_, outgoing) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    Ok(outgoing
        .into_iter()
        .filter_map(|outgoing| match outgoing {
            Outgoing::Message(message @ Message::PublishError { .. }) => Some(message),
            _ => None,
        })
        .collect())
}

fn code_of(message: &Message) -> (Option<ErrorCode>, Option<RetryClass>) {
    match message {
        Message::PublishError { code, retry, .. } => (code.clone(), *retry),
        _ => panic!("expected publish_error"),
    }
}

#[tokio::test]
async fn a_client_is_told_forbidden() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", vec![]);
    let errors = publish_errors(broker, &auth).await?;
    assert_eq!(errors.len(), 1);
    assert_eq!(
        code_of(&errors[0]),
        (Some(ErrorCode::Forbidden), Some(RetryClass::Fatal))
    );
    Ok(())
}

#[tokio::test]
async fn a_client_is_told_not_found() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let errors = publish_errors(broker, &auth).await?;
    assert_eq!(errors.len(), 1);
    assert_eq!(
        code_of(&errors[0]),
        (Some(ErrorCode::NotFound), Some(RetryClass::RetryAfter))
    );
    let Message::PublishError { message, .. } = &errors[0] else {
        unreachable!()
    };
    assert!(message.starts_with("stream not found"));
    Ok(())
}

/// A stream the broker does not know is `not_found` to a client that
/// declared a width for it, not `stale_width`: a deleted stream has to read
/// as gone, so the client waits for it rather than asking a width it lacks.
#[tokio::test]
async fn a_declared_batch_to_an_unknown_stream_is_not_found() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let frames = vec![
        Ok(Some(frame_from_message(auth_offering(&auth)))),
        Ok(Some(frame_from_message(Message::PublishWidth {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            shards: 4,
            routing: None,
        }))),
        Ok(Some(frame_from_message(Message::PublishBatch {
            shard: None,
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payloads: vec![b"payload".to_vec()],
            key: Some(bytes::Bytes::from_static(b"customer-7")),
            request_id: Some(1),
            ack: Some(felix_wire::AckMode::PerBatch),
        }))),
    ];
    let (_, outgoing) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    let errors: Vec<_> = outgoing
        .into_iter()
        .filter_map(|outgoing| match outgoing {
            Outgoing::Message(message @ Message::PublishError { .. }) => Some(message),
            _ => None,
        })
        .collect();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(code_of(&errors[0]).0, Some(ErrorCode::NotFound));
    Ok(())
}
