//! A client that offers `FEATURE_ERROR_CODES` gets codes; one that does not
//! gets exactly the frames it always did.

use felix_wire::{ErrorCode, RetryClass};

use super::*;
use crate::serving::quic::client_error::ErrorCodeSupport;

fn auth_offering(fixture: &AuthFixture, features: Option<u32>) -> Message {
    Message::Auth {
        tenant_id: fixture.tenant_id.clone(),
        token: fixture.token.clone(),
        client_flags: Some(felix_wire::KNOWN_FLAGS),
        client_features: features,
        client_features_hi: None,
    }
}

fn publish(request_id: u64) -> Message {
    Message::Publish {
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
async fn publish_errors(
    broker: Arc<Broker>,
    auth: &AuthFixture,
    features: Option<u32>,
) -> Result<Vec<Message>> {
    let error_codes = Arc::new(ErrorCodeSupport::default());
    let frames = vec![
        Ok(Some(frame_from_message(auth_offering(auth, features)))),
        Ok(Some(frame_from_message(publish(7)))),
    ];
    let (_, outgoing) = run_control_loop_with_codes(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
        Arc::clone(&error_codes),
    )
    .await?;
    Ok(outgoing
        .into_iter()
        .filter_map(|outgoing| match outgoing {
            Outgoing::Message(message @ Message::PublishError { .. }) => {
                Some(error_codes.shape(message))
            }
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
async fn a_negotiated_client_is_told_forbidden() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", vec![]);
    let errors = publish_errors(broker, &auth, Some(felix_wire::FEATURE_ERROR_CODES)).await?;
    assert_eq!(errors.len(), 1);
    assert_eq!(
        code_of(&errors[0]),
        (Some(ErrorCode::Forbidden), Some(RetryClass::Fatal))
    );
    Ok(())
}

#[tokio::test]
async fn a_negotiated_client_is_told_not_found() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let errors = publish_errors(broker, &auth, Some(felix_wire::FEATURE_ERROR_CODES)).await?;
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

/// The same failure for a client that did not offer the bit, whether it
/// negotiated other capabilities or none at all, carries no code.
#[tokio::test]
async fn an_unnegotiated_client_gets_the_old_frame() -> Result<()> {
    for features in [None, Some(felix_wire::FEATURE_REDIRECT)] {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let auth = auth_fixture("t1", vec![]);
        let errors = publish_errors(broker, &auth, features).await?;
        assert_eq!(errors, vec![Message::publish_error(7, "forbidden")]);
    }
    Ok(())
}
