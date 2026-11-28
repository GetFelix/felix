//! A client that does not offer the whole 0.8 baseline is refused with
//! `below_baseline`, and never answered with `ok` or `auth_ok`.

use felix_wire::ErrorCode;

use super::*;

/// What the control loop sends for a stream whose only frame is `offer`, and
/// whether the stream stayed open afterwards.
async fn answers_to(offer: Message, token: &str) -> Result<(bool, Vec<Outgoing>)> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = auth_fixture("t1", default_perms());
    let offer = match offer {
        Message::Auth {
            client_flags,
            client_features,
            client_features_hi,
            client_flags_ext,
            ..
        } => Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: token.to_string(),
            client_flags,
            client_features,
            client_features_hi,
            client_flags_ext,
        },
        other => other,
    };
    run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        vec![Ok(Some(frame_from_message(offer)))],
        BrokerConfig::default(),
    )
    .await
}

fn assert_refused(result: bool, messages: &[Outgoing]) {
    assert!(!result, "the stream must close");
    assert!(
        matches!(
            messages.first(),
            Some(Outgoing::Message(Message::Error {
                code: Some(ErrorCode::BelowBaseline),
                ..
            }))
        ),
        "{messages:?}"
    );
    assert!(
        !messages.iter().any(|message| matches!(
            message,
            Outgoing::Message(Message::Ok | Message::AuthOk { .. })
        )),
        "a refused client was answered ok: {messages:?}"
    );
}

#[tokio::test]
async fn an_auth_missing_one_baseline_flag_is_refused() -> Result<()> {
    let auth = auth_fixture("t1", default_perms());
    let mut offer = felix_wire::baseline_auth(&auth.tenant_id, &auth.token);
    if let Message::Auth { client_flags, .. } = &mut offer {
        *client_flags =
            Some(felix_wire::CLIENT_BASELINE_FLAGS & !felix_wire::FLAG_BINARY_PUBLISH_KEYED);
    }
    let (result, messages) = answers_to(offer, &auth.token).await?;
    assert_refused(result, &messages);
    Ok(())
}

#[tokio::test]
async fn an_auth_missing_one_baseline_feature_is_refused() -> Result<()> {
    let auth = auth_fixture("t1", default_perms());
    let mut offer = felix_wire::baseline_auth(&auth.tenant_id, &auth.token);
    if let Message::Auth {
        client_features, ..
    } = &mut offer
    {
        *client_features =
            client_features.map(|features| features & !felix_wire::FEATURE_ERROR_CODES);
    }
    let (result, messages) = answers_to(offer, &auth.token).await?;
    assert_refused(result, &messages);
    Ok(())
}

#[tokio::test]
async fn an_auth_with_no_client_flags_gets_no_plain_ok() -> Result<()> {
    let auth = auth_fixture("t1", default_perms());
    let offer = Message::Auth {
        tenant_id: auth.tenant_id.clone(),
        token: auth.token.clone(),
        client_flags: None,
        client_features: None,
        client_features_hi: None,
        client_flags_ext: None,
    };
    let (result, messages) = answers_to(offer, &auth.token).await?;
    assert_refused(result, &messages);
    Ok(())
}

/// The refusal comes before the credential is checked, so a client that is
/// both below the baseline and unauthorised hears about the baseline.
#[tokio::test]
async fn the_baseline_is_checked_before_the_credential() -> Result<()> {
    let offer = Message::Auth {
        tenant_id: "t1".to_string(),
        token: "invalid".to_string(),
        client_flags: None,
        client_features: None,
        client_features_hi: None,
        client_flags_ext: None,
    };
    let (result, messages) = answers_to(offer, "invalid").await?;
    assert_refused(result, &messages);
    Ok(())
}

#[tokio::test]
async fn a_baseline_auth_is_answered_auth_ok() -> Result<()> {
    let auth = auth_fixture("t1", default_perms());
    let offer = felix_wire::baseline_auth(&auth.tenant_id, &auth.token);
    let (_, messages) = answers_to(offer, &auth.token).await?;
    assert!(
        matches!(
            messages.first(),
            Some(Outgoing::Message(Message::AuthOk { .. }))
        ),
        "{messages:?}"
    );
    Ok(())
}
