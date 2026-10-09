//! `shard_inspect` and `subscriptions_list` are for operators: a token
//! without `node.view:cluster:*` is refused, one with it is answered for any
//! tenant.

use super::*;

fn inspect(tenant_id: &str) -> Message {
    Message::ShardInspect {
        tenant_id: tenant_id.to_string(),
        namespace: "default".to_string(),
        name: "orders".to_string(),
        kind: felix_wire::ShardKind::Stream,
        shard: 0,
        request_id: 7,
    }
}

async fn ask(perms: Vec<String>, tenant_id: &str) -> Result<Vec<Outgoing>> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t2").await?;
    broker.register_namespace("t2", "default").await?;
    broker
        .register_stream("t2", "default", "orders", Default::default())
        .await?;
    let auth = auth_fixture("t1", perms);
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(inspect(tenant_id)))),
    ];
    let (_, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    Ok(messages)
}

fn refused(messages: &[Outgoing]) -> bool {
    messages.iter().any(|message| {
        matches!(
            message,
            Outgoing::Message(Message::Error { message, .. }) if message.contains("node.view:cluster:*")
        )
    })
}

fn answer(messages: &[Outgoing]) -> Option<&felix_wire::ShardInspection> {
    messages.iter().find_map(|message| match message {
        Outgoing::Message(Message::ShardInspectInfo {
            view,
            request_id: 7,
        }) => Some(view.as_ref()),
        _ => None,
    })
}

#[tokio::test]
async fn a_tenant_token_is_refused() -> Result<()> {
    let messages = ask(default_perms(), "t1").await?;
    assert!(refused(&messages), "{messages:?}");
    assert!(answer(&messages).is_none());
    Ok(())
}

/// A wildcard that happens to match the string is not cluster scope.
#[tokio::test]
async fn a_wildcard_is_not_cluster_scope() -> Result<()> {
    let messages = ask(vec!["node.view:*".to_string()], "t1").await?;
    assert!(refused(&messages), "{messages:?}");
    Ok(())
}

/// Cluster scope covers every tenant, not only the one the token
/// authenticated under.
#[tokio::test]
async fn cluster_scope_inspects_any_tenant() -> Result<()> {
    let messages = ask(vec!["node.view:cluster:*".to_string()], "t2").await?;
    let view = answer(&messages).expect("answered");
    assert_eq!(view.role, "leader");
    assert!(view.serving);
    assert_eq!(view.shards, 1);
    Ok(())
}

async fn list(perms: Vec<String>, limit: Option<u32>) -> Result<Vec<Outgoing>> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t2").await?;
    broker.register_namespace("t2", "default").await?;
    broker
        .register_stream("t2", "default", "orders", Default::default())
        .await?;
    let _held = [
        broker.subscribe("t2", "default", "orders", 0).await?,
        broker.subscribe("t2", "default", "orders", 0).await?,
    ];
    let auth = auth_fixture("t1", perms);
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(Message::SubscriptionsList {
            filter: Box::default(),
            limit,
            cursor: None,
            request_id: 8,
        }))),
    ];
    let (_, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    Ok(messages)
}

fn listed(messages: &[Outgoing]) -> Option<(usize, bool)> {
    messages.iter().find_map(|message| match message {
        Outgoing::Message(Message::SubscriptionsListInfo {
            subscriptions,
            next_cursor,
            request_id: 8,
            ..
        }) => Some((subscriptions.len(), next_cursor.is_some())),
        _ => None,
    })
}

/// Principals and addresses across every tenant are cluster scope too.
#[tokio::test]
async fn listing_subscriptions_needs_cluster_scope() -> Result<()> {
    let messages = list(default_perms(), None).await?;
    assert!(refused(&messages), "{messages:?}");
    assert!(listed(&messages).is_none());

    let messages = list(vec!["node.view:cluster:*".to_string()], None).await?;
    assert_eq!(listed(&messages), Some((2, false)), "{messages:?}");
    let messages = list(vec!["node.view:cluster:*".to_string()], Some(1)).await?;
    assert_eq!(listed(&messages), Some((1, true)), "{messages:?}");
    Ok(())
}
