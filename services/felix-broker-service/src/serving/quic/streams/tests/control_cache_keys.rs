//! Key-scoped cache grants, checked on every cache request the control stream
//! serves.

use felix_authz::Action;
use felix_wire::CacheCondition;

use super::*;

/// Every cache request on `key`, by name. A watch on `key` is a key watch;
/// `watch prefix` watches every key starting with `key`.
fn requests(key: &str) -> Vec<(&'static str, Action, Message)> {
    let (tenant_id, namespace, cache) = ("t1".to_string(), "ns".to_string(), "rooms".to_string());
    let watch = |key: Option<&str>, prefix: Option<&str>| Message::CacheWatch {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
        cache: cache.clone(),
        key: key.map(str::to_string),
        prefix: prefix.map(str::to_string),
        shard: None,
        from_offset: None,
        retained: false,
        subscription_id: None,
    };
    vec![
        (
            "get",
            Action::CacheRead,
            Message::CacheGet {
                tenant_id: tenant_id.clone(),
                namespace: namespace.clone(),
                cache: cache.clone(),
                key: key.to_string(),
                request_id: Some(1),
            },
        ),
        (
            "put",
            Action::CacheWrite,
            Message::CachePut {
                tenant_id: tenant_id.clone(),
                namespace: namespace.clone(),
                cache: cache.clone(),
                key: key.to_string(),
                value: Bytes::from_static(b"v"),
                request_id: Some(1),
                ttl_ms: None,
            },
        ),
        (
            "delete",
            Action::CacheWrite,
            Message::CacheDelete {
                tenant_id: tenant_id.clone(),
                namespace: namespace.clone(),
                cache: cache.clone(),
                key: key.to_string(),
                request_id: Some(1),
            },
        ),
        (
            "put_if",
            Action::CacheWrite,
            Message::CachePutIf {
                tenant_id: tenant_id.clone(),
                namespace: namespace.clone(),
                cache: cache.clone(),
                key: key.to_string(),
                value: Bytes::from_static(b"v"),
                ttl_ms: None,
                condition: CacheCondition::Absent,
                request_id: 1,
            },
        ),
        (
            "delete_if",
            Action::CacheWrite,
            Message::CacheDeleteIf {
                tenant_id: tenant_id.clone(),
                namespace: namespace.clone(),
                cache: cache.clone(),
                key: key.to_string(),
                version: 1,
                request_id: 1,
            },
        ),
        (
            "counter_add",
            Action::CacheWrite,
            Message::CounterAdd {
                tenant_id: tenant_id.clone(),
                namespace: namespace.clone(),
                cache: cache.clone(),
                key: key.to_string(),
                delta: 1,
                request_id: 1,
            },
        ),
        (
            "counter_get",
            Action::CacheRead,
            Message::CounterGet {
                tenant_id: tenant_id.clone(),
                namespace: namespace.clone(),
                cache: cache.clone(),
                key: key.to_string(),
                request_id: 1,
            },
        ),
        ("watch key", Action::CacheRead, watch(Some(key), None)),
        ("watch prefix", Action::CacheRead, watch(None, Some(key))),
    ]
}

/// Whether the broker refused `request` as forbidden for a principal holding
/// `perms`. Anything else -- a value, an ack, a miss -- means it got past
/// authorization.
async fn forbidden(perms: &[String], request: Message) -> Result<bool> {
    let dir = tempfile::tempdir()?;
    let cache = felix_storage::LogCache::open(
        dir.path(),
        felix_storage::log::LogConfig {
            fsync_mode: felix_storage::log::FsyncMode::None,
            preallocate_segments: false,
            ..felix_storage::log::LogConfig::default()
        },
    )?;
    let broker = Arc::new(Broker::new(Box::new(cache)));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "ns").await?;
    broker
        .register_cache("t1", "ns", "rooms", felix_broker::CacheMetadata::default())
        .await?;
    let auth = auth_fixture("t1", perms.to_vec());
    let frames = vec![
        Ok(Some(frame_from_message(auth_message(&auth)))),
        Ok(Some(frame_from_message(request))),
        Ok(None),
    ];
    let (_, messages) = run_control_loop_with_frames(
        broker,
        Arc::clone(&auth.auth),
        frames,
        BrokerConfig::default(),
    )
    .await?;
    Ok(messages.iter().any(|message| {
        matches!(
            message,
            Outgoing::Message(Message::Error { message, .. }) if message.contains("forbidden")
        )
    }))
}

fn grants(object: &str) -> Vec<String> {
    vec![
        format!("cache.read:{object}"),
        format!("cache.write:{object}"),
    ]
}

/// Checks every request on `key` against `perms` and returns the names of
/// those whose outcome differs from `allowed(name)`.
async fn mismatches(
    perms: &[String],
    key: &str,
    allowed: impl Fn(&str) -> bool,
) -> Result<Vec<String>> {
    let mut wrong = Vec::new();
    for (name, _, request) in requests(key) {
        if forbidden(perms, request).await? == allowed(name) {
            wrong.push(format!("{name} {key:?}"));
        }
    }
    Ok(wrong)
}

/// A whole-cache grant still covers every request on every key, as it did
/// before key grants existed.
#[tokio::test]
async fn a_whole_cache_grant_allows_every_request() -> Result<()> {
    for object in ["cache:t1/ns/rooms", "cache:t1/ns/*", "cache:t1/*/*"] {
        for key in ["room1/a", "", "anything"] {
            let wrong = mismatches(&grants(object), key, |_| true).await?;
            assert!(wrong.is_empty(), "{object}: {wrong:?}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_prefix_grant_allows_only_keys_and_watches_under_it() -> Result<()> {
    let perms = grants("cache:t1/ns/rooms/room1/*");
    for key in ["room1/", "room1/a", "room1/a/b"] {
        let wrong = mismatches(&perms, key, |_| true).await?;
        assert!(wrong.is_empty(), "{wrong:?}");
    }
    // `room10/a` shares the text `room1` but not the granted `room1/`, and a
    // prefix watch on `room1` or on nothing would read past the grant.
    for key in ["room10/a", "room1", "room2/a", ""] {
        let wrong = mismatches(&perms, key, |_| false).await?;
        assert!(wrong.is_empty(), "{wrong:?}");
    }
    Ok(())
}

#[tokio::test]
async fn an_exact_key_grant_allows_that_key_but_no_prefix_watch() -> Result<()> {
    let perms = grants("cache:t1/ns/rooms/user:1");
    let wrong = mismatches(&perms, "user:1", |name| name != "watch prefix").await?;
    assert!(wrong.is_empty(), "{wrong:?}");
    for key in ["user:10", "user:", "user:2"] {
        let wrong = mismatches(&perms, key, |_| false).await?;
        assert!(wrong.is_empty(), "{wrong:?}");
    }
    Ok(())
}

/// A key grant carries only its own action: read on a key is not write.
#[tokio::test]
async fn a_key_grant_allows_only_its_action() -> Result<()> {
    let reader = vec!["cache.read:cache:t1/ns/rooms/room1/*".to_string()];
    let writer = vec!["cache.write:cache:t1/ns/rooms/room1/*".to_string()];
    for (name, action, request) in requests("room1/a") {
        assert_eq!(
            forbidden(&reader, request.clone()).await?,
            action != Action::CacheRead,
            "reader {name}"
        );
        assert_eq!(
            forbidden(&writer, request).await?,
            action != Action::CacheWrite,
            "writer {name}"
        );
    }
    Ok(())
}

/// A key grant on another cache, or a wildcard over cache names, does not
/// reach this cache's keys.
#[tokio::test]
async fn a_key_grant_stays_in_its_cache() -> Result<()> {
    for object in [
        "cache:t1/ns/other/room1/*",
        "cache:t1/other/rooms/room1/*",
        // `*` crosses `/` in plain matching; per segment it cannot turn the
        // cache name into part of the key.
        "cache:t1/*/room1",
    ] {
        for key in ["room1", "room1/a"] {
            let wrong = mismatches(&grants(object), key, |_| false).await?;
            assert!(wrong.is_empty(), "{object}: {wrong:?}");
        }
    }
    Ok(())
}
