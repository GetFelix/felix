//! A token source that fails is a final answer for the operation that asked,
//! not something the cluster client retries.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Result;
use felix_wire::Message;
use tokio::sync::Notify;

use super::connections::{default_config, policy};
use super::stub_broker::StubBroker;
use crate::cluster::ClusterClient;
use crate::test_support::build_server_config;
use crate::{AuthError, AuthErrorKind, RefreshingToken};

fn subscribed() -> Message {
    Message::Subscribed {
        subscription_id: 0,
        start_offset: None,
        live_offset: None,
        queue_capacity: None,
    }
}

/// What the provider does on every call after the connect's.
#[derive(Clone, Copy)]
enum Then {
    Fail,
    Hang,
}

struct Provider {
    token: Arc<RefreshingToken>,
    calls: Arc<AtomicUsize>,
    release: Arc<Notify>,
}

/// A provider whose first call (the connect's) succeeds and whose later calls
/// do `then`. A hung call answers when `release` is notified.
fn provider(then: Then) -> Provider {
    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let token = Arc::new(RefreshingToken::new({
        let calls = Arc::clone(&calls);
        let release = Arc::clone(&release);
        move || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            let release = Arc::clone(&release);
            async move {
                if n == 0 {
                    return Ok("first".to_string());
                }
                match then {
                    Then::Fail => Err(anyhow::anyhow!("idp said: no refresh for you")),
                    Then::Hang => {
                        release.notified().await;
                        Ok("late".to_string())
                    }
                }
            }
        }
    }));
    Provider {
        token,
        calls,
        release,
    }
}

async fn connected(
    provider: &Provider,
    script: impl Fn(u64) -> Message + Send + Sync + 'static,
) -> Result<(StubBroker, ClusterClient)> {
    let (server_config, cert) = build_server_config()?;
    let broker = StubBroker::start_with(server_config, script)?;
    let mut config = default_config(cert)?;
    config.auth_token = None;
    config.token_provider = Some(provider.token.clone());
    let cluster =
        ClusterClient::connect_with_policy(&[broker.addr], "localhost", config, policy()).await?;
    // Drop the connect's token so the next stream open must ask again.
    crate::TokenProvider::invalidate(provider.token.as_ref(), "first");
    provider.calls.store(1, Ordering::SeqCst);
    Ok((broker, cluster))
}

async fn open(cluster: &ClusterClient) -> Result<()> {
    cluster
        .open_shard("t1", "default", "orders", 0, None)
        .await
        .map(drop)
}

/// **A provider that fails during a subscribe open is asked once.** The open
/// fails with the auth error and the provider's own message, instead of being
/// retried for every attempt (each call can rotate a refresh token).
#[tokio::test]
async fn a_failing_provider_fails_the_open_after_one_call() -> Result<()> {
    let p = provider(Then::Fail);
    let (_broker, cluster) = connected(&p, |_| subscribed()).await?;

    let err = open(&cluster).await.expect_err("the provider failed");

    let auth = err
        .chain()
        .find_map(|e| e.downcast_ref::<AuthError>())
        .expect("typed as an auth error");
    assert_eq!(auth.kind, AuthErrorKind::Token);
    assert!(
        format!("{err:#}").contains("idp said: no refresh for you"),
        "{err:#}"
    );
    assert_eq!(p.calls.load(Ordering::SeqCst), 2, "connect's call plus one");
    Ok(())
}

/// **A provider that hangs fails the open at the fetch timeout, once.** Not
/// once per attempt.
#[tokio::test]
async fn a_hung_provider_fails_the_open_at_the_fetch_timeout() -> Result<()> {
    let p = provider(Then::Hang);
    let (_broker, cluster) = connected(&p, |_| subscribed()).await?;
    tokio::time::pause();

    let started = tokio::time::Instant::now();
    let err = open(&cluster).await.expect_err("the provider hung");

    let elapsed = started.elapsed();
    assert!(
        elapsed >= crate::FETCH_TIMEOUT && elapsed < crate::FETCH_TIMEOUT + Duration::from_secs(1),
        "failed after {elapsed:?}"
    );
    assert!(err.chain().any(|e| e.is::<AuthError>()), "{err:#}");
    assert_eq!(p.calls.load(Ordering::SeqCst), 2, "connect's call plus one");
    Ok(())
}

/// **The call a timed-out open left running is joined, not repeated.** A
/// later open waits on it and uses its late answer.
#[tokio::test]
async fn a_later_open_joins_the_outstanding_call() -> Result<()> {
    let p = provider(Then::Hang);
    let (_broker, cluster) = connected(&p, |_| subscribed()).await?;
    let cluster = Arc::new(cluster);
    tokio::time::pause();

    open(&cluster).await.expect_err("the provider hung");
    // Real time again: the rest is a handshake with a broker.
    tokio::time::resume();

    let second = tokio::spawn({
        let cluster = Arc::clone(&cluster);
        async move { open(&cluster).await }
    });
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(!second.is_finished(), "the open waits on the call");
    p.release.notify_one();

    tokio::time::timeout(Duration::from_secs(10), second)
        .await
        .expect("the open finishes")??;
    assert_eq!(p.calls.load(Ordering::SeqCst), 2, "one call after connect");
    Ok(())
}

/// **An error that only mentions a token is still retried.** Nothing marks it
/// as an auth failure, and the cluster does not guess from its words.
#[tokio::test]
async fn an_untyped_error_about_a_token_is_retried() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let broker = StubBroker::start_with(server_config, |_| Message::Error {
        message: "token endpoint unavailable".to_string(),
        code: None,
        retry: None,
        detail: None,
    })?;
    let cluster = ClusterClient::connect_with_policy(
        &[broker.addr],
        "localhost",
        default_config(cert)?,
        policy(),
    )
    .await?;

    assert!(open(&cluster).await.is_err());

    assert_eq!(broker.subscribes(), policy().attempts);
    Ok(())
}
