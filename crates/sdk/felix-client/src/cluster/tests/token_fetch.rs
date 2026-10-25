//! A slow token provider cannot hold a publish's ack through owner learning.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use felix_wire::binary::PublishOwner;

use super::connections::{default_config, policy};
use super::stub_broker::StubBroker;
use crate::cluster::{ClusterClient, discovery};
use crate::test_support::build_server_config;
use crate::{TokenFuture, TokenProvider};

/// Answers at once, except that one call after `slow` is set takes longer
/// than the owner-connect timer, as a provider refreshing an expired token
/// does.
struct SlowOnce {
    slow: Arc<AtomicBool>,
}

impl TokenProvider for SlowOnce {
    fn token(&self) -> TokenFuture<'_> {
        Box::pin(async move {
            if self.slow.swap(false, Ordering::SeqCst) {
                tokio::time::sleep(discovery::CONNECT_TIMEOUT + Duration::from_millis(500)).await;
            }
            Ok("test-token".to_string())
        })
    }
}

/// **A provider slower than the owner-connect timer costs the route, not the
/// publish.** Learning an owner runs before a publish's ack is returned, so
/// the fetch is inside the connect timer: `remember_owner` gives up at that
/// timer without caching the owner, and a later report learns it.
#[tokio::test]
async fn a_slow_provider_does_not_hold_owner_learning() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let a = StubBroker::start_with(server_config.clone(), |_| felix_wire::Message::Ok)?;
    let b = StubBroker::start_with(server_config, |_| felix_wire::Message::Ok)?;
    let slow = Arc::new(AtomicBool::new(false));
    let mut config = default_config(cert)?;
    config.token_provider = Some(Arc::new(SlowOnce {
        slow: Arc::clone(&slow),
    }));
    let cluster =
        ClusterClient::connect_with_policy(&[a.addr], "localhost", config, policy()).await?;
    slow.store(true, Ordering::SeqCst);
    let key = (
        "t1".to_string(),
        "default".to_string(),
        "orders".to_string(),
        0,
    );
    let owner = || PublishOwner {
        node_id: "b".to_string(),
        addr: Some(b.addr.to_string()),
        generation: 1,
    };

    let started = std::time::Instant::now();
    cluster.remember_owner(key.clone(), owner(), None).await;
    assert!(
        started.elapsed() < discovery::CONNECT_TIMEOUT + Duration::from_millis(400),
        "the slow provider held owner learning: {:?}",
        started.elapsed()
    );
    assert!(!cluster.owners.read().await.contains_key(&key));

    cluster.remember_owner(key.clone(), owner(), None).await;
    assert!(cluster.owners.read().await.contains_key(&key));
    Ok(())
}
