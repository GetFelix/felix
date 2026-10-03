//! Where a [`ClusterClient`]'s cache requests go, against stub brokers.

use std::time::Duration;

use anyhow::Result;
use felix_wire::{ErrorCode, ErrorDetail, Message, ShardOwner};

use super::stub_broker::StubBroker;
use crate::cluster::{ClusterClient, ReconnectPolicy};
use crate::test_support::{build_client_config_with_overrides, build_server_config};

fn fast_policy() -> ReconnectPolicy {
    ReconnectPolicy {
        attempts: 2,
        backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        deadline: None,
    }
}

fn success(_: u64) -> Message {
    Message::Ok
}

/// An entry broker that says `owner` owns a one-shard cache, and the owner.
fn entry_and_owner(
    owner_script: impl Fn(u64) -> Message + Send + Sync + 'static,
) -> Result<(
    StubBroker,
    StubBroker,
    rustls::pki_types::CertificateDer<'static>,
)> {
    let (server_config, cert) = build_server_config()?;
    let owner = StubBroker::start_with(server_config.clone(), owner_script)?;
    let entry = StubBroker::start_with(server_config, success)?;
    entry.set_cache_owners(vec![ShardOwner {
        shard: 0,
        node_id: Some("broker-2".into()),
        addr: Some(owner.addr.to_string()),
        generation: 1,
        unavailable: None,
    }]);
    Ok((entry, owner, cert))
}

/// **A cache request goes straight to the shard's owner.** The entry broker
/// is asked who owns the cache, once, and then carries nothing.
#[tokio::test]
#[serial_test::serial]
async fn a_cache_request_goes_to_the_owner() -> Result<()> {
    let (entry, owner, cert) = entry_and_owner(success)?;
    let cluster = ClusterClient::connect_with_policy(
        &[entry.addr],
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
        fast_policy(),
    )
    .await?;

    cluster
        .cache_put("t1", "default", "sessions", "alice", "v".into(), None)
        .await?;
    let value = cluster
        .cache_get("t1", "default", "sessions", "alice")
        .await?;

    assert_eq!(value.as_deref(), Some(&b"stub"[..]));
    assert_eq!(owner.cache_requests(), 2);
    assert_eq!(entry.cache_requests(), 0);
    Ok(())
}

/// **An owner that says it no longer serves the shard is forgotten and the
/// put goes through the entry broker.** It applied nothing, so sending it on
/// cannot apply it twice.
#[tokio::test]
#[serial_test::serial]
async fn an_owner_that_lost_the_shard_sends_the_put_through_the_entry() -> Result<()> {
    let (entry, owner, cert) = entry_and_owner(|_| Message::Error {
        message: "moved".into(),
        code: Some(ErrorCode::ShardUnavailable),
        retry: Some(ErrorCode::ShardUnavailable.default_retry()),
        detail: Some(ErrorDetail {
            reason: Some("not_assigned".into()),
            ..ErrorDetail::default()
        }),
    })?;
    let cluster = ClusterClient::connect_with_policy(
        &[entry.addr],
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
        fast_policy(),
    )
    .await?;

    cluster
        .cache_put("t1", "default", "sessions", "alice", "v".into(), None)
        .await?;

    assert_eq!(owner.cache_requests(), 1);
    assert_eq!(entry.cache_requests(), 1);
    Ok(())
}
