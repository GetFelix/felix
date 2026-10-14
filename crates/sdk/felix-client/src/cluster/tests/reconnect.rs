//! What a reconnect replaces, and how long dialling dead addresses takes.

use std::sync::Arc;

use anyhow::Result;
use felix_wire::Message;

use super::connections::{default_config, policy};
use super::stub_broker::StubBroker;
use crate::cluster::ClusterClient;
use crate::test_support::build_server_config;

fn publish_ok(id: u64) -> Message {
    Message::PublishOk {
        request_id: id,
        offset: None,
    }
}

/// **A reconnect for a client already replaced does nothing.** Every
/// operation that failed on the same broker asks for a reconnect; only the
/// first may replace the client, or each one after it throws away the
/// working replacement and dials again.
#[tokio::test]
async fn a_reconnect_for_a_client_already_replaced_does_nothing() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let a = StubBroker::start_with(server_config.clone(), publish_ok)?;
    let b = StubBroker::start_with(server_config, publish_ok)?;
    let cluster = ClusterClient::connect_with_policy(
        &[a.addr, b.addr],
        "localhost",
        default_config(cert)?,
        policy(),
    )
    .await?;
    let failed = cluster.client().await;
    assert_eq!(failed.dialled(), a.addr);

    cluster.reconnect(&failed).await?;
    let replacement = cluster.client().await;
    assert_eq!(replacement.dialled(), b.addr);

    cluster.reconnect(&failed).await?;
    assert!(
        Arc::ptr_eq(&replacement, &cluster.client().await),
        "the replacement was kept"
    );
    assert_eq!(b.accepted_connections(), 1);
    assert_eq!(a.accepted_connections(), 1, "A was not dialled again");
    Ok(())
}
