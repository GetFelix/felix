//! The same checks through `felix-client`, the reference client.

use std::time::Duration;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use felix_client::Connection;
use felix_wire::AckMode;
use rustls::pki_types::CertificateDer;

use super::checks::{ensure_cache_expired, ensure_cache_value, ensure_client_event};
use super::fixture::{AuthFixture, build_client_config};

pub(crate) async fn run_client_pubsub(
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    auth: &AuthFixture,
) -> Result<()> {
    println!("Running client pub/sub checks...");
    let client = Connection::connect(addr, "localhost", build_client_config(cert, auth)?).await?;
    let mut subscription = client.subscribe("t1", "default", "conformance").await?;
    let publisher = client.publisher().await?;
    publisher
        .publish(
            "t1",
            "default",
            "conformance",
            b"client-alpha".to_vec(),
            AckMode::None,
        )
        .await?;
    let event = subscription
        .next_event()
        .await?
        .ok_or_else(|| anyhow!("client subscription ended early"))?;
    ensure_client_event(&event.payload, Bytes::from_static(b"client-alpha"))?;
    publisher.finish().await?;
    Ok(())
}

pub(crate) async fn run_client_cache(
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    auth: &AuthFixture,
) -> Result<()> {
    println!("Running client cache checks...");
    let client = Connection::connect(addr, "localhost", build_client_config(cert, auth)?).await?;
    client
        .cache_put(
            "t1",
            "default",
            "primary",
            "client-key",
            Bytes::from_static(b"value"),
            Some(100),
        )
        .await?;
    let value = client
        .cache_get("t1", "default", "primary", "client-key")
        .await?;
    ensure_cache_value(
        value.clone(),
        Bytes::from_static(b"value"),
        "client cache get",
    )?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let expired = client
        .cache_get("t1", "default", "primary", "client-key")
        .await?;
    ensure_cache_expired(expired, "client cache entry should be expired")?;

    // Delete reports what it removed, and removes it.
    client
        .cache_put(
            "t1",
            "default",
            "primary",
            "doomed",
            Bytes::from_static(b"gone soon"),
            None,
        )
        .await?;
    let removed = client
        .cache_delete("t1", "default", "primary", "doomed")
        .await?;
    ensure_cache_value(
        removed,
        Bytes::from_static(b"gone soon"),
        "client cache delete should report the value it removed",
    )?;
    let after = client
        .cache_get("t1", "default", "primary", "doomed")
        .await?;
    ensure_cache_expired(after, "a deleted key should read as absent")?;

    // Deleting what is not there is an answer, not a failure.
    let missing = client
        .cache_delete("t1", "default", "primary", "never-written")
        .await?;
    ensure_cache_expired(missing, "deleting a missing key should report nothing")?;
    Ok(())
}
