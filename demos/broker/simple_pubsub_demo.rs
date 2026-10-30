//! Pub/sub demo binary for the QUIC transport.
//!
//! Demonstrates the end-to-end flow for broker boot, subscription, publish, and
//! message receive using felix-wire frames over QUIC.
//!
//! This is a developer-facing demo; it favors clarity over performance.
use anyhow::{Context, Result};
use felix_broker::{Broker, StreamMetadata};
use felix_broker_service::serving::auth::{BrokerAuth, demo::demo_auth_for_tenant};
use felix_broker_service::serving::quic;
use felix_client::{Client, ClientConfig};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use tokio::time::Duration;

type DemoAuthResult = Result<(Arc<BrokerAuth>, Option<(String, String)>)>;

async fn run_demo() -> Result<()> {
    // Keep the demo output readable and step-by-step.
    println!("== Felix QUIC Pub/Sub Demo ==");
    println!("Goal: demonstrate publish/subscribe over QUIC (not cache).");
    println!("This demo spins up an in-process broker + QUIC server, then runs a client.");

    // Create the broker with an ephemeral in-memory cache.
    println!("Step 1/6: booting in-process broker + QUIC server.");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));

    // Seed tenant/namespace so scoped streams are accepted.
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;

    // Register the demo stream so publishes and subscriptions succeed.
    broker
        .register_stream("t1", "default", "demo-topic", StreamMetadata::default())
        .await?;
    let (server_config, cert) = build_server_config().context("build server config")?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    // Start the broker with QUIC transport in a background task to accept client connections.
    let config = felix_broker_service::config::BrokerConfig::from_env()?;
    let (auth, auth_override) = resolve_demo_auth(&config)?;
    let server_task = tokio::spawn(quic::serve(Arc::clone(&server), broker, config, auth));

    println!("Step 2/6: connecting QUIC client.");
    let client_config = apply_demo_auth(build_client_config(cert)?, auth_override);
    let client = Arc::new(Client::connect(&[addr], "localhost", client_config).await?);

    println!("Step 3/6: opening a subscription stream.");
    let mut subscription = client.subscribe("t1", "default", "demo-topic").await?;
    println!("Subscribe response: Subscribed");

    println!("Step 4/6: publishing two messages on the same stream.");
    client
        .publish(
            "t1",
            "default",
            "demo-topic",
            b"hello".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    client
        .publish(
            "t1",
            "default",
            "demo-topic",
            b"world".to_vec(),
            AckMode::PerMessage,
        )
        .await?;

    println!("Step 5/6: receiving events.");
    for _ in 0..2 {
        match tokio::time::timeout(Duration::from_secs(1), subscription.next_event()).await {
            Ok(Ok(Some(event))) => {
                println!(
                    "Event on {}: {}",
                    event.stream,
                    String::from_utf8_lossy(event.payload.as_ref())
                );
            }
            Ok(Ok(None)) => {
                println!("Event stream closed early.");
                break;
            }
            Ok(Err(err)) => {
                println!("Event stream error: {err}");
                break;
            }
            Err(_) => {
                println!("Timed out waiting for event.");
                break;
            }
        }
    }
    println!("Shutting down demo.");
    server_task.abort();
    println!("Demo complete.");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    run_demo().await
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("build server config")?;
    Ok((server_config, cert_der))
}

fn build_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
    ClientConfig::from_env_or_yaml(quinn, None)
}

fn resolve_demo_auth(config: &felix_broker_service::config::BrokerConfig) -> DemoAuthResult {
    if let Some(controlplane_url) = config.controlplane_url.clone() {
        return Ok((Arc::new(BrokerAuth::new(controlplane_url)), None));
    }

    let demo = demo_auth_for_tenant("t1")?;
    Ok((demo.auth, Some((demo.tenant_id, demo.token))))
}

fn apply_demo_auth(
    mut config: ClientConfig,
    auth_override: Option<(String, String)>,
) -> ClientConfig {
    if let Some((tenant_id, token)) = auth_override {
        config.auth_tenant_id = Some(tenant_id);
        config.auth_token = Some(token);
    }
    config
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use std::time::Duration;

    #[tokio::test]
    async fn pubsub_demo_end_to_end() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), run_demo())
            .await
            .context("pubsub demo timeout")?
    }

    #[test]
    fn pubsub_demo_resolve_auth_uses_controlplane_url() -> Result<()> {
        let mut config = felix_broker_service::config::BrokerConfig::from_env()?;
        config.controlplane_url = Some("http://localhost:9999".to_string());
        let result = resolve_demo_auth(&config)?;
        assert!(result.1.is_none());
        Ok(())
    }

    #[test]
    fn pubsub_demo_resolve_auth_demo_path_sets_override() -> Result<()> {
        let mut config = felix_broker_service::config::BrokerConfig::from_env()?;
        config.controlplane_url = None;
        let (_auth, override_creds) = resolve_demo_auth(&config)?;
        let (tenant, token) = override_creds.expect("demo auth override");
        assert_eq!(tenant, "t1");
        assert!(!token.is_empty());
        Ok(())
    }

    #[test]
    fn pubsub_demo_apply_demo_auth_optional_override() -> Result<()> {
        let cert = build_server_config()?.1;
        let mut base = build_client_config(cert)?;
        let untouched = apply_demo_auth(base.clone(), None);
        assert_eq!(untouched.auth_tenant_id, None);
        assert_eq!(untouched.auth_token, None);

        base.auth_tenant_id = Some("ignored".to_string());
        base.auth_token = Some("ignored".to_string());
        let updated = apply_demo_auth(base, Some(("t1".to_string(), "tok".to_string())));
        assert_eq!(updated.auth_tenant_id.as_deref(), Some("t1"));
        assert_eq!(updated.auth_token.as_deref(), Some("tok"));
        Ok(())
    }
}
