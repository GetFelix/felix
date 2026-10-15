//! Fixtures shared by the unit tests: a self-signed QUIC server and client,
//! and the environment a test client connects with.

use std::sync::Arc;

use anyhow::{Context, Result};
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

use crate::ClientConfig;

pub(crate) struct EnvGuard;

impl Drop for EnvGuard {
    fn drop(&mut self) {
        unsafe {
            std::env::remove_var("FELIX_PUB_CONN_POOL");
            std::env::remove_var("FELIX_PUB_STREAMS_PER_CONN");
            std::env::remove_var("FELIX_PUB_SHARD_STREAMS");
            std::env::remove_var("FELIX_CACHE_CONN_POOL");
            std::env::remove_var("FELIX_CACHE_STREAMS_PER_CONN");
            std::env::remove_var("FELIX_EVENT_CONN_POOL");
            std::env::remove_var("FELIX_AUTH_TENANT");
            std::env::remove_var("FELIX_AUTH_TOKEN");
            std::env::remove_var("FELIX_CLIENT_CONFIG");
        }
    }
}

pub(crate) fn set_client_env_with_event_pool(event_pool: usize) -> EnvGuard {
    unsafe {
        std::env::set_var("FELIX_PUB_CONN_POOL", "1");
        std::env::set_var("FELIX_PUB_STREAMS_PER_CONN", "1");
        // The stub brokers serve one stream per connection: the pool's.
        std::env::set_var("FELIX_PUB_SHARD_STREAMS", "0");
        std::env::set_var("FELIX_CACHE_CONN_POOL", "1");
        std::env::set_var("FELIX_CACHE_STREAMS_PER_CONN", "1");
        std::env::set_var("FELIX_EVENT_CONN_POOL", event_pool.to_string());
        std::env::set_var("FELIX_AUTH_TENANT", "t1");
        std::env::set_var("FELIX_AUTH_TOKEN", "demo-token");
        std::env::remove_var("FELIX_CLIENT_CONFIG");
    }
    EnvGuard
}

pub(crate) fn set_client_env() -> EnvGuard {
    set_client_env_with_event_pool(1)
}

pub(crate) fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("build server config")?;
    Ok((server_config, cert_der))
}

/// A QUIC client config that trusts `cert`.
pub(crate) fn quinn_client_config(cert: CertificateDer<'static>) -> Result<quinn::ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    Ok(quinn::ClientConfig::with_root_certificates(Arc::new(
        roots,
    ))?)
}

pub(crate) fn build_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
    let quinn = quinn_client_config(cert)?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    config.auth_tenant_id = Some("t1".to_string());
    config.auth_token = Some("test-token".to_string());
    config.publish_conn_pool = 1;
    config.publish_streams_per_conn = 1;
    config.cache_conn_pool = 1;
    config.cache_streams_per_conn = 1;
    config.event_conn_pool = 1;
    Ok(config)
}

pub(crate) fn build_client_config_with_overrides(
    cert: CertificateDer<'static>,
    event_pool: usize,
) -> Result<ClientConfig> {
    let mut config = build_client_config(cert)?;
    config.event_conn_pool = event_pool;
    config.auth_tenant_id = Some("t1".to_string());
    config.auth_token = Some("demo-token".to_string());
    Ok(config)
}
