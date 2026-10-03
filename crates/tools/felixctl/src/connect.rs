//! Reaching the brokers: addresses, TLS and the client configuration, built
//! from resolved [`Settings`] with `felix-client`'s public API only.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context as _;
use felix_client::{Client, ClientConfig, ClusterClient};
use rustls::RootCertStore;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::context::Settings;
use crate::error::{Exit, MarkExit, fail};

/// A connection to the cluster and what is needed to open more.
pub(crate) struct Broker {
    pub(crate) cluster: Arc<ClusterClient>,
    pub(crate) tenant: String,
    pub(crate) namespace: String,
    config: ClientConfig,
    server_name: String,
}

impl Broker {
    /// Connect to whichever configured broker answers first.
    pub(crate) async fn connect(settings: &Settings) -> anyhow::Result<Self> {
        let addrs = addresses(settings.brokers()?).await?;
        let config = client_config(settings)?;
        let server_name = server_name(settings);
        let cluster = ClusterClient::connect(&addrs, &server_name, config.clone())
            .await
            .mark(
                Exit::Connection,
                format!("connect to {}", settings.brokers.join(", ")),
            )?;
        Ok(Self {
            cluster: Arc::new(cluster),
            tenant: settings.tenant()?.to_string(),
            namespace: settings.namespace.clone(),
            config,
            server_name,
        })
    }

    /// A single-broker client at `addr`, for following a redirect by hand.
    pub(crate) async fn client_at(&self, addr: SocketAddr) -> anyhow::Result<Client> {
        Client::connect(addr, &self.server_name, self.config.clone())
            .await
            .mark(Exit::Connection, format!("connect to {addr}"))
    }
}

/// The client configuration for `settings`: the library's defaults and
/// `FELIX_*` tuning overrides, then this tenant and token, over QUIC TLS
/// built from the CA and client certificate files.
pub(crate) fn client_config(settings: &Settings) -> anyhow::Result<ClientConfig> {
    let tenant = settings.tenant()?.to_string();
    let token = settings.token()?;
    let quinn = quic_tls(settings)?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)
        .mark(Exit::Usage, "read the client configuration")?;
    config.auth_tenant_id = Some(tenant);
    config.auth_token = Some(token);
    Ok(config)
}

/// The TLS server name: explicit, else the first broker's host name, else
/// `localhost`, which is what a broker's generated certificate names.
pub(crate) fn server_name(settings: &Settings) -> String {
    if let Some(name) = &settings.server_name {
        return name.clone();
    }
    settings
        .brokers
        .first()
        .and_then(|broker| broker.rsplit_once(':').map(|(host, _)| host))
        .map(|host| host.trim_start_matches('[').trim_end_matches(']'))
        .filter(|host| host.parse::<std::net::IpAddr>().is_err() && !host.is_empty())
        .unwrap_or("localhost")
        .to_string()
}

/// Resolve `host:port` strings. Every name is resolved and every address it
/// yields is kept, so a DNS name for several brokers works as a seed list.
pub(crate) async fn addresses(brokers: &[String]) -> anyhow::Result<Vec<SocketAddr>> {
    let mut addrs = Vec::new();
    for broker in brokers {
        if !broker.contains(':') {
            return Err(fail(
                Exit::Usage,
                format!("broker address {broker:?} needs a port, as host:port"),
            ));
        }
        let resolved = tokio::net::lookup_host(broker.as_str())
            .await
            .mark(Exit::Connection, format!("resolve {broker}"))?;
        addrs.extend(resolved);
    }
    Ok(addrs)
}

fn quic_tls(settings: &Settings) -> anyhow::Result<quinn::ClientConfig> {
    let roots = settings
        .ca_file
        .as_deref()
        .map(root_store)
        .transpose()?
        .map(Arc::new);
    let identity = match (&settings.client_cert_file, &settings.client_key_file) {
        (None, None) => None,
        (Some(cert), Some(key)) => Some((cert.as_path(), key.as_path())),
        _ => {
            return Err(fail(
                Exit::Usage,
                "a client certificate needs both --client-cert-file and --client-key-file",
            ));
        }
    };
    let Some((cert, key)) = identity else {
        // The library's own setup covers everything but a client certificate.
        return felix_client::quic_client_config(roots, settings.alpn)
            .mark(Exit::Usage, "set up TLS");
    };

    // felix-client has no helper for a client certificate; its docs say to
    // build the rustls config, which is what this does.
    let chain = CertificateDer::pem_file_iter(cert)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .mark(Exit::Usage, format!("read {}", cert.display()))?;
    let key =
        PrivateKeyDer::from_pem_file(key).mark(Exit::Usage, format!("read {}", key.display()))?;
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .context("TLS 1.3")?;
    let builder = match roots {
        Some(roots) => builder.with_root_certificates(roots),
        None => {
            use rustls_platform_verifier::BuilderVerifierExt;
            builder
                .with_platform_verifier()
                .context("the platform trust store")?
        }
    };
    let mut tls = builder
        .with_client_auth_cert(chain, key)
        .mark(Exit::Usage, "use the client certificate")?;
    if settings.alpn {
        tls.alpn_protocols = vec![felix_wire::CLIENT_ALPN.to_vec()];
    }
    let crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).context("QUIC client TLS")?;
    Ok(quinn::ClientConfig::new(Arc::new(crypto)))
}

fn root_store(path: &Path) -> anyhow::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    let certs = CertificateDer::pem_file_iter(path)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .mark(Exit::Usage, format!("read {}", path.display()))?;
    if certs.is_empty() {
        return Err(fail(
            Exit::Usage,
            format!("{} holds no certificates", path.display()),
        ));
    }
    for cert in certs {
        roots.add(cert).mark(
            Exit::Usage,
            format!("trust a certificate from {}", path.display()),
        )?;
    }
    Ok(roots)
}

#[cfg(test)]
mod tests;
