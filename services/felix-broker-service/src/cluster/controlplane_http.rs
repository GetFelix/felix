//! The HTTP clients the broker reaches the control plane with.
//!
//! Every control-plane call must trust the same roots, and the clients are
//! built in several places (auth's JWKS fetch, the catalog sync, membership),
//! so the extra CA from `FELIX_CONTROLPLANE_CA` and the client certificate
//! from `FELIX_CONTROLPLANE_CLIENT_CERT` are installed once, process wide, at
//! startup, and every client is built through [`builder`].

use std::sync::RwLock;

use anyhow::{Context, Result};

use crate::config::ControlPlaneClientCert;

static EXTRA_ROOTS: RwLock<Vec<reqwest::Certificate>> = RwLock::new(Vec::new());
static IDENTITY: RwLock<Option<reqwest::Identity>> = RwLock::new(None);

/// Trust the PEM bundle at `path`, in addition to the system trust store, for
/// every control-plane client built after this. `None` trusts the system
/// store only.
///
/// Fails at startup on an unreadable or empty bundle rather than letting
/// every later call fail verification.
pub(crate) fn trust_ca(path: Option<&str>) -> Result<()> {
    let roots = match path {
        Some(path) => {
            let pem = std::fs::read(path)
                .with_context(|| format!("read FELIX_CONTROLPLANE_CA {path}"))?;
            let roots = reqwest::Certificate::from_pem_bundle(&pem)
                .with_context(|| format!("parse FELIX_CONTROLPLANE_CA {path}"))?;
            anyhow::ensure!(
                !roots.is_empty(),
                "no certificates in FELIX_CONTROLPLANE_CA {path}"
            );
            roots
        }
        None => Vec::new(),
    };
    *EXTRA_ROOTS.write().unwrap_or_else(|e| e.into_inner()) = roots;
    Ok(())
}

/// Present `cert` to the control plane from every client built after this.
/// Read once: a rotated certificate is picked up at the next restart.
///
/// Fails at startup on unreadable files or a key that does not parse.
pub(crate) fn present_cert(cert: Option<&ControlPlaneClientCert>) -> Result<()> {
    let identity = match cert {
        Some(cert) => {
            let mut pem = std::fs::read(&cert.cert_path).with_context(|| {
                format!("read FELIX_CONTROLPLANE_CLIENT_CERT {}", cert.cert_path)
            })?;
            pem.push(b'\n');
            pem.extend(std::fs::read(&cert.key_path).with_context(|| {
                format!("read FELIX_CONTROLPLANE_CLIENT_KEY {}", cert.key_path)
            })?);
            Some(reqwest::Identity::from_pem(&pem).with_context(|| {
                format!(
                    "parse FELIX_CONTROLPLANE_CLIENT_CERT {} and FELIX_CONTROLPLANE_CLIENT_KEY {}",
                    cert.cert_path, cert.key_path
                )
            })?)
        }
        None => None,
    };
    *IDENTITY.write().unwrap_or_else(|e| e.into_inner()) = identity;
    Ok(())
}

/// A client builder that trusts the configured control-plane CA and presents
/// the configured client certificate.
pub(crate) fn builder() -> reqwest::ClientBuilder {
    let roots = EXTRA_ROOTS.read().unwrap_or_else(|e| e.into_inner());
    let builder = reqwest::Client::builder().tls_certs_merge(roots.iter().cloned());
    match IDENTITY.read().unwrap_or_else(|e| e.into_inner()).as_ref() {
        Some(identity) => builder.identity(identity.clone()),
        None => builder,
    }
}

#[cfg(test)]
mod tests;
