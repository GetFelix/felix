//! TLS for the client side of a QUIC connection.
//!
//! QUIC has no unencrypted mode, so every Python client needs a trust
//! decision. Two are offered and no third: the platform trust store (what a
//! broker with a real certificate needs) or an explicit CA file (what a
//! self-signed development broker needs). There is deliberately no
//! "skip verification" switch — it is the one setting that silently turns a
//! secure deployment insecure, and a CA file covers the development case
//! without it.
//!
//! `offer_alpn` offers the `felix/1` ALPN, which a broker with
//! `FELIX_TLS_REQUIRE_ALPN=true` needs. It is off by default because a broker
//! that predates ALPN refuses a client that offers it.
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use quinn::ClientConfig;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;

/// The QUIC config for a connection. `client_cert_file` and `client_key_file`
/// go together: a certificate chain and its private key, presented to a broker
/// that asks clients to authenticate with one (`FELIX_TLS_CLIENT_CA`).
pub(crate) fn client_config(
    ca_file: Option<&str>,
    client_cert_file: Option<&str>,
    client_key_file: Option<&str>,
    offer_alpn: bool,
) -> PyResult<ClientConfig> {
    build(ca_file, client_cert_file, client_key_file, offer_alpn).map_err(PyValueError::new_err)
}

/// [`client_config`] with the refusal as plain text, which the unit tests can
/// read without a Python interpreter.
fn build(
    ca_file: Option<&str>,
    client_cert_file: Option<&str>,
    client_key_file: Option<&str>,
    offer_alpn: bool,
) -> Result<ClientConfig, String> {
    let roots = match ca_file {
        Some(path) => {
            let mut roots = RootCertStore::empty();
            let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
                .map_err(|err| format!("could not read ca_file {path:?}: {err}"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| format!("ca_file {path:?} is not valid PEM: {err}"))?;
            if certs.is_empty() {
                return Err(format!("ca_file {path:?} contained no certificates"));
            }
            for cert in certs {
                roots.add(cert).map_err(|err| {
                    format!("ca_file {path:?} holds an unusable certificate: {err}")
                })?;
            }
            Some(Arc::new(roots))
        }
        None => None,
    };
    let identity = match (client_cert_file, client_key_file) {
        (None, None) => None,
        (Some(cert), Some(key)) => Some(
            felix_client::ClientIdentity::from_pem_files(cert, key)
                .map_err(|err| format!("could not load the client certificate: {err:#}"))?,
        ),
        _ => {
            return Err("client_cert_file and client_key_file must be given together".to_string());
        }
    };
    let platform_only = roots.is_none() && identity.is_none();
    let built = match identity {
        Some(identity) => {
            felix_client::quic_client_config_with_identity(roots, identity, offer_alpn)
        }
        None => felix_client::quic_client_config(roots, offer_alpn),
    };
    built.map_err(|err| {
        if platform_only {
            format!(
                "could not use the platform trust store: {err:#}. \
                 Pass ca_file= to trust a specific CA instead."
            )
        } else {
            format!("could not build a TLS config: {err:#}")
        }
    })
}

#[cfg(test)]
mod tests;
