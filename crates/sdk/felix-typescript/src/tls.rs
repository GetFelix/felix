//! TLS for the client side of a QUIC connection.
//!
//! QUIC has no unencrypted mode, so every client needs a trust decision. Two
//! are offered and no third: the platform trust store (what a broker with a
//! real certificate needs) or an explicit CA file (what a self-signed
//! development broker needs). There is deliberately no "skip verification"
//! switch — it is the one setting that silently turns a secure deployment
//! insecure, and a CA file covers the development case without it. The Python
//! binding makes the same two-way choice, for the same reason.
//!
//! `offer_alpn` offers the `felix/1` ALPN, which a broker with
//! `FELIX_TLS_REQUIRE_ALPN=true` needs. It is off by default because a broker
//! that predates ALPN refuses a client that offers it.
use std::sync::Arc;

use crate::errors::invalid;
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
) -> napi::Result<ClientConfig> {
    let roots = match ca_file {
        Some(path) => {
            let mut roots = RootCertStore::empty();
            let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
                .map_err(|err| invalid(format!("could not read caFile {path:?}: {err}")))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| invalid(format!("caFile {path:?} is not valid PEM: {err}")))?;
            if certs.is_empty() {
                return Err(invalid(format!(
                    "caFile {path:?} contained no certificates"
                )));
            }
            for cert in certs {
                roots.add(cert).map_err(|err| {
                    invalid(format!(
                        "caFile {path:?} holds an unusable certificate: {err}"
                    ))
                })?;
            }
            Some(Arc::new(roots))
        }
        None => None,
    };
    let identity = match (client_cert_file, client_key_file) {
        (None, None) => None,
        (Some(cert), Some(key)) => Some(
            felix_client::ClientIdentity::from_pem_files(cert, key).map_err(|err| {
                invalid(format!("could not load the client certificate: {err:#}"))
            })?,
        ),
        _ => {
            return Err(invalid(
                "clientCertFile and clientKeyFile must be given together",
            ));
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
            invalid(format!(
                "could not use the platform trust store: {err:#}. \
                 Pass caFile to trust a specific CA instead."
            ))
        } else {
            invalid(format!("could not build a TLS config: {err:#}"))
        }
    })
}
